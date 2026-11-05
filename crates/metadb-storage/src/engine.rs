use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, RwLock};

use metadb_core::{HlcTimestamp, Result, ShardId};

use crate::compaction::{CompactionStats, LocalityCompactor};
use crate::mvcc::{MemMvccStore, MvccValue};
use crate::raft_log::{LogSequenceNumber, RaftLog, RaftLogPayload};

/// Entry stored in the persistent LSM/KV layer (simulated SSTable level store).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistentEntry {
    /// Live persisted key-value pair.
    Value(Vec<u8>),
    /// Physical LSM-tree tombstone awaiting range compaction (§6.2).
    PhysicalTombstone,
}

/// Operational metrics demonstrating the benefits of In-Memory MVCC and Locality-Aware Compaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageMetrics {
    pub in_memory_mvcc_writes: u64,
    pub pruned_mvcc_versions: u64,
    pub persistent_kv_writes: u64,
    pub active_physical_tombstones: usize,
    pub tombstones_traversed_on_scans: u64,
    pub compaction_stats: CompactionStats,
}

struct PersistentStoreInner {
    entries: BTreeMap<Vec<u8>, PersistentEntry>,
    persistent_kv_writes: u64,
    tombstones_traversed_on_scans: u64,
}

/// Lifecycle-Aware Storage Engine integrating:
/// - `RaftLog` (sole durability log; native LSM WAL disabled, §3.2)
/// - `MemMvccStore` (In-Memory MVCC, §6.1)
/// - Persistent sorted KV store with `LocalityCompactor` (§6.2)
pub struct LifecycleStorageEngine {
    shard_id: ShardId,
    raft_log: Arc<RaftLog>,
    mvcc: Arc<MemMvccStore>,
    compactor: Arc<LocalityCompactor>,
    persistent: RwLock<PersistentStoreInner>,
    auto_locality_compaction: bool,
}

impl LifecycleStorageEngine {
    pub fn new(
        shard_id: ShardId,
        mvcc_retention_ms: u64,
        tombstone_density_threshold: usize,
        auto_locality_compaction: bool,
    ) -> Self {
        Self {
            shard_id,
            raft_log: Arc::new(RaftLog::new(shard_id)),
            mvcc: Arc::new(MemMvccStore::new(mvcc_retention_ms)),
            compactor: Arc::new(LocalityCompactor::new(tombstone_density_threshold)),
            persistent: RwLock::new(PersistentStoreInner {
                entries: BTreeMap::new(),
                persistent_kv_writes: 0,
                tombstones_traversed_on_scans: 0,
            }),
            auto_locality_compaction,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn raft_log(&self) -> &Arc<RaftLog> {
        &self.raft_log
    }

    pub fn mvcc(&self) -> &Arc<MemMvccStore> {
        &self.mvcc
    }

    pub fn compactor(&self) -> &Arc<LocalityCompactor> {
        &self.compactor
    }

    /// Appends a `Put` to the Raft log and applies it to `MemMvccStore`.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>, hlc: HlcTimestamp) -> LogSequenceNumber {
        let lsn = self.raft_log.append(
            hlc,
            RaftLogPayload::Put {
                key: key.clone(),
                value: value.clone(),
            },
        );
        self.mvcc.put(key, value, hlc, lsn);
        lsn
    }

    /// Appends a `Delete` to the Raft log and records an in-memory MVCC tombstone.
    pub fn delete(&self, key: Vec<u8>, hlc: HlcTimestamp) -> LogSequenceNumber {
        let lsn = self
            .raft_log
            .append(hlc, RaftLogPayload::Delete { key: key.clone() });
        self.mvcc.delete(key, hlc, lsn);
        lsn
    }

    /// Point lookup at `read_hlc` consulting `MemMvccStore` first and falling back to persistent storage.
    pub fn get(&self, key: &[u8], read_hlc: HlcTimestamp) -> Result<Option<Vec<u8>>> {
        if let Some(mvcc_val) = self.mvcc.get_at(key, read_hlc)? {
            return match mvcc_val {
                MvccValue::Value(v) => Ok(Some(v)),
                MvccValue::Tombstone => Ok(None),
            };
        }

        let guard = self.persistent.read().unwrap();
        match guard.entries.get(key) {
            Some(PersistentEntry::Value(v)) => Ok(Some(v.clone())),
            Some(PersistentEntry::PhysicalTombstone) | None => Ok(None),
        }
    }

    /// Range scan over `[start_key, end_key)` at `read_hlc`.
    /// Merges in-memory MVCC versions with persistent LSM entries and records any uncompacted
    /// physical tombstones encountered during the scan.
    pub fn scan_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        read_hlc: HlcTimestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut merged = self.mvcc.range_at(start_key, end_key, read_hlc)?;
        let mut tombstones_scanned = 0u64;

        {
            let guard = self.persistent.read().unwrap();
            for (k, entry) in guard
                .entries
                .range::<[u8], _>((Bound::Included(start_key), Bound::Excluded(end_key)))
            {
                if merged.contains_key(k) {
                    continue;
                }
                match entry {
                    PersistentEntry::Value(v) => {
                        merged.insert(k.clone(), MvccValue::Value(v.clone()));
                    }
                    PersistentEntry::PhysicalTombstone => {
                        tombstones_scanned += 1;
                    }
                }
            }
        }

        if tombstones_scanned > 0 {
            let mut guard = self.persistent.write().unwrap();
            guard.tombstones_traversed_on_scans += tombstones_scanned;
        }

        let mut results = Vec::new();
        for (k, val) in merged {
            if let MvccValue::Value(v) = val {
                results.push((k, v));
            }
        }
        Ok(results)
    }

    /// Flushes `MemMvccStore` to the persistent KV store:
    /// 1. Prunes stale MVCC versions and MVCC tombstones in memory.
    /// 2. Writes only the latest value or a single physical LSM tombstone per deleted key.
    /// 3. Triggers `LocalityCompactor` on any key range whose tombstone density crosses threshold.
    /// 4. Advances `storage_flushed_lsn` on the Raft log.
    pub fn flush_mvcc_to_persistent(&self) -> LogSequenceNumber {
        let (flushed_keys, max_lsn) = self.mvcc.prepare_flush();
        let mut ranges_to_compact = Vec::new();

        {
            let mut guard = self.persistent.write().unwrap();
            for item in flushed_keys {
                guard.persistent_kv_writes += 1;
                match item.latest_value {
                    Some(v) => {
                        guard.entries.insert(item.key, PersistentEntry::Value(v));
                    }
                    None => {
                        let key_clone = item.key.clone();
                        guard
                            .entries
                            .insert(item.key, PersistentEntry::PhysicalTombstone);
                        if self.auto_locality_compaction {
                            if let Some(bounds) = self.compactor.record_tombstone(&key_clone) {
                                ranges_to_compact.push(bounds);
                            }
                        }
                    }
                }
            }
        }

        for (start, end) in ranges_to_compact {
            self.compact_range(&start, &end);
        }

        self.raft_log.advance_storage_flushed_lsn(max_lsn);
        max_lsn
    }

    /// Performs localized range compaction over `[start_key, end_key)`, purging all physical
    /// LSM tombstones in that key range (§6.2).
    pub fn compact_range(&self, start_key: &[u8], end_key: &[u8]) -> usize {
        let mut guard = self.persistent.write().unwrap();
        let keys_to_purge: Vec<Vec<u8>> = guard
            .entries
            .range::<[u8], _>((Bound::Included(start_key), Bound::Excluded(end_key)))
            .filter_map(|(k, v)| match v {
                PersistentEntry::PhysicalTombstone => Some(k.clone()),
                PersistentEntry::Value(_) => None,
            })
            .collect();

        let purged = keys_to_purge.len();
        for k in keys_to_purge {
            guard.entries.remove(&k);
        }
        drop(guard);

        self.compactor.on_range_compacted(start_key, purged);
        purged
    }

    /// Returns the approximate storage byte size of live entries in this shard.
    pub fn approximate_size_bytes(&self, read_hlc: HlcTimestamp) -> usize {
        let start = [0x00u8];
        let end = [0xFFu8];
        self.scan_range(&start, &end, read_hlc)
            .map(|rows| rows.iter().map(|(k, v)| k.len() + v.len()).sum())
            .unwrap_or(0)
    }

    /// Returns a snapshot of storage engine metrics.
    pub fn metrics(&self) -> StorageMetrics {
        let (mvcc_writes, pruned_versions) = self.mvcc.stats();
        let guard = self.persistent.read().unwrap();
        let active_tombstones = guard
            .entries
            .values()
            .filter(|e| matches!(e, PersistentEntry::PhysicalTombstone))
            .count();

        StorageMetrics {
            in_memory_mvcc_writes: mvcc_writes,
            pruned_mvcc_versions: pruned_versions,
            persistent_kv_writes: guard.persistent_kv_writes,
            active_physical_tombstones: active_tombstones,
            tombstones_traversed_on_scans: guard.tombstones_traversed_on_scans,
            compaction_stats: self.compactor.stats(),
        }
    }
}
