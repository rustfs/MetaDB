use std::path::Path;
use std::sync::Arc;

use metadb_core::{HlcTimestamp, Result, ShardId};

pub use crate::backend::PersistentEntry;
use crate::backend::{InMemoryBackend, LsmStorageBackend, StorageBackend};
use crate::bloom::DEFAULT_BLOOM_BITS_PER_KEY;
use crate::cache::ShardedBlockCache;
use crate::compaction::{CompactionStats, LocalityCompactor};
use crate::manifest::DurableShardCheckpoint;
use crate::mvcc::{MemMvccStore, MvccValue, PreFlushHook, SnapshotGuard};
use crate::raft_log::{LogSequenceNumber, RaftLog, RaftLogPayload};

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

/// Configuration for opening a disk-backed `LifecycleStorageEngine`.
#[derive(Debug, Clone)]
pub struct LsmEngineConfig {
    pub mvcc_retention_ms: u64,
    pub tombstone_density_threshold: usize,
    pub tombstone_min_ratio_permille: usize,
    pub bloom_bits_per_key: usize,
    pub auto_locality_compaction: bool,
}

impl Default for LsmEngineConfig {
    fn default() -> Self {
        Self {
            mvcc_retention_ms: crate::mvcc::DEFAULT_MVCC_RETENTION_MS,
            tombstone_density_threshold: 128,
            tombstone_min_ratio_permille: 500,
            bloom_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
            auto_locality_compaction: true,
        }
    }
}

/// Lifecycle-Aware Storage Engine integrating:
/// - WAL-less `StorageBackend` (`LsmStorageBackend` on disk or `InMemoryBackend` in unit tests)
/// - `MemMvccStore` (`SnapshotGuard` `ReadCut` isolation + Safe-Flush Gate)
/// - `LocalityCompactor` (bottommost-safe range tombstone compaction)
pub struct LifecycleStorageEngine {
    shard_id: ShardId,
    raft_log: Arc<RaftLog>,
    mvcc: Arc<MemMvccStore>,
    compactor: Arc<LocalityCompactor>,
    backend: Arc<dyn StorageBackend>,
    auto_locality_compaction: bool,
}

impl LifecycleStorageEngine {
    /// Creates an in-memory-backed `LifecycleStorageEngine` (used by lightweight unit tests).
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
            backend: Arc::new(InMemoryBackend::new()),
            auto_locality_compaction,
        }
    }

    /// Opens or recovers a persistent WAL-less `LsmStorageBackend` at `lsm_dir`.
    pub fn open_lsm(
        shard_id: ShardId,
        lsm_dir: impl AsRef<Path>,
        block_cache: Arc<ShardedBlockCache>,
        config: LsmEngineConfig,
    ) -> Result<Self> {
        let lsm = Arc::new(LsmStorageBackend::open(
            shard_id,
            lsm_dir,
            block_cache,
            config.bloom_bits_per_key,
        )?);
        let cp = lsm.current_version().checkpoint.clone();
        let mvcc = Arc::new(MemMvccStore::with_recovered_watermarks(
            config.mvcc_retention_ms,
            cp.flushed_raft_index,
            cp.flushed_max_hlc,
        ));
        let compactor = Arc::new(LocalityCompactor::with_ratio(
            config.tombstone_density_threshold,
            config.tombstone_min_ratio_permille,
        ));
        Ok(Self {
            shard_id,
            raft_log: Arc::new(RaftLog::new(shard_id)),
            mvcc,
            compactor,
            backend: lsm,
            auto_locality_compaction: config.auto_locality_compaction,
        })
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

    pub fn backend(&self) -> &Arc<dyn StorageBackend> {
        &self.backend
    }

    pub fn durable_checkpoint(&self) -> DurableShardCheckpoint {
        self.backend.current_version().checkpoint.clone()
    }

    /// Applies an externally Raft-logged `Put` at `(raft_index, hlc)` directly into `MemMvccStore`.
    pub fn apply_committed_put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        hlc: HlcTimestamp,
        raft_index: LogSequenceNumber,
    ) {
        self.mvcc.put(key, value, hlc, raft_index);
    }

    /// Applies an externally Raft-logged `Delete` at `(raft_index, hlc)` directly into `MemMvccStore`.
    pub fn apply_committed_delete(
        &self,
        key: Vec<u8>,
        hlc: HlcTimestamp,
        raft_index: LogSequenceNumber,
    ) {
        self.mvcc.delete(key, hlc, raft_index);
    }

    /// Appends a `Put` to the internal `RaftLog` and applies it to `MemMvccStore`.
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

    /// Appends a `Delete` to the internal `RaftLog` and records an in-memory MVCC tombstone.
    pub fn delete(&self, key: Vec<u8>, hlc: HlcTimestamp) -> LogSequenceNumber {
        let lsn = self
            .raft_log
            .append(hlc, RaftLogPayload::Delete { key: key.clone() });
        self.mvcc.delete(key, hlc, lsn);
        lsn
    }

    /// Acquires a linearizable `SnapshotGuard` pinning `(ReadCut, Arc<Version>, ImmMemTables)`.
    pub fn acquire_linearizable_snapshot(
        &self,
        min_read_index: LogSequenceNumber,
    ) -> Result<SnapshotGuard> {
        self.mvcc
            .acquire_linearizable_snapshot(min_read_index, self.backend.current_version())
    }

    /// Acquires a fixed-HLC `SnapshotGuard` at `barrier_hlc`.
    pub fn acquire_fixed_snapshot(&self, barrier_hlc: HlcTimestamp) -> Result<SnapshotGuard> {
        self.mvcc
            .acquire_fixed_snapshot(barrier_hlc, self.backend.current_version())
    }

    /// Point lookup using a pinned `SnapshotGuard` and bounded step-lease check.
    pub fn get_with_snapshot(&self, key: &[u8], snap: &SnapshotGuard) -> Result<Option<Vec<u8>>> {
        snap.with_step_lease(|| {
            if let Some(mvcc_val) = self.mvcc.get_with_guard(key, snap) {
                return Ok(match mvcc_val {
                    MvccValue::Value(v) => Some(v),
                    MvccValue::Tombstone => None,
                });
            }
            match self.backend.get_in_version(snap.pinned_version(), key)? {
                Some(PersistentEntry::Value(v)) => Ok(Some(v)),
                Some(PersistentEntry::PhysicalTombstone) | None => Ok(None),
            }
        })
    }

    /// Range scan over `[start_key, end_key)` using a pinned `SnapshotGuard` (`Resolution B1`).
    pub fn scan_with_snapshot(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        snap: &SnapshotGuard,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        snap.with_step_lease(|| {
            let mut merged = self.mvcc.range_with_guard(start_key, end_key, snap);
            let (disk_entries, disk_tombs) =
                self.backend
                    .scan_in_version(snap.pinned_version(), start_key, end_key)?;

            let mut unshadowed_tombs = 0u64;
            for (k, entry) in disk_entries {
                if merged.contains_key(&k) {
                    continue;
                }
                match entry {
                    PersistentEntry::Value(v) => {
                        merged.insert(k, MvccValue::Value(v));
                    }
                    PersistentEntry::PhysicalTombstone => {
                        unshadowed_tombs += 1;
                    }
                }
            }
            let _ = disk_tombs;
            self.backend.record_scanned_tombstones(unshadowed_tombs);

            let mut out = Vec::new();
            for (k, v) in merged {
                if let MvccValue::Value(bytes) = v {
                    out.push((k, bytes));
                }
            }
            Ok(out)
        })
    }

    /// Point lookup at `read_hlc` consulting `MemMvccStore` first and falling back to persistent storage.
    pub fn get(&self, key: &[u8], read_hlc: HlcTimestamp) -> Result<Option<Vec<u8>>> {
        if let Some(mvcc_val) = self.mvcc.get_at(key, read_hlc)? {
            return match mvcc_val {
                MvccValue::Value(v) => Ok(Some(v)),
                MvccValue::Tombstone => Ok(None),
            };
        }
        let version = self.backend.current_version();
        match self.backend.get_in_version(&version, key)? {
            Some(PersistentEntry::Value(v)) => Ok(Some(v)),
            Some(PersistentEntry::PhysicalTombstone) | None => Ok(None),
        }
    }

    /// Range scan over `[start_key, end_key)` at `read_hlc`.
    pub fn scan_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        read_hlc: HlcTimestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut merged = self.mvcc.range_at(start_key, end_key, read_hlc)?;
        let version = self.backend.current_version();
        let (disk_entries, _) = self.backend.scan_in_version(&version, start_key, end_key)?;
        let mut tombstones_scanned = 0u64;

        for (k, entry) in disk_entries {
            if merged.contains_key(&k) {
                continue;
            }
            match entry {
                PersistentEntry::Value(v) => {
                    merged.insert(k, MvccValue::Value(v));
                }
                PersistentEntry::PhysicalTombstone => {
                    tombstones_scanned += 1;
                }
            }
        }
        self.backend.record_scanned_tombstones(tombstones_scanned);

        let mut results = Vec::new();
        for (k, val) in merged {
            if let MvccValue::Value(v) = val {
                results.push((k, v));
            }
        }
        Ok(results)
    }

    /// Executes a Coordinated Flush Barrier (`seal_active_memtable` + `flush_eligible_imms` +
    /// `LocalityCompactor`), returning `(state_flushed_index, aux_outbox_durable_index)`.
    pub fn seal_and_flush_with_hook(
        &self,
        checkpoint: DurableShardCheckpoint,
        hook: Option<&dyn PreFlushHook>,
    ) -> Result<(LogSequenceNumber, LogSequenceNumber)> {
        self.mvcc
            .seal_active_memtable(self.backend.as_ref(), checkpoint, hook)?;
        self.flush_queued_imms_with_hook(hook)
    }

    /// Flushes already-sealed `ImmMemTable`s that pass the `ActiveSnapshotRegistry` Safe-Flush Gate.
    pub fn flush_queued_imms_with_hook(
        &self,
        hook: Option<&dyn PreFlushHook>,
    ) -> Result<(LogSequenceNumber, LogSequenceNumber)> {
        let (flushed_keys, state_flushed_lsn, aux_frontier) =
            self.mvcc.flush_eligible_imms(self.backend.as_ref(), hook)?;

        let mut ranges_to_compact = Vec::new();
        for item in &flushed_keys {
            match &item.latest_value {
                Some(_) => self.compactor.record_live_write(&item.key),
                None => {
                    if self.auto_locality_compaction {
                        if let Some(bounds) = self.compactor.record_tombstone(&item.key) {
                            if !ranges_to_compact.contains(&bounds) {
                                ranges_to_compact.push(bounds);
                            }
                        }
                    }
                }
            }
        }

        for (start, end) in ranges_to_compact {
            let _ = self.compact_range(&start, &end);
        }

        self.raft_log.advance_storage_flushed_lsn(state_flushed_lsn);
        Ok((state_flushed_lsn, aux_frontier))
    }

    /// Convenience synchronous flush without external hook.
    pub fn flush_mvcc_to_persistent(&self) -> LogSequenceNumber {
        let (latest_lsn, latest_hlc, _, _) = self.mvcc.latest_watermarks();
        let cp = DurableShardCheckpoint {
            flushed_raft_index: latest_lsn,
            flushed_max_hlc: latest_hlc,
            ..self.durable_checkpoint()
        };
        let (flushed, _) = self
            .seal_and_flush_with_hook(cp, None)
            .expect("flush_mvcc_to_persistent");
        flushed
    }

    /// Performs localized bottommost range compaction over `[start_key, end_key)`.
    pub fn compact_range(&self, start_key: &[u8], end_key: &[u8]) -> usize {
        let purged = self.backend.compact_range(start_key, end_key).unwrap_or(0);
        self.compactor.on_range_compacted(start_key, purged);
        purged
    }

    /// Creates a hard-linked SSTable + standalone `MANIFEST` checkpoint at `dest_dir`.
    pub fn create_checkpoint(&self, dest_dir: &Path) -> Result<DurableShardCheckpoint> {
        self.backend.create_checkpoint(dest_dir)
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
        let (kv_writes, active_tombs, tombs_scanned) = self.backend.backend_counters();

        StorageMetrics {
            in_memory_mvcc_writes: mvcc_writes,
            pruned_mvcc_versions: pruned_versions,
            persistent_kv_writes: kv_writes,
            active_physical_tombstones: active_tombs,
            tombstones_traversed_on_scans: tombs_scanned,
            compaction_stats: self.compactor.stats(),
        }
    }
}
