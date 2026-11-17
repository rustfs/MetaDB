use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use metadb_core::{HlcTimestamp, MetaDbError, Result, ShardId};

pub use crate::backend::PersistentEntry;
use crate::backend::{InMemoryBackend, LsmStorageBackend, StorageBackend};
use crate::block::BlockEntryValue;
use crate::bloom::DEFAULT_BLOOM_BITS_PER_KEY;
use crate::cache::ShardedBlockCache;
use crate::compaction::{CompactionStats, LocalityCompactor, RangeBucketPrefix};
use crate::manifest::DurableShardCheckpoint;
use crate::mvcc::{
    MemMvccStore, MvccValue, PinnedSnapshotData, PreFlushHook, ReadCut, SnapshotGuard,
};
use crate::raft_log::{LogSequenceNumber, RaftLog, RaftLogPayload};
use crate::version_set::Version;

/// Caller of [`LifecycleStorageEngine::scan_live_until`] stops or keeps scanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanControl {
    Continue,
    Stop,
}

/// Raft identity stamped onto a size-triggered flush checkpoint.
#[derive(Debug, Clone)]
pub struct FlushIdentity {
    pub term: u64,
    pub leader: u64,
    pub membership: Vec<u8>,
    pub descriptor: Vec<u8>,
    pub aux_index: u64,
}

/// Operational metrics demonstrating the benefits of In-Memory MVCC and Locality-Aware Compaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageMetrics {
    pub in_memory_mvcc_writes: u64,
    pub pruned_mvcc_versions: u64,
    pub persistent_kv_writes: u64,
    pub active_physical_tombstones: usize,
    pub tombstones_traversed_on_scans: u64,
    /// Keys the limited scanner actually stepped to, including physical tombstones.
    pub entries_examined_on_scans: u64,
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
    /// Seal and flush the active memtable once its accounted bytes reach this size.
    pub memtable_size_bytes: usize,
    /// Immutable memtables queued above this cap block writers until one flushes.
    pub max_imm_memtables: usize,
}

impl Default for LsmEngineConfig {
    fn default() -> Self {
        Self {
            mvcc_retention_ms: crate::mvcc::DEFAULT_MVCC_RETENTION_MS,
            tombstone_density_threshold: 128,
            tombstone_min_ratio_permille: 500,
            bloom_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
            auto_locality_compaction: true,
            memtable_size_bytes: 32 * 1024 * 1024,
            max_imm_memtables: 4,
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
    memtable_size_bytes: usize,
    max_imm_memtables: usize,
    budget_mu: Mutex<()>,
    flush_hook: RwLock<Option<Arc<dyn PreFlushHook>>>,
    flush_identity: RwLock<Option<FlushIdentity>>,
    entries_examined: AtomicU64,
    last_aux_frontier: AtomicU64,
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
            memtable_size_bytes: 32 * 1024 * 1024,
            max_imm_memtables: 4,
            budget_mu: Mutex::new(()),
            flush_hook: RwLock::new(None),
            flush_identity: RwLock::new(None),
            entries_examined: AtomicU64::new(0),
            last_aux_frontier: AtomicU64::new(0),
        }
    }

    /// Opens or recovers a persistent WAL-less `LsmStorageBackend` at `lsm_dir`.
    pub fn open_lsm(
        shard_id: ShardId,
        lsm_dir: impl AsRef<Path>,
        block_cache: Arc<ShardedBlockCache>,
        config: LsmEngineConfig,
    ) -> Result<Self> {
        let lsm =
            LsmStorageBackend::open(shard_id, lsm_dir, block_cache, config.bloom_bits_per_key)?;
        Self::with_lsm_backend(shard_id, lsm, config)
    }

    /// Opens a complete existing checkpoint without creating or repairing its manifest.
    pub fn open_lsm_existing(
        shard_id: ShardId,
        lsm_dir: impl AsRef<Path>,
        block_cache: Arc<ShardedBlockCache>,
        config: LsmEngineConfig,
    ) -> Result<Self> {
        let lsm = LsmStorageBackend::open_existing(
            shard_id,
            lsm_dir,
            block_cache,
            config.bloom_bits_per_key,
        )?;
        Self::with_lsm_backend(shard_id, lsm, config)
    }

    fn with_lsm_backend(
        shard_id: ShardId,
        lsm: LsmStorageBackend,
        config: LsmEngineConfig,
    ) -> Result<Self> {
        let lsm = Arc::new(lsm);
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
        rebuild_density(&compactor, &lsm.current_version())?;
        Ok(Self {
            shard_id,
            raft_log: Arc::new(RaftLog::new(shard_id)),
            mvcc,
            compactor,
            backend: lsm,
            auto_locality_compaction: config.auto_locality_compaction,
            memtable_size_bytes: config.memtable_size_bytes.max(1),
            max_imm_memtables: config.max_imm_memtables.max(1),
            budget_mu: Mutex::new(()),
            flush_hook: RwLock::new(None),
            flush_identity: RwLock::new(None),
            entries_examined: AtomicU64::new(0),
            last_aux_frontier: AtomicU64::new(0),
        })
    }

    /// Installs the Raft pre-flush hook used by size-triggered seals.
    pub fn set_pre_flush_hook(&self, hook: Arc<dyn PreFlushHook>) {
        *self.flush_hook.write().unwrap() = Some(hook);
    }

    /// Records the Raft log identity to stamp on the next size-triggered checkpoint.
    pub fn set_flush_identity(&self, identity: FlushIdentity) {
        *self.flush_identity.write().unwrap() = Some(identity);
    }

    pub fn last_aux_frontier(&self) -> u64 {
        self.last_aux_frontier.load(Ordering::Acquire)
    }

    /// Rewrites only the durable checkpoint. SSTables are left in place.
    pub fn persist_checkpoint(&self, checkpoint: DurableShardCheckpoint) -> Result<()> {
        self.backend.persist_checkpoint_only(checkpoint)?;
        Ok(())
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
        self.maintain_memtables().expect("memtable size flush");
    }

    /// Applies an externally Raft-logged `Delete` at `(raft_index, hlc)` directly into `MemMvccStore`.
    pub fn apply_committed_delete(
        &self,
        key: Vec<u8>,
        hlc: HlcTimestamp,
        raft_index: LogSequenceNumber,
    ) {
        self.mvcc.delete(key, hlc, raft_index);
        self.maintain_memtables().expect("memtable size flush");
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
        self.maintain_memtables().expect("memtable size flush");
        lsn
    }

    /// Appends a `Delete` to the internal `RaftLog` and records an in-memory MVCC tombstone.
    pub fn delete(&self, key: Vec<u8>, hlc: HlcTimestamp) -> LogSequenceNumber {
        let lsn = self
            .raft_log
            .append(hlc, RaftLogPayload::Delete { key: key.clone() });
        self.mvcc.delete(key, hlc, lsn);
        self.maintain_memtables().expect("memtable size flush");
        lsn
    }

    /// Acquires a linearizable `SnapshotGuard` pinning `(ReadCut, Arc<Version>, ImmMemTables)`.
    pub fn acquire_linearizable_snapshot(
        &self,
        min_read_index: LogSequenceNumber,
    ) -> Result<SnapshotGuard> {
        self.mvcc
            .acquire_linearizable_snapshot(min_read_index, self.backend.as_ref())
    }

    /// Acquires a fixed-HLC `SnapshotGuard` at `barrier_hlc`.
    pub fn acquire_fixed_snapshot(&self, barrier_hlc: HlcTimestamp) -> Result<SnapshotGuard> {
        self.mvcc
            .acquire_fixed_snapshot(barrier_hlc, self.backend.as_ref())
    }

    /// Point lookup using a pinned `SnapshotGuard` and bounded step-lease check.
    pub fn get_with_snapshot(&self, key: &[u8], snap: &SnapshotGuard) -> Result<Option<Vec<u8>>> {
        let cut = snap.cut();
        snap.with_step_lease(|pinned| {
            if let Some(mvcc_val) = self.mvcc.get_with_pinned(key, cut, pinned) {
                return Ok(match mvcc_val {
                    MvccValue::Value(v) => Some(v),
                    MvccValue::Tombstone => None,
                });
            }
            match self.backend.get_in_version(&pinned.version, key)? {
                Some(PersistentEntry::Value(v)) => Ok(Some(v)),
                Some(PersistentEntry::PhysicalTombstone) | None => Ok(None),
            }
        })
    }

    /// Range scan over `[start_key, end_key)` using a pinned `SnapshotGuard` with bounded 256-key
    /// per-step read leases (`Resolution B1` — guarantees `<100µs` preemption on 5s forced expiration).
    pub fn scan_with_snapshot(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        snap: &SnapshotGuard,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        const SCAN_STEP_PAGE_KEYS: usize = 256;
        if start_key >= end_key {
            return snap.with_step_lease(|_| Ok(Vec::new()));
        }

        let cut = snap.cut();
        let mut cursor = start_key.to_vec();
        let mut out = Vec::new();

        loop {
            if cursor.as_slice() >= end_key {
                break;
            }
            let (page_live, next_cursor) = snap.with_step_lease(|pinned| {
                let mut merged = self.mvcc.range_page_with_pinned(
                    &cursor,
                    end_key,
                    cut,
                    pinned,
                    SCAN_STEP_PAGE_KEYS,
                );
                let (disk_entries, _) = self.backend.scan_in_version_page(
                    &pinned.version,
                    &cursor,
                    end_key,
                    SCAN_STEP_PAGE_KEYS,
                )?;

                let mut disk_tomb_keys = std::collections::HashSet::new();
                for (k, entry) in disk_entries {
                    if merged.contains_key(&k) {
                        continue;
                    }
                    match entry {
                        PersistentEntry::Value(v) => {
                            merged.insert(k, MvccValue::Value(v));
                        }
                        PersistentEntry::PhysicalTombstone => {
                            disk_tomb_keys.insert(k.clone());
                            merged.insert(k, MvccValue::Tombstone);
                        }
                    }
                }

                let raw_count = merged.len();
                while merged.len() > SCAN_STEP_PAGE_KEYS {
                    merged.pop_last();
                }

                let next_cur = if raw_count >= SCAN_STEP_PAGE_KEYS {
                    merged.last_key_value().map(|(last_k, _)| {
                        let mut nxt = last_k.clone();
                        nxt.push(0x00);
                        nxt
                    })
                } else {
                    None
                };

                let mut unshadowed_tombs = 0u64;
                let mut live_page = Vec::new();
                for (k, v) in merged {
                    match v {
                        MvccValue::Value(bytes) => live_page.push((k, bytes)),
                        MvccValue::Tombstone => {
                            if disk_tomb_keys.contains(&k) {
                                unshadowed_tombs += 1;
                            }
                        }
                    }
                }
                self.backend.record_scanned_tombstones(unshadowed_tombs);
                Ok((live_page, next_cur))
            })?;

            out.extend(page_live);
            match next_cursor {
                Some(nxt) => cursor = nxt,
                None => break,
            }
        }

        Ok(out)
    }

    /// Visits live keys in `[start_key, end_key)` in order and stops when `visit` returns
    /// [`ScanControl::Stop`]. Physical tombstones before that point are counted; keys after
    /// the stop are not read.
    pub fn scan_live_until(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        snap: &SnapshotGuard,
        mut visit: impl FnMut(&[u8], &[u8]) -> Result<ScanControl>,
    ) -> Result<()> {
        if start_key >= end_key {
            return Ok(());
        }
        let mut disk_from = start_key.to_vec();
        let mut mvcc_from = start_key.to_vec();
        let cut = snap.cut();
        loop {
            let pulled = snap.with_step_lease(|pinned| {
                self.pull_next(pinned, cut, &mut disk_from, &mut mvcc_from, end_key)
            })?;
            match pulled {
                Pulled::Done => break,
                Pulled::MvccTombstone => {
                    self.entries_examined.fetch_add(1, Ordering::Relaxed);
                }
                Pulled::PhysicalTombstone => {
                    self.entries_examined.fetch_add(1, Ordering::Relaxed);
                    self.backend.record_scanned_tombstones(1);
                }
                Pulled::Live(key, value) => {
                    self.entries_examined.fetch_add(1, Ordering::Relaxed);
                    if visit(&key, &value)? == ScanControl::Stop {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn pull_next(
        &self,
        pinned: &PinnedSnapshotData,
        cut: ReadCut,
        disk_from: &mut Vec<u8>,
        mvcc_from: &mut Vec<u8>,
        end_key: &[u8],
    ) -> Result<Pulled> {
        if mvcc_from.as_slice() >= end_key && disk_from.as_slice() >= end_key {
            return Ok(Pulled::Done);
        }
        let mvcc_item = if mvcc_from.as_slice() >= end_key {
            None
        } else {
            self.mvcc
                .range_page_with_pinned(mvcc_from, end_key, cut, pinned, 1)
                .into_iter()
                .next()
        };
        let disk_item = if disk_from.as_slice() >= end_key {
            None
        } else {
            let (page, _) =
                self.backend
                    .scan_in_version_page(&pinned.version, disk_from, end_key, 1)?;
            page.into_iter().next()
        };
        match (mvcc_item, disk_item) {
            (None, None) => Ok(Pulled::Done),
            (Some((key, value)), None) => {
                *mvcc_from = key_successor(&key);
                Ok(pulled_from_mvcc(key, value))
            }
            (None, Some((key, value))) => {
                *disk_from = key_successor(&key);
                Ok(pulled_from_disk(key, value))
            }
            (Some((mvcc_key, mvcc_value)), Some((disk_key, disk_value))) => {
                if mvcc_key < disk_key {
                    *mvcc_from = key_successor(&mvcc_key);
                    Ok(pulled_from_mvcc(mvcc_key, mvcc_value))
                } else if disk_key < mvcc_key {
                    *disk_from = key_successor(&disk_key);
                    Ok(pulled_from_disk(disk_key, disk_value))
                } else {
                    *mvcc_from = key_successor(&mvcc_key);
                    *disk_from = key_successor(&disk_key);
                    Ok(pulled_from_mvcc(mvcc_key, mvcc_value))
                }
            }
        }
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
        let _budget = self.budget_mu.lock().unwrap();
        self.seal_and_flush_locked(checkpoint, hook)
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
            self.try_compact_range(&start, &end)?;
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

    /// Performs localized bottommost range compaction over `[start_key, end_key)`, updating
    /// `LocalityCompactor` bucket counts strictly when `backend.compact_range` succeeds (`Ok`).
    pub fn try_compact_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<usize> {
        let (snap_tombs, snap_live) = self.compactor.snapshot_bucket_counts(start_key);
        let purged = self.backend.compact_range(start_key, end_key)?;
        self.compactor
            .on_range_compacted(start_key, snap_tombs, snap_live, purged);
        Ok(purged)
    }

    /// Performs localized bottommost range compaction over `[start_key, end_key)`.
    pub fn compact_range(&self, start_key: &[u8], end_key: &[u8]) -> usize {
        self.try_compact_range(start_key, end_key).unwrap_or(0)
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
            entries_examined_on_scans: self.entries_examined.load(Ordering::Relaxed),
            compaction_stats: self.compactor.stats(),
        }
    }

    fn maintain_memtables(&self) -> Result<()> {
        if self.mvcc.active_memtable_bytes() < self.memtable_size_bytes {
            return Ok(());
        }
        let _budget = self.budget_mu.lock().unwrap();
        if self.mvcc.active_memtable_bytes() < self.memtable_size_bytes {
            return Ok(());
        }
        let (lsn, hlc, _, _) = self.mvcc.latest_watermarks();
        let checkpoint = self.checkpoint_for_flush(lsn, hlc);
        let hook = self.flush_hook.read().unwrap().clone();
        let hook_ref = hook.as_ref().map(|installed| installed.as_ref());
        self.seal_and_flush_locked(checkpoint, hook_ref)?;
        Ok(())
    }

    fn seal_and_flush_locked(
        &self,
        checkpoint: DurableShardCheckpoint,
        hook: Option<&dyn PreFlushHook>,
    ) -> Result<(LogSequenceNumber, LogSequenceNumber)> {
        self.ensure_imm_capacity(hook)?;
        self.mvcc
            .seal_active_memtable(self.backend.as_ref(), checkpoint, hook)?;
        let flushed = self.flush_queued_imms_with_hook(hook)?;
        self.last_aux_frontier.store(flushed.1, Ordering::Release);
        Ok(flushed)
    }

    fn ensure_imm_capacity(&self, hook: Option<&dyn PreFlushHook>) -> Result<()> {
        let started = Instant::now();
        let limit = Duration::from_millis(self.mvcc.retention_ms().saturating_add(1_000));
        while self.mvcc.imm_queue_len() >= self.max_imm_memtables {
            self.flush_queued_imms_with_hook(hook)?;
            if self.mvcc.imm_queue_len() < self.max_imm_memtables {
                return Ok(());
            }
            if started.elapsed() > limit {
                self.mvcc.snapshot_registry().expire_stale_and_min_bounds();
                self.flush_queued_imms_with_hook(hook)?;
                if self.mvcc.imm_queue_len() >= self.max_imm_memtables {
                    return Err(MetaDbError::StorageIoError(format!(
                        "immutable memtable cap {} is pinned past the MVCC retention window",
                        self.max_imm_memtables
                    )));
                }
                return Ok(());
            }
            self.mvcc
                .snapshot_registry()
                .wait_for_flush_progress(Duration::from_millis(20));
        }
        Ok(())
    }

    fn checkpoint_for_flush(&self, lsn: u64, hlc: HlcTimestamp) -> DurableShardCheckpoint {
        let current = self.durable_checkpoint();
        match self.flush_identity.read().unwrap().clone() {
            Some(identity) => DurableShardCheckpoint {
                flushed_raft_term: identity.term,
                flushed_raft_leader: identity.leader,
                flushed_raft_index: lsn,
                flushed_max_hlc: hlc,
                contiguous_aux_acked_index: identity.aux_index,
                membership_bytes: identity.membership,
                descriptor_bytes: identity.descriptor,
            },
            None => DurableShardCheckpoint {
                flushed_raft_index: lsn,
                flushed_max_hlc: hlc,
                ..current
            },
        }
    }
}

enum Pulled {
    Done,
    MvccTombstone,
    PhysicalTombstone,
    Live(Vec<u8>, Vec<u8>),
}

fn pulled_from_mvcc(key: Vec<u8>, value: MvccValue) -> Pulled {
    match value {
        MvccValue::Value(bytes) => Pulled::Live(key, bytes),
        MvccValue::Tombstone => Pulled::MvccTombstone,
    }
}

fn pulled_from_disk(key: Vec<u8>, value: PersistentEntry) -> Pulled {
    match value {
        PersistentEntry::Value(bytes) => Pulled::Live(key, bytes),
        PersistentEntry::PhysicalTombstone => Pulled::PhysicalTombstone,
    }
}

fn key_successor(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0x00);
    next
}

fn rebuild_density(compactor: &LocalityCompactor, version: &Version) -> Result<()> {
    let mut cursor = version.merged_cursor(&[], None)?;
    let mut counts: BTreeMap<RangeBucketPrefix, (usize, usize)> = BTreeMap::new();
    while let Some((key, value)) = cursor.next_owned()? {
        let Some(bucket) = LocalityCompactor::bucket_for_key(&key) else {
            continue;
        };
        let slot = counts.entry(bucket).or_insert((0, 0));
        match value {
            BlockEntryValue::PhysicalTombstone => slot.0 += 1,
            BlockEntryValue::Value(_) => slot.1 += 1,
        }
    }
    compactor.restore_counts(counts);
    Ok(())
}
