//! Pluggable `StorageBackend` trait with pure-Rust WAL-less `LsmStorageBackend` and `InMemoryBackend`.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use metadb_core::{MetaDbError, Result, ShardId};

use crate::block::BlockEntryValue;
use crate::cache::ShardedBlockCache;
use crate::manifest::{DurableShardCheckpoint, ManifestLog, VersionEdit};
use crate::sstable::{sst_file_name, sync_dir, SstFileMeta, SstableBuilder, SstableReader};
use crate::version_set::{Version, NUM_LSM_LEVELS};

pub const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;
pub const TARGET_L1_SST_BYTES: usize = 64 * 1024 * 1024;

/// Entry stored in the persistent sorted backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistentEntry {
    /// Live persisted key-value pair.
    Value(Vec<u8>),
    /// Physical LSM-tree tombstone awaiting bottommost range compaction.
    PhysicalTombstone,
}

impl From<PersistentEntry> for BlockEntryValue {
    fn from(e: PersistentEntry) -> Self {
        match e {
            PersistentEntry::Value(v) => BlockEntryValue::Value(v),
            PersistentEntry::PhysicalTombstone => BlockEntryValue::PhysicalTombstone,
        }
    }
}

impl From<BlockEntryValue> for PersistentEntry {
    fn from(e: BlockEntryValue) -> Self {
        match e {
            BlockEntryValue::Value(v) => PersistentEntry::Value(v),
            BlockEntryValue::PhysicalTombstone => PersistentEntry::PhysicalTombstone,
        }
    }
}

/// Abstract sorted persistent storage engine behind `MemMvccStore`.
pub trait StorageBackend: Send + Sync {
    /// Returns the current immutable `Version` (or synthetic empty version for in-memory backends).
    fn current_version(&self) -> Arc<Version>;

    /// Point lookup against `pinned_version` (used by `SnapshotGuard`).
    fn get_in_version(&self, version: &Version, key: &[u8]) -> Result<Option<PersistentEntry>>;

    /// Range scan over `[start_key, end_key)` against `pinned_version`.
    fn scan_in_version(
        &self,
        version: &Version,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<(BTreeMap<Vec<u8>, PersistentEntry>, u64)>;

    /// Bounded page scan returning up to `limit` smallest merged keys in `[start_key, end_key)` against `pinned_version`.
    fn scan_in_version_page(
        &self,
        version: &Version,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<(BTreeMap<Vec<u8>, PersistentEntry>, u64)>;

    /// Atomically flushes sorted collapsed `(key, PersistentEntry)` items as a new `L0` SSTable
    /// and updates `MANIFEST` with `checkpoint`.
    fn flush_l0_batch(
        &self,
        sorted_entries: &[(Vec<u8>, PersistentEntry)],
        checkpoint: DurableShardCheckpoint,
    ) -> Result<Arc<Version>>;

    /// Updates only `DurableShardCheckpoint` in `MANIFEST` (e.g., when `ImmMemTable` had zero keys
    /// or during control-plane metadata checkpointing).
    fn persist_checkpoint_only(&self, checkpoint: DurableShardCheckpoint) -> Result<Arc<Version>>;

    /// Executes bottommost-safe localized compaction over `[start_key, end_key)`,
    /// returning the number of `PhysicalTombstone`s permanently dropped.
    fn compact_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<usize>;

    /// Materializes a hard-linked SSTable + standalone `MANIFEST` checkpoint into `dest_dir`,
    /// returning the exact `DurableShardCheckpoint` pinned by that snapshot (`Resolution B6`).
    fn create_checkpoint(&self, dest_dir: &Path) -> Result<DurableShardCheckpoint>;

    /// Counters `(persistent_kv_writes, active_physical_tombstones, tombstones_traversed_on_scans)`.
    fn backend_counters(&self) -> (u64, usize, u64);

    /// Records physical tombstones traversed during a foreground scan.
    fn record_scanned_tombstones(&self, count: u64);
}

struct LsmState {
    next_file_id: u64,
    next_flush_generation: u64,
    manifest: ManifestLog,
    persistent_kv_writes: u64,
    tombstones_traversed: u64,
    edits_since_rotation: usize,
}

/// Disk-persistent pure-Rust WAL-less LSM engine (`L0..L3` SSTables + `MANIFEST` + `ShardedBlockCache`).
pub struct LsmStorageBackend {
    shard_id: ShardId,
    lsm_dir: PathBuf,
    _lock_file: File,
    block_cache: Arc<ShardedBlockCache>,
    bloom_bits_per_key: usize,
    l0_compaction_trigger: usize,
    active_version: RwLock<Arc<Version>>,
    state: Mutex<LsmState>,
    compaction_lock: Mutex<()>,
}

impl LsmStorageBackend {
    pub fn open(
        shard_id: ShardId,
        lsm_dir: impl AsRef<Path>,
        block_cache: Arc<ShardedBlockCache>,
        bloom_bits_per_key: usize,
    ) -> Result<Self> {
        let lsm_dir = lsm_dir.as_ref().to_path_buf();
        fs::create_dir_all(&lsm_dir)?;
        let lock_path = lsm_dir.join("LOCK");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file.try_lock().map_err(|e| {
            MetaDbError::StorageIoError(format!(
                "failed to acquire exclusive lock on {:?}: {}",
                lock_path, e
            ))
        })?;
        let (manifest, recovered) = ManifestLog::open_or_create(&lsm_dir)?;

        let mut readers = Vec::with_capacity(recovered.live_files.len());
        for meta in recovered.live_files.values() {
            let sst_path = lsm_dir.join(sst_file_name(meta.file_id));
            let reader = SstableReader::open(shard_id, sst_path, block_cache.clone())?;
            readers.push(Arc::new(reader));
        }

        let version = Arc::new(Version::from_readers(readers, recovered.checkpoint));

        Ok(Self {
            shard_id,
            lsm_dir,
            _lock_file: lock_file,
            block_cache,
            bloom_bits_per_key: bloom_bits_per_key.max(4),
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            active_version: RwLock::new(version),
            state: Mutex::new(LsmState {
                next_file_id: recovered.next_file_id,
                next_flush_generation: recovered.next_flush_generation,
                manifest,
                persistent_kv_writes: 0,
                tombstones_traversed: 0,
                edits_since_rotation: 0,
            }),
            compaction_lock: Mutex::new(()),
        })
    }

    pub fn lsm_dir(&self) -> &Path {
        &self.lsm_dir
    }

    fn maybe_compact_l0(&self) -> Result<()> {
        let v = self.current_version();
        if v.levels[0].len() < self.l0_compaction_trigger {
            return Ok(());
        }
        let min_key = v.levels[0]
            .iter()
            .map(|r| r.meta().min_key.clone())
            .min()
            .unwrap();
        let mut max_end = v.levels[0]
            .iter()
            .map(|r| r.meta().max_key.clone())
            .max()
            .unwrap();
        max_end.push(0x00);
        let _ = self.compact_range(&min_key, &max_end)?;
        Ok(())
    }

    fn cleanup_unreferenced_ssts(&self, deleted_ids: &[u64]) {
        let v = self.current_version();
        let still_live: HashSet<u64> = v
            .levels
            .iter()
            .flat_map(|l| l.iter().map(|r| r.meta().file_id))
            .collect();
        for &fid in deleted_ids {
            if !still_live.contains(&fid) {
                let _ = fs::remove_file(self.lsm_dir.join(sst_file_name(fid)));
            }
        }
    }
}

impl StorageBackend for LsmStorageBackend {
    fn current_version(&self) -> Arc<Version> {
        self.active_version.read().unwrap().clone()
    }

    fn get_in_version(&self, version: &Version, key: &[u8]) -> Result<Option<PersistentEntry>> {
        Ok(version.get(key)?.map(PersistentEntry::from))
    }

    fn scan_in_version(
        &self,
        version: &Version,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<(BTreeMap<Vec<u8>, PersistentEntry>, u64)> {
        let (raw, tombs) = version.scan_range_merged(start_key, end_key)?;
        let converted = raw
            .into_iter()
            .map(|(k, v)| (k, PersistentEntry::from(v)))
            .collect();
        Ok((converted, tombs))
    }

    fn scan_in_version_page(
        &self,
        version: &Version,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<(BTreeMap<Vec<u8>, PersistentEntry>, u64)> {
        let (raw, tombs) = version.scan_range_page_merged(start_key, end_key, limit)?;
        let converted = raw
            .into_iter()
            .map(|(k, v)| (k, PersistentEntry::from(v)))
            .collect();
        Ok((converted, tombs))
    }

    fn flush_l0_batch(
        &self,
        sorted_entries: &[(Vec<u8>, PersistentEntry)],
        checkpoint: DurableShardCheckpoint,
    ) -> Result<Arc<Version>> {
        if sorted_entries.is_empty() {
            return self.persist_checkpoint_only(checkpoint);
        }

        let (file_id, flush_gen) = {
            let mut st = self.state.lock().unwrap();
            let fid = st.next_file_id;
            st.next_file_id += 1;
            let fgen = st.next_flush_generation;
            st.next_flush_generation += 1;
            (fid, fgen)
        };

        let block_entries: Vec<(Vec<u8>, BlockEntryValue)> = sorted_entries
            .iter()
            .map(|(k, v)| (k.clone(), BlockEntryValue::from(v.clone())))
            .collect();

        let builder = SstableBuilder::new(
            0,
            file_id,
            flush_gen,
            checkpoint.flushed_raft_index,
            checkpoint.flushed_max_hlc,
        )
        .with_bloom_bits(self.bloom_bits_per_key);

        let meta = builder.write_to_dir(&self.lsm_dir, &block_entries)?;
        let reader = Arc::new(SstableReader::open(
            self.shard_id,
            self.lsm_dir.join(sst_file_name(file_id)),
            self.block_cache.clone(),
        )?);

        let new_version = {
            let mut st = self.state.lock().unwrap();
            let edit = VersionEdit {
                next_file_id: Some(st.next_file_id),
                next_flush_generation: Some(st.next_flush_generation),
                checkpoint: Some(checkpoint.clone()),
                deleted_files: vec![],
                added_files: vec![meta],
            };
            st.manifest.append(&edit)?;
            st.persistent_kv_writes += sorted_entries.len() as u64;
            st.edits_since_rotation += 1;

            let mut v_guard = self.active_version.write().unwrap();
            let mut next_v = (**v_guard).clone();
            next_v.levels[0].push(reader);
            Version::normalize_levels(&mut next_v.levels);
            next_v.checkpoint = checkpoint;
            let arc_v = Arc::new(next_v);
            *v_guard = arc_v.clone();

            if st.edits_since_rotation >= 64 {
                let snap_edit = VersionEdit {
                    next_file_id: Some(st.next_file_id),
                    next_flush_generation: Some(st.next_flush_generation),
                    checkpoint: Some(arc_v.checkpoint.clone()),
                    deleted_files: vec![],
                    added_files: arc_v.all_metas(),
                };
                st.manifest.rotate(&snap_edit)?;
                st.edits_since_rotation = 0;
            }
            arc_v
        };

        self.maybe_compact_l0()?;
        Ok(self.current_version().max_by_raft(new_version))
    }

    fn persist_checkpoint_only(&self, checkpoint: DurableShardCheckpoint) -> Result<Arc<Version>> {
        let mut st = self.state.lock().unwrap();
        let edit = VersionEdit {
            next_file_id: Some(st.next_file_id),
            next_flush_generation: Some(st.next_flush_generation),
            checkpoint: Some(checkpoint.clone()),
            deleted_files: vec![],
            added_files: vec![],
        };
        st.manifest.append(&edit)?;
        let mut v_guard = self.active_version.write().unwrap();
        let mut next_v = (**v_guard).clone();
        next_v.checkpoint = checkpoint;
        let arc_v = Arc::new(next_v);
        *v_guard = arc_v.clone();
        Ok(arc_v)
    }

    fn compact_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<usize> {
        let _comp_guard = self.compaction_lock.lock().unwrap();
        let base_version = self.current_version();

        // Step 1: Collect all overlapping SSTables across L0..L3 for `[start_key, end_key)`.
        let mut participating: [Vec<Arc<SstableReader>>; NUM_LSM_LEVELS] =
            std::array::from_fn(|_| Vec::new());
        let mut deepest_level = 1usize;

        for (lvl_idx, lvl) in base_version.levels.iter().enumerate() {
            for sst in lvl {
                if sst.meta().max_key.as_slice() >= start_key
                    && sst.meta().min_key.as_slice() < end_key
                {
                    participating[lvl_idx].push(sst.clone());
                    if lvl_idx > deepest_level {
                        deepest_level = lvl_idx;
                    }
                }
            }
        }

        if participating.iter().all(|v| v.is_empty()) {
            return Ok(0);
        }

        // Step 2: Compute full key span `[union_min, union_max]` across all participating SSTables
        // and pull in any non-overlapping SSTables at `target_level` overlapping `[union_min, union_max]`
        // so `target_level` stays strictly non-overlapping after full rewrite (`Resolution B4`).
        let mut union_min: Option<Vec<u8>> = None;
        let mut union_max: Option<Vec<u8>> = None;
        for lvl in &participating {
            for sst in lvl {
                union_min = Some(match union_min {
                    Some(cur) => cur.min(sst.meta().min_key.clone()),
                    None => sst.meta().min_key.clone(),
                });
                union_max = Some(match union_max {
                    Some(cur) => cur.max(sst.meta().max_key.clone()),
                    None => sst.meta().max_key.clone(),
                });
            }
        }
        let (mut union_min, mut union_max) = (union_min.unwrap(), union_max.unwrap());
        let target_level = deepest_level.max(1);

        // Iteratively expand participating SSTables across ALL levels `0..=target_level` overlapping
        // `[union_min, union_max]` until fixed point so rewriting wide `L0` files into `target_level >= 2`
        // never moves newer keys beneath an untouched intermediate `L1` SSTable!
        loop {
            let mut added_any = false;
            for (lvl_idx, part_lvl) in participating.iter_mut().enumerate().take(target_level + 1) {
                let existing_ids: HashSet<u64> =
                    part_lvl.iter().map(|r| r.meta().file_id).collect();
                for sst in &base_version.levels[lvl_idx] {
                    if !existing_ids.contains(&sst.meta().file_id)
                        && sst.meta().max_key >= union_min
                        && sst.meta().min_key <= union_max
                    {
                        union_min = union_min.min(sst.meta().min_key.clone());
                        union_max = union_max.max(sst.meta().max_key.clone());
                        part_lvl.push(sst.clone());
                        added_any = true;
                    }
                }
            }
            if !added_any {
                break;
            }
        }
        participating[0].sort_by(|a, b| {
            b.meta()
                .flush_generation
                .cmp(&a.meta().flush_generation)
                .then_with(|| b.meta().file_id.cmp(&a.meta().file_id))
        });
        for lvl in &mut participating[1..] {
            lvl.sort_by(|a, b| a.meta().min_key.cmp(&b.meta().min_key));
        }

        // Step 3: Merge 100% of keys from all participating SSTables in deterministic precedence order!
        let mut merged: BTreeMap<Vec<u8>, BlockEntryValue> = BTreeMap::new();
        let mut max_raft_idx = 0u64;
        let mut max_hlc = metadb_core::HlcTimestamp::ZERO;
        let mut input_tombstones_in_range = 0usize;

        for sst in &participating[0] {
            max_raft_idx = max_raft_idx.max(sst.meta().max_raft_index);
            max_hlc = max_hlc.max(sst.meta().max_hlc);
            for (k, v) in sst.read_all_entries()? {
                merged.entry(k).or_insert(v);
            }
        }
        for lvl in &participating[1..] {
            for sst in lvl {
                max_raft_idx = max_raft_idx.max(sst.meta().max_raft_index);
                max_hlc = max_hlc.max(sst.meta().max_hlc);
                for (k, v) in sst.read_all_entries()? {
                    merged.entry(k).or_insert(v);
                }
            }
        }

        // Step 4: Bottommost Tombstone Drop Invariant (`Resolution B4`):
        // Drop `PhysicalTombstone` iff zero non-participating SSTables in levels `> target_level`
        // overlap `K`. For `K ∈ [start_key, end_key)`, `target_level` already absorbed the deepest
        // overlapping SSTable so `has_overlap_above` is guaranteed `false`; for outer boundary keys,
        // `base_version.has_overlap_above(target_level, &k)` preserves tombstones whenever needed!
        let mut output_entries: Vec<(Vec<u8>, BlockEntryValue)> = Vec::with_capacity(merged.len());
        let mut purged_tombstones = 0usize;

        for (k, v) in merged {
            match v {
                BlockEntryValue::Value(bytes) => {
                    output_entries.push((k, BlockEntryValue::Value(bytes)));
                }
                BlockEntryValue::PhysicalTombstone => {
                    let in_target_range = k.as_slice() >= start_key && k.as_slice() < end_key;
                    if in_target_range {
                        input_tombstones_in_range += 1;
                    }
                    if !base_version.has_overlap_above(target_level, &k) {
                        purged_tombstones += 1;
                    } else {
                        output_entries.push((k, BlockEntryValue::PhysicalTombstone));
                    }
                }
            }
        }
        let _ = input_tombstones_in_range;

        // Partition output into <= TARGET_L1_SST_BYTES non-overlapping SSTables at `target_level`.
        let mut new_metas: Vec<SstFileMeta> = Vec::new();
        let mut new_readers: Vec<Arc<SstableReader>> = Vec::new();

        let mut chunk: Vec<(Vec<u8>, BlockEntryValue)> = Vec::new();
        let mut chunk_bytes = 0usize;

        let write_chunk =
            |items: &[(Vec<u8>, BlockEntryValue)]| -> Result<(SstFileMeta, Arc<SstableReader>)> {
                let (fid, fgen) = {
                    let mut st = self.state.lock().unwrap();
                    let fid = st.next_file_id;
                    st.next_file_id += 1;
                    let fgen = st.next_flush_generation;
                    st.next_flush_generation += 1;
                    (fid, fgen)
                };
                let meta =
                    SstableBuilder::new(target_level as u8, fid, fgen, max_raft_idx, max_hlc)
                        .with_bloom_bits(self.bloom_bits_per_key)
                        .write_to_dir(&self.lsm_dir, items)?;
                let reader = Arc::new(SstableReader::open(
                    self.shard_id,
                    self.lsm_dir.join(sst_file_name(fid)),
                    self.block_cache.clone(),
                )?);
                Ok((meta, reader))
            };

        for (k, v) in output_entries {
            let sz = k.len()
                + match &v {
                    BlockEntryValue::Value(b) => b.len(),
                    BlockEntryValue::PhysicalTombstone => 0,
                }
                + 16;
            chunk.push((k, v));
            chunk_bytes += sz;
            if chunk_bytes >= TARGET_L1_SST_BYTES {
                let (m, r) = write_chunk(&chunk)?;
                new_metas.push(m);
                new_readers.push(r);
                chunk.clear();
                chunk_bytes = 0;
            }
        }
        if !chunk.is_empty() {
            let (m, r) = write_chunk(&chunk)?;
            new_metas.push(m);
            new_readers.push(r);
        }

        // Step 5: Atomically publish `VersionEdit` (taking care to retain any newer L0 files flushed
        // concurrently while compaction was running!).
        let participating_set: HashSet<(u8, u64)> = participating
            .iter()
            .enumerate()
            .flat_map(|(lvl, files)| files.iter().map(move |r| (lvl as u8, r.meta().file_id)))
            .collect();
        let deleted_files: Vec<(u8, u64)> = participating_set.iter().copied().collect();
        let deleted_ids: Vec<u64> = deleted_files.iter().map(|&(_, id)| id).collect();

        {
            let mut st = self.state.lock().unwrap();
            let edit = VersionEdit {
                next_file_id: Some(st.next_file_id),
                next_flush_generation: Some(st.next_flush_generation),
                checkpoint: None,
                deleted_files,
                added_files: new_metas,
            };
            st.manifest.append(&edit)?;

            let mut v_guard = self.active_version.write().unwrap();
            let mut next_v = (**v_guard).clone();
            for (lvl_idx, lvl) in next_v.levels.iter_mut().enumerate() {
                lvl.retain(|r| !participating_set.contains(&(lvl_idx as u8, r.meta().file_id)));
            }
            next_v.levels[target_level].extend(new_readers);
            Version::normalize_levels(&mut next_v.levels);
            *v_guard = Arc::new(next_v);
        }

        self.cleanup_unreferenced_ssts(&deleted_ids);
        Ok(purged_tombstones)
    }

    fn create_checkpoint(&self, dest_dir: &Path) -> Result<DurableShardCheckpoint> {
        if dest_dir.exists() {
            return Err(MetaDbError::StorageIoError(format!(
                "checkpoint destination {:?} already exists",
                dest_dir
            )));
        }
        let pinned = self.current_version();
        let st_snapshot = {
            let st = self.state.lock().unwrap();
            (st.next_file_id, st.next_flush_generation)
        };

        let parent = dest_dir.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let tmp_dir = dest_dir.with_extension("ckpt.tmp");
        if tmp_dir.exists() {
            fs::remove_dir_all(&tmp_dir)?;
        }
        fs::create_dir_all(&tmp_dir)?;

        // Hard-link every immutable `.sst` file from `pinned` into `tmp_dir`.
        for lvl in &pinned.levels {
            for reader in lvl {
                let dst = tmp_dir.join(sst_file_name(reader.meta().file_id));
                fs::hard_link(reader.path(), &dst)?;
            }
        }

        let snap_edit = VersionEdit {
            next_file_id: Some(st_snapshot.0),
            next_flush_generation: Some(st_snapshot.1),
            checkpoint: Some(pinned.checkpoint.clone()),
            deleted_files: vec![],
            added_files: pinned.all_metas(),
        };
        ManifestLog::write_standalone_checkpoint(&tmp_dir, &snap_edit)?;
        fs::rename(&tmp_dir, dest_dir)?;
        sync_dir(parent)?;
        Ok(pinned.checkpoint.clone())
    }

    fn backend_counters(&self) -> (u64, usize, u64) {
        let v = self.current_version();
        let active_tombs = v.count_physical_tombstones();
        let st = self.state.lock().unwrap();
        (
            st.persistent_kv_writes,
            active_tombs,
            st.tombstones_traversed,
        )
    }

    fn record_scanned_tombstones(&self, count: u64) {
        if count > 0 {
            self.state.lock().unwrap().tombstones_traversed += count;
        }
    }
}

trait VersionSelectExt {
    fn max_by_raft(self, other: Arc<Version>) -> Arc<Version>;
}

impl VersionSelectExt for Arc<Version> {
    fn max_by_raft(self, other: Arc<Version>) -> Arc<Version> {
        if self.checkpoint.flushed_raft_index >= other.checkpoint.flushed_raft_index {
            self
        } else {
            other
        }
    }
}

struct InMemInner {
    entries: BTreeMap<Vec<u8>, PersistentEntry>,
    version: Arc<Version>,
    persistent_kv_writes: u64,
    tombstones_traversed: u64,
}

/// Fast in-memory `StorageBackend` implementation preserving identical semantics for unit tests.
pub struct InMemoryBackend {
    inner: RwLock<InMemInner>,
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(InMemInner {
                entries: BTreeMap::new(),
                version: Arc::new(Version::empty()),
                persistent_kv_writes: 0,
                tombstones_traversed: 0,
            }),
        }
    }
}

impl StorageBackend for InMemoryBackend {
    fn current_version(&self) -> Arc<Version> {
        self.inner.read().unwrap().version.clone()
    }

    fn get_in_version(&self, _version: &Version, key: &[u8]) -> Result<Option<PersistentEntry>> {
        Ok(self.inner.read().unwrap().entries.get(key).cloned())
    }

    fn scan_in_version(
        &self,
        _version: &Version,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<(BTreeMap<Vec<u8>, PersistentEntry>, u64)> {
        let guard = self.inner.read().unwrap();
        let mut out = BTreeMap::new();
        let mut tombs = 0u64;
        for (k, v) in guard
            .entries
            .range::<[u8], _>((Bound::Included(start_key), Bound::Excluded(end_key)))
        {
            if matches!(v, PersistentEntry::PhysicalTombstone) {
                tombs += 1;
            }
            out.insert(k.clone(), v.clone());
        }
        Ok((out, tombs))
    }

    fn scan_in_version_page(
        &self,
        _version: &Version,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<(BTreeMap<Vec<u8>, PersistentEntry>, u64)> {
        if limit == 0 || start_key >= end_key {
            return Ok((BTreeMap::new(), 0));
        }
        let guard = self.inner.read().unwrap();
        let mut out = BTreeMap::new();
        let mut tombs = 0u64;
        for (k, v) in guard
            .entries
            .range::<[u8], _>((Bound::Included(start_key), Bound::Excluded(end_key)))
            .take(limit)
        {
            if matches!(v, PersistentEntry::PhysicalTombstone) {
                tombs += 1;
            }
            out.insert(k.clone(), v.clone());
        }
        Ok((out, tombs))
    }

    fn flush_l0_batch(
        &self,
        sorted_entries: &[(Vec<u8>, PersistentEntry)],
        checkpoint: DurableShardCheckpoint,
    ) -> Result<Arc<Version>> {
        let mut guard = self.inner.write().unwrap();
        for (k, v) in sorted_entries {
            guard.persistent_kv_writes += 1;
            guard.entries.insert(k.clone(), v.clone());
        }
        let mut v = Version::empty();
        v.checkpoint = checkpoint;
        let arc_v = Arc::new(v);
        guard.version = arc_v.clone();
        Ok(arc_v)
    }

    fn persist_checkpoint_only(&self, checkpoint: DurableShardCheckpoint) -> Result<Arc<Version>> {
        let mut guard = self.inner.write().unwrap();
        let mut v = Version::empty();
        v.checkpoint = checkpoint;
        let arc_v = Arc::new(v);
        guard.version = arc_v.clone();
        Ok(arc_v)
    }

    fn compact_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<usize> {
        let mut guard = self.inner.write().unwrap();
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
        Ok(purged)
    }

    fn create_checkpoint(&self, dest_dir: &Path) -> Result<DurableShardCheckpoint> {
        fs::create_dir_all(dest_dir)?;
        let cp = self.current_version().checkpoint.clone();
        let edit = VersionEdit {
            next_file_id: Some(1),
            next_flush_generation: Some(1),
            checkpoint: Some(cp.clone()),
            deleted_files: vec![],
            added_files: vec![],
        };
        ManifestLog::write_standalone_checkpoint(dest_dir, &edit)?;
        Ok(cp)
    }

    fn backend_counters(&self) -> (u64, usize, u64) {
        let guard = self.inner.read().unwrap();
        let tombs = guard
            .entries
            .values()
            .filter(|e| matches!(e, PersistentEntry::PhysicalTombstone))
            .count();
        (
            guard.persistent_kv_writes,
            tombs,
            guard.tombstones_traversed,
        )
    }

    fn record_scanned_tombstones(&self, count: u64) {
        if count > 0 {
            self.inner.write().unwrap().tombstones_traversed += count;
        }
    }
}
