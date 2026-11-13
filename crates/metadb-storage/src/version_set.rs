//! Immutable `Version` views and deterministic `MergingIterator` across `L0..L3` SSTables.

use std::collections::BTreeMap;
use std::sync::Arc;

use metadb_core::Result;

use crate::block::BlockEntryValue;
use crate::manifest::DurableShardCheckpoint;
use crate::sstable::{SstFileMeta, SstableReader};

pub const NUM_LSM_LEVELS: usize = 4;

/// Point-in-time immutable LSM SSTable tree pinned by `SnapshotGuard` via `Arc<Version>`.
#[derive(Clone)]
pub struct Version {
    /// `levels[0]`: sorted by **descending `flush_generation`** (newest first).
    /// `levels[1..4]`: non-overlapping runs sorted by **ascending `min_key`**.
    pub levels: [Vec<Arc<SstableReader>>; NUM_LSM_LEVELS],
    pub checkpoint: DurableShardCheckpoint,
}

impl Version {
    pub fn empty() -> Self {
        Self {
            levels: std::array::from_fn(|_| Vec::new()),
            checkpoint: DurableShardCheckpoint::default(),
        }
    }

    pub fn from_readers(
        readers: impl IntoIterator<Item = Arc<SstableReader>>,
        checkpoint: DurableShardCheckpoint,
    ) -> Self {
        let mut levels: [Vec<Arc<SstableReader>>; NUM_LSM_LEVELS] =
            std::array::from_fn(|_| Vec::new());
        for r in readers {
            let lvl = (r.meta().level as usize).min(NUM_LSM_LEVELS - 1);
            levels[lvl].push(r);
        }
        Self::normalize_levels(&mut levels);
        Self { levels, checkpoint }
    }

    pub fn normalize_levels(levels: &mut [Vec<Arc<SstableReader>>; NUM_LSM_LEVELS]) {
        // L0: newest flush_generation first!
        levels[0].sort_by(|a, b| {
            b.meta()
                .flush_generation
                .cmp(&a.meta().flush_generation)
                .then_with(|| b.meta().file_id.cmp(&a.meta().file_id))
        });
        // L1..L3: non-overlapping runs ordered by min_key ascending.
        for lvl in &mut levels[1..] {
            lvl.sort_by(|a, b| a.meta().min_key.cmp(&b.meta().min_key));
        }
    }

    /// Point lookup across `L0` (descending `flush_generation`) then `L1 -> L2 -> L3`.
    pub fn get(&self, key: &[u8]) -> Result<Option<BlockEntryValue>> {
        for sst in &self.levels[0] {
            if let Some(entry) = sst.get(key)? {
                return Ok(Some(entry));
            }
        }
        for lvl in &self.levels[1..] {
            let idx = lvl.partition_point(|sst| sst.meta().max_key.as_slice() < key);
            if idx < lvl.len() && key >= lvl[idx].meta().min_key.as_slice() {
                if let Some(entry) = lvl[idx].get(key)? {
                    return Ok(Some(entry));
                }
            }
        }
        Ok(None)
    }

    /// Scans `[start_key, end_key)` across `L0..L3` enforcing deterministic source precedence:
    /// `L0 (descending flush_generation) > L1 > L2 > L3`.
    /// Also returns the count of `PhysicalTombstone`s encountered during the scan.
    pub fn scan_range_merged(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<(BTreeMap<Vec<u8>, BlockEntryValue>, u64)> {
        let mut merged: BTreeMap<Vec<u8>, BlockEntryValue> = BTreeMap::new();
        let mut tombstones_seen = 0u64;

        // Visit in strict precedence order: first writer into `merged` wins!
        for sst in &self.levels[0] {
            for (k, v) in sst.scan_range(start_key, end_key)? {
                if matches!(v, BlockEntryValue::PhysicalTombstone) {
                    tombstones_seen += 1;
                }
                merged.entry(k).or_insert(v);
            }
        }
        for lvl in &self.levels[1..] {
            let first_idx = lvl.partition_point(|sst| sst.meta().max_key.as_slice() < start_key);
            for sst in &lvl[first_idx..] {
                if sst.meta().min_key.as_slice() >= end_key {
                    break;
                }
                for (k, v) in sst.scan_range(start_key, end_key)? {
                    if matches!(v, BlockEntryValue::PhysicalTombstone) {
                        tombstones_seen += 1;
                    }
                    merged.entry(k).or_insert(v);
                }
            }
        }

        Ok((merged, tombstones_seen))
    }

    /// Returns `true` if any SSTable in levels strictly deeper than `target_level` has
    /// `min_key <= key <= max_key` (`storage_lsm_mvcc.md` §8.2 Bottommost Tombstone Rule).
    pub fn has_overlap_above(&self, target_level: usize, key: &[u8]) -> bool {
        for lvl in &self.levels[(target_level + 1).min(NUM_LSM_LEVELS)..] {
            let idx = lvl.partition_point(|sst| sst.meta().max_key.as_slice() < key);
            if idx < lvl.len() && key >= lvl[idx].meta().min_key.as_slice() {
                return true;
            }
        }
        false
    }

    pub fn all_metas(&self) -> Vec<SstFileMeta> {
        self.levels
            .iter()
            .flat_map(|l| l.iter().map(|r| r.meta().clone()))
            .collect()
    }

    pub fn count_physical_tombstones(&self) -> usize {
        self.levels
            .iter()
            .flat_map(|l| l.iter().map(|r| r.meta().num_tombstones as usize))
            .sum()
    }

    /// Returns sparse `(block_last_key, block_len)` samples from SSTable `IndexBlock`s
    /// without reading `DataBlock`s from disk (used by `PolicyShardSplitter`).
    pub fn index_key_samples(&self) -> Vec<(Vec<u8>, u64)> {
        let mut samples = Vec::new();
        for lvl in &self.levels {
            for sst in lvl {
                for ie in sst.index_entries() {
                    samples.push((ie.last_key.clone(), u64::from(ie.block_len)));
                }
            }
        }
        samples
    }
}
