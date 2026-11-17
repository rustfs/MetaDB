//! Immutable `Version` views and deterministic `MergingIterator` across `L0..L3` SSTables.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

use metadb_core::Result;

use crate::block::BlockEntryValue;
use crate::manifest::DurableShardCheckpoint;
use crate::sstable::{SstCursor, SstFileMeta, SstableReader};

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

    /// Scans up to `limit` smallest merged distinct keys in `[start_key, end_key)` across `L0..L3`
    /// in $O(limit)$ block reads per SSTable.
    pub fn scan_range_page_merged(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<(BTreeMap<Vec<u8>, BlockEntryValue>, u64)> {
        if limit == 0 || start_key >= end_key {
            return Ok((BTreeMap::new(), 0));
        }
        let mut merged: BTreeMap<Vec<u8>, BlockEntryValue> = BTreeMap::new();
        let mut tombstones_seen = 0u64;

        for sst in &self.levels[0] {
            for (k, v) in sst.scan_range_limit(start_key, end_key, limit)? {
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
                let remaining = limit;
                for (k, v) in sst.scan_range_limit(start_key, end_key, remaining)? {
                    if matches!(v, BlockEntryValue::PhysicalTombstone) {
                        tombstones_seen += 1;
                    }
                    merged.entry(k).or_insert(v);
                }
                if sst.meta().max_key.as_slice() >= end_key && merged.len() >= limit {
                    // Any subsequent non-overlapping SSTable in this level has min_key > max_key >= end_key.
                    break;
                }
            }
        }

        while merged.len() > limit {
            merged.pop_last();
        }
        Ok((merged, tombstones_seen))
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

    /// Streaming merge of the newest value of each key in `[start, end)`.
    /// `end == None` reads through the last key of every selected SSTable.
    pub(crate) fn merged_cursor(&self, start: &[u8], end: Option<&[u8]>) -> Result<MergedCursor> {
        let mut readers = Vec::new();
        for sst in &self.levels[0] {
            readers.push(sst.clone());
        }
        for level in &self.levels[1..] {
            let first = if start.is_empty() {
                0
            } else {
                level.partition_point(|sst| sst.meta().max_key.as_slice() < start)
            };
            for sst in &level[first..] {
                if end.is_some_and(|bound| sst.meta().min_key.as_slice() >= bound) {
                    break;
                }
                readers.push(sst.clone());
            }
        }
        MergedCursor::open(readers, start, end)
    }
}

/// K-way merge. Earlier readers win on equal keys (`L0` newest-first, then `L1..L3`).
pub(crate) struct MergedCursor {
    sources: Vec<MergeSource>,
    heap: BinaryHeap<HeapNode>,
    staged: Option<StagedEntry>,
    exhausted: bool,
}

struct MergeSource {
    cursor: SstCursor,
    precedence: u32,
    generation: u64,
    needs_reheap: bool,
}

struct StagedEntry {
    key: Vec<u8>,
    value: BlockEntryValue,
    source_ids: Vec<u32>,
}

#[derive(Clone, Eq, PartialEq)]
struct HeapNode {
    key: Vec<u8>,
    precedence: u32,
    cursor_id: u32,
    generation: u64,
}

impl PartialOrd for HeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` pops the greatest node. Invert key order so the smallest key wins.
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.precedence.cmp(&self.precedence))
            .then_with(|| self.cursor_id.cmp(&other.cursor_id))
            .then_with(|| self.generation.cmp(&other.generation))
    }
}

impl MergedCursor {
    pub(crate) fn open(
        readers: Vec<Arc<SstableReader>>,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self> {
        let mut sources = Vec::with_capacity(readers.len());
        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.into_iter().enumerate() {
            let mut cursor = SstCursor::open(reader, start, end)?;
            let cursor_id = index as u32;
            if let Some((key, _)) = cursor.peek()? {
                heap.push(HeapNode {
                    key,
                    precedence: cursor_id,
                    cursor_id,
                    generation: 0,
                });
            }
            sources.push(MergeSource {
                cursor,
                precedence: cursor_id,
                generation: 0,
                needs_reheap: false,
            });
        }
        Ok(Self {
            sources,
            heap,
            staged: None,
            exhausted: false,
        })
    }

    pub(crate) fn peek(&mut self) -> Result<Option<(Vec<u8>, BlockEntryValue)>> {
        self.ensure_staged()?;
        Ok(self
            .staged
            .as_ref()
            .map(|entry| (entry.key.clone(), entry.value.clone())))
    }

    /// Drops the current winner. The successor is not decoded until the next peek.
    pub(crate) fn advance(&mut self) -> Result<()> {
        self.ensure_staged()?;
        let Some(staged) = self.staged.take() else {
            return Ok(());
        };
        for source_id in staged.source_ids {
            let source = &mut self.sources[source_id as usize];
            source.cursor.advance();
            source.generation = source.generation.wrapping_add(1);
            source.needs_reheap = true;
        }
        Ok(())
    }

    pub(crate) fn next_owned(&mut self) -> Result<Option<(Vec<u8>, BlockEntryValue)>> {
        let Some((key, value)) = self.peek()? else {
            return Ok(None);
        };
        self.advance()?;
        Ok(Some((key, value)))
    }

    fn ensure_staged(&mut self) -> Result<()> {
        if self.staged.is_some() || self.exhausted {
            return Ok(());
        }
        self.reheap_pending()?;
        loop {
            let Some(node) = self.heap.pop() else {
                self.exhausted = true;
                return Ok(());
            };
            let cursor_id = node.cursor_id as usize;
            if self.sources[cursor_id].generation != node.generation {
                continue;
            }
            let Some((key, value)) = self.sources[cursor_id].cursor.peek()? else {
                continue;
            };
            if key != node.key {
                continue;
            }
            let mut best_precedence = node.precedence;
            let mut best_value = value;
            let mut source_ids = vec![node.cursor_id];
            while self.heap.peek().is_some_and(|next| next.key == node.key) {
                let other = self.heap.pop().unwrap();
                let other_index = other.cursor_id as usize;
                if self.sources[other_index].generation != other.generation {
                    continue;
                }
                let Some((other_key, other_value)) = self.sources[other_index].cursor.peek()?
                else {
                    continue;
                };
                if other_key != node.key {
                    continue;
                }
                source_ids.push(other.cursor_id);
                if other.precedence < best_precedence {
                    best_precedence = other.precedence;
                    best_value = other_value;
                }
            }
            self.staged = Some(StagedEntry {
                key: node.key,
                value: best_value,
                source_ids,
            });
            return Ok(());
        }
    }

    fn reheap_pending(&mut self) -> Result<()> {
        for cursor_id in 0..self.sources.len() {
            if !self.sources[cursor_id].needs_reheap {
                continue;
            }
            self.sources[cursor_id].needs_reheap = false;
            let generation = self.sources[cursor_id].generation;
            let precedence = self.sources[cursor_id].precedence;
            let Some((key, _)) = self.sources[cursor_id].cursor.peek()? else {
                continue;
            };
            self.heap.push(HeapNode {
                key,
                precedence,
                cursor_id: cursor_id as u32,
                generation,
            });
        }
        Ok(())
    }
}
