use std::collections::BTreeMap;
use std::sync::RwLock;

/// Statistics tracked by the Locality-Aware Compaction subsystem (§6.2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub localized_compactions_triggered: u64,
    pub tombstones_purged: u64,
}

/// Range bucket key (uses the 9-byte `[table_prefix][PID/prefix: 8B]` prefix to track spatial locality).
pub type RangeBucketPrefix = [u8; 9];

#[derive(Debug, Clone, Copy, Default)]
struct BucketDensity {
    tombstones: usize,
    live_writes: usize,
}

struct LocalityTrackerInner {
    buckets: BTreeMap<RangeBucketPrefix, BucketDensity>,
    stats: CompactionStats,
}

/// Locality-Aware Compaction controller (§6.2 of `arXiv:2610.32247v1` & `storage_lsm_mvcc.md` §8.2).
///
/// Tracks consecutive bulk deletions by 9-byte spatial prefix bucket (`[table_prefix][group_key: 8B]`).
/// Triggers targeted bottommost range compaction over `[prefix, prefix + 1)` when both:
/// 1. `tombstones >= density_threshold`, AND
/// 2. `tombstones / (tombstones + live_writes) >= min_tombstone_ratio` (default `0.50`).
pub struct LocalityCompactor {
    density_threshold: usize,
    min_ratio_permille: usize,
    inner: RwLock<LocalityTrackerInner>,
}

impl LocalityCompactor {
    pub fn new(density_threshold: usize) -> Self {
        Self::with_ratio(density_threshold, 500)
    }

    pub fn with_ratio(density_threshold: usize, min_ratio_permille: usize) -> Self {
        Self {
            density_threshold: density_threshold.max(1),
            min_ratio_permille: min_ratio_permille.clamp(1, 1000),
            inner: RwLock::new(LocalityTrackerInner {
                buckets: BTreeMap::new(),
                stats: CompactionStats::default(),
            }),
        }
    }

    pub fn density_threshold(&self) -> usize {
        self.density_threshold
    }

    /// Extracts the 9-byte `[table_prefix][group_key: 8B]` bucket prefix from a raw key.
    pub fn bucket_for_key(key: &[u8]) -> Option<RangeBucketPrefix> {
        if key.len() >= 9 {
            let mut prefix = [0u8; 9];
            prefix.copy_from_slice(&key[0..9]);
            Some(prefix)
        } else {
            None
        }
    }

    /// Records a live value flushed for `key`.
    pub fn record_live_write(&self, key: &[u8]) {
        let Some(bucket) = Self::bucket_for_key(key) else {
            return;
        };
        let mut guard = self.inner.write().unwrap();
        guard.buckets.entry(bucket).or_default().live_writes += 1;
    }

    /// Records a physical LSM tombstone written during a flush.
    /// Decrements estimated live keys in that spatial bucket so tombstone ratio reflects
    /// `tombstones / (tombstones + remaining_live_keys)`.
    /// Returns `Some((start_key, end_key))` when both density count and ratio thresholds are met.
    pub fn record_tombstone(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        let bucket = Self::bucket_for_key(key)?;
        let mut guard = self.inner.write().unwrap();
        let entry = guard.buckets.entry(bucket).or_default();
        entry.live_writes = entry.live_writes.saturating_sub(1);
        entry.tombstones += 1;
        let total = entry.tombstones + entry.live_writes;
        if entry.tombstones >= self.density_threshold
            && entry.tombstones * 1000 >= total * self.min_ratio_permille
        {
            Some(Self::bucket_bounds(&bucket))
        } else {
            None
        }
    }

    /// Captures the pre-compaction `(tombstones, live_writes)` snapshot for `start_key`'s bucket.
    pub fn snapshot_bucket_counts(&self, start_key: &[u8]) -> (usize, usize) {
        let Some(bucket) = Self::bucket_for_key(start_key) else {
            return (0, 0);
        };
        let guard = self.inner.read().unwrap();
        guard
            .buckets
            .get(&bucket)
            .map(|b| (b.tombstones, b.live_writes))
            .unwrap_or((0, 0))
    }

    /// Subtracts the pre-compaction `(compacted_tombstones, compacted_live)` counts strictly after
    /// `backend.compact_range` succeeds (`Ok`), preserving any concurrent writes/tombstones recorded
    /// during compaction as well as preserving full density if compaction errored.
    pub fn on_range_compacted(
        &self,
        start_key: &[u8],
        compacted_tombstones: usize,
        compacted_live: usize,
        purged_count: usize,
    ) {
        let mut guard = self.inner.write().unwrap();
        if let Some(bucket) = Self::bucket_for_key(start_key) {
            let should_remove = if let Some(entry) = guard.buckets.get_mut(&bucket) {
                entry.tombstones = entry.tombstones.saturating_sub(compacted_tombstones);
                entry.live_writes = entry.live_writes.saturating_sub(compacted_live);
                entry.tombstones == 0 && entry.live_writes == 0
            } else {
                false
            };
            if should_remove {
                guard.buckets.remove(&bucket);
            }
        }
        guard.stats.localized_compactions_triggered += 1;
        guard.stats.tombstones_purged += purged_count as u64;
    }

    pub fn stats(&self) -> CompactionStats {
        self.inner.read().unwrap().stats
    }

    fn bucket_bounds(bucket: &RangeBucketPrefix) -> (Vec<u8>, Vec<u8>) {
        let start = bucket.to_vec();
        let mut end = bucket.to_vec();
        for i in (0..9).rev() {
            if end[i] < 0xFF {
                end[i] += 1;
                break;
            } else {
                end[i] = 0x00;
            }
        }
        (start, end)
    }
}
