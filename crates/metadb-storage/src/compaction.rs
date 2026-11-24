use std::collections::BTreeMap;
use std::sync::RwLock;

/// Statistics tracked by the Locality-Aware & Leveled Compaction subsystem (§6.2 & `storage_lsm_mvcc.md` §8).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionStats {
    pub localized_compactions_triggered: u64,
    pub tombstones_purged: u64,
}

/// Default leveled compaction byte budgets (`L1` = `512 MiB`, `L2` = `4 GiB`, `L3` = bottommost).
pub const DEFAULT_L1_MAX_BYTES: u64 = 512 * 1024 * 1024;
pub const DEFAULT_L2_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Leveled Compaction planner (`storage_lsm_mvcc.md` §8.1) promoting `L1 -> L2` and `L2 -> L3`
/// when a level's cumulative SSTable byte footprint exceeds its configured target budget.
#[derive(Debug, Clone, Copy)]
pub struct LeveledCompactor {
    pub l1_max_bytes: u64,
    pub l2_max_bytes: u64,
}

impl Default for LeveledCompactor {
    fn default() -> Self {
        Self {
            l1_max_bytes: DEFAULT_L1_MAX_BYTES,
            l2_max_bytes: DEFAULT_L2_MAX_BYTES,
        }
    }
}

impl LeveledCompactor {
    pub const fn new(l1_max_bytes: u64, l2_max_bytes: u64) -> Self {
        Self {
            l1_max_bytes: if l1_max_bytes < 4096 {
                4096
            } else {
                l1_max_bytes
            },
            l2_max_bytes: if l2_max_bytes < 8192 {
                8192
            } else {
                l2_max_bytes
            },
        }
    }

    pub const fn max_bytes_for_level(&self, level: usize) -> Option<u64> {
        match level {
            1 => Some(self.l1_max_bytes),
            2 => Some(self.l2_max_bytes),
            _ => None,
        }
    }
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

    #[inline]
    fn meets_threshold(&self, entry: &BucketDensity) -> bool {
        if entry.tombstones < self.density_threshold {
            return false;
        }
        let total = (entry.tombstones as u128) + (entry.live_writes as u128);
        (entry.tombstones as u128) * 1000 >= total * (self.min_ratio_permille as u128)
    }

    /// Replaces in-memory bucket counters. Used when opening an LSM so compaction
    /// decisions follow the tombstones and live keys already on disk.
    pub fn restore_counts(&self, counts: BTreeMap<RangeBucketPrefix, (usize, usize)>) {
        let mut guard = self.inner.write().unwrap();
        guard.buckets.clear();
        for (prefix, (tombstones, live_writes)) in counts {
            if tombstones == 0 && live_writes == 0 {
                continue;
            }
            guard.buckets.insert(
                prefix,
                BucketDensity {
                    tombstones,
                    live_writes,
                },
            );
        }
    }

    /// Returns `[start_key, end_key)` bounds for all buckets currently exceeding the density threshold.
    pub fn over_threshold_ranges(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let guard = self.inner.read().unwrap();
        guard
            .buckets
            .iter()
            .filter(|(_, entry)| self.meets_threshold(entry))
            .map(|(bucket, _)| Self::bucket_bounds(bucket))
            .collect()
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
        let entry = guard.buckets.entry(bucket).or_default();
        entry.live_writes = entry.live_writes.saturating_add(1);
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
        entry.tombstones = entry.tombstones.saturating_add(1);
        if self.meets_threshold(entry) {
            Some(Self::bucket_bounds(&bucket))
        } else {
            None
        }
    }

    /// Captures the pre-compaction `tombstones` snapshot for all buckets overlapping `[start_key, end_key)`.
    pub fn snapshot_range_tombstones(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<(RangeBucketPrefix, usize)> {
        let guard = self.inner.read().unwrap();
        guard
            .buckets
            .iter()
            .filter(|(bucket, entry)| {
                entry.tombstones > 0 && bucket_overlaps_range(bucket, start_key, end_key)
            })
            .map(|(&bucket, entry)| (bucket, entry.tombstones))
            .collect()
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

    /// Updates bucket tombstone counts strictly after `backend.compact_range` succeeds (`Ok`),
    /// preserving surviving `live_writes` and any concurrent writes/tombstones recorded during compaction.
    pub fn on_range_compacted_buckets(
        &self,
        snap_tombstones: &[(RangeBucketPrefix, usize)],
        purged_count: usize,
    ) {
        let mut guard = self.inner.write().unwrap();
        for &(bucket, compacted_tombstones) in snap_tombstones {
            let should_remove = if let Some(entry) = guard.buckets.get_mut(&bucket) {
                entry.tombstones = entry.tombstones.saturating_sub(compacted_tombstones);
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

    /// Legacy single-bucket notification wrapper preserving surviving `live_writes`.
    pub fn on_range_compacted(
        &self,
        start_key: &[u8],
        compacted_tombstones: usize,
        _compacted_live: usize,
        purged_count: usize,
    ) {
        let snap = Self::bucket_for_key(start_key)
            .map(|b| vec![(b, compacted_tombstones)])
            .unwrap_or_default();
        self.on_range_compacted_buckets(&snap, purged_count);
    }

    pub fn stats(&self) -> CompactionStats {
        self.inner.read().unwrap().stats
    }

    pub fn bucket_bounds(bucket: &RangeBucketPrefix) -> (Vec<u8>, Vec<u8>) {
        let start = bucket.to_vec();
        let mut end = bucket.to_vec();
        let mut incremented = false;
        for i in (0..9).rev() {
            if end[i] < 0xFF {
                end[i] += 1;
                incremented = true;
                break;
            } else {
                end[i] = 0x00;
            }
        }
        if !incremented {
            // `[0xFF; 9]` has no 9-byte lexicographical successor; `[0xFF; 9]` keys of length <= 9
            // are bounded by `[0xFF; 65560]` so `start < end` always holds without wrapping to `[0; 9]`.
            end = vec![0xFFu8; 65_560];
        }
        (start, end)
    }
}

fn bucket_overlaps_range(bucket: &RangeBucketPrefix, start_key: &[u8], end_key: &[u8]) -> bool {
    let (b_start, b_end) = LocalityCompactor::bucket_bounds(bucket);
    start_key < b_end.as_slice() && end_key > b_start.as_slice()
}
