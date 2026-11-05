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

struct LocalityTrackerInner {
    tombstones_by_bucket: BTreeMap<RangeBucketPrefix, usize>,
    stats: CompactionStats,
}

/// Locality-Aware Compaction controller (§6.2 of `arXiv:2610.32247v1`).
///
/// Tracks consecutive bulk deletions (e.g., object TTL expirations or directory purges)
/// by spatial prefix bucket. When the number of physical LSM tombstones in a bucket
/// reaches `density_threshold`, MetaDB proactively compacts only that narrow key range
/// `[prefix, prefix + 1)` rather than waiting for passive full-shard compaction.
pub struct LocalityCompactor {
    density_threshold: usize,
    inner: RwLock<LocalityTrackerInner>,
}

impl LocalityCompactor {
    pub fn new(density_threshold: usize) -> Self {
        Self {
            density_threshold: density_threshold.max(1),
            inner: RwLock::new(LocalityTrackerInner {
                tombstones_by_bucket: BTreeMap::new(),
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

    /// Records a physical LSM tombstone written during a flush.
    /// Returns `Some((start_key, end_key))` if the bucket's tombstone density reached the threshold
    /// and a localized range compaction should be triggered immediately.
    pub fn record_tombstone(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        let bucket = Self::bucket_for_key(key)?;
        let mut guard = self.inner.write().unwrap();
        let count = guard.tombstones_by_bucket.entry(bucket).or_insert(0);
        *count += 1;
        if *count >= self.density_threshold {
            Some(Self::bucket_bounds(&bucket))
        } else {
            None
        }
    }

    /// Marks a bucket as compacted and records how many physical tombstones were purged.
    pub fn on_range_compacted(&self, start_key: &[u8], purged_count: usize) {
        let mut guard = self.inner.write().unwrap();
        if let Some(bucket) = Self::bucket_for_key(start_key) {
            guard.tombstones_by_bucket.remove(&bucket);
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
        // Increment the 9-byte big-endian prefix by 1 to form the exclusive upper bound.
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
