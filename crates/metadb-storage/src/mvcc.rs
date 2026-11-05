use std::collections::{BTreeMap, VecDeque};
use std::sync::RwLock;

use metadb_core::{HlcTimestamp, MetaDbError, Result};

use crate::raft_log::LogSequenceNumber;

/// Default transaction timeout and in-memory MVCC retention window (`5,000 ms`, §6.1).
pub const DEFAULT_MVCC_RETENTION_MS: u64 = 5_000;

/// Value state of an in-memory MVCC version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccValue {
    /// Live byte value.
    Value(Vec<u8>),
    /// Logical MVCC tombstone (kept strictly in memory; never persisted as an MVCC version).
    Tombstone,
}

/// A single timestamped version in memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccVersion {
    pub hlc: HlcTimestamp,
    pub lsn: LogSequenceNumber,
    pub value: MvccValue,
}

/// Output produced when flushing `MemMvccStore` to the persistent LSM store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushedKeyState {
    pub key: Vec<u8>,
    pub lsn: LogSequenceNumber,
    /// `Some(bytes)` writes a `Put` to the persistent KV store;
    /// `None` writes a single physical LSM tombstone (eliminating the MVCC+LSM double tombstone).
    pub latest_value: Option<Vec<u8>>,
    /// Number of stale in-memory MVCC versions/tombstones pruned without hitting disk.
    pub pruned_versions: usize,
}

struct MemMvccInner {
    versions: BTreeMap<Vec<u8>, VecDeque<MvccVersion>>,
    latest_hlc: HlcTimestamp,
    last_flushed_lsn: LogSequenceNumber,
    total_mvcc_writes: u64,
    total_pruned_versions: u64,
}

/// In-Memory MVCC layer (§6.1 of `arXiv:2610.32247v1`).
///
/// Keeps multiple committed versions and MVCC tombstones strictly in memory for the short
/// metadata transaction lifetime (`5s`). When flushing to persistent storage, stale versions
/// and MVCC tombstones are pruned in memory so only the latest committed state (or a single
/// physical LSM tombstone) is written to disk.
pub struct MemMvccStore {
    retention_ms: u64,
    inner: RwLock<MemMvccInner>,
}

impl MemMvccStore {
    pub fn new(retention_ms: u64) -> Self {
        Self {
            retention_ms,
            inner: RwLock::new(MemMvccInner {
                versions: BTreeMap::new(),
                latest_hlc: HlcTimestamp::ZERO,
                last_flushed_lsn: 0,
                total_mvcc_writes: 0,
                total_pruned_versions: 0,
            }),
        }
    }

    /// Inserts a new committed version into the in-memory MVCC chain.
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>, hlc: HlcTimestamp, lsn: LogSequenceNumber) {
        let mut guard = self.inner.write().unwrap();
        guard.latest_hlc = guard.latest_hlc.max(hlc);
        guard.total_mvcc_writes += 1;
        let chain = guard.versions.entry(key).or_default();
        chain.push_back(MvccVersion {
            hlc,
            lsn,
            value: MvccValue::Value(value),
        });
    }

    /// Inserts a logical MVCC tombstone into the in-memory MVCC chain.
    pub fn delete(&self, key: Vec<u8>, hlc: HlcTimestamp, lsn: LogSequenceNumber) {
        let mut guard = self.inner.write().unwrap();
        guard.latest_hlc = guard.latest_hlc.max(hlc);
        guard.total_mvcc_writes += 1;
        let chain = guard.versions.entry(key).or_default();
        chain.push_back(MvccVersion {
            hlc,
            lsn,
            value: MvccValue::Tombstone,
        });
    }

    /// Reads the version of `key` visible at `read_hlc` from memory, if present.
    ///
    /// Returns:
    /// - `Ok(Some(MvccValue))` if a visible version is present in memory.
    /// - `Ok(None)` if the key has no in-memory version at or before `read_hlc` (caller should fall back to persistent store).
    /// - `Err(MetaDbError::TransactionTimeout)` if `read_hlc` is older than the retention window.
    pub fn get_at(&self, key: &[u8], read_hlc: HlcTimestamp) -> Result<Option<MvccValue>> {
        let guard = self.inner.read().unwrap();
        self.check_retention_window(&guard, read_hlc)?;

        if let Some(chain) = guard.versions.get(key) {
            for version in chain.iter().rev() {
                if version.hlc <= read_hlc {
                    return Ok(Some(version.value.clone()));
                }
            }
        }
        Ok(None)
    }

    /// Collects all in-memory keys in `[start_key, end_key)` and their visible `MvccValue` at `read_hlc`.
    pub fn range_at(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        read_hlc: HlcTimestamp,
    ) -> Result<BTreeMap<Vec<u8>, MvccValue>> {
        let guard = self.inner.read().unwrap();
        self.check_retention_window(&guard, read_hlc)?;

        let mut out = BTreeMap::new();
        for (k, chain) in guard
            .versions
            .range::<[u8], _>((std::ops::Bound::Included(start_key), std::ops::Bound::Excluded(end_key)))
        {
            for version in chain.iter().rev() {
                if version.hlc <= read_hlc {
                    out.insert(k.clone(), version.value.clone());
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Flushes dirty keys up to `current_hlc` to the persistent store while retaining
    /// in-memory versions younger than the `retention_ms` window so active snapshots
    /// within the 5-second window continue to see consistent point-in-time state.
    ///
    /// Crucially, only the latest committed state per key is emitted for disk persistence,
    /// pruning intermediate versions and MVCC tombstones in memory (§6.1).
    pub fn prepare_flush(&self) -> (Vec<FlushedKeyState>, LogSequenceNumber) {
        let mut guard = self.inner.write().unwrap();
        let cutoff_phys_ms = guard
            .latest_hlc
            .physical_ms()
            .saturating_sub(self.retention_ms);
        let cutoff_hlc = HlcTimestamp::new(cutoff_phys_ms, 0);

        let mut flushed = Vec::new();
        let mut max_flushed_lsn = guard.last_flushed_lsn;

        let keys: Vec<Vec<u8>> = guard.versions.keys().cloned().collect();
        for key in keys {
            let mut pruned_for_key = 0usize;
            let mut emit_state: Option<FlushedKeyState> = None;
            let mut remove_entry = false;

            if let Some(chain) = guard.versions.get_mut(&key) {
                if let Some(latest) = chain.back().cloned() {
                    if latest.lsn > max_flushed_lsn {
                        max_flushed_lsn = latest.lsn;
                    }
                    // Count all older versions and the MVCC tombstone itself as pruned from disk writes.
                    pruned_for_key = chain.len().saturating_sub(1);
                    let latest_value = match &latest.value {
                        MvccValue::Value(v) => Some(v.clone()),
                        MvccValue::Tombstone => {
                            pruned_for_key += 1; // MVCC tombstone pruned; only single physical LSM tombstone emitted
                            None
                        }
                    };
                    emit_state = Some(FlushedKeyState {
                        key: key.clone(),
                        lsn: latest.lsn,
                        latest_value,
                        pruned_versions: pruned_for_key,
                    });
                }

                // Prune versions older than the 5-second active transaction window,
                // keeping at most one baseline version <= cutoff_hlc if needed by active snapshots.
                while chain.len() > 1 {
                    if chain[0].hlc < cutoff_hlc && chain[1].hlc <= cutoff_hlc {
                        chain.pop_front();
                    } else {
                        break;
                    }
                }
                if chain.len() == 1 && chain[0].hlc <= cutoff_hlc {
                    // Once flushed to persistent KV and older than the 5s snapshot window,
                    // the in-memory chain can be dropped completely.
                    chain.pop_front();
                }
                remove_entry = chain.is_empty();
            }

            if remove_entry {
                guard.versions.remove(&key);
            }
            if let Some(state) = emit_state {
                guard.total_pruned_versions += pruned_for_key as u64;
                flushed.push(state);
            }
        }

        guard.last_flushed_lsn = max_flushed_lsn;
        (flushed, max_flushed_lsn)
    }

    pub fn stats(&self) -> (u64, u64) {
        let guard = self.inner.read().unwrap();
        (guard.total_mvcc_writes, guard.total_pruned_versions)
    }

    fn check_retention_window(&self, guard: &MemMvccInner, read_hlc: HlcTimestamp) -> Result<()> {
        let latest_phys = guard.latest_hlc.physical_ms();
        if latest_phys > self.retention_ms {
            let oldest_phys = latest_phys - self.retention_ms;
            if read_hlc.physical_ms() < oldest_phys {
                return Err(MetaDbError::TransactionTimeout {
                    snapshot_hlc: read_hlc.as_u64(),
                    oldest_retained_hlc: HlcTimestamp::new(oldest_phys, 0).as_u64(),
                });
            }
        }
        Ok(())
    }
}
