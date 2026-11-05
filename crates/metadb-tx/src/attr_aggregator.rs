use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use metadb_core::{HlcTimestamp, InodeAttr, InodeId, MetaKey, Result};
use metadb_storage::{LifecycleStorageEngine, LogSequenceNumber, RaftLogPayload};

/// A single queued parent-directory attribute modification (§5.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttrDeltaItem {
    pub pid: InodeId,
    pub delta_children: i64,
    pub delta_bytes: i64,
    pub modify_time: HlcTimestamp,
    /// Globally monotonic sequence number across shard splits for atomic reconciliation handoff.
    pub seq: u64,
    pub lsn: LogSequenceNumber,
}

/// Conflict-Free Asynchronous Attribute Aggregator (§5.2 of `arXiv:2610.32247v1`).
///
/// Instead of synchronously locking `(PID, "_attr_")` on every file creation or deletion,
/// MetaDB appends an `AttrDelta` to the Raft log and enqueues it in memory:
/// - `num_children` and `size_bytes` are aggregated by summing deltas.
/// - `modify_time` is aggregated by taking the maximum committed `HlcTimestamp`.
/// - **Read-Path Reconciliation**: `reconcile_attr()` folds all pending deltas with
///   `delta.seq > persisted_attr.applied_lsn` into the persisted `_attr_` record so clients
///   always observe linearizable, up-to-date directory attributes without write lock contention.
pub struct AsyncAttrAggregator {
    next_seq: AtomicU64,
    pending_by_pid: RwLock<BTreeMap<InodeId, VecDeque<AttrDeltaItem>>>,
}

impl Default for AsyncAttrAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncAttrAggregator {
    pub fn new() -> Self {
        Self {
            next_seq: AtomicU64::new(1),
            pending_by_pid: RwLock::new(BTreeMap::new()),
        }
    }

    /// Persists an attribute delta to the shard's Raft log and enqueues it in memory
    /// without locking the parent directory's `_attr_` row in `MetaTable`.
    pub fn record_delta(
        &self,
        engine: &LifecycleStorageEngine,
        pid: InodeId,
        delta_children: i64,
        delta_bytes: i64,
        modify_time: HlcTimestamp,
    ) -> LogSequenceNumber {
        let mut guard = self.pending_by_pid.write().unwrap();
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        let lsn = engine.raft_log().append(
            modify_time,
            RaftLogPayload::AttrDelta {
                pid,
                delta_children,
                delta_bytes,
                modify_time,
            },
        );
        guard.entry(pid).or_default().push_back(AttrDeltaItem {
            pid,
            delta_children,
            delta_bytes,
            modify_time,
            seq,
            lsn,
        });
        lsn
    }

    /// Reconciles a persisted `InodeAttr` (`_attr_`) with all un-flushed in-memory deltas
    /// whose `seq > persisted.applied_lsn` (§5.2 Read-Path Reconciliation).
    pub fn reconcile_attr(&self, pid: InodeId, mut persisted: InodeAttr) -> InodeAttr {
        let guard = self.pending_by_pid.read().unwrap();
        if let Some(queue) = guard.get(&pid) {
            for item in queue.iter() {
                if item.seq > persisted.applied_lsn {
                    persisted.num_children = (persisted.num_children + item.delta_children).max(0);
                    persisted.size_bytes = (persisted.size_bytes + item.delta_bytes).max(0);
                    if item.modify_time > persisted.modify_time {
                        persisted.modify_time = item.modify_time;
                    }
                    persisted.applied_lsn = item.seq;
                }
            }
        }
        persisted
    }

    /// Folds all pending deltas in memory and writes batched `_attr_` updates into
    /// the storage engine, advancing `attr_aggregated_lsn` on the Raft log.
    ///
    /// Acquires the write lock across the fold + engine write so concurrent readers
    /// calling `reconcile_attr()` never undercount or double-count deltas (Goldfish Finding F1).
    pub fn flush_aggregated_deltas(
        &self,
        engine: &LifecycleStorageEngine,
        flush_hlc: HlcTimestamp,
    ) -> Result<usize> {
        let mut guard = self.pending_by_pid.write().unwrap();
        let mut flushed_dirs = 0usize;

        let pids: Vec<InodeId> = guard.keys().copied().collect();
        for pid in pids {
            let Some(queue) = guard.get_mut(&pid) else {
                continue;
            };
            if queue.is_empty() {
                continue;
            }

            let attr_key = MetaKey::dir_attr(pid).encode();
            if let Some(raw_bytes) = engine.get(&attr_key, flush_hlc)? {
                let mut attr = InodeAttr::decode(&raw_bytes)?;
                for item in queue.iter() {
                    if item.seq > attr.applied_lsn {
                        attr.num_children = (attr.num_children + item.delta_children).max(0);
                        attr.size_bytes = (attr.size_bytes + item.delta_bytes).max(0);
                        if item.modify_time > attr.modify_time {
                            attr.modify_time = item.modify_time;
                        }
                        attr.applied_lsn = item.seq;
                    }
                }
                engine.put(attr_key, attr.encode(), flush_hlc);
                flushed_dirs += 1;
                queue.clear();
            }
        }

        guard.retain(|_, q| !q.is_empty());
        if guard.is_empty() {
            engine
                .raft_log()
                .advance_attr_aggregated_lsn(engine.raft_log().last_lsn());
        }
        Ok(flushed_dirs)
    }

    /// Returns the total number of un-flushed attribute delta items across all directories.
    pub fn pending_count(&self) -> usize {
        let guard = self.pending_by_pid.read().unwrap();
        guard.values().map(|q| q.len()).sum()
    }
}
