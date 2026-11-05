use std::collections::{BTreeMap, VecDeque};
use std::sync::RwLock;

use metadb_core::{
    FileId, HlcTimestamp, InodeId, QuotaRecord, SecondaryIndexRecord, ShardId, UserId,
};
use metadb_storage::{LifecycleStorageEngine, LogSequenceNumber, RaftLogPayload};

/// Asynchronous update task for auxiliary metadata (`QuotaTable` and `SecondaryIndexTable`, §5.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuxUpdateTask {
    /// Per-user quota delta carrying `(origin_shard_id, lsn, hlc)` for exact-once application.
    QuotaDelta {
        origin_shard_id: ShardId,
        lsn: LogSequenceNumber,
        hlc: HlcTimestamp,
        uid: UserId,
        delta_bytes: i64,
        delta_inodes: i64,
    },
    /// Secondary index upsert (`FID -> (PID, name)`).
    IndexUpsert {
        origin_shard_id: ShardId,
        lsn: LogSequenceNumber,
        hlc: HlcTimestamp,
        fid: FileId,
        pid: InodeId,
        name: String,
        create_time: HlcTimestamp,
    },
    /// Secondary index deletion when a file is unlinked.
    IndexDelete {
        origin_shard_id: ShardId,
        lsn: LogSequenceNumber,
        hlc: HlcTimestamp,
        fid: FileId,
    },
}

impl AuxUpdateTask {
    pub fn lsn(&self) -> LogSequenceNumber {
        match self {
            AuxUpdateTask::QuotaDelta { lsn, .. }
            | AuxUpdateTask::IndexUpsert { lsn, .. }
            | AuxUpdateTask::IndexDelete { lsn, .. } => *lsn,
        }
    }
}

struct AuxStateInner {
    queue: VecDeque<AuxUpdateTask>,
    quota_table: BTreeMap<UserId, QuotaRecord>,
    secondary_index_table: BTreeMap<FileId, SecondaryIndexRecord>,
    /// Tracks highest deleted HLC per `FID` so a delayed/replayed older upsert never revives a deleted index row.
    deleted_fid_hlc: BTreeMap<FileId, HlcTimestamp>,
}

/// Decoupled Asynchronous Auxiliary Metadata Sink (§5.1 of `arXiv:2610.32247v1`).
///
/// Decouples `QuotaTable` and `SecondaryIndexTable` updates from the primary metadata
/// transaction critical path, eliminating cross-shard 2PC on file creation and deletion:
/// 1. Each task is first persisted in the local shard's Raft log before entering the in-memory queue.
/// 2. `QuotaDelta` deduplication tracks `applied_lsn_by_shard` per `(uid, origin_shard_id)`,
///    guaranteeing exact-once additive accounting across retries and Raft log replays (Goldfish Finding F3).
/// 3. `SecondaryIndexTable` tracks `last_applied_hlc` and `deleted_fid_hlc` so out-of-order
///    `rename` or replay events never overwrite newer locations.
pub struct AsyncAuxiliarySink {
    inner: RwLock<AuxStateInner>,
}

impl Default for AsyncAuxiliarySink {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncAuxiliarySink {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(AuxStateInner {
                queue: VecDeque::new(),
                quota_table: BTreeMap::new(),
                secondary_index_table: BTreeMap::new(),
                deleted_fid_hlc: BTreeMap::new(),
            }),
        }
    }

    /// Persists a quota update request in the local Raft log and enqueues it in memory.
    pub fn enqueue_quota_delta(
        &self,
        engine: &LifecycleStorageEngine,
        uid: UserId,
        delta_bytes: i64,
        delta_inodes: i64,
        hlc: HlcTimestamp,
    ) -> LogSequenceNumber {
        let origin_shard_id = engine.shard_id();
        let lsn = engine.raft_log().append(
            hlc,
            RaftLogPayload::AuxQuotaDelta {
                origin_shard_id,
                uid,
                delta_bytes,
                delta_inodes,
            },
        );
        let mut guard = self.inner.write().unwrap();
        guard.queue.push_back(AuxUpdateTask::QuotaDelta {
            origin_shard_id,
            lsn,
            hlc,
            uid,
            delta_bytes,
            delta_inodes,
        });
        lsn
    }

    /// Persists a secondary index upsert in the local Raft log and enqueues it in memory.
    pub fn enqueue_index_upsert(
        &self,
        engine: &LifecycleStorageEngine,
        fid: FileId,
        pid: InodeId,
        name: String,
        create_time: HlcTimestamp,
        hlc: HlcTimestamp,
    ) -> LogSequenceNumber {
        let origin_shard_id = engine.shard_id();
        let lsn = engine.raft_log().append(
            hlc,
            RaftLogPayload::AuxIndexUpsert {
                fid,
                pid,
                name: name.clone(),
                create_time,
            },
        );
        let mut guard = self.inner.write().unwrap();
        guard.queue.push_back(AuxUpdateTask::IndexUpsert {
            origin_shard_id,
            lsn,
            hlc,
            fid,
            pid,
            name,
            create_time,
        });
        lsn
    }

    /// Persists a secondary index deletion in the local Raft log and enqueues it in memory.
    pub fn enqueue_index_delete(
        &self,
        engine: &LifecycleStorageEngine,
        fid: FileId,
        hlc: HlcTimestamp,
    ) -> LogSequenceNumber {
        let origin_shard_id = engine.shard_id();
        let lsn = engine
            .raft_log()
            .append(hlc, RaftLogPayload::AuxIndexDelete { fid });
        let mut guard = self.inner.write().unwrap();
        guard.queue.push_back(AuxUpdateTask::IndexDelete {
            origin_shard_id,
            lsn,
            hlc,
            fid,
        });
        lsn
    }

    /// Drains all queued auxiliary tasks in batch, applying them idempotently to `QuotaTable`
    /// and `SecondaryIndexTable` and advancing `aux_sink_acked_lsn` on the given shard engines.
    pub fn drain_and_apply(&self, engines: &[&LifecycleStorageEngine]) -> usize {
        let tasks: Vec<AuxUpdateTask> = {
            let mut guard = self.inner.write().unwrap();
            guard.queue.drain(..).collect()
        };

        let count = tasks.len();
        let mut max_lsn_by_shard: BTreeMap<ShardId, LogSequenceNumber> = BTreeMap::new();

        for task in tasks {
            let (shard_id, lsn) = match &task {
                AuxUpdateTask::QuotaDelta {
                    origin_shard_id,
                    lsn,
                    ..
                }
                | AuxUpdateTask::IndexUpsert {
                    origin_shard_id,
                    lsn,
                    ..
                }
                | AuxUpdateTask::IndexDelete {
                    origin_shard_id,
                    lsn,
                    ..
                } => (*origin_shard_id, *lsn),
            };
            max_lsn_by_shard
                .entry(shard_id)
                .and_modify(|cur| *cur = (*cur).max(lsn))
                .or_insert(lsn);
            self.apply_task_idempotent(task);
        }

        for engine in engines {
            let target_lsn = max_lsn_by_shard
                .get(&engine.shard_id())
                .copied()
                .unwrap_or(0)
                .max(engine.raft_log().last_lsn());
            engine.raft_log().advance_aux_sink_acked_lsn(target_lsn);
        }
        count
    }

    /// Applies an `AuxUpdateTask` with exact-once idempotency guarantees.
    /// Returns `true` if applied, or `false` if suppressed as a duplicate/stale replay.
    pub fn apply_task_idempotent(&self, task: AuxUpdateTask) -> bool {
        let mut guard = self.inner.write().unwrap();
        match task {
            AuxUpdateTask::QuotaDelta {
                origin_shard_id,
                lsn,
                hlc,
                uid,
                delta_bytes,
                delta_inodes,
            } => {
                let record = guard
                    .quota_table
                    .entry(uid)
                    .or_insert_with(|| QuotaRecord::new(uid, u64::MAX));
                let prev_lsn = record
                    .applied_lsn_by_shard
                    .get(&origin_shard_id)
                    .copied()
                    .unwrap_or(0);
                if lsn <= prev_lsn {
                    return false; // Duplicate replay from Raft log recovery suppressed!
                }
                record.used_bytes = (record.used_bytes + delta_bytes).max(0);
                record.used_inodes = (record.used_inodes + delta_inodes).max(0);
                record.last_applied_hlc = record.last_applied_hlc.max(hlc);
                record.applied_lsn_by_shard.insert(origin_shard_id, lsn);
                true
            }
            AuxUpdateTask::IndexUpsert {
                hlc,
                fid,
                pid,
                name,
                create_time,
                ..
            } => {
                if let Some(&del_hlc) = guard.deleted_fid_hlc.get(&fid) {
                    if hlc <= del_hlc {
                        return false;
                    }
                }
                if let Some(existing) = guard.secondary_index_table.get(&fid) {
                    if hlc <= existing.last_applied_hlc {
                        return false;
                    }
                }
                guard.secondary_index_table.insert(
                    fid,
                    SecondaryIndexRecord {
                        fid,
                        pid,
                        name,
                        create_time,
                        last_applied_hlc: hlc,
                    },
                );
                true
            }
            AuxUpdateTask::IndexDelete { hlc, fid, .. } => {
                if let Some(&del_hlc) = guard.deleted_fid_hlc.get(&fid) {
                    if hlc <= del_hlc {
                        return false;
                    }
                }
                if let Some(existing) = guard.secondary_index_table.get(&fid) {
                    if hlc < existing.last_applied_hlc {
                        return false;
                    }
                }
                guard.secondary_index_table.remove(&fid);
                guard.deleted_fid_hlc.insert(fid, hlc);
                true
            }
        }
    }

    pub fn get_quota(&self, uid: UserId) -> Option<QuotaRecord> {
        self.inner.read().unwrap().quota_table.get(&uid).cloned()
    }

    pub fn get_secondary_index(&self, fid: FileId) -> Option<SecondaryIndexRecord> {
        self.inner
            .read()
            .unwrap()
            .secondary_index_table
            .get(&fid)
            .cloned()
    }

    pub fn pending_count(&self) -> usize {
        self.inner.read().unwrap().queue.len()
    }
}
