//! Deterministic Raft State Machine implementation (`TxStateMachineHandler`) executing:
//! - `1-RTT` single-shard pushdown (`CreateFile`, `Unlink`, `SetAttr`, colocated `Mkdir`/`Rmdir`/`Rename`)
//! - Exact-once contiguous-frontier `QuotaTable` & total-order `SecondaryIndexTable` application
//! - Primary-Anchored 2PC state transitions (`[0x05] WriteIntent` & `[0x06] TxnRecord`)
//! - `PreFlushHook` Coordinated Flush Barrier (`ReplicatedAttrAggregator` + `ShardAuxOutbox` `[0x07]` spill)

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use metadb_core::schema::validate_user_entry_name;
use metadb_core::{
    AttrDeltaSpec, AuxIntentSpec, AuxOutboxEntry, AuxTaskPayload, CompositeKeyRange,
    DirShardDeltaKey, DirShardDeltaRecord, EntryType, FileId, HlcTimestamp, InodeAttr, InodeId,
    IntentOp, MetaDbError, MetaEntry, MetaKey, QuotaKey, QuotaRecord, Result, SecondaryIndexKey,
    SecondaryIndexRecord, ShardEpoch, ShardId, TxnId, TxnRecordKey, TxnState, TxnStatusRecord,
    UserId, WriteIntentKey, WriteIntentRecord,
};
use metadb_raft::{MetaDbRaftRequest, ShardApplyHandler, ShardWatermarkTracker};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, MvccValue, PreFlushHook, ViewAtRaftIndex,
};

use crate::aux_outbox::{ReplicatedAttrAggregator, ShardAuxOutbox, DEFAULT_OUTBOX_SPILL_THRESHOLD};

/// Partial attribute mutation specification for `SetAttr`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetAttrPatch {
    pub mode: Option<u32>,
    pub uid: Option<UserId>,
    pub size_bytes: Option<i64>,
    pub fid: Option<FileId>,
    pub modify_time: Option<HlcTimestamp>,
}

/// Deterministic command replicated through a shard's OpenRaft log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxRaftCmd {
    InitDirAttr {
        dir_id: InodeId,
        parent_pid: InodeId,
        uid: UserId,
        mode: u32,
    },
    InitDirShardDelta {
        record: DirShardDeltaRecord,
    },
    CreateFile {
        pid: InodeId,
        name: String,
        inode_id: InodeId,
        fid: FileId,
        uid: UserId,
        mode: u32,
        size_bytes: i64,
    },
    MkdirSingleShard {
        pid: InodeId,
        name: String,
        new_dir_id: InodeId,
        uid: UserId,
        mode: u32,
    },
    Unlink {
        pid: InodeId,
        name: String,
    },
    RmdirSingleShard {
        pid: InodeId,
        name: String,
    },
    SetAttrSingleShard {
        pid: InodeId,
        name: String,
        patch: SetAttrPatch,
    },
    RenameSingleShard {
        src_pid: InodeId,
        src_name: String,
        dst_pid: InodeId,
        dst_name: String,
        overwrite: bool,
    },
    SetQuotaLimit {
        uid: UserId,
        quota_limit_bytes: u64,
    },
    ApplyAuxBatch {
        entries: Vec<AuxOutboxEntry>,
    },
    AckAuxTasks {
        acked_keys: Vec<(u64, u16)>,
    },
    Prepare2pc {
        txn_id: TxnId,
        is_primary: bool,
        primary_key: Vec<u8>,
        participant_keys: Vec<Vec<u8>>,
        start_hlc: HlcTimestamp,
        expire_hlc: HlcTimestamp,
        rename_fence_token: Option<u64>,
        intents: Vec<(Vec<u8>, IntentOp)>,
    },
    CommitPrimary2pc {
        txn_id: TxnId,
        commit_hlc: HlcTimestamp,
        rename_fence_token: Option<u64>,
    },
    CommitSecondary2pc {
        txn_id: TxnId,
        commit_hlc: HlcTimestamp,
        target_user_keys: Vec<Vec<u8>>,
    },
    Abort2pc {
        txn_id: TxnId,
        target_user_keys: Vec<Vec<u8>>,
    },
    MarkParticipantAcked {
        txn_id: TxnId,
        acked_user_keys: Vec<Vec<u8>>,
    },
    CheckAndAbortExpiredTxn {
        txn_id: TxnId,
        caller_hlc: HlcTimestamp,
    },
    UpdateShardRangeOnSplit {
        new_range: CompositeKeyRange,
        new_epoch: ShardEpoch,
        keys_to_delete: Vec<Vec<u8>>,
        updated_anchor_attr: Option<(InodeId, InodeAttr)>,
        updated_shard_delta: Option<DirShardDeltaRecord>,
    },
}

/// Typed output serialized inside `MetaDbRaftResponse.result`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxRaftOutput {
    Unit,
    Entry(MetaEntry),
    Attr(InodeAttr),
    Quota(QuotaRecord),
    AppliedCount(usize),
    AckedWatermark(u64),
    Prepared {
        prepared_hlc: HlcTimestamp,
    },
    Committed {
        commit_hlc: HlcTimestamp,
        already_applied: bool,
    },
    TxnStatus(Option<TxnState>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedShardRouteMeta {
    epoch: ShardEpoch,
    range: CompositeKeyRange,
}

/// Stateful per-shard transaction handler implementing both `ShardApplyHandler` and `PreFlushHook`.
pub struct TxStateMachineHandler {
    shard_id: ShardId,
    meta: RwLock<PersistedShardRouteMeta>,
    attr_agg: Arc<ReplicatedAttrAggregator>,
    outbox: Arc<ShardAuxOutbox>,
}

impl TxStateMachineHandler {
    pub fn new(
        shard_id: ShardId,
        initial_epoch: ShardEpoch,
        initial_range: CompositeKeyRange,
        watermarks: Arc<ShardWatermarkTracker>,
    ) -> Self {
        Self::with_spill_threshold(
            shard_id,
            initial_epoch,
            initial_range,
            DEFAULT_OUTBOX_SPILL_THRESHOLD,
            watermarks,
        )
    }

    pub fn with_spill_threshold(
        shard_id: ShardId,
        initial_epoch: ShardEpoch,
        initial_range: CompositeKeyRange,
        spill_threshold: usize,
        watermarks: Arc<ShardWatermarkTracker>,
    ) -> Self {
        Self {
            shard_id,
            meta: RwLock::new(PersistedShardRouteMeta {
                epoch: initial_epoch,
                range: initial_range,
            }),
            attr_agg: Arc::new(ReplicatedAttrAggregator::new()),
            outbox: Arc::new(ShardAuxOutbox::new(shard_id, spill_threshold, watermarks)),
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn key_range(&self) -> CompositeKeyRange {
        self.meta.read().unwrap().range.clone()
    }

    pub fn attr_aggregator(&self) -> &Arc<ReplicatedAttrAggregator> {
        &self.attr_agg
    }

    pub fn outbox(&self) -> &Arc<ShardAuxOutbox> {
        &self.outbox
    }

    /// Reads the latest committed bytes of `raw_key` inside deterministic `apply`.
    fn read_latest_in_apply(
        engine: &LifecycleStorageEngine,
        raw_key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let (_, latest_hlc, _, _) = engine.mvcc().latest_watermarks();
        engine.get(raw_key, latest_hlc)
    }

    fn scan_latest_in_apply(
        engine: &LifecycleStorageEngine,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (_, latest_hlc, _, _) = engine.mvcc().latest_watermarks();
        engine.scan_range(start_key, end_key, latest_hlc)
    }

    /// Checks that `user_key` does not carry a conflicting exclusive `[0x05] WriteIntent`.
    fn ensure_no_conflicting_intent(
        engine: &LifecycleStorageEngine,
        user_key: &[u8],
        allow_shared_dir_fence: bool,
    ) -> Result<()> {
        let intent_key = WriteIntentKey::new(user_key.to_vec()).encode();
        if let Some(raw) = Self::read_latest_in_apply(engine, &intent_key)? {
            let rec = WriteIntentRecord::decode(&raw)?;
            if allow_shared_dir_fence && matches!(rec.op, IntentOp::DirExistsFenceSet { .. }) {
                return Ok(());
            }
            return Err(MetaDbError::WriteIntentConflict {
                conflicting_txn_id: rec.txn_id,
                primary_key: rec.primary_key,
                expire_hlc: rec.expire_hlc.0,
            });
        }
        Ok(())
    }

    /// Verifies directory `pid` exists on this shard (either via `(pid, "_attr_")` or `[0x04][pid]`)
    /// and is NOT locked by a `DeletingDir` intent (`Rmdir` in progress).
    fn verify_parent_dir_ready(&self, engine: &LifecycleStorageEngine, pid: InodeId) -> Result<()> {
        let attr_user_key = MetaKey::dir_attr(pid).encode();
        Self::ensure_no_conflicting_intent(engine, &attr_user_key, true)?;

        if Self::read_latest_in_apply(engine, &attr_user_key)?.is_some() {
            return Ok(());
        }
        let delta_key = DirShardDeltaKey { pid }.encode();
        if let Some(raw_sub) = Self::read_latest_in_apply(engine, &delta_key)? {
            let sub = DirShardDeltaRecord::decode(&raw_sub)?;
            if sub.deleting_fence {
                return Err(MetaDbError::WriteIntentConflict {
                    conflicting_txn_id: 0,
                    primary_key: attr_user_key,
                    expire_hlc: u64::MAX,
                });
            }
            return Ok(());
        }
        Err(MetaDbError::DirectoryNotFound(pid))
    }

    fn ensure_user_key_in_range(&self, user_key: &[u8]) -> Result<()> {
        let meta = self.meta.read().unwrap();
        if !meta.range.contains_key(user_key) {
            return Err(MetaDbError::EpochMismatch {
                shard_id: self.shard_id,
                expected: meta.epoch,
                actual: meta.epoch,
            });
        }
        Ok(())
    }

    fn apply_tx_cmd(
        &self,
        raft_index: u64,
        commit_hlc: HlcTimestamp,
        cmd: TxRaftCmd,
        engine: &LifecycleStorageEngine,
    ) -> Result<TxRaftOutput> {
        match cmd {
            TxRaftCmd::InitDirAttr {
                dir_id,
                parent_pid,
                uid,
                mode,
            } => {
                let key = MetaKey::dir_attr(dir_id).encode();
                self.ensure_user_key_in_range(&key)?;
                let attr = InodeAttr {
                    inode_id: dir_id,
                    parent_pid,
                    entry_type: EntryType::DirectoryAttr,
                    uid,
                    mode,
                    link_count: 1,
                    num_children: 0,
                    size_bytes: 0,
                    create_time: commit_hlc,
                    modify_time: commit_hlc,
                    fid: None,
                    applied_lsn: raft_index,
                };
                engine.apply_committed_put(key, attr.encode(), commit_hlc, raft_index);
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Attr(attr))
            }
            TxRaftCmd::InitDirShardDelta { record } => {
                let key = DirShardDeltaKey {
                    pid: record.inode_id,
                }
                .encode()
                .to_vec();
                engine.apply_committed_put(key, record.encode(), commit_hlc, raft_index);
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Unit)
            }
            TxRaftCmd::CreateFile {
                pid,
                name,
                inode_id,
                fid,
                uid,
                mode,
                size_bytes,
            } => {
                validate_user_entry_name(&name)?;
                let child_key = MetaKey::child(pid, &name);
                let child_bytes = child_key.encode();
                self.ensure_user_key_in_range(&child_bytes)?;
                self.verify_parent_dir_ready(engine, pid)?;
                Self::ensure_no_conflicting_intent(engine, &child_bytes, false)?;

                if Self::read_latest_in_apply(engine, &child_bytes)?.is_some() {
                    return Err(MetaDbError::AlreadyExists { pid, name });
                }

                let attr = InodeAttr {
                    inode_id,
                    parent_pid: pid,
                    entry_type: EntryType::File,
                    uid,
                    mode,
                    link_count: 1,
                    num_children: 0,
                    size_bytes,
                    create_time: commit_hlc,
                    modify_time: commit_hlc,
                    fid: Some(fid),
                    applied_lsn: 0,
                };
                engine.apply_committed_put(child_bytes, attr.encode(), commit_hlc, raft_index);
                self.attr_agg
                    .record_delta(pid, raft_index, commit_hlc, 1, size_bytes);
                self.outbox.record_entry_specs(
                    raft_index,
                    commit_hlc,
                    vec![
                        AuxIntentSpec::QuotaDelta {
                            uid,
                            delta_bytes: size_bytes,
                            delta_inodes: 1,
                        },
                        AuxIntentSpec::IndexUpsert {
                            fid,
                            pid,
                            name: name.clone(),
                            create_time: commit_hlc,
                        },
                    ],
                );
                Ok(TxRaftOutput::Entry(MetaEntry {
                    key: child_key,
                    attr,
                }))
            }
            TxRaftCmd::MkdirSingleShard {
                pid,
                name,
                new_dir_id,
                uid,
                mode,
            } => {
                validate_user_entry_name(&name)?;
                let child_key = MetaKey::child(pid, &name);
                let child_bytes = child_key.encode();
                let new_attr_bytes = MetaKey::dir_attr(new_dir_id).encode();
                self.ensure_user_key_in_range(&child_bytes)?;
                self.ensure_user_key_in_range(&new_attr_bytes)?;
                self.verify_parent_dir_ready(engine, pid)?;
                Self::ensure_no_conflicting_intent(engine, &child_bytes, false)?;
                Self::ensure_no_conflicting_intent(engine, &new_attr_bytes, false)?;

                if Self::read_latest_in_apply(engine, &child_bytes)?.is_some() {
                    return Err(MetaDbError::AlreadyExists { pid, name });
                }

                let dentry_attr = InodeAttr {
                    inode_id: new_dir_id,
                    parent_pid: pid,
                    entry_type: EntryType::Directory,
                    uid,
                    mode,
                    link_count: 1,
                    num_children: 0,
                    size_bytes: 0,
                    create_time: commit_hlc,
                    modify_time: commit_hlc,
                    fid: None,
                    applied_lsn: 0,
                };
                let self_attr = InodeAttr {
                    entry_type: EntryType::DirectoryAttr,
                    applied_lsn: raft_index,
                    ..dentry_attr.clone()
                };

                engine.apply_committed_put(
                    child_bytes,
                    dentry_attr.encode(),
                    commit_hlc,
                    raft_index,
                );
                engine.apply_committed_put(
                    new_attr_bytes,
                    self_attr.encode(),
                    commit_hlc,
                    raft_index,
                );
                self.attr_agg
                    .record_delta(pid, raft_index, commit_hlc, 1, 0);
                self.outbox.record_entry_specs(
                    raft_index,
                    commit_hlc,
                    vec![AuxIntentSpec::QuotaDelta {
                        uid,
                        delta_bytes: 0,
                        delta_inodes: 1,
                    }],
                );
                Ok(TxRaftOutput::Entry(MetaEntry {
                    key: child_key,
                    attr: dentry_attr,
                }))
            }
            TxRaftCmd::Unlink { pid, name } => {
                validate_user_entry_name(&name)?;
                let child_bytes = MetaKey::child(pid, &name).encode();
                self.ensure_user_key_in_range(&child_bytes)?;
                Self::ensure_no_conflicting_intent(engine, &child_bytes, false)?;

                let Some(raw) = Self::read_latest_in_apply(engine, &child_bytes)? else {
                    return Err(MetaDbError::NotFound { pid, name });
                };
                let attr = InodeAttr::decode(&raw)?;
                if attr.entry_type != EntryType::File {
                    return Err(MetaDbError::InvalidEntryType {
                        expected: "File".to_string(),
                        actual: "Directory".to_string(),
                    });
                }

                engine.apply_committed_delete(child_bytes, commit_hlc, raft_index);
                self.attr_agg
                    .record_delta(pid, raft_index, commit_hlc, -1, -attr.size_bytes);

                let mut aux = vec![AuxIntentSpec::QuotaDelta {
                    uid: attr.uid,
                    delta_bytes: -attr.size_bytes,
                    delta_inodes: -1,
                }];
                if let Some(fid) = attr.fid {
                    aux.push(AuxIntentSpec::IndexDelete { fid });
                }
                self.outbox.record_entry_specs(raft_index, commit_hlc, aux);
                Ok(TxRaftOutput::Attr(attr))
            }
            TxRaftCmd::RmdirSingleShard { pid, name } => {
                validate_user_entry_name(&name)?;
                let child_bytes = MetaKey::child(pid, &name).encode();
                self.ensure_user_key_in_range(&child_bytes)?;
                Self::ensure_no_conflicting_intent(engine, &child_bytes, false)?;

                let Some(raw) = Self::read_latest_in_apply(engine, &child_bytes)? else {
                    return Err(MetaDbError::NotFound { pid, name });
                };
                let dentry_attr = InodeAttr::decode(&raw)?;
                if dentry_attr.entry_type != EntryType::Directory {
                    return Err(MetaDbError::InvalidEntryType {
                        expected: "Directory".to_string(),
                        actual: "File".to_string(),
                    });
                }

                let target_dir_id = dentry_attr.inode_id;
                let dir_attr_bytes = MetaKey::dir_attr(target_dir_id).encode();
                self.ensure_user_key_in_range(&dir_attr_bytes)?;
                // Conflict with any active `DirExistsFenceSet` (`Prepare2pc` into `target_dir_id`)!
                Self::ensure_no_conflicting_intent(engine, &dir_attr_bytes, false)?;

                let start = MetaKey::encode_pid_prefix_start(target_dir_id);
                let end = MetaKey::encode_pid_prefix_end(target_dir_id);
                let rows = Self::scan_latest_in_apply(engine, &start, &end)?;
                let mut live_children = 0usize;
                for (k, _) in rows {
                    if !MetaKey::decode(&k)?.is_attr_key() {
                        live_children += 1;
                    }
                }
                if live_children > 0 {
                    return Err(MetaDbError::DirectoryNotEmpty {
                        dir_id: target_dir_id,
                        child_count: live_children,
                    });
                }
                // Also verify zero prepared `[0x05]` child intents exist under `target_dir_id`.
                let intent_start = WriteIntentKey::encode_range_bound(&start);
                let intent_end = WriteIntentKey::encode_range_bound(&end);
                if !Self::scan_latest_in_apply(engine, &intent_start, &intent_end)?.is_empty() {
                    return Err(MetaDbError::DirectoryNotEmpty {
                        dir_id: target_dir_id,
                        child_count: 1,
                    });
                }

                engine.apply_committed_delete(child_bytes, commit_hlc, raft_index);
                engine.apply_committed_delete(dir_attr_bytes, commit_hlc, raft_index);
                self.attr_agg
                    .record_delta(pid, raft_index, commit_hlc, -1, 0);
                self.outbox.record_entry_specs(
                    raft_index,
                    commit_hlc,
                    vec![AuxIntentSpec::QuotaDelta {
                        uid: dentry_attr.uid,
                        delta_bytes: 0,
                        delta_inodes: -1,
                    }],
                );
                Ok(TxRaftOutput::Attr(dentry_attr))
            }
            TxRaftCmd::SetAttrSingleShard { pid, name, patch } => {
                validate_user_entry_name(&name)?;
                let child_key = MetaKey::child(pid, &name);
                let child_bytes = child_key.encode();
                self.ensure_user_key_in_range(&child_bytes)?;
                Self::ensure_no_conflicting_intent(engine, &child_bytes, false)?;

                let Some(raw) = Self::read_latest_in_apply(engine, &child_bytes)? else {
                    return Err(MetaDbError::NotFound { pid, name });
                };
                let mut attr = InodeAttr::decode(&raw)?;
                let old_uid = attr.uid;
                let old_size = attr.size_bytes;
                let old_fid = attr.fid;

                if let Some(m) = patch.mode {
                    attr.mode = m;
                }
                if let Some(u) = patch.uid {
                    attr.uid = u;
                }
                if let Some(sz) = patch.size_bytes {
                    attr.size_bytes = sz;
                }
                if let Some(fid) = patch.fid {
                    attr.fid = Some(fid);
                }
                attr.modify_time = patch.modify_time.unwrap_or(commit_hlc);

                // Resolution B10: If target is a Directory, atomically update authoritative `(dir_id, "_attr_")`!
                if attr.entry_type == EntryType::Directory {
                    let dir_attr_key = MetaKey::dir_attr(attr.inode_id).encode();
                    self.ensure_user_key_in_range(&dir_attr_key)?;
                    Self::ensure_no_conflicting_intent(engine, &dir_attr_key, true)?;
                    if let Some(dir_raw) = Self::read_latest_in_apply(engine, &dir_attr_key)? {
                        let mut dir_self = InodeAttr::decode(&dir_raw)?;
                        dir_self.mode = attr.mode;
                        dir_self.uid = attr.uid;
                        dir_self.modify_time = attr.modify_time;
                        engine.apply_committed_put(
                            dir_attr_key,
                            dir_self.encode(),
                            commit_hlc,
                            raft_index,
                        );
                    }
                }

                engine.apply_committed_put(child_bytes, attr.encode(), commit_hlc, raft_index);

                let delta_bytes = attr.size_bytes - old_size;
                if delta_bytes != 0 {
                    self.attr_agg
                        .record_delta(pid, raft_index, commit_hlc, 0, delta_bytes);
                }

                let mut aux = Vec::new();
                if attr.uid != old_uid {
                    let inodes = 1;
                    aux.push(AuxIntentSpec::QuotaDelta {
                        uid: old_uid,
                        delta_bytes: -old_size,
                        delta_inodes: -inodes,
                    });
                    aux.push(AuxIntentSpec::QuotaDelta {
                        uid: attr.uid,
                        delta_bytes: attr.size_bytes,
                        delta_inodes: inodes,
                    });
                } else if delta_bytes != 0 {
                    aux.push(AuxIntentSpec::QuotaDelta {
                        uid: attr.uid,
                        delta_bytes,
                        delta_inodes: 0,
                    });
                }
                if attr.fid != old_fid {
                    if let Some(prev_fid) = old_fid {
                        aux.push(AuxIntentSpec::IndexDelete { fid: prev_fid });
                    }
                    if let Some(new_fid) = attr.fid {
                        aux.push(AuxIntentSpec::IndexUpsert {
                            fid: new_fid,
                            pid,
                            name: name.clone(),
                            create_time: attr.create_time,
                        });
                    }
                }
                self.outbox.record_entry_specs(raft_index, commit_hlc, aux);
                Ok(TxRaftOutput::Entry(MetaEntry {
                    key: child_key,
                    attr,
                }))
            }
            TxRaftCmd::RenameSingleShard {
                src_pid,
                src_name,
                dst_pid,
                dst_name,
                overwrite,
            } => {
                validate_user_entry_name(&src_name)?;
                validate_user_entry_name(&dst_name)?;
                let src_key = MetaKey::child(src_pid, &src_name);
                let dst_key = MetaKey::child(dst_pid, &dst_name);
                let src_bytes = src_key.encode();
                let dst_bytes = dst_key.encode();
                self.ensure_user_key_in_range(&src_bytes)?;
                self.ensure_user_key_in_range(&dst_bytes)?;
                self.verify_parent_dir_ready(engine, dst_pid)?;
                Self::ensure_no_conflicting_intent(engine, &src_bytes, false)?;
                Self::ensure_no_conflicting_intent(engine, &dst_bytes, false)?;

                if src_pid == dst_pid && src_name == dst_name {
                    let Some(raw) = Self::read_latest_in_apply(engine, &src_bytes)? else {
                        return Err(MetaDbError::NotFound {
                            pid: src_pid,
                            name: src_name,
                        });
                    };
                    let attr = InodeAttr::decode(&raw)?;
                    self.outbox
                        .record_entry_specs(raft_index, commit_hlc, vec![]);
                    return Ok(TxRaftOutput::Entry(MetaEntry { key: dst_key, attr }));
                }

                let Some(src_raw) = Self::read_latest_in_apply(engine, &src_bytes)? else {
                    return Err(MetaDbError::NotFound {
                        pid: src_pid,
                        name: src_name,
                    });
                };
                let mut src_attr = InodeAttr::decode(&src_raw)?;

                // Cycle check if moving a directory inside the single shard (`Resolution B13`).
                if src_attr.entry_type == EntryType::Directory && src_pid != dst_pid {
                    let mut cursor = dst_pid;
                    for _ in 0..1024 {
                        if cursor == src_attr.inode_id {
                            return Err(MetaDbError::InvalidRenameCycle {
                                dir_id: src_attr.inode_id,
                                dst_pid,
                            });
                        }
                        if cursor <= metadb_core::ROOT_INODE_ID {
                            break;
                        }
                        let anc_key = MetaKey::dir_attr(cursor).encode();
                        let Some(anc_raw) = Self::read_latest_in_apply(engine, &anc_key)? else {
                            break;
                        };
                        cursor = InodeAttr::decode(&anc_raw)?.parent_pid;
                    }
                }

                let mut aux_specs = Vec::new();
                let mut victim_opt: Option<InodeAttr> = None;

                if let Some(dst_raw) = Self::read_latest_in_apply(engine, &dst_bytes)? {
                    if !overwrite {
                        return Err(MetaDbError::AlreadyExists {
                            pid: dst_pid,
                            name: dst_name,
                        });
                    }
                    let victim = InodeAttr::decode(&dst_raw)?;
                    if victim.entry_type != EntryType::File
                        || src_attr.entry_type != EntryType::File
                    {
                        return Err(MetaDbError::InvalidEntryType {
                            expected: "File".to_string(),
                            actual: "Directory".to_string(),
                        });
                    }
                    // Resolution B11: Deterministic victim quota & FID cleanup on POSIX overwrite!
                    aux_specs.push(AuxIntentSpec::QuotaDelta {
                        uid: victim.uid,
                        delta_bytes: -victim.size_bytes,
                        delta_inodes: -1,
                    });
                    if let Some(vfid) = victim.fid {
                        aux_specs.push(AuxIntentSpec::IndexDelete { fid: vfid });
                    }
                    victim_opt = Some(victim);
                }

                src_attr.parent_pid = dst_pid;
                src_attr.modify_time = commit_hlc;

                if src_attr.entry_type == EntryType::Directory && src_pid != dst_pid {
                    let dir_attr_key = MetaKey::dir_attr(src_attr.inode_id).encode();
                    self.ensure_user_key_in_range(&dir_attr_key)?;
                    if let Some(dir_raw) = Self::read_latest_in_apply(engine, &dir_attr_key)? {
                        let mut dir_self = InodeAttr::decode(&dir_raw)?;
                        dir_self.parent_pid = dst_pid;
                        dir_self.modify_time = commit_hlc;
                        engine.apply_committed_put(
                            dir_attr_key,
                            dir_self.encode(),
                            commit_hlc,
                            raft_index,
                        );
                    }
                }

                engine.apply_committed_delete(src_bytes, commit_hlc, raft_index);
                engine.apply_committed_put(dst_bytes, src_attr.encode(), commit_hlc, raft_index);

                match (src_pid == dst_pid, victim_opt) {
                    (true, Some(victim)) => {
                        self.attr_agg.record_delta(
                            dst_pid,
                            raft_index,
                            commit_hlc,
                            -1,
                            -victim.size_bytes,
                        );
                    }
                    (true, None) => {
                        self.attr_agg
                            .record_delta(dst_pid, raft_index, commit_hlc, 0, 0);
                    }
                    (false, Some(victim)) => {
                        self.attr_agg.record_delta(
                            src_pid,
                            raft_index,
                            commit_hlc,
                            -1,
                            -src_attr.size_bytes,
                        );
                        self.attr_agg.record_delta(
                            dst_pid,
                            raft_index,
                            commit_hlc,
                            0,
                            src_attr.size_bytes - victim.size_bytes,
                        );
                    }
                    (false, None) => {
                        self.attr_agg.record_delta(
                            src_pid,
                            raft_index,
                            commit_hlc,
                            -1,
                            -src_attr.size_bytes,
                        );
                        self.attr_agg.record_delta(
                            dst_pid,
                            raft_index,
                            commit_hlc,
                            1,
                            src_attr.size_bytes,
                        );
                    }
                }

                if let Some(fid) = src_attr.fid {
                    aux_specs.push(AuxIntentSpec::IndexUpsert {
                        fid,
                        pid: dst_pid,
                        name: dst_name,
                        create_time: src_attr.create_time,
                    });
                }
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, aux_specs);
                Ok(TxRaftOutput::Entry(MetaEntry {
                    key: dst_key,
                    attr: src_attr,
                }))
            }
            TxRaftCmd::SetQuotaLimit {
                uid,
                quota_limit_bytes,
            } => {
                let qk = QuotaKey { uid }.encode();
                self.ensure_user_key_in_range(&qk)?;
                let mut rec = match Self::read_latest_in_apply(engine, &qk)? {
                    Some(raw) => QuotaRecord::decode(&raw)?,
                    None => QuotaRecord::new(uid, quota_limit_bytes),
                };
                rec.quota_limit_bytes = quota_limit_bytes;
                rec.last_applied_hlc = rec.last_applied_hlc.max(commit_hlc);
                engine.apply_committed_put(qk, rec.encode(), commit_hlc, raft_index);
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Quota(rec))
            }
            TxRaftCmd::ApplyAuxBatch { entries } => {
                let mut applied_count = 0usize;
                for item in entries {
                    match item.payload {
                        AuxTaskPayload::QuotaDelta {
                            uid,
                            delta_bytes,
                            delta_inodes,
                            prev_uid_cursor,
                        } => {
                            let qk = QuotaKey { uid }.encode();
                            self.ensure_user_key_in_range(&qk)?;
                            let mut rec = match Self::read_latest_in_apply(engine, &qk)? {
                                Some(raw) => QuotaRecord::decode(&raw)?,
                                None => QuotaRecord::new(uid, u64::MAX),
                            };
                            let prev = rec
                                .applied_by_shard
                                .get(&item.origin_shard_id)
                                .copied()
                                .unwrap_or((0, 0));
                            let cur_cursor = (item.origin_raft_index, item.task_ordinal);
                            if cur_cursor <= prev {
                                continue; // Exact-once duplicate replay suppressed!
                            }
                            if prev_uid_cursor > prev {
                                // Resolution B8: Contiguous frontier gap rejected!
                                return Err(MetaDbError::AuxOutOfOrderGap {
                                    origin_shard_id: item.origin_shard_id,
                                    uid,
                                    expected_after: prev,
                                    got_prev: prev_uid_cursor,
                                });
                            }
                            rec.used_bytes = (rec.used_bytes + delta_bytes).max(0);
                            rec.used_inodes = (rec.used_inodes + delta_inodes).max(0);
                            rec.last_applied_hlc = rec.last_applied_hlc.max(item.hlc);
                            rec.applied_by_shard
                                .insert(item.origin_shard_id, cur_cursor);
                            engine.apply_committed_put(qk, rec.encode(), commit_hlc, raft_index);
                            applied_count += 1;
                        }
                        AuxTaskPayload::IndexUpsert {
                            fid,
                            pid,
                            name,
                            create_time,
                        } => {
                            let ik = SecondaryIndexKey { fid }.encode();
                            self.ensure_user_key_in_range(&ik)?;
                            let incoming_ver = (
                                item.hlc,
                                item.origin_shard_id,
                                item.origin_raft_index,
                                item.task_ordinal,
                            );
                            let existing_opt = match Self::read_latest_in_apply(engine, &ik)? {
                                Some(raw) => Some(SecondaryIndexRecord::decode(&raw)?),
                                None => None,
                            };
                            if let Some(ex) = &existing_opt {
                                if incoming_ver <= ex.last_ver.max(ex.tombstone_ver) {
                                    continue;
                                }
                            }
                            let new_rec =
                                SecondaryIndexRecord {
                                    fid,
                                    pid,
                                    name,
                                    create_time,
                                    last_ver: incoming_ver,
                                    tombstone_ver: existing_opt
                                        .map(|e| e.tombstone_ver)
                                        .unwrap_or((HlcTimestamp::ZERO, 0, 0, 0)),
                                    is_deleted: false,
                                };
                            engine.apply_committed_put(
                                ik,
                                new_rec.encode(),
                                commit_hlc,
                                raft_index,
                            );
                            applied_count += 1;
                        }
                        AuxTaskPayload::IndexDelete { fid } => {
                            let ik = SecondaryIndexKey { fid }.encode();
                            self.ensure_user_key_in_range(&ik)?;
                            let incoming_ver = (
                                item.hlc,
                                item.origin_shard_id,
                                item.origin_raft_index,
                                item.task_ordinal,
                            );
                            let mut rec = match Self::read_latest_in_apply(engine, &ik)? {
                                Some(raw) => SecondaryIndexRecord::decode(&raw)?,
                                None => SecondaryIndexRecord {
                                    fid,
                                    pid: 0,
                                    name: String::new(),
                                    create_time: HlcTimestamp::ZERO,
                                    last_ver: (HlcTimestamp::ZERO, 0, 0, 0),
                                    tombstone_ver: incoming_ver,
                                    is_deleted: true,
                                },
                            };
                            if incoming_ver <= rec.last_ver.max(rec.tombstone_ver) {
                                continue;
                            }
                            rec.tombstone_ver = incoming_ver;
                            rec.is_deleted = true;
                            engine.apply_committed_put(ik, rec.encode(), commit_hlc, raft_index);
                            applied_count += 1;
                        }
                    }
                }
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::AppliedCount(applied_count))
            }
            TxRaftCmd::AckAuxTasks { acked_keys } => {
                let contig =
                    self.outbox
                        .apply_acked_tasks(raft_index, commit_hlc, &acked_keys, engine);
                Ok(TxRaftOutput::AckedWatermark(contig))
            }
            TxRaftCmd::Prepare2pc {
                txn_id,
                is_primary,
                primary_key,
                participant_keys,
                start_hlc,
                expire_hlc,
                rename_fence_token,
                intents,
            } => {
                if commit_hlc > expire_hlc {
                    return Err(MetaDbError::TransactionAborted(
                        "2PC lease already expired before Prepare".to_string(),
                    ));
                }

                // Validate all local intents before writing any mutation!
                let mut encoded_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(intents.len());
                for (user_key, op) in intents {
                    self.ensure_user_key_in_range(&user_key)?;
                    let intent_raw_key = WriteIntentKey::new(user_key.clone()).encode();
                    let existing_intent = match Self::read_latest_in_apply(engine, &intent_raw_key)?
                    {
                        Some(raw) => Some(WriteIntentRecord::decode(&raw)?),
                        None => None,
                    };

                    match op {
                        IntentOp::DirExistsFenceSet { .. } => {
                            // Verify target directory actually exists!
                            if Self::read_latest_in_apply(engine, &user_key)?.is_none() {
                                let mk = MetaKey::decode(&user_key)?;
                                let dk = DirShardDeltaKey { pid: mk.pid }.encode();
                                if Self::read_latest_in_apply(engine, &dk)?.is_none() {
                                    return Err(MetaDbError::DirectoryNotFound(mk.pid));
                                }
                            }
                            let mut holders = match existing_intent {
                                None => std::collections::BTreeMap::new(),
                                Some(ex) => match ex.op {
                                    IntentOp::DirExistsFenceSet { holders } => holders,
                                    _ => {
                                        return Err(MetaDbError::WriteIntentConflict {
                                            conflicting_txn_id: ex.txn_id,
                                            primary_key: ex.primary_key,
                                            expire_hlc: ex.expire_hlc.0,
                                        })
                                    }
                                },
                            };
                            holders.insert(txn_id, (primary_key.clone(), expire_hlc));
                            let rec = WriteIntentRecord {
                                txn_id,
                                primary_shard_hint: self.shard_id,
                                primary_key: primary_key.clone(),
                                start_hlc,
                                expire_hlc,
                                rename_fence_token,
                                op: IntentOp::DirExistsFenceSet { holders },
                            };
                            encoded_writes.push((intent_raw_key, rec.encode()));
                        }
                        IntentOp::DeletingDir {
                            dir_id,
                            attr_deltas,
                            aux_specs,
                        } => {
                            if let Some(ex) = existing_intent {
                                if ex.txn_id != txn_id {
                                    return Err(MetaDbError::WriteIntentConflict {
                                        conflicting_txn_id: ex.txn_id,
                                        primary_key: ex.primary_key,
                                        expire_hlc: ex.expire_hlc.0,
                                    });
                                }
                            }
                            // Verify zero live children AND zero prepared child intents in `[dir_id, dir_id+1)`!
                            let start = MetaKey::encode_pid_prefix_start(dir_id);
                            let end = MetaKey::encode_pid_prefix_end(dir_id);
                            for (k, _) in Self::scan_latest_in_apply(engine, &start, &end)? {
                                if !MetaKey::decode(&k)?.is_attr_key() {
                                    return Err(MetaDbError::DirectoryNotEmpty {
                                        dir_id,
                                        child_count: 1,
                                    });
                                }
                            }
                            let i_start = WriteIntentKey::encode_range_bound(&start);
                            let i_end = WriteIntentKey::encode_range_bound(&end);
                            for (ik, _) in Self::scan_latest_in_apply(engine, &i_start, &i_end)? {
                                let decoded_ik = WriteIntentKey::decode(&ik)?;
                                if decoded_ik.target_user_key != user_key {
                                    return Err(MetaDbError::DirectoryNotEmpty {
                                        dir_id,
                                        child_count: 1,
                                    });
                                }
                            }
                            let rec = WriteIntentRecord {
                                txn_id,
                                primary_shard_hint: self.shard_id,
                                primary_key: primary_key.clone(),
                                start_hlc,
                                expire_hlc,
                                rename_fence_token,
                                op: IntentOp::DeletingDir {
                                    dir_id,
                                    attr_deltas,
                                    aux_specs,
                                },
                            };
                            encoded_writes.push((intent_raw_key, rec.encode()));
                        }
                        other_op => {
                            if let Some(ex) = existing_intent {
                                if ex.txn_id != txn_id {
                                    return Err(MetaDbError::WriteIntentConflict {
                                        conflicting_txn_id: ex.txn_id,
                                        primary_key: ex.primary_key,
                                        expire_hlc: ex.expire_hlc.0,
                                    });
                                }
                            }
                            let rec = WriteIntentRecord {
                                txn_id,
                                primary_shard_hint: self.shard_id,
                                primary_key: primary_key.clone(),
                                start_hlc,
                                expire_hlc,
                                rename_fence_token,
                                op: other_op,
                            };
                            encoded_writes.push((intent_raw_key, rec.encode()));
                        }
                    }
                }

                for (ik, iv) in encoded_writes {
                    engine.apply_committed_put(ik, iv, commit_hlc, raft_index);
                }

                if is_primary {
                    let local_range = self.key_range();
                    let pending_ack_keys: BTreeSet<Vec<u8>> = participant_keys
                        .iter()
                        .filter(|k| !local_range.contains_key(k))
                        .cloned()
                        .collect();
                    let txn_rec = TxnStatusRecord {
                        txn_id,
                        primary_key,
                        participant_keys,
                        pending_ack_keys,
                        start_hlc,
                        expire_hlc,
                        rename_fence_token,
                        state: TxnState::Pending,
                    };
                    let tk = TxnRecordKey { txn_id }.encode().to_vec();
                    engine.apply_committed_put(tk, txn_rec.encode(), commit_hlc, raft_index);
                }

                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Prepared {
                    prepared_hlc: commit_hlc,
                })
            }
            TxRaftCmd::CommitPrimary2pc {
                txn_id,
                commit_hlc: tx_commit_hlc,
                rename_fence_token,
            } => {
                let tk = TxnRecordKey { txn_id }.encode().to_vec();
                let Some(raw_rec) = Self::read_latest_in_apply(engine, &tk)? else {
                    return Err(MetaDbError::TransactionAborted(
                        "missing TxnStatusRecord on PrimaryShard".to_string(),
                    ));
                };
                let mut txn_rec = TxnStatusRecord::decode(&raw_rec)?;

                match txn_rec.state {
                    TxnState::Aborted => {
                        return Err(MetaDbError::TransactionAborted(
                            "transaction already aborted by recovery".to_string(),
                        ));
                    }
                    TxnState::Committed { commit_hlc: c } => {
                        self.outbox
                            .record_entry_specs(raft_index, commit_hlc, vec![]);
                        return Ok(TxRaftOutput::Committed {
                            commit_hlc: c,
                            already_applied: true,
                        });
                    }
                    TxnState::Pending => {}
                }

                // Resolution B13: Verify lease / expiry fence at Commit!
                if rename_fence_token != txn_rec.rename_fence_token
                    || (rename_fence_token.is_some() && tx_commit_hlc > txn_rec.expire_hlc)
                {
                    txn_rec.state = TxnState::Aborted;
                    engine.apply_committed_put(tk, txn_rec.encode(), commit_hlc, raft_index);
                    return Err(MetaDbError::TransactionAborted(
                        "DirRenameLease expired or fence token mismatch at commit".to_string(),
                    ));
                }

                let final_commit_hlc = tx_commit_hlc.max(commit_hlc);
                let local_range = self.key_range();
                let local_keys: Vec<Vec<u8>> = txn_rec
                    .participant_keys
                    .iter()
                    .filter(|k| local_range.contains_key(k))
                    .cloned()
                    .collect();

                let mut aux_specs = Vec::new();
                for uk in &local_keys {
                    self.materialize_intent_for_key(
                        engine,
                        txn_id,
                        uk,
                        raft_index,
                        final_commit_hlc,
                        &mut aux_specs,
                    )?;
                }

                txn_rec.state = TxnState::Committed {
                    commit_hlc: final_commit_hlc,
                };
                if txn_rec.pending_ack_keys.is_empty() {
                    engine.apply_committed_delete(tk, final_commit_hlc, raft_index);
                } else {
                    engine.apply_committed_put(tk, txn_rec.encode(), final_commit_hlc, raft_index);
                }

                self.outbox
                    .record_entry_specs(raft_index, final_commit_hlc, aux_specs);
                Ok(TxRaftOutput::Committed {
                    commit_hlc: final_commit_hlc,
                    already_applied: false,
                })
            }
            TxRaftCmd::CommitSecondary2pc {
                txn_id,
                commit_hlc: tx_commit_hlc,
                target_user_keys,
            } => {
                let mut any_applied = false;
                let mut aux_specs = Vec::new();
                for uk in &target_user_keys {
                    if self.materialize_intent_for_key(
                        engine,
                        txn_id,
                        uk,
                        raft_index,
                        tx_commit_hlc,
                        &mut aux_specs,
                    )? {
                        any_applied = true;
                    }
                }
                self.outbox
                    .record_entry_specs(raft_index, tx_commit_hlc, aux_specs);
                Ok(TxRaftOutput::Committed {
                    commit_hlc: tx_commit_hlc,
                    already_applied: !any_applied,
                })
            }
            TxRaftCmd::Abort2pc {
                txn_id,
                target_user_keys,
            } => {
                for uk in &target_user_keys {
                    self.abort_intent_for_key(engine, txn_id, uk, raft_index, commit_hlc)?;
                }
                let tk = TxnRecordKey { txn_id }.encode().to_vec();
                if let Some(raw_rec) = Self::read_latest_in_apply(engine, &tk)? {
                    let mut txn_rec = TxnStatusRecord::decode(&raw_rec)?;
                    if matches!(txn_rec.state, TxnState::Pending) {
                        txn_rec.state = TxnState::Aborted;
                        if txn_rec.pending_ack_keys.is_empty() {
                            engine.apply_committed_delete(tk, commit_hlc, raft_index);
                        } else {
                            engine.apply_committed_put(
                                tk,
                                txn_rec.encode(),
                                commit_hlc,
                                raft_index,
                            );
                        }
                    }
                }
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Unit)
            }
            TxRaftCmd::MarkParticipantAcked {
                txn_id,
                acked_user_keys,
            } => {
                let tk = TxnRecordKey { txn_id }.encode().to_vec();
                if let Some(raw_rec) = Self::read_latest_in_apply(engine, &tk)? {
                    let mut txn_rec = TxnStatusRecord::decode(&raw_rec)?;
                    for k in &acked_user_keys {
                        txn_rec.pending_ack_keys.remove(k);
                    }
                    if txn_rec.pending_ack_keys.is_empty() {
                        engine.apply_committed_delete(tk, commit_hlc, raft_index);
                    } else {
                        engine.apply_committed_put(tk, txn_rec.encode(), commit_hlc, raft_index);
                    }
                }
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Unit)
            }
            TxRaftCmd::CheckAndAbortExpiredTxn { txn_id, caller_hlc } => {
                let tk = TxnRecordKey { txn_id }.encode().to_vec();
                let Some(raw_rec) = Self::read_latest_in_apply(engine, &tk)? else {
                    self.outbox
                        .record_entry_specs(raft_index, commit_hlc, vec![]);
                    return Ok(TxRaftOutput::TxnStatus(Some(TxnState::Aborted)));
                };
                let mut txn_rec = TxnStatusRecord::decode(&raw_rec)?;
                if matches!(txn_rec.state, TxnState::Pending)
                    && caller_hlc.max(commit_hlc) > txn_rec.expire_hlc
                {
                    let local_range = self.key_range();
                    for uk in txn_rec.participant_keys.clone() {
                        if local_range.contains_key(&uk) {
                            self.abort_intent_for_key(engine, txn_id, &uk, raft_index, commit_hlc)?;
                        }
                    }
                    txn_rec.state = TxnState::Aborted;
                    if txn_rec.pending_ack_keys.is_empty() {
                        engine.apply_committed_delete(tk, commit_hlc, raft_index);
                    } else {
                        engine.apply_committed_put(tk, txn_rec.encode(), commit_hlc, raft_index);
                    }
                }
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::TxnStatus(Some(txn_rec.state)))
            }
            TxRaftCmd::UpdateShardRangeOnSplit {
                new_range,
                new_epoch,
                keys_to_delete,
                updated_anchor_attr,
                updated_shard_delta,
            } => {
                for k in keys_to_delete {
                    engine.apply_committed_delete(k, commit_hlc, raft_index);
                }
                if let Some((pid, mut attr)) = updated_anchor_attr {
                    attr.applied_lsn = raft_index;
                    engine.apply_committed_put(
                        MetaKey::dir_attr(pid).encode(),
                        attr.encode(),
                        commit_hlc,
                        raft_index,
                    );
                }
                if let Some(mut sub) = updated_shard_delta {
                    sub.applied_raft_index = raft_index;
                    let dk = DirShardDeltaKey { pid: sub.inode_id }.encode().to_vec();
                    engine.apply_committed_put(dk, sub.encode(), commit_hlc, raft_index);
                }
                {
                    let mut m = self.meta.write().unwrap();
                    m.range = new_range;
                    m.epoch = new_epoch;
                }
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Unit)
            }
        }
    }

    /// Materializes `[0x05][user_key]` if held by `txn_id` (`Resolution B12` exact-once check).
    fn materialize_intent_for_key(
        &self,
        engine: &LifecycleStorageEngine,
        txn_id: TxnId,
        user_key: &[u8],
        raft_index: u64,
        commit_hlc: HlcTimestamp,
        aux_out: &mut Vec<AuxIntentSpec>,
    ) -> Result<bool> {
        let ik = WriteIntentKey::new(user_key.to_vec()).encode();
        let Some(raw) = Self::read_latest_in_apply(engine, &ik)? else {
            return Ok(false); // Already materialized on an earlier retry!
        };
        let rec = WriteIntentRecord::decode(&raw)?;
        match rec.op {
            IntentOp::DirExistsFenceSet { mut holders } => {
                let existed = holders.remove(&txn_id).is_some();
                if holders.is_empty() {
                    engine.apply_committed_delete(ik, commit_hlc, raft_index);
                } else {
                    let updated = WriteIntentRecord {
                        op: IntentOp::DirExistsFenceSet { holders },
                        ..rec
                    };
                    engine.apply_committed_put(ik, updated.encode(), commit_hlc, raft_index);
                }
                Ok(existed)
            }
            _ if rec.txn_id != txn_id => Ok(false),
            IntentOp::Put {
                value,
                attr_deltas,
                aux_specs,
            } => {
                // Stamp `modify_time = commit_hlc` if `value` is an 88-byte `InodeAttr`.
                let final_val = if let Ok(mut decoded_attr) = InodeAttr::decode(&value) {
                    decoded_attr.modify_time = commit_hlc;
                    if decoded_attr.entry_type == EntryType::DirectoryAttr {
                        decoded_attr.applied_lsn = raft_index;
                    }
                    decoded_attr.encode()
                } else {
                    value
                };
                engine.apply_committed_put(user_key.to_vec(), final_val, commit_hlc, raft_index);
                engine.apply_committed_delete(ik, commit_hlc, raft_index);
                self.record_attr_specs(&attr_deltas, raft_index, commit_hlc);
                aux_out.extend(aux_specs);
                Ok(true)
            }
            IntentOp::Delete {
                attr_deltas,
                aux_specs,
            } => {
                engine.apply_committed_delete(user_key.to_vec(), commit_hlc, raft_index);
                engine.apply_committed_delete(ik, commit_hlc, raft_index);
                self.record_attr_specs(&attr_deltas, raft_index, commit_hlc);
                aux_out.extend(aux_specs);
                Ok(true)
            }
            IntentOp::DeletingDir {
                dir_id,
                attr_deltas,
                aux_specs,
            } => {
                engine.apply_committed_delete(user_key.to_vec(), commit_hlc, raft_index);
                let dk = DirShardDeltaKey { pid: dir_id }.encode().to_vec();
                engine.apply_committed_delete(dk, commit_hlc, raft_index);
                engine.apply_committed_delete(ik, commit_hlc, raft_index);
                self.record_attr_specs(&attr_deltas, raft_index, commit_hlc);
                aux_out.extend(aux_specs);
                Ok(true)
            }
        }
    }

    fn abort_intent_for_key(
        &self,
        engine: &LifecycleStorageEngine,
        txn_id: TxnId,
        user_key: &[u8],
        raft_index: u64,
        hlc: HlcTimestamp,
    ) -> Result<()> {
        let ik = WriteIntentKey::new(user_key.to_vec()).encode();
        let Some(raw) = Self::read_latest_in_apply(engine, &ik)? else {
            return Ok(());
        };
        let rec = WriteIntentRecord::decode(&raw)?;
        match rec.op {
            IntentOp::DirExistsFenceSet { mut holders } => {
                holders.remove(&txn_id);
                if holders.is_empty() {
                    engine.apply_committed_delete(ik, hlc, raft_index);
                } else {
                    let updated = WriteIntentRecord {
                        op: IntentOp::DirExistsFenceSet { holders },
                        ..rec
                    };
                    engine.apply_committed_put(ik, updated.encode(), hlc, raft_index);
                }
            }
            _ if rec.txn_id == txn_id => {
                engine.apply_committed_delete(ik, hlc, raft_index);
            }
            _ => {}
        }
        Ok(())
    }

    fn record_attr_specs(&self, specs: &[AttrDeltaSpec], raft_index: u64, hlc: HlcTimestamp) {
        for s in specs {
            self.attr_agg
                .record_delta(s.pid, raft_index, hlc, s.delta_children, s.delta_bytes);
        }
    }
}

impl PreFlushHook for TxStateMachineHandler {
    fn on_seal_memtable(
        &self,
        sealed_raft_index: u64,
        sealed_max_hlc: HlcTimestamp,
        view_at_m: &ViewAtRaftIndex<'_>,
    ) -> Result<Vec<(Vec<u8>, HlcTimestamp, MvccValue)>> {
        let mut out = self
            .attr_agg
            .fold_into_seal_batch(sealed_raft_index, view_at_m)?;
        let spilled = self
            .outbox
            .spill_into_seal_batch(sealed_raft_index, sealed_max_hlc);
        out.extend(spilled);
        Ok(out)
    }

    fn on_flush_durable(&self, durable_raft_index: u64, _durable_max_hlc: HlcTimestamp) -> u64 {
        self.attr_agg.on_flush_durable(durable_raft_index);
        self.outbox.on_flush_durable(durable_raft_index)
    }
}

impl ShardApplyHandler for TxStateMachineHandler {
    fn current_epoch(&self) -> ShardEpoch {
        self.meta.read().unwrap().epoch
    }

    fn on_membership_changed(&self, new_epoch: ShardEpoch) {
        self.meta.write().unwrap().epoch = new_epoch;
    }

    fn encode_descriptor_bytes(&self) -> Vec<u8> {
        bincode::serialize(&*self.meta.read().unwrap()).unwrap_or_default()
    }

    fn apply_command(
        &self,
        raft_index: u64,
        commit_hlc: HlcTimestamp,
        request: &MetaDbRaftRequest,
        engine: &LifecycleStorageEngine,
    ) -> Result<Vec<u8>> {
        let cmd: TxRaftCmd = bincode::deserialize(&request.payload)
            .map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        let out = self.apply_tx_cmd(raft_index, commit_hlc, cmd, engine)?;
        bincode::serialize(&out).map_err(|e| MetaDbError::CodecError(e.to_string()))
    }

    fn pre_flush_hook(&self) -> Option<&dyn PreFlushHook> {
        Some(self)
    }

    fn prepare_for_snapshot(&self, _up_to_raft_index: u64) {
        self.outbox.set_force_spill_next_seal();
    }

    fn on_snapshot_installed(
        &self,
        checkpoint: &DurableShardCheckpoint,
        engine: &LifecycleStorageEngine,
    ) {
        if !checkpoint.descriptor_bytes.is_empty() {
            if let Ok(decoded) =
                bincode::deserialize::<PersistedShardRouteMeta>(&checkpoint.descriptor_bytes)
            {
                *self.meta.write().unwrap() = decoded;
            }
        }
        self.attr_agg.clear_all();
        let _ = self.outbox.rehydrate_from_storage(
            checkpoint.contiguous_aux_acked_index,
            checkpoint.flushed_raft_index,
            engine,
        );
    }
}
