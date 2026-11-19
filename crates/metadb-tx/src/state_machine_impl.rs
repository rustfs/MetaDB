//! Deterministic Raft State Machine implementation (`TxStateMachineHandler`) executing:
//! - `1-RTT` single-shard pushdown (`CreateFile`, `Unlink`, `SetAttr`, colocated `Mkdir`/`Rmdir`/`Rename`)
//! - Exact-once contiguous-frontier `QuotaTable` & total-order `SecondaryIndexTable` application
//! - Primary-Anchored 2PC state transitions (`[0x05] WriteIntent` & `[0x06] TxnRecord`)
//! - `PreFlushHook` Coordinated Flush Barrier (`ReplicatedAttrAggregator` + `ShardAuxOutbox` `[0x07]` spill)

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use metadb_core::codec;
use metadb_core::schema::validate_user_entry_name;
use metadb_core::{
    negate_accounted_size, saturating_add_i64, validate_dir_inode_id, validate_file_size,
    AttrDeltaSpec, AuxIntentSpec, AuxOutboxEntry, AuxTaskPayload, CompositeKeyRange,
    DirShardDeltaKey, DirShardDeltaRecord, EntryType, FileId, HlcTimestamp, InodeAttr, InodeId,
    IntentOp, MetaDbError, MetaEntry, MetaKey, QuotaKey, QuotaRecord, Result, SecondaryIndexKey,
    SecondaryIndexRecord, ShardEpoch, ShardId, TxnId, TxnRecordKey, TxnState, TxnStatusRecord,
    UserId, WriteIntentKey, WriteIntentRecord, ATTR_ENTRY_NAME, PROPOSAL_ENVELOPE_MAGIC,
};
use metadb_raft::{MetaDbRaftRequest, ShardApplyHandler, ShardWatermarkTracker};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, MvccValue, PreFlushHook, ScanControl,
    ViewAtRaftIndex,
};

use crate::aux_outbox::{ReplicatedAttrAggregator, ShardAuxOutbox, DEFAULT_OUTBOX_SPILL_THRESHOLD};
use crate::rename_cycle::RenameCycleWalker;

/// Partial attribute mutation specification for `SetAttr`.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct SetAttrPatch {
    pub mode: Option<u32>,
    pub uid: Option<UserId>,
    pub size_bytes: Option<i64>,
    pub fid: Option<FileId>,
    pub modify_time: Option<HlcTimestamp>,
}

/// Deterministic command replicated through a shard's OpenRaft log.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
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
    FenceShardRangeForSplit {
        new_range: CompositeKeyRange,
        new_epoch: ShardEpoch,
    },
    UpdateShardRangeOnSplit {
        new_range: CompositeKeyRange,
        new_epoch: ShardEpoch,
        keys_to_delete: Vec<Vec<u8>>,
        updated_anchor_attr: Option<(InodeId, InodeAttr)>,
        updated_shard_delta: Option<DirShardDeltaRecord>,
    },
    /// Undo a split fence that was committed without a published child route.
    /// `new_epoch.version` must be `>=` the fenced epoch so replay keeps this command last.
    RestoreShardRange {
        restored_range: CompositeKeyRange,
        new_epoch: ShardEpoch,
    },
    /// Prepare with compare-and-set predicates for the coordinator's committed reads.
    /// Append new variants to preserve the bincode tags of existing Raft log entries.
    Prepare2pcChecked {
        txn_id: TxnId,
        is_primary: bool,
        primary_key: Vec<u8>,
        participant_keys: Vec<Vec<u8>>,
        start_hlc: HlcTimestamp,
        expire_hlc: HlcTimestamp,
        rename_fence_token: Option<u64>,
        intents: Vec<(Vec<u8>, IntentOp)>,
        expected_values: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    },
    UpsertQuotaLimit {
        uid: UserId,
        insert_quota_limit_bytes: u64,
        update_quota_limit_bytes: u64,
    },
    /// Replicated node-instance binding. Appended so earlier command tags stay stable.
    /// Apply keeps the first instance for a node id and ignores a conflicting one.
    BindNodeInstance {
        node_id: u64,
        instance_id: u128,
        grpc_addr: String,
    },
    /// Renew a pending 2PC lease. Appended so earlier command tags stay stable.
    /// The new `expire_hlc` is replicated with the command; it is not a memory-only update.
    Extend2pcLease {
        txn_id: TxnId,
        /// Must equal the high 64 bits of `txn_id` (the coordinator that allocated it).
        coord_node_id: u64,
        expire_hlc: HlcTimestamp,
        /// Transaction start. On the primary this must match the durable record.
        start_hlc: HlcTimestamp,
        rename_fence_token: Option<u64>,
        target_user_keys: Vec<Vec<u8>>,
    },
}

/// Longest a renewed cross-shard lease may reach past the transaction's `start_hlc`.
///
/// This is two 30s routing budgets (prepare, then commit). A shorter cap would
/// expire a Prepare that is still retrying inside `ROUTE_RETRY_TIMEOUT`.
pub const MAX_2PC_LEASE_LIFETIME_MS: u64 = 60_000;

/// Typed output serialized inside `MetaDbRaftResponse.result`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
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

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]
struct PersistedShardRouteMeta {
    epoch: ShardEpoch,
    range: CompositeKeyRange,
}

/// Durable `(instance id, address)` stored under prefix `0x09` on every replica.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct NodeInstanceRecord {
    pub instance_id: u128,
    pub grpc_addr: String,
}

/// Prefix for a committed proposal receipt. Not a user-table key.
const PROPOSAL_RECEIPT_PREFIX: u8 = 0x0A;
/// `[0x0B | commit_hlc BE | proposal_id BE]` orders receipts for bounded expiry.
const PROPOSAL_EXPIRY_PREFIX: u8 = 0x0B;
/// Resume key for the bounded legacy-receipt stamp sweep.
const PROPOSAL_SWEEP_CURSOR: &[u8] = &[0x0C];
const RECEIPT_VALUE_MAGIC: &[u8] = b"MDBR0001";
/// Longer than the 30s write-retry budget and the 45s join budget.
pub const PROPOSAL_RECEIPT_RETENTION_MS: u64 = 10 * 60 * 1000;
const RECEIPT_EXPIRE_BATCH: usize = 32;
const LEGACY_STAMP_BATCH: usize = 8;

/// Allocates an id for one logical client write. Zero is reserved for legacy payloads.
pub fn fresh_proposal_id() -> u128 {
    let mut bytes = [0u8; 16];
    if let Ok(mut file) = std::fs::File::open("/dev/urandom") {
        if std::io::Read::read_exact(&mut file, &mut bytes).is_ok() {
            let id = u128::from_le_bytes(bytes);
            if id != 0 {
                return id;
            }
        }
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(1);
    let mixed = nanos ^ ((std::process::id() as u128) << 96);
    if mixed == 0 {
        1
    } else {
        mixed
    }
}

pub fn proposal_receipt_key(proposal_id: u128) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(PROPOSAL_RECEIPT_PREFIX);
    key.extend_from_slice(&proposal_id.to_be_bytes());
    key
}

/// Bytes replicated for `cmd`. A retry of the same `proposal_id` must send these bytes again.
pub fn encode_proposed_command(proposal_id: u128, cmd: &TxRaftCmd) -> Result<Vec<u8>> {
    if proposal_id == 0 {
        return Err(MetaDbError::CodecError(
            "proposal id 0 is reserved for a legacy command".to_string(),
        ));
    }
    let cmd_bytes = codec::serialize(cmd).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
    let mut out = Vec::with_capacity(8 + 16 + 4 + cmd_bytes.len());
    out.extend_from_slice(PROPOSAL_ENVELOPE_MAGIC);
    out.extend_from_slice(&proposal_id.to_le_bytes());
    out.extend_from_slice(&(cmd_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&cmd_bytes);
    Ok(out)
}

/// A new logical write. Retries of one write must reuse [`encode_proposed_command`] instead.
pub fn command_payload(cmd: &TxRaftCmd) -> Result<Vec<u8>> {
    encode_proposed_command(fresh_proposal_id(), cmd)
}

fn decode_proposed_command(payload: &[u8]) -> Result<(u128, TxRaftCmd)> {
    if !payload.starts_with(PROPOSAL_ENVELOPE_MAGIC) {
        let cmd =
            codec::deserialize(payload).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        return Ok((0, cmd));
    }
    let rest = &payload[PROPOSAL_ENVELOPE_MAGIC.len()..];
    if rest.len() < 20 {
        return Err(MetaDbError::CodecError(
            "truncated write proposal envelope".to_string(),
        ));
    }
    let mut id_bytes = [0u8; 16];
    id_bytes.copy_from_slice(&rest[..16]);
    let proposal_id = u128::from_le_bytes(id_bytes);
    let cmd_len = u32::from_le_bytes(rest[16..20].try_into().unwrap()) as usize;
    let cmd_end = 20usize.checked_add(cmd_len).ok_or_else(|| {
        MetaDbError::CodecError("write proposal envelope length overflow".to_string())
    })?;
    if cmd_end != rest.len() {
        return Err(MetaDbError::CodecError(
            "write proposal envelope length does not match its command".to_string(),
        ));
    }
    let cmd = codec::deserialize(&rest[20..cmd_end])
        .map_err(|e| MetaDbError::CodecError(e.to_string()))?;
    Ok((proposal_id, cmd))
}

fn encode_receipt(commit_hlc: HlcTimestamp, result: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + 8 + result.len());
    bytes.extend_from_slice(RECEIPT_VALUE_MAGIC);
    bytes.extend_from_slice(&commit_hlc.as_u64().to_le_bytes());
    bytes.extend_from_slice(result);
    bytes
}

fn decode_receipt(value: &[u8]) -> (Option<HlcTimestamp>, Vec<u8>) {
    if value.starts_with(RECEIPT_VALUE_MAGIC) && value.len() >= 16 {
        let mut raw = [0u8; 8];
        raw.copy_from_slice(&value[8..16]);
        return (
            Some(HlcTimestamp(u64::from_le_bytes(raw))),
            value[16..].to_vec(),
        );
    }
    (None, value.to_vec())
}

fn receipt_expired(stored: HlcTimestamp, commit_hlc: HlcTimestamp) -> bool {
    commit_hlc
        .physical_ms()
        .saturating_sub(stored.physical_ms())
        > PROPOSAL_RECEIPT_RETENTION_MS
}

fn expiry_index_key(commit_hlc: HlcTimestamp, proposal_id: u128) -> Vec<u8> {
    let mut key = Vec::with_capacity(25);
    key.push(PROPOSAL_EXPIRY_PREFIX);
    key.extend_from_slice(&commit_hlc.as_u64().to_be_bytes());
    key.extend_from_slice(&proposal_id.to_be_bytes());
    key
}

fn proposal_id_from_receipt_key(key: &[u8]) -> Option<u128> {
    if key.len() != 17 || key[0] != PROPOSAL_RECEIPT_PREFIX {
        return None;
    }
    let mut raw = [0u8; 16];
    raw.copy_from_slice(&key[1..]);
    Some(u128::from_be_bytes(raw))
}

fn maintain_proposal_receipts(
    engine: &LifecycleStorageEngine,
    commit_hlc: HlcTimestamp,
    raft_index: u64,
) -> Result<()> {
    let snap = engine.acquire_linearizable_snapshot(raft_index)?;
    let cutoff = HlcTimestamp::new(
        commit_hlc
            .physical_ms()
            .saturating_sub(PROPOSAL_RECEIPT_RETENTION_MS),
        0,
    );
    let mut expiry_end = vec![PROPOSAL_EXPIRY_PREFIX];
    expiry_end.extend_from_slice(&cutoff.as_u64().to_be_bytes());
    let mut expired = Vec::new();
    engine.scan_live_until(&[PROPOSAL_EXPIRY_PREFIX], &expiry_end, &snap, |key, _| {
        expired.push(key.to_vec());
        if expired.len() >= RECEIPT_EXPIRE_BATCH {
            Ok(ScanControl::Stop)
        } else {
            Ok(ScanControl::Continue)
        }
    })?;
    let cursor = engine
        .get_with_snapshot(PROPOSAL_SWEEP_CURSOR, &snap)?
        .unwrap_or_else(|| vec![PROPOSAL_RECEIPT_PREFIX]);
    let mut legacy = Vec::new();
    let mut sweep_finished = true;
    engine.scan_live_until(&cursor, &[PROPOSAL_EXPIRY_PREFIX], &snap, |key, value| {
        if key != PROPOSAL_SWEEP_CURSOR {
            legacy.push((key.to_vec(), value.to_vec()));
        }
        if legacy.len() >= LEGACY_STAMP_BATCH {
            sweep_finished = false;
            return Ok(ScanControl::Stop);
        }
        Ok(ScanControl::Continue)
    })?;
    drop(snap);

    for key in expired {
        if key.len() != 25 || key[0] != PROPOSAL_EXPIRY_PREFIX {
            continue;
        }
        let mut id_raw = [0u8; 16];
        id_raw.copy_from_slice(&key[9..]);
        let proposal_id = u128::from_be_bytes(id_raw);
        engine.apply_committed_delete(proposal_receipt_key(proposal_id), commit_hlc, raft_index);
        engine.apply_committed_delete(key, commit_hlc, raft_index);
    }
    let mut next_cursor = if sweep_finished {
        vec![PROPOSAL_RECEIPT_PREFIX]
    } else {
        let mut next = legacy.last().map(|(key, _)| key.clone()).unwrap_or(cursor);
        next.push(0);
        next
    };
    for (key, value) in &legacy {
        if value.starts_with(RECEIPT_VALUE_MAGIC) {
            continue;
        }
        let Some(proposal_id) = proposal_id_from_receipt_key(key) else {
            continue;
        };
        engine.apply_committed_put(
            key.clone(),
            encode_receipt(commit_hlc, value),
            commit_hlc,
            raft_index,
        );
        engine.apply_committed_put(
            expiry_index_key(commit_hlc, proposal_id),
            Vec::new(),
            commit_hlc,
            raft_index,
        );
    }
    if next_cursor.is_empty() {
        next_cursor = vec![PROPOSAL_RECEIPT_PREFIX];
    }
    engine.apply_committed_put(
        PROPOSAL_SWEEP_CURSOR.to_vec(),
        next_cursor,
        commit_hlc,
        raft_index,
    );
    Ok(())
}

fn committed_proposal_result(
    engine: &LifecycleStorageEngine,
    proposal_id: u128,
    commit_hlc: HlcTimestamp,
) -> Result<Option<Vec<u8>>> {
    if proposal_id == 0 {
        return Ok(None);
    }
    let Some(raw) =
        TxStateMachineHandler::read_latest_in_apply(engine, &proposal_receipt_key(proposal_id))?
    else {
        return Ok(None);
    };
    let (stored, result) = decode_receipt(&raw);
    if stored.is_some_and(|stored| receipt_expired(stored, commit_hlc)) {
        return Ok(None);
    }
    Ok(Some(result))
}

pub fn node_instance_key(node_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.push(0x09);
    key.extend_from_slice(&node_id.to_be_bytes());
    key
}

/// Process-local copy of Raft-applied `(instance id, grpc address)` bindings.
pub type SharedNodeInstanceBindings = Arc<std::sync::Mutex<BTreeMap<u64, (u128, String)>>>;

/// Stateful per-shard transaction handler implementing both `ShardApplyHandler` and `PreFlushHook`.
pub struct TxStateMachineHandler {
    shard_id: ShardId,
    meta: RwLock<PersistedShardRouteMeta>,
    attr_agg: Arc<ReplicatedAttrAggregator>,
    outbox: Arc<ShardAuxOutbox>,
    /// Filled on apply, including replay, so every replica learns node-instance bindings.
    instance_bindings: std::sync::Mutex<Option<SharedNodeInstanceBindings>>,
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
            instance_bindings: std::sync::Mutex::new(None),
        }
    }

    /// Shares the cell's binding map. Set this before `ShardRaftGroup::start` so replay fills it.
    pub fn set_instance_bindings(&self, bindings: SharedNodeInstanceBindings) {
        *self.instance_bindings.lock().unwrap() = Some(bindings);
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

    /// `Ok(true)` when `incoming` is the next contiguous quota cursor and must be applied.
    /// `Ok(false)` when it is a duplicate replay. A claimed predecessor that is not exactly
    /// the stored cursor is a fork or a gap (`Resolution B8`) and is rejected.
    fn check_quota_cursor(
        origin_shard_id: u64,
        uid: u64,
        stored: (u64, u16),
        claimed_prev: (u64, u16),
        incoming: (u64, u16),
    ) -> Result<bool> {
        if incoming <= stored {
            return Ok(false);
        }
        if claimed_prev != stored {
            return Err(MetaDbError::AuxOutOfOrderGap {
                origin_shard_id,
                uid,
                expected_after: stored,
                got_prev: claimed_prev,
            });
        }
        Ok(true)
    }

    fn already_exists_for_user_key(user_key: &[u8]) -> MetaDbError {
        match MetaKey::decode(user_key) {
            Ok(mk) => MetaDbError::AlreadyExists {
                pid: mk.pid,
                name: mk.name,
            },
            Err(_) => MetaDbError::AlreadyExists {
                pid: 0,
                name: String::new(),
            },
        }
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

    fn intent_deltas(op: &IntentOp) -> &[AttrDeltaSpec] {
        match op {
            IntentOp::Put { attr_deltas, .. }
            | IntentOp::Insert { attr_deltas, .. }
            | IntentOp::Delete { attr_deltas, .. }
            | IntentOp::DeletingDir { attr_deltas, .. } => attr_deltas,
            IntentOp::DirExistsFenceSet { .. } => &[],
        }
    }

    /// Reserve positive capacity for prepared transactions. A pending deletion
    /// cannot release capacity until it commits, so other writers cannot overbook
    /// the directory while its transaction is waiting for a decision.
    fn validate_accounting(
        &self,
        engine: &LifecycleStorageEngine,
        deltas: &[AttrDeltaSpec],
        own_txn: Option<TxnId>,
    ) -> Result<()> {
        use crate::validation::checked_count;
        let mut sums: BTreeMap<InodeId, (i128, i128)> = BTreeMap::new();
        for delta in deltas {
            let sum = sums.entry(delta.pid).or_default();
            sum.0 += i128::from(delta.delta_children);
            sum.1 += i128::from(delta.delta_bytes);
        }
        if sums.is_empty() {
            return Ok(());
        }
        let mut reserved: BTreeMap<InodeId, (i128, i128)> = BTreeMap::new();
        for (_, raw) in Self::scan_latest_in_apply(engine, &[0x05], &[0x06])? {
            let rec = WriteIntentRecord::decode(&raw)?;
            if Some(rec.txn_id) == own_txn {
                continue;
            }
            for delta in Self::intent_deltas(&rec.op) {
                let sum = reserved.entry(delta.pid).or_default();
                sum.0 += i128::from(delta.delta_children.max(0));
                sum.1 += i128::from(delta.delta_bytes.max(0));
            }
        }
        for (pid, delta) in sums {
            let (children, bytes, baseline) = if let Some(raw) =
                Self::read_latest_in_apply(engine, &MetaKey::dir_attr(pid).encode())?
            {
                let attr = InodeAttr::decode(&raw)?;
                (attr.num_children, attr.size_bytes, attr.applied_lsn)
            } else if let Some(raw) =
                Self::read_latest_in_apply(engine, &DirShardDeltaKey { pid }.encode())?
            {
                let sub = DirShardDeltaRecord::decode(&raw)?;
                (sub.num_children, sub.size_bytes, sub.applied_raft_index)
            } else {
                return Err(MetaDbError::DirectoryNotFound(pid));
            };
            let pending = self.attr_agg.pending_totals_since(pid, baseline);
            let children = i128::from(children) + pending.0 + delta.0;
            let bytes = i128::from(bytes) + pending.1 + delta.1;
            checked_count(children)?;
            checked_count(bytes)?;
            let reserve = reserved.get(&pid).copied().unwrap_or_default();
            checked_count(children + reserve.0)?;
            checked_count(bytes + reserve.1)?;
        }
        Ok(())
    }

    fn validate_intent_value(
        &self,
        engine: &LifecycleStorageEngine,
        key: &[u8],
        op: &IntentOp,
    ) -> Result<()> {
        use crate::validation::{validate_new_directory, validate_size};
        if key.first().copied() != Some(metadb_core::TABLE_PREFIX_META) {
            return Ok(());
        }
        let mk = MetaKey::decode(key)?;
        if !mk.is_attr_key() {
            validate_user_entry_name(&mk.name)?;
        }
        if let IntentOp::Put { value, .. } = op {
            let attr = InodeAttr::decode(value)?;
            validate_size(attr.size_bytes)?;
            crate::validation::checked_count(i128::from(attr.num_children))?;
            if attr.entry_type == EntryType::Directory {
                validate_new_directory(mk.pid, attr.inode_id)?;
            } else if attr.entry_type == EntryType::DirectoryAttr {
                if mk.pid != attr.inode_id || !metadb_core::is_dir_inode_id(attr.inode_id) {
                    return Err(MetaDbError::CodecError(
                        "directory anchor inode mismatch".into(),
                    ));
                }
                if Self::read_latest_in_apply(engine, key)?.is_none() {
                    validate_new_directory(attr.parent_pid, attr.inode_id)?;
                    if Self::read_latest_in_apply(
                        engine,
                        &DirShardDeltaKey { pid: attr.inode_id }.encode(),
                    )?
                    .is_some()
                    {
                        return Err(MetaDbError::AlreadyExists {
                            pid: attr.inode_id,
                            name: metadb_core::ATTR_ENTRY_NAME.into(),
                        });
                    }
                }
            }
        }
        if let Some(raw) = Self::read_latest_in_apply(engine, key)? {
            validate_size(InodeAttr::decode(&raw)?.size_bytes)?;
        }
        Ok(())
    }

    /// Validate values and every derived counter before changing keys, deltas or
    /// the outbox. This also protects deterministic replay from malformed input.
    fn validate_command(&self, engine: &LifecycleStorageEngine, cmd: &TxRaftCmd) -> Result<()> {
        use crate::validation::{validate_new_directory, validate_size};
        let read_child = |pid, name: &str| -> Result<Option<InodeAttr>> {
            Self::read_latest_in_apply(engine, &MetaKey::child(pid, name).encode())?
                .map(|raw| InodeAttr::decode(&raw))
                .transpose()
        };
        let mut deltas = Vec::new();
        let mut own_txn = None;
        match cmd {
            TxRaftCmd::InitDirAttr {
                dir_id, parent_pid, ..
            } => {
                if !metadb_core::is_dir_inode_id(*dir_id)
                    || !metadb_core::is_dir_inode_id(*parent_pid)
                {
                    return Err(MetaDbError::CodecError("invalid directory inode".into()));
                }
            }
            TxRaftCmd::InitDirShardDelta { record } => {
                if !metadb_core::is_dir_inode_id(record.inode_id) {
                    return Err(MetaDbError::CodecError("invalid directory inode".into()));
                }
                validate_size(record.size_bytes)?;
                crate::validation::checked_count(i128::from(record.num_children))?;
            }
            TxRaftCmd::CreateFile {
                pid, size_bytes, ..
            } => {
                validate_size(*size_bytes)?;
                deltas.push(AttrDeltaSpec {
                    pid: *pid,
                    delta_children: 1,
                    delta_bytes: *size_bytes,
                });
            }
            TxRaftCmd::MkdirSingleShard {
                pid, new_dir_id, ..
            } => {
                validate_new_directory(*pid, *new_dir_id)?;
                let key = MetaKey::dir_attr(*new_dir_id).encode();
                if Self::read_latest_in_apply(engine, &key)?.is_some()
                    || Self::read_latest_in_apply(
                        engine,
                        &DirShardDeltaKey { pid: *new_dir_id }.encode(),
                    )?
                    .is_some()
                {
                    return Err(MetaDbError::AlreadyExists {
                        pid: *new_dir_id,
                        name: metadb_core::ATTR_ENTRY_NAME.into(),
                    });
                }
                deltas.push(AttrDeltaSpec {
                    pid: *pid,
                    delta_children: 1,
                    delta_bytes: 0,
                });
            }
            TxRaftCmd::Unlink { pid, name } => {
                if let Some(attr) = read_child(*pid, name)? {
                    validate_size(attr.size_bytes)?;
                    deltas.push(AttrDeltaSpec {
                        pid: *pid,
                        delta_children: -1,
                        delta_bytes: -attr.size_bytes,
                    });
                }
            }
            TxRaftCmd::RmdirSingleShard { pid, name } => {
                if read_child(*pid, name)?.is_some() {
                    deltas.push(AttrDeltaSpec {
                        pid: *pid,
                        delta_children: -1,
                        delta_bytes: 0,
                    });
                }
            }
            TxRaftCmd::SetAttrSingleShard { pid, name, patch } => {
                if let Some(size) = patch.size_bytes {
                    validate_size(size)?;
                }
                if let Some(attr) = read_child(*pid, name)? {
                    validate_size(attr.size_bytes)?;
                    if attr.entry_type != EntryType::File
                        && (patch.size_bytes.is_some() || patch.fid.is_some())
                    {
                        return Err(MetaDbError::CodecError(
                            "directory size and fid are derived attributes".into(),
                        ));
                    }
                    deltas.push(AttrDeltaSpec {
                        pid: *pid,
                        delta_children: 0,
                        delta_bytes: patch.size_bytes.unwrap_or(attr.size_bytes) - attr.size_bytes,
                    });
                }
            }
            TxRaftCmd::RenameSingleShard {
                src_pid,
                src_name,
                dst_pid,
                dst_name,
                ..
            } => {
                if let Some(attr) = read_child(*src_pid, src_name)? {
                    validate_size(attr.size_bytes)?;
                    if src_pid == dst_pid && src_name == dst_name {
                        return Ok(());
                    }
                    let victim = read_child(*dst_pid, dst_name)?;
                    if let Some(victim) = &victim {
                        validate_size(victim.size_bytes)?;
                    }
                    deltas.push(AttrDeltaSpec {
                        pid: *src_pid,
                        delta_children: -1,
                        delta_bytes: -attr.size_bytes,
                    });
                    deltas.push(AttrDeltaSpec {
                        pid: *dst_pid,
                        delta_children: i64::from(victim.is_none()),
                        delta_bytes: attr.size_bytes - victim.map(|v| v.size_bytes).unwrap_or(0),
                    });
                }
            }
            TxRaftCmd::Prepare2pc {
                txn_id, intents, ..
            } => {
                own_txn = Some(*txn_id);
                for (key, op) in intents {
                    self.validate_intent_value(engine, key, op)?;
                    deltas.extend_from_slice(Self::intent_deltas(op));
                }
            }
            TxRaftCmd::CommitSecondary2pc {
                txn_id,
                target_user_keys,
                ..
            } => {
                own_txn = Some(*txn_id);
                for key in target_user_keys {
                    if let Some(raw) = Self::read_latest_in_apply(
                        engine,
                        &WriteIntentKey::new(key.clone()).encode(),
                    )? {
                        let rec = WriteIntentRecord::decode(&raw)?;
                        if rec.txn_id == *txn_id {
                            self.validate_intent_value(engine, key, &rec.op)?;
                            deltas.extend_from_slice(Self::intent_deltas(&rec.op));
                        }
                    }
                }
            }
            TxRaftCmd::CommitPrimary2pc { txn_id, .. } => {
                own_txn = Some(*txn_id);
                for (ik, raw) in Self::scan_latest_in_apply(engine, &[0x05], &[0x06])? {
                    let rec = WriteIntentRecord::decode(&raw)?;
                    if rec.txn_id == *txn_id {
                        let key = WriteIntentKey::decode(&ik)?.target_user_key;
                        self.validate_intent_value(engine, &key, &rec.op)?;
                        deltas.extend_from_slice(Self::intent_deltas(&rec.op));
                    }
                }
            }
            _ => {}
        }
        self.validate_accounting(engine, &deltas, own_txn)
    }

    fn apply_tx_cmd(
        &self,
        raft_index: u64,
        commit_hlc: HlcTimestamp,
        cmd: TxRaftCmd,
        engine: &LifecycleStorageEngine,
    ) -> Result<TxRaftOutput> {
        self.validate_command(engine, &cmd)?;
        match cmd {
            TxRaftCmd::Prepare2pcChecked {
                txn_id,
                is_primary,
                primary_key,
                participant_keys,
                start_hlc,
                expire_hlc,
                rename_fence_token,
                intents,
                expected_values,
            } => {
                // Check every read before installing any intent or transaction record.
                // The intent then excludes mutations until commit or abort.
                for (user_key, expected) in expected_values {
                    self.ensure_user_key_in_range(&user_key)?;
                    if !intents.iter().any(|(key, _)| *key == user_key) {
                        return Err(MetaDbError::CodecError(
                            "2PC expected value must have a matching local intent".to_string(),
                        ));
                    }
                    if Self::read_latest_in_apply(engine, &user_key)? != expected {
                        return Err(MetaDbError::TransactionAborted(
                            "2PC value changed after coordinator read".to_string(),
                        ));
                    }
                }
                self.apply_tx_cmd(
                    raft_index,
                    commit_hlc,
                    TxRaftCmd::Prepare2pc {
                        txn_id,
                        is_primary,
                        primary_key,
                        participant_keys,
                        start_hlc,
                        expire_hlc,
                        rename_fence_token,
                        intents,
                    },
                    engine,
                )
            }
            TxRaftCmd::InitDirAttr {
                dir_id,
                parent_pid,
                uid,
                mode,
            } => {
                validate_dir_inode_id(dir_id)?;
                let key = MetaKey::dir_attr(dir_id).encode();
                self.ensure_user_key_in_range(&key)?;
                Self::ensure_no_conflicting_intent(engine, &key, false)?;
                if Self::read_latest_in_apply(engine, &key)?.is_some() {
                    return Err(MetaDbError::AlreadyExists {
                        pid: dir_id,
                        name: ATTR_ENTRY_NAME.to_string(),
                    });
                }
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
                validate_file_size(size_bytes)?;
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
                validate_dir_inode_id(new_dir_id)?;
                let child_key = MetaKey::child(pid, &name);
                let child_bytes = child_key.encode();
                let new_attr_bytes = MetaKey::dir_attr(new_dir_id).encode();
                self.ensure_user_key_in_range(&child_bytes)?;
                self.ensure_user_key_in_range(&new_attr_bytes)?;
                if Self::read_latest_in_apply(engine, &new_attr_bytes)?.is_some() {
                    return Err(MetaDbError::AlreadyExists {
                        pid: new_dir_id,
                        name: ATTR_ENTRY_NAME.to_string(),
                    });
                }
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
                let released = negate_accounted_size(attr.size_bytes);
                self.attr_agg
                    .record_delta(pid, raft_index, commit_hlc, -1, released);

                let mut aux = vec![AuxIntentSpec::QuotaDelta {
                    uid: attr.uid,
                    delta_bytes: released,
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
                    validate_file_size(sz)?;
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

                let delta_bytes =
                    saturating_add_i64(attr.size_bytes, negate_accounted_size(old_size));
                if delta_bytes != 0 {
                    self.attr_agg
                        .record_delta(pid, raft_index, commit_hlc, 0, delta_bytes);
                }

                let mut aux = Vec::new();
                if attr.uid != old_uid {
                    let inodes = 1;
                    aux.push(AuxIntentSpec::QuotaDelta {
                        uid: old_uid,
                        delta_bytes: negate_accounted_size(old_size),
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
                // Fail closed on deep chains; a 1024-step cap used to allow the rename.
                if src_attr.entry_type == EntryType::Directory && src_pid != dst_pid {
                    RenameCycleWalker::new(src_attr.inode_id, dst_pid).walk(|cursor| {
                        let anc_key = MetaKey::dir_attr(cursor).encode();
                        match Self::read_latest_in_apply(engine, &anc_key)? {
                            Some(anc_raw) => Ok(Some(InodeAttr::decode(&anc_raw)?.parent_pid)),
                            None => Ok(None),
                        }
                    })?;
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
                        delta_bytes: negate_accounted_size(victim.size_bytes),
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

                let src_accounted = src_attr.size_bytes.max(0);
                match (src_pid == dst_pid, victim_opt) {
                    (true, Some(victim)) => {
                        self.attr_agg.record_delta(
                            dst_pid,
                            raft_index,
                            commit_hlc,
                            -1,
                            negate_accounted_size(victim.size_bytes),
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
                            negate_accounted_size(src_accounted),
                        );
                        self.attr_agg.record_delta(
                            dst_pid,
                            raft_index,
                            commit_hlc,
                            0,
                            saturating_add_i64(
                                src_accounted,
                                negate_accounted_size(victim.size_bytes),
                            ),
                        );
                    }
                    (false, None) => {
                        self.attr_agg.record_delta(
                            src_pid,
                            raft_index,
                            commit_hlc,
                            -1,
                            negate_accounted_size(src_accounted),
                        );
                        self.attr_agg.record_delta(
                            dst_pid,
                            raft_index,
                            commit_hlc,
                            1,
                            src_accounted,
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
            TxRaftCmd::UpsertQuotaLimit {
                uid,
                insert_quota_limit_bytes,
                update_quota_limit_bytes,
            } => {
                let qk = QuotaKey { uid }.encode();
                self.ensure_user_key_in_range(&qk)?;
                let mut rec = match Self::read_latest_in_apply(engine, &qk)? {
                    Some(raw) => {
                        let mut rec = QuotaRecord::decode(&raw)?;
                        rec.quota_limit_bytes = update_quota_limit_bytes;
                        rec
                    }
                    None => QuotaRecord::new(uid, insert_quota_limit_bytes),
                };
                rec.last_applied_hlc = rec.last_applied_hlc.max(commit_hlc);
                engine.apply_committed_put(qk, rec.encode(), commit_hlc, raft_index);
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Quota(rec))
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
                // Reject gaps and range errors before any `apply_committed_put`, so a failed
                // batch does not leave a prefix of the command durable in the memtable.
                let mut quota_cursor: BTreeMap<(u64, u64), (u64, u16)> = BTreeMap::new();
                for item in &entries {
                    match &item.payload {
                        AuxTaskPayload::QuotaDelta {
                            uid,
                            prev_uid_cursor,
                            ..
                        } => {
                            let qk = QuotaKey { uid: *uid }.encode();
                            self.ensure_user_key_in_range(&qk)?;
                            let key = (item.origin_shard_id, *uid);
                            let prev = if let Some(cur) = quota_cursor.get(&key) {
                                *cur
                            } else {
                                match Self::read_latest_in_apply(engine, &qk)? {
                                    Some(raw) => QuotaRecord::decode(&raw)?
                                        .applied_by_shard
                                        .get(&item.origin_shard_id)
                                        .copied()
                                        .unwrap_or((0, 0)),
                                    None => (0, 0),
                                }
                            };
                            let cur_cursor = (item.origin_raft_index, item.task_ordinal);
                            if Self::check_quota_cursor(
                                item.origin_shard_id,
                                *uid,
                                prev,
                                *prev_uid_cursor,
                                cur_cursor,
                            )? {
                                quota_cursor.insert(key, cur_cursor);
                            }
                        }
                        AuxTaskPayload::IndexUpsert { fid, .. }
                        | AuxTaskPayload::IndexDelete { fid } => {
                            let ik = SecondaryIndexKey { fid: *fid }.encode();
                            self.ensure_user_key_in_range(&ik)?;
                        }
                    }
                }
                let mut staged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
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
                            let mut rec = match staged.get(&qk).cloned().map_or_else(
                                || Self::read_latest_in_apply(engine, &qk),
                                |raw| Ok(Some(raw)),
                            )? {
                                Some(raw) => QuotaRecord::decode(&raw)?,
                                None => QuotaRecord::new(uid, u64::MAX),
                            };
                            let prev = rec
                                .applied_by_shard
                                .get(&item.origin_shard_id)
                                .copied()
                                .unwrap_or((0, 0));
                            let cur_cursor = (item.origin_raft_index, item.task_ordinal);
                            if !Self::check_quota_cursor(
                                item.origin_shard_id,
                                uid,
                                prev,
                                prev_uid_cursor,
                                cur_cursor,
                            )? {
                                continue; // Exact-once duplicate replay suppressed.
                            }
                            // Keep the exact sum, including a temporary negative, so a
                            // later delta from another origin can cancel it. Overflow
                            // rejects the whole batch before anything in it is written.
                            // Readers clamp a negative balance to zero.
                            let new_bytes =
                                rec.used_bytes.checked_add(delta_bytes).ok_or_else(|| {
                                    MetaDbError::CodecError(format!(
                                        "quota byte delta overflow for uid {uid}"
                                    ))
                                })?;
                            let new_inodes =
                                rec.used_inodes.checked_add(delta_inodes).ok_or_else(|| {
                                    MetaDbError::CodecError(format!(
                                        "quota inode delta overflow for uid {uid}"
                                    ))
                                })?;
                            rec.used_bytes = new_bytes;
                            rec.used_inodes = new_inodes;
                            rec.last_applied_hlc = rec.last_applied_hlc.max(item.hlc);
                            rec.applied_by_shard
                                .insert(item.origin_shard_id, cur_cursor);
                            staged.insert(qk, rec.encode());
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
                            let existing_opt = match staged.get(&ik).cloned().map_or_else(
                                || Self::read_latest_in_apply(engine, &ik),
                                |raw| Ok(Some(raw)),
                            )? {
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
                            staged.insert(ik, new_rec.encode());
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
                            let mut rec = match staged.get(&ik).cloned().map_or_else(
                                || Self::read_latest_in_apply(engine, &ik),
                                |raw| Ok(Some(raw)),
                            )? {
                                Some(raw) => SecondaryIndexRecord::decode(&raw)?,
                                None => SecondaryIndexRecord {
                                    fid,
                                    pid: 0,
                                    name: String::new(),
                                    create_time: HlcTimestamp::ZERO,
                                    last_ver: (HlcTimestamp::ZERO, 0, 0, 0),
                                    tombstone_ver: (HlcTimestamp::ZERO, 0, 0, 0),
                                    is_deleted: true,
                                },
                            };
                            if incoming_ver <= rec.last_ver.max(rec.tombstone_ver) {
                                continue;
                            }
                            rec.tombstone_ver = incoming_ver;
                            rec.is_deleted = true;
                            staged.insert(ik, rec.encode());
                            applied_count += 1;
                        }
                    }
                }
                for (key, value) in staged {
                    engine.apply_committed_put(key, value, commit_hlc, raft_index);
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
                let mut hold_keys = Vec::with_capacity(intents.len());
                for (user_key, op) in intents {
                    hold_keys.push(user_key.clone());
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
                            let delta_key = DirShardDeltaKey { pid: dir_id }.encode();
                            if let Some(raw_sub) = Self::read_latest_in_apply(engine, &delta_key)? {
                                let mut sub = DirShardDeltaRecord::decode(&raw_sub)?;
                                if sub.deleting_fence {
                                    return Err(MetaDbError::WriteIntentConflict {
                                        conflicting_txn_id: 0,
                                        primary_key: primary_key.clone(),
                                        expire_hlc: expire_hlc.0,
                                    });
                                }
                                sub.deleting_fence = true;
                                encoded_writes.push((delta_key.to_vec(), sub.encode()));
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
                        IntentOp::Insert {
                            value,
                            attr_deltas,
                            aux_specs,
                        } => {
                            if let Some(ex) = &existing_intent {
                                if ex.txn_id != txn_id {
                                    return Err(MetaDbError::WriteIntentConflict {
                                        conflicting_txn_id: ex.txn_id,
                                        primary_key: ex.primary_key.clone(),
                                        expire_hlc: ex.expire_hlc.0,
                                    });
                                }
                            }
                            if Self::read_latest_in_apply(engine, &user_key)?.is_some() {
                                return Err(Self::already_exists_for_user_key(&user_key));
                            }
                            let rec = WriteIntentRecord {
                                txn_id,
                                primary_shard_hint: self.shard_id,
                                primary_key: primary_key.clone(),
                                start_hlc,
                                expire_hlc,
                                rename_fence_token,
                                op: IntentOp::Insert {
                                    value,
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
                self.install_lease_hold(
                    engine, txn_id, start_hlc, expire_hlc, commit_hlc, &hold_keys,
                );
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
                    self.release_lease_hold(engine, txn_id);
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
                self.release_lease_hold(engine, txn_id);
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
                for key in &target_user_keys {
                    self.ensure_user_key_in_range(key)?;
                }
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
                for key in &target_user_keys {
                    self.ensure_user_key_in_range(key)?;
                }
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
                self.release_lease_hold(engine, txn_id);
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
                    self.release_lease_hold(engine, txn_id);
                }
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::TxnStatus(Some(txn_rec.state)))
            }
            TxRaftCmd::Extend2pcLease {
                txn_id,
                coord_node_id,
                expire_hlc,
                start_hlc,
                rename_fence_token,
                target_user_keys,
            } => {
                self.apply_extend_lease(
                    engine,
                    raft_index,
                    commit_hlc,
                    txn_id,
                    coord_node_id,
                    expire_hlc,
                    start_hlc,
                    rename_fence_token,
                    &target_user_keys,
                )?;
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Unit)
            }
            TxRaftCmd::FenceShardRangeForSplit {
                new_range,
                new_epoch,
            } => {
                // This check shares Raft's serialized apply boundary with the
                // fence. Prepare cannot race between the worker's scan and fence.
                if !Self::scan_latest_in_apply(engine, &[0x05], &[0x07])?.is_empty() {
                    return Err(MetaDbError::SplitDeferred(
                        "all intents and decisions must be drained before fencing".into(),
                    ));
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
            TxRaftCmd::UpdateShardRangeOnSplit {
                new_range,
                new_epoch,
                keys_to_delete,
                updated_anchor_attr,
                updated_shard_delta,
            } => {
                // The worker's partition snapshot can precede further writes on
                // the retained range. Rebuild its accounting from the serialized
                // apply view, and preserve current metadata before any mutation.
                let updated_anchor_attr = updated_anchor_attr
                    .map(|(pid, supplied)| {
                        let key = MetaKey::dir_attr(pid).encode();
                        let mut attr = Self::read_latest_in_apply(engine, &key)?
                            .map(|raw| InodeAttr::decode(&raw))
                            .transpose()?
                            .unwrap_or(supplied);
                        let (children, bytes, mtime) = self.retained_dir_stats(
                            engine,
                            &new_range,
                            pid,
                            attr.modify_time,
                            raft_index,
                        )?;
                        attr.num_children = children;
                        attr.size_bytes = bytes;
                        attr.modify_time = mtime;
                        Ok::<_, MetaDbError>((pid, attr))
                    })
                    .transpose()?;
                let updated_shard_delta = updated_shard_delta
                    .map(|supplied| {
                        let key = DirShardDeltaKey {
                            pid: supplied.inode_id,
                        }
                        .encode();
                        let mut sub = Self::read_latest_in_apply(engine, &key)?
                            .map(|raw| DirShardDeltaRecord::decode(&raw))
                            .transpose()?
                            .unwrap_or(supplied);
                        let (children, bytes, mtime) = self.retained_dir_stats(
                            engine,
                            &new_range,
                            sub.inode_id,
                            sub.modify_time,
                            raft_index,
                        )?;
                        sub.num_children = children;
                        sub.size_bytes = bytes;
                        sub.modify_time = mtime;
                        Ok::<_, MetaDbError>(sub)
                    })
                    .transpose()?;
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
            TxRaftCmd::RestoreShardRange {
                restored_range,
                new_epoch,
            } => {
                {
                    let mut m = self.meta.write().unwrap();
                    m.range = restored_range;
                    m.epoch = new_epoch;
                }
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Unit)
            }
            TxRaftCmd::BindNodeInstance {
                node_id,
                instance_id,
                grpc_addr,
            } => {
                let key = node_instance_key(node_id);
                if let Some(raw) = Self::read_latest_in_apply(engine, &key)? {
                    let existing: NodeInstanceRecord = codec::deserialize(&raw)
                        .map_err(|error| MetaDbError::CodecError(error.to_string()))?;
                    if existing.instance_id != instance_id {
                        return Err(MetaDbError::IdentityRejected(format!(
                            "node {node_id} is already bound to instance {:#x}; refusing a reused node identity",
                            existing.instance_id
                        )));
                    }
                }
                let record = NodeInstanceRecord {
                    instance_id,
                    grpc_addr: grpc_addr.clone(),
                };
                let bytes = codec::serialize(&record)
                    .map_err(|error| MetaDbError::CodecError(error.to_string()))?;
                engine.apply_committed_put(key, bytes, commit_hlc, raft_index);
                if let Some(bindings) = self.instance_bindings.lock().unwrap().clone() {
                    bindings
                        .lock()
                        .unwrap()
                        .insert(node_id, (instance_id, grpc_addr));
                }
                self.outbox
                    .record_entry_specs(raft_index, commit_hlc, vec![]);
                Ok(TxRaftOutput::Unit)
            }
        }
    }

    fn retained_dir_stats(
        &self,
        engine: &LifecycleStorageEngine,
        range: &CompositeKeyRange,
        pid: InodeId,
        base_mtime: HlcTimestamp,
        raft_index: u64,
    ) -> Result<(i64, i64, HlcTimestamp)> {
        let mut children = 0i64;
        let mut bytes = 0i64;
        let mut mtime = base_mtime.max(self.attr_agg.latest_delta_hlc(pid, raft_index));
        for (key, raw) in Self::scan_latest_in_apply(
            engine,
            &MetaKey::encode_pid_prefix_start(pid),
            &MetaKey::encode_pid_prefix_end(pid),
        )? {
            if !range.contains_key(&key) || MetaKey::decode(&key)?.is_attr_key() {
                continue;
            }
            let child = InodeAttr::decode(&raw)?;
            children = children.checked_add(1).ok_or_else(|| {
                MetaDbError::CodecError("directory child count overflow during split".into())
            })?;
            bytes = bytes.checked_add(child.size_bytes).ok_or_else(|| {
                MetaDbError::CodecError("directory byte count overflow during split".into())
            })?;
            mtime = mtime.max(child.modify_time);
        }
        Ok((children, bytes, mtime))
    }

    fn lease_cap_ms(start_hlc: HlcTimestamp) -> Result<u64> {
        start_hlc
            .physical_ms()
            .checked_add(MAX_2PC_LEASE_LIFETIME_MS)
            .ok_or_else(|| {
                MetaDbError::TransactionAborted("2PC lease renewal exceeds lifetime cap".into())
            })
    }

    fn install_lease_hold(
        &self,
        engine: &LifecycleStorageEngine,
        txn_id: TxnId,
        start_hlc: HlcTimestamp,
        expire_hlc: HlcTimestamp,
        commit_hlc: HlcTimestamp,
        keys: &[Vec<u8>],
    ) {
        let remain_ms = expire_hlc
            .physical_ms()
            .saturating_sub(commit_hlc.physical_ms())
            .max(1);
        engine.mvcc().snapshot_registry().renew_lease_hold(
            txn_id,
            start_hlc,
            std::time::Instant::now() + std::time::Duration::from_millis(remain_ms),
            keys,
        );
    }

    fn release_lease_hold(&self, engine: &LifecycleStorageEngine, txn_id: TxnId) {
        engine.mvcc().snapshot_registry().release_lease_hold(txn_id);
    }

    /// Renews a pending lease. Reads the durable record and intents at apply time,
    /// so an abort that Raft already ordered ahead of this command wins.
    #[allow(clippy::too_many_arguments)]
    fn apply_extend_lease(
        &self,
        engine: &LifecycleStorageEngine,
        raft_index: u64,
        commit_hlc: HlcTimestamp,
        txn_id: TxnId,
        coord_node_id: u64,
        expire_hlc: HlcTimestamp,
        start_hlc: HlcTimestamp,
        rename_fence_token: Option<u64>,
        target_user_keys: &[Vec<u8>],
    ) -> Result<()> {
        if (txn_id >> 64) as u64 != coord_node_id {
            return Err(MetaDbError::TransactionAborted(
                "2PC lease renewal is not held by this coordinator".into(),
            ));
        }
        for key in target_user_keys {
            self.ensure_user_key_in_range(key)?;
        }

        let record_key = TxnRecordKey { txn_id }.encode().to_vec();
        let existing_record = Self::read_latest_in_apply(engine, &record_key)?
            .map(|raw| TxnStatusRecord::decode(&raw))
            .transpose()?;

        let mut record_to_write: Option<TxnStatusRecord> = None;
        let mut pending_anchor = false;
        if let Some(rec) = &existing_record {
            match rec.state {
                TxnState::Aborted => {
                    return Err(MetaDbError::TransactionAborted(
                        "2PC lease renewal cannot revive an aborted transaction".into(),
                    ));
                }
                // Commit already won. Expiry no longer decides the transaction.
                TxnState::Committed { .. } => return Ok(()),
                TxnState::Pending => {
                    if rec.rename_fence_token != rename_fence_token || rec.start_hlc != start_hlc {
                        return Err(MetaDbError::TransactionAborted(
                            "2PC lease renewal fence token or start mismatch".into(),
                        ));
                    }
                    // Re-read of the durable deadline. A concurrent abort is ordered
                    // by Raft; if it applied first, the state match above rejected us.
                    if commit_hlc > rec.expire_hlc {
                        return Err(MetaDbError::TransactionAborted(
                            "2PC lease renewal rejected after expiry".into(),
                        ));
                    }
                    let cap = Self::lease_cap_ms(rec.start_hlc)?;
                    if expire_hlc.physical_ms() > cap {
                        return Err(MetaDbError::TransactionAborted(
                            "2PC lease renewal exceeds lifetime cap".into(),
                        ));
                    }
                    pending_anchor = true;
                    if expire_hlc > rec.expire_hlc {
                        let mut updated = rec.clone();
                        updated.expire_hlc = expire_hlc;
                        record_to_write = Some(updated);
                    }
                }
            }
        }

        let mut intent_writes: Vec<(Vec<u8>, WriteIntentRecord)> = Vec::new();
        let mut saw_participant = pending_anchor;
        for user_key in target_user_keys {
            let intent_key = WriteIntentKey::new(user_key.clone()).encode();
            let Some(raw) = Self::read_latest_in_apply(engine, &intent_key)? else {
                continue;
            };
            let mut intent = WriteIntentRecord::decode(&raw)?;
            let mut write_intent = false;
            if let IntentOp::DirExistsFenceSet { holders } = &mut intent.op {
                let Some((primary_key, holder_expire)) = holders.get(&txn_id).cloned() else {
                    continue;
                };
                if commit_hlc > holder_expire {
                    return Err(MetaDbError::TransactionAborted(
                        "2PC lease renewal rejected after expiry".into(),
                    ));
                }
                let cap = Self::lease_cap_ms(start_hlc)?;
                if expire_hlc.physical_ms() > cap {
                    return Err(MetaDbError::TransactionAborted(
                        "2PC lease renewal exceeds lifetime cap".into(),
                    ));
                }
                saw_participant = true;
                if expire_hlc > holder_expire {
                    holders.insert(txn_id, (primary_key, expire_hlc));
                    write_intent = true;
                }
            } else if intent.txn_id == txn_id {
                if intent.rename_fence_token != rename_fence_token || intent.start_hlc != start_hlc
                {
                    return Err(MetaDbError::TransactionAborted(
                        "2PC lease renewal fence token or start mismatch".into(),
                    ));
                }
                if commit_hlc > intent.expire_hlc {
                    return Err(MetaDbError::TransactionAborted(
                        "2PC lease renewal rejected after expiry".into(),
                    ));
                }
                let cap = Self::lease_cap_ms(intent.start_hlc)?;
                if expire_hlc.physical_ms() > cap {
                    return Err(MetaDbError::TransactionAborted(
                        "2PC lease renewal exceeds lifetime cap".into(),
                    ));
                }
                saw_participant = true;
                if expire_hlc > intent.expire_hlc {
                    intent.expire_hlc = expire_hlc;
                    write_intent = true;
                }
            }
            if write_intent {
                if intent.txn_id == txn_id && expire_hlc > intent.expire_hlc {
                    intent.expire_hlc = expire_hlc;
                }
                intent_writes.push((intent_key, intent));
            }
        }

        if !saw_participant {
            return Err(MetaDbError::TransactionAborted(
                "2PC lease renewal found no live transaction".into(),
            ));
        }

        if let Some(updated) = record_to_write {
            engine.apply_committed_put(record_key, updated.encode(), commit_hlc, raft_index);
        }
        for (key, intent) in intent_writes {
            engine.apply_committed_put(key, intent.encode(), commit_hlc, raft_index);
        }
        self.install_lease_hold(
            engine,
            txn_id,
            start_hlc,
            expire_hlc,
            commit_hlc,
            target_user_keys,
        );
        Ok(())
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
        let create_only = matches!(rec.op, IntentOp::Insert { .. });
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
            }
            | IntentOp::Insert {
                value,
                attr_deltas,
                aux_specs,
            } => {
                if create_only && Self::read_latest_in_apply(engine, user_key)?.is_some() {
                    return Err(Self::already_exists_for_user_key(user_key));
                }
                // Stamp `modify_time = commit_hlc` if `value` is an 88-byte `InodeAttr`.
                let final_val = if let Ok(mut decoded_attr) = InodeAttr::decode(&value) {
                    decoded_attr.modify_time = commit_hlc;
                    if decoded_attr.entry_type == EntryType::DirectoryAttr {
                        if let Some(raw) = Self::read_latest_in_apply(engine, user_key)? {
                            let current = InodeAttr::decode(&raw)?;
                            // Metadata-only 2PC must retain the current aggregation
                            // baseline, including flushes since Prepare. Pending
                            // deltas remain above this frontier for read/flush folding.
                            decoded_attr.num_children = current.num_children;
                            decoded_attr.size_bytes = current.size_bytes;
                            decoded_attr.applied_lsn = current.applied_lsn;
                        } else {
                            decoded_attr.applied_lsn = raft_index;
                        }
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
                let canonical_attr_key = MetaKey::dir_attr(dir_id).encode();
                if user_key == canonical_attr_key.as_slice() {
                    engine.apply_committed_delete(canonical_attr_key, commit_hlc, raft_index);
                }
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
            IntentOp::DeletingDir { dir_id, .. } if rec.txn_id == txn_id => {
                let delta_key = DirShardDeltaKey { pid: dir_id }.encode();
                if let Some(raw_sub) = Self::read_latest_in_apply(engine, &delta_key)? {
                    let mut sub = DirShardDeltaRecord::decode(&raw_sub)?;
                    if sub.deleting_fence {
                        sub.deleting_fence = false;
                        engine.apply_committed_put(
                            delta_key.to_vec(),
                            sub.encode(),
                            hlc,
                            raft_index,
                        );
                    }
                }
                engine.apply_committed_delete(ik, hlc, raft_index);
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

    /// After log purge, replay will not see the original Prepare. Pending leases
    /// still live in the snapshot, so rebuild their MVCC holds from that state.
    fn rehydrate_lease_holds(&self, engine: &LifecycleStorageEngine) {
        let latest = engine.mvcc().latest_watermarks().1;
        let Ok(rows) = Self::scan_latest_in_apply(engine, &[0x06], &[0x07]) else {
            return;
        };
        for (_, raw) in rows {
            let Ok(rec) = TxnStatusRecord::decode(&raw) else {
                continue;
            };
            if matches!(rec.state, TxnState::Pending) && latest <= rec.expire_hlc {
                self.install_lease_hold(
                    engine,
                    rec.txn_id,
                    rec.start_hlc,
                    rec.expire_hlc,
                    latest,
                    &rec.participant_keys,
                );
            }
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
        codec::serialize(&*self.meta.read().unwrap()).unwrap_or_default()
    }

    fn previously_committed_proposal(
        &self,
        request: &MetaDbRaftRequest,
        engine: &LifecycleStorageEngine,
        commit_hlc: HlcTimestamp,
    ) -> Result<Option<Vec<u8>>> {
        let (proposal_id, _) = decode_proposed_command(&request.payload)?;
        committed_proposal_result(engine, proposal_id, commit_hlc)
    }

    fn apply_command(
        &self,
        raft_index: u64,
        commit_hlc: HlcTimestamp,
        request: &MetaDbRaftRequest,
        engine: &LifecycleStorageEngine,
    ) -> Result<Vec<u8>> {
        maintain_proposal_receipts(engine, commit_hlc, raft_index)?;
        let (proposal_id, cmd) = decode_proposed_command(&request.payload)?;
        if let Some(prior) = committed_proposal_result(engine, proposal_id, commit_hlc)? {
            return Ok(prior);
        }
        let out = self.apply_tx_cmd(raft_index, commit_hlc, cmd, engine)?;
        self.outbox
            .persist_quota_cursors(engine, commit_hlc, raft_index);
        let bytes = codec::serialize(&out).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        if proposal_id != 0 {
            engine.apply_committed_put(
                proposal_receipt_key(proposal_id),
                encode_receipt(commit_hlc, &bytes),
                commit_hlc,
                raft_index,
            );
            engine.apply_committed_put(
                expiry_index_key(commit_hlc, proposal_id),
                Vec::new(),
                commit_hlc,
                raft_index,
            );
        }
        Ok(bytes)
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
                codec::deserialize::<PersistedShardRouteMeta>(&checkpoint.descriptor_bytes)
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
        self.rehydrate_lease_holds(engine);
    }
}
