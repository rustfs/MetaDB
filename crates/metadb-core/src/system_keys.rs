//! Internal shard-local system keyspaces (`0x04..0x07`) and split `affinity_key` partitioning.
//!
//! Disjoint 1-byte prefixes inside each shard's `LsmStorageBackend`:
//! - `0x04`: `DirShardDelta` (`DualModeFineGrained` non-anchor directory partition stats & lease)
//! - `0x05`: `WriteIntent` (Raft-replicated 2PC participant locks)
//! - `0x06`: `TxnRecord` (Primary-Anchored 2PC commit/abort source of truth)
//! - `0x07`: `SpilledAuxOutbox` (`[0x07][origin_raft_index: 8B BE][task_ordinal: 2B BE]`)
//! - `0x08`: `OriginQuotaCursor` (`[0x08][uid: 8B BE]`), retained after the task is acknowledged
//! - `0x0A`: committed write-proposal receipt (`[0x0A][proposal_id: 16B BE]`). A receipt is
//!   deleted once a later committed HLC is more than 10 minutes ahead of the receipt's HLC.
//! - `0x0D`: 2PC lease pre-image (`[0x0D][txn_id: 16B BE][user_key]`). The value is the
//!   prepared key at `start_hlc`, version 1 plus CRC32C. It is dropped when that lease ends.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::checksum::{append_crc32c_be, verify_crc32c_be};
use crate::epoch::TxnId;
use crate::error::{MetaDbError, Result};
use crate::hlc::HlcTimestamp;
use crate::schema::{
    FileId, InodeId, MetaKey, ShardId, UserId, SYSTEM_PREFIX_DIR_SHARD_DELTA,
    SYSTEM_PREFIX_LEASE_PREIMAGE, SYSTEM_PREFIX_QUOTA_ORIGIN_CURSOR,
    SYSTEM_PREFIX_SPILLED_AUX_OUTBOX, SYSTEM_PREFIX_TXN_RECORD, SYSTEM_PREFIX_WRITE_INTENT,
    TABLE_PREFIX_META, TABLE_PREFIX_SECONDARY_INDEX,
};

const SYSTEM_CODEC_V1: u8 = 1;
const SYSTEM_CODEC_V2: u8 = 2;

/// Key of `DirShardDelta` (`[0x04][pid: 8B BE]`).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct DirShardDeltaKey {
    pub pid: InodeId,
}

impl DirShardDeltaKey {
    pub fn encode(&self) -> [u8; 9] {
        let mut buf = [0u8; 9];
        buf[0] = SYSTEM_PREFIX_DIR_SHARD_DELTA;
        buf[1..9].copy_from_slice(&self.pid.to_be_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 9 || bytes[0] != SYSTEM_PREFIX_DIR_SHARD_DELTA {
            return Err(MetaDbError::CodecError(
                "invalid DirShardDeltaKey".to_string(),
            ));
        }
        Ok(Self {
            pid: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
        })
    }
}

/// Value of `DirShardDelta` stored on non-anchor `DualModeFineGrained` sub-shards of `pid`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct DirShardDeltaRecord {
    pub inode_id: InodeId,
    pub parent_pid: InodeId,
    pub uid: UserId,
    pub mode: u32,
    pub create_time: HlcTimestamp,
    pub num_children: i64,
    pub size_bytes: i64,
    pub modify_time: HlcTimestamp,
    pub applied_raft_index: u64,
    pub deleting_fence: bool,
}

impl DirShardDeltaRecord {
    pub fn encode(&self) -> Vec<u8> {
        let payload = crate::codec::serialize(self).expect("DirShardDeltaRecord serialize");
        let mut out = Vec::with_capacity(1 + payload.len() + 4);
        out.push(SYSTEM_CODEC_V1);
        out.extend_from_slice(&payload);
        append_crc32c_be(&mut out);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body = verify_crc32c_be(bytes, "DirShardDeltaRecord")?;
        if body.is_empty() || body[0] != SYSTEM_CODEC_V1 {
            return Err(MetaDbError::CodecError(
                "unsupported DirShardDeltaRecord codec version".to_string(),
            ));
        }
        crate::codec::deserialize(&body[1..]).map_err(|e| MetaDbError::CodecError(e.to_string()))
    }
}

/// Parent directory attribute delta embedded in a 2PC `WriteIntent`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct AttrDeltaSpec {
    pub pid: InodeId,
    pub delta_children: i64,
    pub delta_bytes: i64,
}

/// Auxiliary update payload emitted into `AuxOutbox` / `[0x07] SpilledAuxOutbox`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub enum AuxTaskPayload {
    QuotaDelta {
        uid: UserId,
        delta_bytes: i64,
        delta_inodes: i64,
        /// Exact previous `(origin_raft_index, task_ordinal)` emitted by `origin_shard_id` for `uid`.
        prev_uid_cursor: (u64, u16),
    },
    IndexUpsert {
        fid: FileId,
        pid: InodeId,
        name: String,
        create_time: HlcTimestamp,
    },
    IndexDelete {
        fid: FileId,
    },
}

/// Auxiliary task blueprint carried inside a 2PC `WriteIntent` before `commit_hlc` is assigned.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub enum AuxIntentSpec {
    QuotaDelta {
        uid: UserId,
        delta_bytes: i64,
        delta_inodes: i64,
    },
    IndexUpsert {
        fid: FileId,
        pid: InodeId,
        name: String,
        create_time: HlcTimestamp,
    },
    IndexDelete {
        fid: FileId,
    },
}

/// Lock operation stored inside `[0x05][target_user_key]`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub enum IntentOp {
    /// Exclusive write lock proposing a key upsert. Overwrites an existing value.
    Put {
        value: Vec<u8>,
        attr_deltas: Vec<AttrDeltaSpec>,
        aux_specs: Vec<AuxIntentSpec>,
    },
    /// Exclusive write lock proposing a key deletion.
    Delete {
        attr_deltas: Vec<AttrDeltaSpec>,
        aux_specs: Vec<AuxIntentSpec>,
    },
    /// Exclusive directory deletion lock placed on `(dir_id, "_attr_")` or `[0x04][dir_id]` during `Rmdir`.
    DeletingDir {
        dir_id: InodeId,
        attr_deltas: Vec<AttrDeltaSpec>,
        aux_specs: Vec<AuxIntentSpec>,
    },
    /// Shared multi-transaction existence fence on `(dst_pid, "_attr_")` conflicting ONLY with `DeletingDir`.
    DirExistsFenceSet {
        /// Map from `txn_id` -> `(primary_key, expire_hlc)` for concurrent cross-shard `Rename`/`Mkdir`.
        ///
        /// This variant has no per-holder start. Only the record's own `txn_id` has a
        /// start, and it is `WriteIntentRecord::start_hlc`. Other holders are unknown.
        holders: BTreeMap<TxnId, (Vec<u8>, HlcTimestamp)>,
    },
    /// Exclusive create. Prepare and commit both reject the intent when `user_key` already exists.
    ///
    /// Appended so the bincode tags of `Put`, `Delete`, `DeletingDir`, and `DirExistsFenceSet` stay stable.
    Insert {
        value: Vec<u8>,
        attr_deltas: Vec<AttrDeltaSpec>,
        aux_specs: Vec<AuxIntentSpec>,
    },
    /// Shared fence that stores each holder's prepare `start_hlc`.
    ///
    /// Appended after `Insert` so the tags of `Put`, `Delete`, `DeletingDir`,
    /// `DirExistsFenceSet`, and `Insert` stay stable. `start_hlc: None` is an
    /// older holder whose start was not stored. It is not a timestamp to invent.
    DirExistsFenceV2 {
        holders: BTreeMap<TxnId, FenceHolder>,
    },
}

/// One holder of a shared directory-existence fence.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct FenceHolder {
    pub primary_key: Vec<u8>,
    pub expire_hlc: HlcTimestamp,
    /// `None` means the start was not durable. Do not substitute `expire - 60s`.
    pub start_hlc: Option<HlcTimestamp>,
}

impl IntentOp {
    pub fn is_dir_exists_fence(&self) -> bool {
        matches!(
            self,
            IntentOp::DirExistsFenceSet { .. } | IntentOp::DirExistsFenceV2 { .. }
        )
    }
}

impl WriteIntentRecord {
    /// Fence holders with a start only where one was actually stored.
    ///
    /// For [`IntentOp::DirExistsFenceSet`], the record's own `txn_id` uses
    /// `WriteIntentRecord::start_hlc`. Every other holder stays `None`.
    pub fn resolved_fence_holders(&self) -> Option<BTreeMap<TxnId, FenceHolder>> {
        match &self.op {
            IntentOp::DirExistsFenceSet { holders } => Some(
                holders
                    .iter()
                    .map(|(txn_id, (primary_key, expire_hlc))| {
                        let start_hlc = (*txn_id == self.txn_id).then_some(self.start_hlc);
                        (
                            *txn_id,
                            FenceHolder {
                                primary_key: primary_key.clone(),
                                expire_hlc: *expire_hlc,
                                start_hlc,
                            },
                        )
                    })
                    .collect(),
            ),
            IntentOp::DirExistsFenceV2 { holders } => Some(holders.clone()),
            _ => None,
        }
    }
}

/// `true` when this `0x0D` row must be deleted.
///
/// A row is an orphan when it does not decode, or when its transaction is not in
/// `live_txns`. A live holder or pending record keeps the row. This does not
/// invent a pre-image and does not wait for tombstone compaction.
pub fn lease_preimage_is_orphan(
    storage_key: &[u8],
    raw: &[u8],
    live_txns: &BTreeSet<TxnId>,
) -> bool {
    if storage_key.first().copied() != Some(SYSTEM_PREFIX_LEASE_PREIMAGE) {
        return false;
    }
    match decode_lease_preimage(storage_key, raw) {
        Some((txn_id, _, _, _)) => !live_txns.contains(&txn_id),
        None => true,
    }
}

/// A live `0x0D` row blocks a split. A corrupt row is fatal. Absence is success.
pub fn lease_preimage_blocks_split(storage_key: &[u8], raw: &[u8]) -> Result<()> {
    if storage_key.first().copied() != Some(SYSTEM_PREFIX_LEASE_PREIMAGE) {
        return Ok(());
    }
    match decode_lease_preimage(storage_key, raw) {
        Some(_) => Err(MetaDbError::SplitDeferred(
            "live 2PC lease pre-image must be cleared before fencing".to_string(),
        )),
        None => Err(MetaDbError::CodecError(
            "corrupt 2PC lease pre-image blocks split".to_string(),
        )),
    }
}

/// Key of `WriteIntent`: `[0x05][target_user_key]`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct WriteIntentKey {
    pub target_user_key: Vec<u8>,
}

impl WriteIntentKey {
    pub fn new(target_user_key: Vec<u8>) -> Self {
        Self { target_user_key }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.target_user_key.len());
        out.push(SYSTEM_PREFIX_WRITE_INTENT);
        out.extend_from_slice(&self.target_user_key);
        out
    }

    /// Encodes `[0x05] || bound_user_key` for scanning intents in `[split_key, end_key)` (Resolution B14).
    pub fn encode_range_bound(bound_user_key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + bound_user_key.len());
        out.push(SYSTEM_PREFIX_WRITE_INTENT);
        out.extend_from_slice(bound_user_key);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 2 || bytes[0] != SYSTEM_PREFIX_WRITE_INTENT {
            return Err(MetaDbError::CodecError(
                "invalid WriteIntentKey".to_string(),
            ));
        }
        Ok(Self {
            target_user_key: bytes[1..].to_vec(),
        })
    }
}

/// Value stored at `[0x05][target_user_key]`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct WriteIntentRecord {
    pub txn_id: TxnId,
    pub primary_shard_hint: ShardId,
    pub primary_key: Vec<u8>,
    pub start_hlc: HlcTimestamp,
    pub expire_hlc: HlcTimestamp,
    pub rename_fence_token: Option<u64>,
    pub op: IntentOp,
}

impl WriteIntentRecord {
    pub fn encode(&self) -> Vec<u8> {
        let payload = crate::codec::serialize(self).expect("WriteIntentRecord serialize");
        let mut out = Vec::with_capacity(1 + payload.len() + 4);
        out.push(SYSTEM_CODEC_V1);
        out.extend_from_slice(&payload);
        append_crc32c_be(&mut out);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body = verify_crc32c_be(bytes, "WriteIntentRecord")?;
        if body.is_empty() || body[0] != SYSTEM_CODEC_V1 {
            return Err(MetaDbError::CodecError(
                "unsupported WriteIntentRecord codec version".to_string(),
            ));
        }
        crate::codec::deserialize(&body[1..]).map_err(|e| MetaDbError::CodecError(e.to_string()))
    }
}

/// Key of `TxnRecord` on the Primary Shard: `[0x06][txn_id: 16B BE]`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct TxnRecordKey {
    pub txn_id: TxnId,
}

impl TxnRecordKey {
    pub fn encode(&self) -> [u8; 17] {
        let mut out = [0u8; 17];
        out[0] = SYSTEM_PREFIX_TXN_RECORD;
        out[1..17].copy_from_slice(&self.txn_id.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 17 || bytes[0] != SYSTEM_PREFIX_TXN_RECORD {
            return Err(MetaDbError::CodecError("invalid TxnRecordKey".to_string()));
        }
        Ok(Self {
            txn_id: u128::from_be_bytes(bytes[1..17].try_into().unwrap()),
        })
    }
}

/// Durable decision state of a Primary-Anchored 2PC transaction.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub enum TxnState {
    Pending,
    Committed { commit_hlc: HlcTimestamp },
    Aborted,
}

/// Value stored at `[0x06][txn_id: 16B BE]`.
/// Retained until `pending_ack_keys` becomes empty (Resolution B12).
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct TxnStatusRecord {
    pub txn_id: TxnId,
    pub primary_key: Vec<u8>,
    pub participant_keys: Vec<Vec<u8>>,
    pub pending_ack_keys: BTreeSet<Vec<u8>>,
    pub start_hlc: HlcTimestamp,
    pub expire_hlc: HlcTimestamp,
    pub rename_fence_token: Option<u64>,
    pub state: TxnState,
}

impl TxnStatusRecord {
    pub fn encode(&self) -> Vec<u8> {
        let payload = crate::codec::serialize(self).expect("TxnStatusRecord serialize");
        let mut out = Vec::with_capacity(1 + payload.len() + 4);
        out.push(SYSTEM_CODEC_V1);
        out.extend_from_slice(&payload);
        append_crc32c_be(&mut out);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body = verify_crc32c_be(bytes, "TxnStatusRecord")?;
        if body.is_empty() || body[0] != SYSTEM_CODEC_V1 {
            return Err(MetaDbError::CodecError(
                "unsupported TxnStatusRecord codec version".to_string(),
            ));
        }
        crate::codec::deserialize(&body[1..]).map_err(|e| MetaDbError::CodecError(e.to_string()))
    }
}

/// Key of `SpilledAuxOutbox`: `[0x07][origin_raft_index: 8B BE][task_ordinal: 2B BE]` (11 bytes, Resolution B7).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct SpilledAuxOutboxKey {
    pub origin_raft_index: u64,
    pub task_ordinal: u16,
}

impl SpilledAuxOutboxKey {
    pub fn new(origin_raft_index: u64, task_ordinal: u16) -> Self {
        Self {
            origin_raft_index,
            task_ordinal,
        }
    }

    pub fn encode(&self) -> [u8; 11] {
        let mut out = [0u8; 11];
        out[0] = SYSTEM_PREFIX_SPILLED_AUX_OUTBOX;
        out[1..9].copy_from_slice(&self.origin_raft_index.to_be_bytes());
        out[9..11].copy_from_slice(&self.task_ordinal.to_be_bytes());
        out
    }

    pub fn prefix_start(after_raft_index: u64) -> Vec<u8> {
        match after_raft_index.checked_add(1) {
            Some(next_idx) => Self::new(next_idx, 0).encode().to_vec(),
            None => vec![SYSTEM_PREFIX_SPILLED_AUX_OUTBOX + 1],
        }
    }

    pub const fn prefix_end() -> [u8; 1] {
        [SYSTEM_PREFIX_SPILLED_AUX_OUTBOX + 1]
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 11 || bytes[0] != SYSTEM_PREFIX_SPILLED_AUX_OUTBOX {
            return Err(MetaDbError::CodecError(
                "invalid SpilledAuxOutboxKey".to_string(),
            ));
        }
        Ok(Self {
            origin_raft_index: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
            task_ordinal: u16::from_be_bytes(bytes[9..11].try_into().unwrap()),
        })
    }
}

/// Last quota cursor emitted by an origin shard for one user (`[0x08][uid: 8B BE]`).
///
/// Kept after the corresponding `[0x07]` task is acknowledged so a restarted outbox
/// does not claim `prev_uid_cursor = (0, 0)` against a cursor the quota shard already applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OriginQuotaCursorKey {
    pub uid: UserId,
}

impl OriginQuotaCursorKey {
    pub fn encode(self) -> [u8; 9] {
        let mut buf = [0u8; 9];
        buf[0] = SYSTEM_PREFIX_QUOTA_ORIGIN_CURSOR;
        buf[1..9].copy_from_slice(&self.uid.to_be_bytes());
        buf
    }

    pub const fn prefix_start() -> [u8; 1] {
        [SYSTEM_PREFIX_QUOTA_ORIGIN_CURSOR]
    }

    pub const fn prefix_end() -> [u8; 1] {
        [SYSTEM_PREFIX_QUOTA_ORIGIN_CURSOR + 1]
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 9 || bytes[0] != SYSTEM_PREFIX_QUOTA_ORIGIN_CURSOR {
            return Err(MetaDbError::CodecError(
                "invalid OriginQuotaCursorKey".to_string(),
            ));
        }
        Ok(Self {
            uid: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
        })
    }
}

pub fn encode_origin_quota_cursor(raft_index: u64, task_ordinal: u16) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 2 + 4);
    buf.push(SYSTEM_CODEC_V1);
    buf.extend_from_slice(&raft_index.to_be_bytes());
    buf.extend_from_slice(&task_ordinal.to_be_bytes());
    append_crc32c_be(&mut buf);
    buf
}

/// Encodes `(latest_cursor, acked_cursor)` in `[0x08][uid]` (`V2`) so unspilled `.raftlog`
/// reconstruction preserves the durable acknowledged predecessor even after L0 folding.
pub fn encode_origin_quota_cursor_pair(latest: (u64, u16), acked: (u64, u16)) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 2 + 8 + 2 + 4);
    buf.push(SYSTEM_CODEC_V2);
    buf.extend_from_slice(&latest.0.to_be_bytes());
    buf.extend_from_slice(&latest.1.to_be_bytes());
    buf.extend_from_slice(&acked.0.to_be_bytes());
    buf.extend_from_slice(&acked.1.to_be_bytes());
    append_crc32c_be(&mut buf);
    buf
}

pub fn decode_origin_quota_cursor(bytes: &[u8]) -> Result<(u64, u16)> {
    let (latest, _) = decode_origin_quota_cursor_pair(bytes)?;
    Ok(latest)
}

pub fn decode_origin_quota_cursor_pair(bytes: &[u8]) -> Result<((u64, u16), (u64, u16))> {
    let body = verify_crc32c_be(bytes, "OriginQuotaCursor")?;
    match body.first().copied() {
        Some(SYSTEM_CODEC_V1) if body.len() == 11 => {
            let cur = (
                u64::from_be_bytes(body[1..9].try_into().unwrap()),
                u16::from_be_bytes(body[9..11].try_into().unwrap()),
            );
            Ok((cur, cur))
        }
        Some(SYSTEM_CODEC_V2) if body.len() == 21 => {
            let latest = (
                u64::from_be_bytes(body[1..9].try_into().unwrap()),
                u16::from_be_bytes(body[9..11].try_into().unwrap()),
            );
            let acked = (
                u64::from_be_bytes(body[11..19].try_into().unwrap()),
                u16::from_be_bytes(body[19..21].try_into().unwrap()),
            );
            Ok((latest, acked))
        }
        _ => Err(MetaDbError::CodecError(
            "invalid OriginQuotaCursor".to_string(),
        )),
    }
}

/// Persisted auxiliary outbox entry stored in `[0x07]` or dispatched via `ApplyAuxBatchRpc`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct AuxOutboxEntry {
    pub origin_shard_id: ShardId,
    pub origin_raft_index: u64,
    pub task_ordinal: u16,
    pub hlc: HlcTimestamp,
    pub payload: AuxTaskPayload,
}

impl AuxOutboxEntry {
    pub fn encode(&self) -> Vec<u8> {
        let payload = crate::codec::serialize(self).expect("AuxOutboxEntry serialize");
        let mut out = Vec::with_capacity(1 + payload.len() + 4);
        out.push(SYSTEM_CODEC_V1);
        out.extend_from_slice(&payload);
        append_crc32c_be(&mut out);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body = verify_crc32c_be(bytes, "AuxOutboxEntry")?;
        if body.is_empty() || body[0] != SYSTEM_CODEC_V1 {
            return Err(MetaDbError::CodecError(
                "unsupported AuxOutboxEntry codec version".to_string(),
            ));
        }
        crate::codec::deserialize(&body[1..]).map_err(|e| MetaDbError::CodecError(e.to_string()))
    }
}

const LEASE_PREIMAGE_CODEC_V1: u8 = 1;
const LEASE_PREIMAGE_TOMBSTONE: u8 = 0;
const LEASE_PREIMAGE_VALUE: u8 = 1;

/// Bytes captured for one prepared user key. Not part of `WriteIntentRecord`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeasePreimageValue {
    Tombstone,
    Value(Vec<u8>),
}

/// `[0x0D][txn_id: 16B BE][user_key]`.
pub fn lease_preimage_key(txn_id: u128, user_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(17 + user_key.len());
    key.extend_from_slice(&lease_preimage_prefix(txn_id));
    key.extend_from_slice(user_key);
    key
}

/// Inclusive lower bound for every pre-image of `txn_id`.
pub fn lease_preimage_prefix(txn_id: u128) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(SYSTEM_PREFIX_LEASE_PREIMAGE);
    key.extend_from_slice(&txn_id.to_be_bytes());
    key
}

/// Exclusive upper bound for every pre-image of `txn_id`.
pub fn lease_preimage_prefix_end(txn_id: u128) -> Vec<u8> {
    match txn_id.checked_add(1) {
        Some(next) => lease_preimage_prefix(next),
        None => vec![SYSTEM_PREFIX_LEASE_PREIMAGE.saturating_add(1)],
    }
}

fn split_lease_preimage_key(storage_key: &[u8]) -> Option<(u128, Vec<u8>)> {
    if storage_key.len() <= 17 || storage_key[0] != SYSTEM_PREFIX_LEASE_PREIMAGE {
        return None;
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&storage_key[1..17]);
    Some((u128::from_be_bytes(id), storage_key[17..].to_vec()))
}

/// Version 1 body is `version || hlc u64 BE || tag || payload`, then a CRC32C trailer.
///
/// A corrupt, truncated, or unknown-version record returns `None`. Callers skip it.
pub fn encode_lease_preimage(hlc: HlcTimestamp, value: &LeasePreimageValue) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(LEASE_PREIMAGE_CODEC_V1);
    out.extend_from_slice(&hlc.as_u64().to_be_bytes());
    match value {
        LeasePreimageValue::Tombstone => out.push(LEASE_PREIMAGE_TOMBSTONE),
        LeasePreimageValue::Value(bytes) => {
            out.push(LEASE_PREIMAGE_VALUE);
            out.extend_from_slice(bytes);
        }
    }
    append_crc32c_be(&mut out);
    out
}

/// Returns `None` for a bad key, a bad checksum, a short body, or an unknown version.
pub fn decode_lease_preimage(
    storage_key: &[u8],
    raw: &[u8],
) -> Option<(u128, Vec<u8>, HlcTimestamp, LeasePreimageValue)> {
    let (txn_id, user_key) = split_lease_preimage_key(storage_key)?;
    let body = verify_crc32c_be(raw, "LeasePreimage").ok()?;
    if body.len() < 10 || body[0] != LEASE_PREIMAGE_CODEC_V1 {
        return None;
    }
    let mut hlc_bytes = [0u8; 8];
    hlc_bytes.copy_from_slice(&body[1..9]);
    let hlc = HlcTimestamp(u64::from_be_bytes(hlc_bytes));
    let value = match body[9] {
        LEASE_PREIMAGE_TOMBSTONE if body.len() == 10 => LeasePreimageValue::Tombstone,
        LEASE_PREIMAGE_VALUE => LeasePreimageValue::Value(body[10..].to_vec()),
        _ => return None,
    };
    Some((txn_id, user_key, hlc, value))
}

/// Computes the logical `affinity_key` in `[0x01, 0x04)` used to partition shard entries
/// during `ExecuteSplit` (`sharding_and_control_plane.md` §5.3):
/// - `0x01..=0x03`: `Some(raw_key)`
/// - `0x04` (`DirShardDelta`): `Some([0x01][pid: 8B BE])`
/// - `0x05` (`WriteIntent`): `Some(target_user_key)`
/// - `0x06` (`TxnRecord`): `Some(record.primary_key)`
/// - `0x07` (`SpilledAuxOutbox`): `None` (always stays owned by parent shard until contiguously ACKed)
pub fn affinity_key_for_raw_entry(raw_key: &[u8], raw_value: &[u8]) -> Result<Option<Vec<u8>>> {
    let Some(&prefix) = raw_key.first() else {
        return Err(MetaDbError::CodecError("empty raw key".to_string()));
    };
    match prefix {
        p if (TABLE_PREFIX_META..=TABLE_PREFIX_SECONDARY_INDEX).contains(&p) => {
            Ok(Some(raw_key.to_vec()))
        }
        SYSTEM_PREFIX_DIR_SHARD_DELTA => {
            let dk = DirShardDeltaKey::decode(raw_key)?;
            Ok(Some(MetaKey::encode_pid_prefix_start(dk.pid)))
        }
        SYSTEM_PREFIX_WRITE_INTENT => {
            let ik = WriteIntentKey::decode(raw_key)?;
            Ok(Some(ik.target_user_key))
        }
        SYSTEM_PREFIX_TXN_RECORD => {
            let _ = TxnRecordKey::decode(raw_key)?;
            if raw_value.is_empty() {
                return Ok(None);
            }
            let rec = TxnStatusRecord::decode(raw_value)?;
            Ok(Some(rec.primary_key))
        }
        SYSTEM_PREFIX_SPILLED_AUX_OUTBOX | SYSTEM_PREFIX_QUOTA_ORIGIN_CURSOR => Ok(None),
        SYSTEM_PREFIX_LEASE_PREIMAGE => {
            let (_, user_key) = split_lease_preimage_key(raw_key).ok_or_else(|| {
                MetaDbError::CodecError("invalid lease pre-image key".to_string())
            })?;
            Ok(Some(user_key))
        }
        other => Err(MetaDbError::CodecError(format!(
            "unknown keyspace prefix 0x{:02x}",
            other
        ))),
    }
}

#[cfg(test)]
mod lease_preimage_codec_tests {
    use super::*;

    #[test]
    fn version_one_preimage_roundtrip_and_corrupt_records_are_rejected() {
        let user_key = MetaKey::child(7, "kept.txt").encode();
        let storage_key = lease_preimage_key(99, &user_key);
        let encoded = encode_lease_preimage(
            HlcTimestamp::new(1_000, 4),
            &LeasePreimageValue::Value(b"v0".to_vec()),
        );
        let (txn_id, decoded_key, hlc, value) =
            decode_lease_preimage(&storage_key, &encoded).unwrap();
        assert_eq!(txn_id, 99);
        assert_eq!(decoded_key, user_key);
        assert_eq!(hlc, HlcTimestamp::new(1_000, 4));
        assert_eq!(value, LeasePreimageValue::Value(b"v0".to_vec()));
        assert_eq!(
            affinity_key_for_raw_entry(&storage_key, &encoded).unwrap(),
            Some(decoded_key)
        );

        let tombstone =
            encode_lease_preimage(HlcTimestamp::new(1_000, 0), &LeasePreimageValue::Tombstone);
        assert_eq!(
            decode_lease_preimage(&storage_key, &tombstone).unwrap().3,
            LeasePreimageValue::Tombstone
        );

        let mut corrupt = encoded.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        assert!(decode_lease_preimage(&storage_key, &corrupt).is_none());
        assert!(decode_lease_preimage(&storage_key, &encoded[..encoded.len() - 1]).is_none());
        let mut unknown = encoded.clone();
        unknown[0] = 2;
        // The checksum covers the version byte, so this is also a checksum failure.
        assert!(decode_lease_preimage(&storage_key, &unknown).is_none());
        assert!(decode_lease_preimage(&[0x0D], &encoded).is_none());
        assert_eq!(lease_preimage_prefix_end(u128::MAX), vec![0x0E]);
        let user_key = MetaKey::child(7, "kept.txt").encode();
        let encoded_pin = encode_lease_preimage(
            HlcTimestamp::new(1_000, 0),
            &LeasePreimageValue::Value(b"v0".to_vec()),
        );
        assert!(lease_preimage_blocks_split(&storage_key, &encoded_pin).is_err());
        assert!(lease_preimage_blocks_split(&storage_key, b"corrupt").is_err());
        assert!(lease_preimage_blocks_split(&user_key, &encoded_pin).is_ok());
    }

    #[test]
    fn old_fence_holders_do_not_receive_an_invented_start() {
        let owner = 10u128;
        let other = 11u128;
        let record = WriteIntentRecord {
            txn_id: owner,
            primary_shard_hint: 1,
            primary_key: b"primary".to_vec(),
            start_hlc: HlcTimestamp::new(5_000, 0),
            expire_hlc: HlcTimestamp::new(8_000, 0),
            rename_fence_token: None,
            op: IntentOp::DirExistsFenceSet {
                holders: std::collections::BTreeMap::from([
                    (owner, (b"primary".to_vec(), HlcTimestamp::new(8_000, 0))),
                    (other, (b"other".to_vec(), HlcTimestamp::new(9_000, 0))),
                ]),
            },
        };
        let holders = record.resolved_fence_holders().unwrap();
        assert_eq!(holders[&owner].start_hlc, Some(HlcTimestamp::new(5_000, 0)));
        assert_eq!(holders[&other].start_hlc, None);
        let upgraded = WriteIntentRecord {
            op: IntentOp::DirExistsFenceV2 {
                holders: holders.clone(),
            },
            ..record
        };
        let again = upgraded.resolved_fence_holders().unwrap();
        assert_eq!(again[&other].start_hlc, None);
        again[&owner].start_hlc.unwrap();
        let encoded = upgraded.encode();
        let decoded = WriteIntentRecord::decode(&encoded).unwrap();
        assert_eq!(
            decoded.resolved_fence_holders().unwrap()[&owner].start_hlc,
            Some(HlcTimestamp::new(5_000, 0))
        );
    }

    #[test]
    fn orphan_and_corrupt_preimages_are_collected_without_touching_a_live_txn() {
        let user_key = MetaKey::child(1, "a").encode();
        let live_key = lease_preimage_key(7, &user_key);
        let orphan_key = lease_preimage_key(8, &user_key);
        let body = encode_lease_preimage(
            HlcTimestamp::new(1_000, 0),
            &LeasePreimageValue::Value(b"v".to_vec()),
        );
        let mut live = std::collections::BTreeSet::new();
        live.insert(7u128);
        assert!(!lease_preimage_is_orphan(&live_key, &body, &live));
        assert!(lease_preimage_is_orphan(&orphan_key, &body, &live));
        assert!(lease_preimage_is_orphan(&orphan_key, b"corrupt", &live));
        assert!(!lease_preimage_is_orphan(&user_key, &body, &live));
    }

    #[test]
    fn legacy_intent_and_txn_records_do_not_carry_a_preimage_field() {
        let intent = WriteIntentRecord {
            txn_id: 1,
            primary_shard_hint: 1,
            primary_key: b"primary".to_vec(),
            start_hlc: HlcTimestamp::new(10, 0),
            expire_hlc: HlcTimestamp::new(20, 0),
            rename_fence_token: None,
            op: IntentOp::Delete {
                attr_deltas: Vec::new(),
                aux_specs: Vec::new(),
            },
        };
        let encoded = intent.encode();
        assert_eq!(encoded[0], 1, "WriteIntentRecord stays codec v1");
        assert_eq!(WriteIntentRecord::decode(&encoded).unwrap(), intent);

        let txn = TxnStatusRecord {
            txn_id: 1,
            primary_key: b"primary".to_vec(),
            participant_keys: vec![b"primary".to_vec()],
            pending_ack_keys: BTreeSet::new(),
            start_hlc: HlcTimestamp::new(10, 0),
            expire_hlc: HlcTimestamp::new(20, 0),
            rename_fence_token: Some(3),
            state: TxnState::Pending,
        };
        let encoded = txn.encode();
        assert_eq!(encoded[0], 1, "TxnStatusRecord stays codec v1");
        assert_eq!(TxnStatusRecord::decode(&encoded).unwrap(), txn);
    }
}
