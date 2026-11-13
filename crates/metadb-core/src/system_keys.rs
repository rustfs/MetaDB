//! Internal shard-local system keyspaces (`0x04..0x07`) and split `affinity_key` partitioning.
//!
//! Disjoint 1-byte prefixes inside each shard's `LsmStorageBackend`:
//! - `0x04`: `DirShardDelta` (`DualModeFineGrained` non-anchor directory partition stats & lease)
//! - `0x05`: `WriteIntent` (Raft-replicated 2PC participant locks)
//! - `0x06`: `TxnRecord` (Primary-Anchored 2PC commit/abort source of truth)
//! - `0x07`: `SpilledAuxOutbox` (`[0x07][origin_raft_index: 8B BE][task_ordinal: 2B BE]`)

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::checksum::{append_crc32c_be, verify_crc32c_be};
use crate::epoch::TxnId;
use crate::error::{MetaDbError, Result};
use crate::hlc::HlcTimestamp;
use crate::schema::{
    FileId, InodeId, MetaKey, ShardId, UserId, SYSTEM_PREFIX_DIR_SHARD_DELTA,
    SYSTEM_PREFIX_SPILLED_AUX_OUTBOX, SYSTEM_PREFIX_TXN_RECORD, SYSTEM_PREFIX_WRITE_INTENT,
    TABLE_PREFIX_META, TABLE_PREFIX_SECONDARY_INDEX,
};

const SYSTEM_CODEC_V1: u8 = 1;

/// Key of `DirShardDelta` (`[0x04][pid: 8B BE]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        let payload = bincode::serialize(self).expect("DirShardDeltaRecord serialize");
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
        bincode::deserialize(&body[1..]).map_err(|e| MetaDbError::CodecError(e.to_string()))
    }
}

/// Parent directory attribute delta embedded in a 2PC `WriteIntent`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttrDeltaSpec {
    pub pid: InodeId,
    pub delta_children: i64,
    pub delta_bytes: i64,
}

/// Auxiliary update payload emitted into `AuxOutbox` / `[0x07] SpilledAuxOutbox`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntentOp {
    /// Exclusive write lock proposing a key upsert.
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
        holders: BTreeMap<TxnId, (Vec<u8>, HlcTimestamp)>,
    },
}

/// Key of `WriteIntent`: `[0x05][target_user_key]`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        let payload = bincode::serialize(self).expect("WriteIntentRecord serialize");
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
        bincode::deserialize(&body[1..]).map_err(|e| MetaDbError::CodecError(e.to_string()))
    }
}

/// Key of `TxnRecord` on the Primary Shard: `[0x06][txn_id: 16B BE]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxnState {
    Pending,
    Committed { commit_hlc: HlcTimestamp },
    Aborted,
}

/// Value stored at `[0x06][txn_id: 16B BE]`.
/// Retained until `pending_ack_keys` becomes empty (Resolution B12).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        let payload = bincode::serialize(self).expect("TxnStatusRecord serialize");
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
        bincode::deserialize(&body[1..]).map_err(|e| MetaDbError::CodecError(e.to_string()))
    }
}

/// Key of `SpilledAuxOutbox`: `[0x07][origin_raft_index: 8B BE][task_ordinal: 2B BE]` (11 bytes, Resolution B7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

    pub fn prefix_start(after_raft_index: u64) -> [u8; 11] {
        Self::new(after_raft_index.saturating_add(1), 0).encode()
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

/// Persisted auxiliary outbox entry stored in `[0x07]` or dispatched via `ApplyAuxBatchRpc`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuxOutboxEntry {
    pub origin_shard_id: ShardId,
    pub origin_raft_index: u64,
    pub task_ordinal: u16,
    pub hlc: HlcTimestamp,
    pub payload: AuxTaskPayload,
}

impl AuxOutboxEntry {
    pub fn encode(&self) -> Vec<u8> {
        let payload = bincode::serialize(self).expect("AuxOutboxEntry serialize");
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
        bincode::deserialize(&body[1..]).map_err(|e| MetaDbError::CodecError(e.to_string()))
    }
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
            let rec = TxnStatusRecord::decode(raw_value)?;
            Ok(Some(rec.primary_key))
        }
        SYSTEM_PREFIX_SPILLED_AUX_OUTBOX => Ok(None),
        other => Err(MetaDbError::CodecError(format!(
            "unknown keyspace prefix 0x{:02x}",
            other
        ))),
    }
}
