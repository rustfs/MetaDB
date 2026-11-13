use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::checksum::{append_crc32c_be, verify_crc32c_be};
use crate::error::{MetaDbError, Result};
use crate::hlc::HlcTimestamp;

/// Identifier of an inode (directory or file) in the namespace hierarchy.
pub type InodeId = u64;
/// Identifier of a user for quota accounting.
pub type UserId = u64;
/// Identifier of a backing data object in the data plane.
pub type FileId = u64;
/// Identifier of a physical/logical database shard.
pub type ShardId = u64;
/// Identifier of a storage backend (BE) node.
pub type NodeId = u64;

/// Root directory inode ID (`/`).
pub const ROOT_INODE_ID: InodeId = 1;

/// Bit 63 tag separating regular file `InodeId`s (`1`) from directory `PID`s (`0`).
pub const FILE_INODE_TAG_BIT: InodeId = 1u64 << 63;

/// Reserved child entry name under `(PID, "_attr_")` used to colocate a directory's
/// attributes with all of its direct children in `MetaTable` (§3.1 of `arXiv:2610.32247v1`).
pub const ATTR_ENTRY_NAME: &str = "_attr_";

/// Fixed byte length of a serialized `InodeAttr` (`88` bytes including version & CRC32C).
pub const INODE_ATTR_ENCODED_LEN: usize = 88;
const INODE_ATTR_CODEC_V1: u8 = 1;
const RECORD_CODEC_V1: u8 = 1;

pub const TABLE_PREFIX_META: u8 = 0x01;
pub const TABLE_PREFIX_QUOTA: u8 = 0x02;
pub const TABLE_PREFIX_SECONDARY_INDEX: u8 = 0x03;
pub const SYSTEM_PREFIX_DIR_SHARD_DELTA: u8 = 0x04;
pub const SYSTEM_PREFIX_WRITE_INTENT: u8 = 0x05;
pub const SYSTEM_PREFIX_TXN_RECORD: u8 = 0x06;
pub const SYSTEM_PREFIX_SPILLED_AUX_OUTBOX: u8 = 0x07;

/// Total-order version tuple `(hlc, origin_shard_id, origin_raft_index, task_ordinal)`
/// used by `SecondaryIndexTable` to break cross-shard HLC ties deterministically.
pub type AuxOrderingTuple = (HlcTimestamp, ShardId, u64, u16);

#[inline]
pub const fn make_file_inode_id(seq: u64) -> InodeId {
    FILE_INODE_TAG_BIT | (seq & !FILE_INODE_TAG_BIT)
}

#[inline]
pub const fn is_file_inode_id(id: InodeId) -> bool {
    (id & FILE_INODE_TAG_BIT) != 0
}

#[inline]
pub const fn is_dir_inode_id(id: InodeId) -> bool {
    id != 0 && (id & FILE_INODE_TAG_BIT) == 0
}

/// Validates that a user-supplied file or directory name does not collide with
/// the reserved `"_attr_"` entry or contain illegal empty/null characters.
pub fn validate_user_entry_name(name: &str) -> Result<()> {
    if name.is_empty() || name == ATTR_ENTRY_NAME || name.contains('\0') {
        return Err(MetaDbError::ReservedEntryName(name.to_string()));
    }
    Ok(())
}

/// Primary key of `MetaTable`: `(PID, name)`.
///
/// Encoded as `[0x01][pid: 8-byte big-endian][name UTF-8 bytes]` so that all entries
/// sharing the same parent directory `pid`—including the special `(pid, "_attr_")`
/// record—are lexicographically contiguous.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MetaKey {
    pub pid: InodeId,
    pub name: String,
}

impl MetaKey {
    /// Creates a `MetaKey` for a child entry under `pid`.
    pub fn child(pid: InodeId, name: impl Into<String>) -> Self {
        Self {
            pid,
            name: name.into(),
        }
    }

    /// Creates the special `(pid, "_attr_")` key storing the attributes of directory `pid`.
    pub fn dir_attr(pid: InodeId) -> Self {
        Self {
            pid,
            name: ATTR_ENTRY_NAME.to_string(),
        }
    }

    /// Returns true if this key represents the colocated directory attribute entry `"_attr_"`.
    pub fn is_attr_key(&self) -> bool {
        self.name == ATTR_ENTRY_NAME
    }

    /// Returns the `group_key` (`PID`) used by policy/size-based sharding (§4.1)
    /// to anchor a directory's attributes and all of its direct children within a single shard.
    #[inline]
    pub const fn group_key(&self) -> InodeId {
        self.pid
    }

    /// Encodes this key into a lexicographically sortable byte vector.
    pub fn encode(&self) -> Vec<u8> {
        let name_bytes = self.name.as_bytes();
        let mut buf = Vec::with_capacity(1 + 8 + name_bytes.len());
        buf.push(TABLE_PREFIX_META);
        buf.extend_from_slice(&self.pid.to_be_bytes());
        buf.extend_from_slice(name_bytes);
        buf
    }

    /// Encodes the lower bound `[0x01][pid: 8B BE]` for scanning all entries in directory `pid`.
    pub fn encode_pid_prefix_start(pid: InodeId) -> Vec<u8> {
        let mut buf = Vec::with_capacity(9);
        buf.push(TABLE_PREFIX_META);
        buf.extend_from_slice(&pid.to_be_bytes());
        buf
    }

    /// Encodes the exclusive upper bound `[0x01][(pid + 1): 8B BE]` for scanning directory `pid`.
    pub fn encode_pid_prefix_end(pid: InodeId) -> Vec<u8> {
        let mut buf = Vec::with_capacity(9);
        if let Some(next_pid) = pid.checked_add(1) {
            buf.push(TABLE_PREFIX_META);
            buf.extend_from_slice(&next_pid.to_be_bytes());
        } else {
            buf.push(TABLE_PREFIX_QUOTA);
        }
        buf
    }

    /// Decodes a `MetaKey` from its binary representation.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 9 || bytes[0] != TABLE_PREFIX_META {
            return Err(MetaDbError::CodecError(
                "invalid MetaKey binary prefix or length".to_string(),
            ));
        }
        let mut pid_bytes = [0u8; 8];
        pid_bytes.copy_from_slice(&bytes[1..9]);
        let pid = u64::from_be_bytes(pid_bytes);
        let name = String::from_utf8(bytes[9..].to_vec())
            .map_err(|e| MetaDbError::CodecError(format!("invalid UTF-8 in MetaKey: {}", e)))?;
        Ok(Self { pid, name })
    }
}

/// Entry type stored in `MetaTable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum EntryType {
    File = 1,
    Directory = 2,
    DirectoryAttr = 3,
}

impl EntryType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(EntryType::File),
            2 => Ok(EntryType::Directory),
            3 => Ok(EntryType::DirectoryAttr),
            _ => Err(MetaDbError::CodecError(format!(
                "unknown EntryType tag: {}",
                v
            ))),
        }
    }
}

/// Directory or file attributes stored inside a `MetaEntry`.
///
/// Encoded into an exact 88-byte versioned + CRC32C-protected payload (`consensus_and_transactions.md` §7.1):
/// includes `parent_pid` for directory-rename cycle detection and `applied_lsn` (`applied_raft_index`)
/// for race-free `AsyncAttrAggregator` reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeAttr {
    pub inode_id: InodeId,
    pub parent_pid: InodeId,
    pub entry_type: EntryType,
    pub uid: UserId,
    pub mode: u32,
    pub link_count: u32,
    pub num_children: i64,
    pub size_bytes: i64,
    pub create_time: HlcTimestamp,
    pub modify_time: HlcTimestamp,
    pub fid: Option<FileId>,
    pub applied_lsn: u64,
}

/// A full row in `MetaTable` keyed by `(PID, name)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaEntry {
    pub key: MetaKey,
    pub attr: InodeAttr,
}

impl InodeAttr {
    #[inline]
    pub const fn applied_raft_index(&self) -> u64 {
        self.applied_lsn
    }

    /// Serializes `InodeAttr` into its deterministic 88-byte big-endian payload with CRC32C.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(INODE_ATTR_ENCODED_LEN);
        buf.push(INODE_ATTR_CODEC_V1); // [0]
        buf.push(self.entry_type as u8); // [1]
        buf.extend_from_slice(&self.inode_id.to_be_bytes()); // [2..10]
        buf.extend_from_slice(&self.parent_pid.to_be_bytes()); // [10..18]
        buf.extend_from_slice(&self.uid.to_be_bytes()); // [18..26]
        buf.extend_from_slice(&self.mode.to_be_bytes()); // [26..30]
        buf.extend_from_slice(&self.link_count.to_be_bytes()); // [30..34]
        buf.extend_from_slice(&self.num_children.to_be_bytes()); // [34..42]
        buf.extend_from_slice(&self.size_bytes.to_be_bytes()); // [42..50]
        buf.extend_from_slice(&self.create_time.0.to_be_bytes()); // [50..58]
        buf.extend_from_slice(&self.modify_time.0.to_be_bytes()); // [58..66]
        match self.fid {
            Some(fid) => {
                buf.push(1); // [66]
                buf.extend_from_slice(&fid.to_be_bytes()); // [67..75]
            }
            None => {
                buf.push(0); // [66]
                buf.extend_from_slice(&0u64.to_be_bytes()); // [67..75]
            }
        }
        buf.extend_from_slice(&self.applied_lsn.to_be_bytes()); // [75..83]
        buf.push(0u8); // [83] reserved_flags
        append_crc32c_be(&mut buf); // [84..88]
        debug_assert_eq!(buf.len(), INODE_ATTR_ENCODED_LEN);
        buf
    }

    /// Deserializes `InodeAttr` from its 88-byte binary payload, validating version & CRC32C.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != INODE_ATTR_ENCODED_LEN {
            return Err(MetaDbError::CodecError(format!(
                "expected {} bytes for InodeAttr, got {}",
                INODE_ATTR_ENCODED_LEN,
                bytes.len()
            )));
        }
        let body = verify_crc32c_be(bytes, "InodeAttr")?;
        if body[0] != INODE_ATTR_CODEC_V1 {
            return Err(MetaDbError::CodecError(format!(
                "unsupported InodeAttr codec version {}",
                body[0]
            )));
        }
        let entry_type = EntryType::from_u8(body[1])?;
        let inode_id = u64::from_be_bytes(body[2..10].try_into().unwrap());
        let parent_pid = u64::from_be_bytes(body[10..18].try_into().unwrap());
        let uid = u64::from_be_bytes(body[18..26].try_into().unwrap());
        let mode = u32::from_be_bytes(body[26..30].try_into().unwrap());
        let link_count = u32::from_be_bytes(body[30..34].try_into().unwrap());
        let num_children = i64::from_be_bytes(body[34..42].try_into().unwrap());
        let size_bytes = i64::from_be_bytes(body[42..50].try_into().unwrap());
        let create_time = HlcTimestamp(u64::from_be_bytes(body[50..58].try_into().unwrap()));
        let modify_time = HlcTimestamp(u64::from_be_bytes(body[58..66].try_into().unwrap()));
        let has_fid = body[66];
        let fid_raw = u64::from_be_bytes(body[67..75].try_into().unwrap());
        let fid = if has_fid == 1 {
            Some(fid_raw)
        } else if has_fid == 0 {
            None
        } else {
            return Err(MetaDbError::CodecError(format!(
                "invalid has_fid flag {} in InodeAttr",
                has_fid
            )));
        };
        let applied_lsn = u64::from_be_bytes(body[75..83].try_into().unwrap());

        Ok(Self {
            inode_id,
            parent_pid,
            entry_type,
            uid,
            mode,
            link_count,
            num_children,
            size_bytes,
            create_time,
            modify_time,
            fid,
            applied_lsn,
        })
    }
}

/// Primary key of `QuotaTable`: `(UID)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QuotaKey {
    pub uid: UserId,
}

impl QuotaKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(9);
        buf.push(TABLE_PREFIX_QUOTA);
        buf.extend_from_slice(&self.uid.to_be_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 9 || bytes[0] != TABLE_PREFIX_QUOTA {
            return Err(MetaDbError::CodecError("invalid QuotaKey".to_string()));
        }
        let uid = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        Ok(Self { uid })
    }
}

/// Row in `QuotaTable` tracking per-user resource consumption and per-origin-shard
/// contiguous `(origin_raft_index, task_ordinal)` cursors for exact-once accounting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuotaRecord {
    pub uid: UserId,
    pub used_bytes: i64,
    pub used_inodes: i64,
    pub quota_limit_bytes: u64,
    pub last_applied_hlc: HlcTimestamp,
    /// Per-source-shard monotonic `(origin_raft_index, task_ordinal)` watermark ensuring
    /// multiple additive quota tasks per Raft entry never collide and are applied once.
    pub applied_by_shard: BTreeMap<ShardId, (u64, u16)>,
}

impl QuotaRecord {
    pub fn new(uid: UserId, quota_limit_bytes: u64) -> Self {
        Self {
            uid,
            used_bytes: 0,
            used_inodes: 0,
            quota_limit_bytes,
            last_applied_hlc: HlcTimestamp::ZERO,
            applied_by_shard: BTreeMap::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(40 + self.applied_by_shard.len() * 18);
        buf.push(RECORD_CODEC_V1);
        buf.extend_from_slice(&self.uid.to_be_bytes());
        buf.extend_from_slice(&self.used_bytes.to_be_bytes());
        buf.extend_from_slice(&self.used_inodes.to_be_bytes());
        buf.extend_from_slice(&self.quota_limit_bytes.to_be_bytes());
        buf.extend_from_slice(&self.last_applied_hlc.0.to_be_bytes());
        buf.extend_from_slice(&(self.applied_by_shard.len() as u32).to_be_bytes());
        for (&shard_id, &(raft_idx, ord)) in &self.applied_by_shard {
            buf.extend_from_slice(&shard_id.to_be_bytes());
            buf.extend_from_slice(&raft_idx.to_be_bytes());
            buf.extend_from_slice(&ord.to_be_bytes());
        }
        append_crc32c_be(&mut buf);
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body = verify_crc32c_be(bytes, "QuotaRecord")?;
        if body.len() < 45 || body[0] != RECORD_CODEC_V1 {
            return Err(MetaDbError::CodecError(
                "invalid QuotaRecord header or version".to_string(),
            ));
        }
        let uid = u64::from_be_bytes(body[1..9].try_into().unwrap());
        let used_bytes = i64::from_be_bytes(body[9..17].try_into().unwrap());
        let used_inodes = i64::from_be_bytes(body[17..25].try_into().unwrap());
        let quota_limit_bytes = u64::from_be_bytes(body[25..33].try_into().unwrap());
        let last_applied_hlc = HlcTimestamp(u64::from_be_bytes(body[33..41].try_into().unwrap()));
        let map_len = u32::from_be_bytes(body[41..45].try_into().unwrap()) as usize;
        let expected_len = 45usize.saturating_add(map_len.saturating_mul(18));
        if body.len() != expected_len {
            return Err(MetaDbError::CodecError(
                "truncated QuotaRecord shard cursor map".to_string(),
            ));
        }
        let mut applied_by_shard = BTreeMap::new();
        let mut offset = 45;
        for _ in 0..map_len {
            let shard_id = u64::from_be_bytes(body[offset..offset + 8].try_into().unwrap());
            let raft_idx = u64::from_be_bytes(body[offset + 8..offset + 16].try_into().unwrap());
            let ord = u16::from_be_bytes(body[offset + 16..offset + 18].try_into().unwrap());
            applied_by_shard.insert(shard_id, (raft_idx, ord));
            offset += 18;
        }
        Ok(Self {
            uid,
            used_bytes,
            used_inodes,
            quota_limit_bytes,
            last_applied_hlc,
            applied_by_shard,
        })
    }
}

/// Primary key of `SecondaryIndexTable`: `(FID)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SecondaryIndexKey {
    pub fid: FileId,
}

impl SecondaryIndexKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(9);
        buf.push(TABLE_PREFIX_SECONDARY_INDEX);
        buf.extend_from_slice(&self.fid.to_be_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 9 || bytes[0] != TABLE_PREFIX_SECONDARY_INDEX {
            return Err(MetaDbError::CodecError(
                "invalid SecondaryIndexKey".to_string(),
            ));
        }
        let fid = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        Ok(Self { fid })
    }
}

/// Row in `SecondaryIndexTable` mapping a data-plane `FID` back to its `(PID, name)`
/// primary metadata entry (`arXiv:2610.32247v1`, Figure 5) with total-order tie-breaking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexRecord {
    pub fid: FileId,
    pub pid: InodeId,
    pub name: String,
    pub create_time: HlcTimestamp,
    pub last_ver: AuxOrderingTuple,
    pub tombstone_ver: AuxOrderingTuple,
    pub is_deleted: bool,
}

impl SecondaryIndexRecord {
    #[inline]
    pub const fn last_applied_hlc(&self) -> HlcTimestamp {
        self.last_ver.0
    }

    pub fn encode(&self) -> Vec<u8> {
        let name_bytes = self.name.as_bytes();
        let mut buf = Vec::with_capacity(1 + 8 + 8 + 4 + name_bytes.len() + 8 + 26 + 26 + 1 + 4);
        buf.push(RECORD_CODEC_V1);
        buf.extend_from_slice(&self.fid.to_be_bytes());
        buf.extend_from_slice(&self.pid.to_be_bytes());
        buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&self.create_time.0.to_be_bytes());
        Self::encode_ver(&mut buf, self.last_ver);
        Self::encode_ver(&mut buf, self.tombstone_ver);
        buf.push(u8::from(self.is_deleted));
        append_crc32c_be(&mut buf);
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body = verify_crc32c_be(bytes, "SecondaryIndexRecord")?;
        if body.len() < 21 || body[0] != RECORD_CODEC_V1 {
            return Err(MetaDbError::CodecError(
                "invalid SecondaryIndexRecord header".to_string(),
            ));
        }
        let fid = u64::from_be_bytes(body[1..9].try_into().unwrap());
        let pid = u64::from_be_bytes(body[9..17].try_into().unwrap());
        let name_len = u32::from_be_bytes(body[17..21].try_into().unwrap()) as usize;
        let expected_len = 21usize
            .saturating_add(name_len)
            .saturating_add(8 + 26 + 26 + 1);
        if body.len() != expected_len {
            return Err(MetaDbError::CodecError(
                "invalid SecondaryIndexRecord length".to_string(),
            ));
        }
        let name = String::from_utf8(body[21..21 + name_len].to_vec())
            .map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        let mut off = 21 + name_len;
        let create_time = HlcTimestamp(u64::from_be_bytes(body[off..off + 8].try_into().unwrap()));
        off += 8;
        let last_ver = Self::decode_ver(&body[off..off + 26]);
        off += 26;
        let tombstone_ver = Self::decode_ver(&body[off..off + 26]);
        off += 26;
        let is_deleted = body[off] != 0;
        Ok(Self {
            fid,
            pid,
            name,
            create_time,
            last_ver,
            tombstone_ver,
            is_deleted,
        })
    }

    fn encode_ver(buf: &mut Vec<u8>, ver: AuxOrderingTuple) {
        buf.extend_from_slice(&ver.0 .0.to_be_bytes());
        buf.extend_from_slice(&ver.1.to_be_bytes());
        buf.extend_from_slice(&ver.2.to_be_bytes());
        buf.extend_from_slice(&ver.3.to_be_bytes());
    }

    fn decode_ver(slice: &[u8]) -> AuxOrderingTuple {
        (
            HlcTimestamp(u64::from_be_bytes(slice[0..8].try_into().unwrap())),
            u64::from_be_bytes(slice[8..16].try_into().unwrap()),
            u64::from_be_bytes(slice[16..24].try_into().unwrap()),
            u16::from_be_bytes(slice[24..26].try_into().unwrap()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::epoch::{CompositeKeyRange, ShardEpoch};
    use crate::system_keys::{
        affinity_key_for_raw_entry, AuxOutboxEntry, AuxTaskPayload, DirShardDeltaKey,
        DirShardDeltaRecord, IntentOp, SpilledAuxOutboxKey, TxnRecordKey, TxnState,
        TxnStatusRecord, WriteIntentKey, WriteIntentRecord,
    };
    use std::collections::BTreeSet;

    #[test]
    fn test_inode_attr_88_byte_codec_and_crc32c_rejection() {
        let attr = InodeAttr {
            inode_id: 2048,
            parent_pid: ROOT_INODE_ID,
            entry_type: EntryType::File,
            uid: 42,
            mode: 0o644,
            link_count: 1,
            num_children: 0,
            size_bytes: 1_048_576,
            create_time: HlcTimestamp::new(10_000, 1),
            modify_time: HlcTimestamp::new(10_005, 3),
            fid: Some(999),
            applied_lsn: 77,
        };
        let mut encoded = attr.encode();
        assert_eq!(encoded.len(), INODE_ATTR_ENCODED_LEN);
        assert_eq!(InodeAttr::decode(&encoded).unwrap(), attr);

        // Flip a payload bit and verify CRC32C rejection.
        encoded[12] ^= 0x01;
        assert!(matches!(
            InodeAttr::decode(&encoded).unwrap_err(),
            MetaDbError::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn test_quota_and_secondary_index_codecs() {
        let mut quota = QuotaRecord::new(1001, 10 * 1024 * 1024);
        quota.used_bytes = 4096;
        quota.used_inodes = 2;
        quota.last_applied_hlc = HlcTimestamp::new(5000, 2);
        quota.applied_by_shard.insert(1, (42, 1));
        quota.applied_by_shard.insert(3, (99, 0));

        let q_bytes = quota.encode();
        assert_eq!(QuotaRecord::decode(&q_bytes).unwrap(), quota);

        let idx = SecondaryIndexRecord {
            fid: 8888,
            pid: 55,
            name: "checkpoint_01.pt".to_string(),
            create_time: HlcTimestamp::new(1000, 0),
            last_ver: (HlcTimestamp::new(2000, 1), 2, 15, 1),
            tombstone_ver: (HlcTimestamp::ZERO, 0, 0, 0),
            is_deleted: false,
        };
        let i_bytes = idx.encode();
        assert_eq!(SecondaryIndexRecord::decode(&i_bytes).unwrap(), idx);
    }

    #[test]
    fn test_system_keys_ordering_and_affinity_partitioning() {
        let k0 = SpilledAuxOutboxKey::new(10, 0).encode();
        let k1 = SpilledAuxOutboxKey::new(10, 1).encode();
        let k2 = SpilledAuxOutboxKey::new(11, 0).encode();
        assert!(k0 < k1 && k1 < k2);
        assert_eq!(k0.len(), 11);

        let entry = AuxOutboxEntry {
            origin_shard_id: 1,
            origin_raft_index: 10,
            task_ordinal: 1,
            hlc: HlcTimestamp::new(500, 0),
            payload: AuxTaskPayload::QuotaDelta {
                uid: 7,
                delta_bytes: 512,
                delta_inodes: 1,
                prev_uid_cursor: (9, 0),
            },
        };
        assert_eq!(
            affinity_key_for_raw_entry(&k1, &entry.encode()).unwrap(),
            None
        );

        let user_key = MetaKey::child(50, "a.txt").encode();
        let intent_key = WriteIntentKey::new(user_key.clone()).encode();
        let intent_rec = WriteIntentRecord {
            txn_id: 123,
            primary_shard_hint: 1,
            primary_key: user_key.clone(),
            start_hlc: HlcTimestamp::new(100, 0),
            expire_hlc: HlcTimestamp::new(3100, 0),
            rename_fence_token: None,
            op: IntentOp::Delete {
                attr_deltas: vec![],
                aux_specs: vec![],
            },
        };
        assert_eq!(
            affinity_key_for_raw_entry(&intent_key, &intent_rec.encode()).unwrap(),
            Some(user_key.clone())
        );

        let txn_key = TxnRecordKey { txn_id: 123 }.encode();
        let txn_rec = TxnStatusRecord {
            txn_id: 123,
            primary_key: user_key.clone(),
            participant_keys: vec![user_key.clone()],
            pending_ack_keys: BTreeSet::new(),
            start_hlc: HlcTimestamp::new(100, 0),
            expire_hlc: HlcTimestamp::new(3100, 0),
            rename_fence_token: Some(7),
            state: TxnState::Pending,
        };
        assert_eq!(
            affinity_key_for_raw_entry(&txn_key, &txn_rec.encode()).unwrap(),
            Some(user_key)
        );

        let dk = DirShardDeltaKey { pid: 50 }.encode();
        let drec = DirShardDeltaRecord {
            inode_id: 50,
            parent_pid: 1,
            uid: 0,
            mode: 0o755,
            create_time: HlcTimestamp::new(1, 0),
            num_children: 3,
            size_bytes: 120,
            modify_time: HlcTimestamp::new(2, 0),
            applied_raft_index: 5,
            deleting_fence: false,
        };
        assert_eq!(
            affinity_key_for_raw_entry(&dk, &drec.encode()).unwrap(),
            Some(MetaKey::encode_pid_prefix_start(50))
        );

        let epoch = ShardEpoch::initial().bump_conf_ver().bump_version();
        assert_eq!(ShardEpoch::decode(&epoch.encode()).unwrap(), epoch);
        let range = CompositeKeyRange::meta_pid_range(10, 20).unwrap();
        assert!(range.contains_key(&MetaKey::child(15, "x").encode()));
        assert!(!range.contains_key(&MetaKey::child(20, "x").encode()));
    }
}
