use std::collections::BTreeMap;

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

/// Reserved child entry name under `(PID, "_attr_")` used to colocate a directory's
/// attributes with all of its direct children in `MetaTable` (§3.1 of `arXiv:2610.32247v1`).
pub const ATTR_ENTRY_NAME: &str = "_attr_";

const TABLE_PREFIX_META: u8 = 0x01;
const TABLE_PREFIX_QUOTA: u8 = 0x02;
const TABLE_PREFIX_SECONDARY_INDEX: u8 = 0x03;

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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
            buf.push(TABLE_PREFIX_META + 1);
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
/// Includes `applied_lsn` so that `AsyncAttrAggregator::reconcile_attr()` folds
/// only un-flushed in-memory deltas with `delta.lsn > applied_lsn`, preventing
/// undercounting or double-counting during concurrent flushes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InodeAttr {
    pub inode_id: InodeId,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaEntry {
    pub key: MetaKey,
    pub attr: InodeAttr,
}

impl InodeAttr {
    /// Serializes `InodeAttr` into a compact 74-byte deterministic binary payload.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(74);
        buf.extend_from_slice(&self.inode_id.to_be_bytes());
        buf.push(self.entry_type as u8);
        buf.extend_from_slice(&self.uid.to_be_bytes());
        buf.extend_from_slice(&self.mode.to_be_bytes());
        buf.extend_from_slice(&self.link_count.to_be_bytes());
        buf.extend_from_slice(&self.num_children.to_be_bytes());
        buf.extend_from_slice(&self.size_bytes.to_be_bytes());
        buf.extend_from_slice(&self.create_time.0.to_be_bytes());
        buf.extend_from_slice(&self.modify_time.0.to_be_bytes());
        match self.fid {
            Some(fid) => {
                buf.push(1);
                buf.extend_from_slice(&fid.to_be_bytes());
            }
            None => {
                buf.push(0);
                buf.extend_from_slice(&0u64.to_be_bytes());
            }
        }
        buf.extend_from_slice(&self.applied_lsn.to_be_bytes());
        buf
    }

    /// Deserializes `InodeAttr` from its 74-byte binary payload.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 74 {
            return Err(MetaDbError::CodecError(format!(
                "expected 74 bytes for InodeAttr, got {}",
                bytes.len()
            )));
        }
        let inode_id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let entry_type = EntryType::from_u8(bytes[8])?;
        let uid = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
        let mode = u32::from_be_bytes(bytes[17..21].try_into().unwrap());
        let link_count = u32::from_be_bytes(bytes[21..25].try_into().unwrap());
        let num_children = i64::from_be_bytes(bytes[25..33].try_into().unwrap());
        let size_bytes = i64::from_be_bytes(bytes[33..41].try_into().unwrap());
        let create_time = HlcTimestamp(u64::from_be_bytes(bytes[41..49].try_into().unwrap()));
        let modify_time = HlcTimestamp(u64::from_be_bytes(bytes[49..57].try_into().unwrap()));
        let has_fid = bytes[57];
        let fid_raw = u64::from_be_bytes(bytes[58..66].try_into().unwrap());
        let fid = if has_fid == 1 { Some(fid_raw) } else { None };
        let applied_lsn = u64::from_be_bytes(bytes[66..74].try_into().unwrap());

        Ok(Self {
            inode_id,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
/// LSN watermarks for exact-once asynchronous delta deduplication (§5.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaRecord {
    pub uid: UserId,
    pub used_bytes: i64,
    pub used_inodes: i64,
    pub quota_limit_bytes: u64,
    pub last_applied_hlc: HlcTimestamp,
    /// Per-source-shard monotonic Raft LSN watermark ensuring additive quota deltas
    /// are applied at most once even under retries, leader failover, or out-of-order delivery.
    pub applied_lsn_by_shard: BTreeMap<ShardId, u64>,
}

impl QuotaRecord {
    pub fn new(uid: UserId, quota_limit_bytes: u64) -> Self {
        Self {
            uid,
            used_bytes: 0,
            used_inodes: 0,
            quota_limit_bytes,
            last_applied_hlc: HlcTimestamp::ZERO,
            applied_lsn_by_shard: BTreeMap::new(),
        }
    }
}

/// Primary key of `SecondaryIndexTable`: `(FID)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
/// primary metadata entry (`arXiv:2610.32247v1`, Figure 5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecondaryIndexRecord {
    pub fid: FileId,
    pub pid: InodeId,
    pub name: String,
    pub create_time: HlcTimestamp,
    pub last_applied_hlc: HlcTimestamp,
}
