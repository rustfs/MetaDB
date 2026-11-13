//! Shard epoch fencing (`ShardEpoch`) and unified `[0x01, 0x04)` composite key intervals.

use serde::{Deserialize, Serialize};

use crate::error::{MetaDbError, Result};
use crate::schema::{
    InodeId, MetaKey, TABLE_PREFIX_META, TABLE_PREFIX_QUOTA, TABLE_PREFIX_SECONDARY_INDEX,
};

/// Identifier of an independently managed cluster Cell.
pub type CellId = u64;
/// Globally unique 128-bit distributed 2PC transaction identifier.
pub type TxnId = u128;

/// Two-component epoch protecting shards against stale membership (`conf_ver`)
/// and stale key-range routing (`version`).
///
/// Per Goldfish Review Resolution B16, all shard data/consensus/read RPCs enforce strict
/// equality `expected_epoch == current_epoch` (`conf_ver` AND `version`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardEpoch {
    /// Incremented by 1 on every Raft membership change (`AddLearner`, `ChangeMembership`).
    pub conf_ver: u64,
    /// Incremented by 1 on every shard key-range split (`ExecuteSplit`).
    pub version: u64,
}

impl Default for ShardEpoch {
    fn default() -> Self {
        Self::initial()
    }
}

impl ShardEpoch {
    /// Initial epoch assigned to a newly bootstrapped root shard.
    #[inline]
    pub const fn initial() -> Self {
        Self {
            conf_ver: 1,
            version: 1,
        }
    }

    #[inline]
    pub const fn bump_conf_ver(self) -> Self {
        Self {
            conf_ver: self.conf_ver.saturating_add(1),
            version: self.version,
        }
    }

    #[inline]
    pub const fn bump_version(self) -> Self {
        Self {
            conf_ver: self.conf_ver,
            version: self.version.saturating_add(1),
        }
    }

    pub fn encode(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0..8].copy_from_slice(&self.conf_ver.to_be_bytes());
        out[8..16].copy_from_slice(&self.version.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 16 {
            return Err(MetaDbError::CodecError(format!(
                "expected 16 bytes for ShardEpoch, got {}",
                bytes.len()
            )));
        }
        Ok(Self {
            conf_ver: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            version: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        })
    }
}

/// Contiguous half-open binary user-key interval `[start_key, end_key)` within `[0x01, 0x04)`.
///
/// Supports:
/// - Table-aligned root ranges (`[0x01, 0x02)`, `[0x02, 0x03)`, `[0x03, 0x04)`),
/// - `DirectoryColocated` `PID`-boundary intervals (`[[0x01][start_pid], [0x01][end_pid])`),
/// - `DualModeFineGrained` composite `(PID, name)` sub-ranges (`[[0x01][pid][start_name], ...)`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CompositeKeyRange {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl CompositeKeyRange {
    pub fn new(start_key: Vec<u8>, end_key: Vec<u8>) -> Result<Self> {
        if start_key.is_empty() || end_key.is_empty() || start_key >= end_key {
            return Err(MetaDbError::ShardRoutingError(
                "invalid CompositeKeyRange: require non-empty start_key < end_key".to_string(),
            ));
        }
        Ok(Self { start_key, end_key })
    }

    /// Unified single-shard range `[0x01, 0x04)` covering all three user tables.
    pub fn all_user_tables() -> Self {
        Self {
            start_key: vec![TABLE_PREFIX_META],
            end_key: vec![TABLE_PREFIX_SECONDARY_INDEX + 1],
        }
    }

    /// Root range `[0x01, 0x02)` covering all of `MetaTable`.
    pub fn meta_table_root() -> Self {
        Self {
            start_key: vec![TABLE_PREFIX_META],
            end_key: vec![TABLE_PREFIX_QUOTA],
        }
    }

    /// Root range `[0x02, 0x03)` covering all of `QuotaTable`.
    pub fn quota_table_root() -> Self {
        Self {
            start_key: vec![TABLE_PREFIX_QUOTA],
            end_key: vec![TABLE_PREFIX_SECONDARY_INDEX],
        }
    }

    /// Root range `[0x03, 0x04)` covering all of `SecondaryIndexTable`.
    pub fn secondary_index_table_root() -> Self {
        Self {
            start_key: vec![TABLE_PREFIX_SECONDARY_INDEX],
            end_key: vec![TABLE_PREFIX_SECONDARY_INDEX + 1],
        }
    }

    /// Constructs a `MetaTable` `PID`-boundary range `[start_pid, end_pid)`.
    pub fn meta_pid_range(start_pid: InodeId, end_pid: InodeId) -> Result<Self> {
        if start_pid >= end_pid {
            return Err(MetaDbError::ShardRoutingError(
                "invalid meta_pid_range: require start_pid < end_pid".to_string(),
            ));
        }
        let start = MetaKey::encode_pid_prefix_start(start_pid);
        let end = if end_pid == u64::MAX {
            vec![TABLE_PREFIX_QUOTA]
        } else {
            MetaKey::encode_pid_prefix_start(end_pid)
        };
        Self::new(start, end)
    }

    /// Returns `true` if `user_key` falls inside `[start_key, end_key)`.
    #[inline]
    pub fn contains_key(&self, user_key: &[u8]) -> bool {
        user_key >= self.start_key.as_slice() && user_key < self.end_key.as_slice()
    }

    /// Returns `true` if non-empty `[query_start, query_end)` intersects `[start_key, end_key)`.
    #[inline]
    pub fn overlaps(&self, query_start: &[u8], query_end: &[u8]) -> bool {
        query_start < query_end
            && query_start < self.end_key.as_slice()
            && query_end > self.start_key.as_slice()
    }
}
