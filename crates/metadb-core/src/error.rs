use std::fmt;

use serde::{Deserialize, Serialize};

use crate::epoch::{ShardEpoch, TxnId};

/// Result alias for MetaDB operations.
pub type Result<T> = std::result::Result<T, MetaDbError>;

/// Canonical error variants across all MetaDB subsystems.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub enum MetaDbError {
    /// Attempted to use a reserved or illegal entry name (`"_attr_"`, `"."`, `".."`, empty, NUL, or a path separator).
    ReservedEntryName(String),
    /// Target file or directory entry already exists.
    AlreadyExists { pid: u64, name: String },
    /// Target file or directory entry was not found.
    NotFound { pid: u64, name: String },
    /// Target directory inode was not found.
    DirectoryNotFound(u64),
    /// Attempted to remove a non-empty directory via `rmdir`.
    DirectoryNotEmpty { dir_id: u64, child_count: usize },
    /// Operation type mismatch (e.g., `rmdir` on a regular file or `unlink` on a directory).
    InvalidEntryType { expected: String, actual: String },
    /// Snapshot read timestamp is older than the In-Memory MVCC retention window (`>5s`).
    TransactionTimeout {
        snapshot_hlc: u64,
        oldest_retained_hlc: u64,
    },
    /// Incoming remote HLC drifted ahead of local physical wall clock beyond `max_clock_drift_ms`.
    ClockSkewExceeded {
        remote_ms: u64,
        local_ms: u64,
        max_drift_ms: u64,
    },
    /// CRC32C checksum validation failed on disk or wire frame.
    ChecksumMismatch {
        context: String,
        expected: u32,
        actual: u32,
    },
    /// Non-final MANIFEST record or SSTable metadata grammar is corrupted.
    ManifestCorrupted(String),
    /// Key or value binary decoding failed.
    CodecError(String),
    /// Shard routing or boundary error.
    ShardRoutingError(String),
    /// Caller's `expected_epoch` did not match the shard's current `(conf_ver, version)` or range.
    EpochMismatch {
        shard_id: u64,
        expected: ShardEpoch,
        actual: ShardEpoch,
    },
    /// Target node is not the current Raft leader for `shard_id`.
    NotLeader {
        shard_id: u64,
        leader_hint: Option<u64>,
    },
    /// Shard cannot be split along `group_key = PID` because a single `PID` dominates it.
    UnsplittableJumboShard { shard_id: u64, pid: u64 },
    /// Encountered a conflicting 2PC `WriteIntent` on a target key.
    WriteIntentConflict {
        conflicting_txn_id: TxnId,
        primary_key: Vec<u8>,
        expire_hlc: u64,
    },
    /// 2PC transaction was aborted due to conflict, timeout, or superseded lease.
    TransactionAborted(String),
    /// Attempted to move a directory into itself or one of its own descendants.
    InvalidRenameCycle { dir_id: u64, dst_pid: u64 },
    /// Out-of-order gap detected in per-origin-shard auxiliary `QuotaDelta` stream.
    AuxOutOfOrderGap {
        origin_shard_id: u64,
        uid: u64,
        expected_after: (u64, u16),
        got_prev: (u64, u16),
    },
    /// Local filesystem or storage I/O failure.
    StorageIoError(String),
    /// Network or gRPC transport error.
    RpcError(String),
    /// Consensus or replication state error.
    ConsensusError(String),
    /// Lightweight SQL template parse error.
    SqlParseError(String),
    /// Caller supplied a negative file size. Sizes are non-negative byte counts.
    ///
    /// Appended so existing bincode variant tags stay stable for `ErrorDetailProto`.
    InvalidFileSize(i64),
    /// The 48-bit physical field and 16-bit logical counter are both at their maximum.
    /// Advancing further would wrap and break strict monotonicity.
    ///
    /// Appended after `InvalidFileSize` so that variant stays bincode index 23.
    HlcSaturated,
    /// A shard split must wait for transaction recovery before fencing its range.
    /// Appended after `HlcSaturated` so earlier variant tags stay stable.
    SplitDeferred(String),
    /// Cluster or node identity is missing, colliding, or reused.
    /// Appended after `SplitDeferred` so earlier variant tags stay stable.
    IdentityRejected(String),
    /// A write or linearizable quorum did not produce a committed result.
    ///
    /// Appended after `IdentityRejected` so earlier variant tags stay stable.
    /// Same-shard writes and 2PC may retry this inside the existing routing budget.
    /// The retry repeats the same proposal id. If that id already committed, the
    /// state machine returns the stored result and does not execute the command
    /// again. `AlreadyExists`, `EpochMismatch`, and other application results are
    /// not this variant. Storage corruption and a stopped Raft node stay
    /// [`ConsensusError`](Self::ConsensusError).
    RetryableConsensus {
        shard_id: u64,
        leader_hint: Option<u64>,
        kind: RetryableConsensusKind,
    },
}

/// Why a consensus operation can be proposed again without hiding a durable loss.
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
pub enum RetryableConsensusKind {
    /// This leader dropped the proposal's log entry while adopting another leader's log.
    /// Whether the entry committed elsewhere is unknown.
    LogDiscarded,
    /// Not enough voters answered to form a quorum. No committed result was returned.
    QuorumUnavailable,
}

impl fmt::Display for MetaDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetaDbError::ReservedEntryName(name) => {
                write!(f, "entry name '{}' is reserved or invalid", name)
            }
            MetaDbError::InvalidFileSize(size) => {
                write!(f, "file size {} is invalid (sizes must be >= 0)", size)
            }
            MetaDbError::AlreadyExists { pid, name } => {
                write!(f, "entry ({}, '{}') already exists", pid, name)
            }
            MetaDbError::NotFound { pid, name } => {
                write!(f, "entry ({}, '{}') not found", pid, name)
            }
            MetaDbError::DirectoryNotFound(id) => {
                write!(f, "directory inode {} not found", id)
            }
            MetaDbError::DirectoryNotEmpty { dir_id, child_count } => write!(
                f,
                "directory {} is not empty ({} live children remain)",
                dir_id, child_count
            ),
            MetaDbError::InvalidEntryType { expected, actual } => {
                write!(f, "invalid entry type: expected {}, got {}", expected, actual)
            }
            MetaDbError::TransactionTimeout {
                snapshot_hlc,
                oldest_retained_hlc,
            } => write!(
                f,
                "transaction snapshot HLC {} expired (oldest retained in-memory MVCC HLC is {})",
                snapshot_hlc, oldest_retained_hlc
            ),
            MetaDbError::ClockSkewExceeded {
                remote_ms,
                local_ms,
                max_drift_ms,
            } => write!(
                f,
                "remote HLC physical_ms {} exceeds local_ms {} + max_drift_ms {}",
                remote_ms, local_ms, max_drift_ms
            ),
            MetaDbError::ChecksumMismatch {
                context,
                expected,
                actual,
            } => write!(
                f,
                "CRC32C mismatch in {}: expected 0x{:08x}, got 0x{:08x}",
                context, expected, actual
            ),
            MetaDbError::ManifestCorrupted(msg) => write!(f, "manifest corrupted: {}", msg),
            MetaDbError::CodecError(msg) => write!(f, "codec error: {}", msg),
            MetaDbError::ShardRoutingError(msg) => write!(f, "shard routing error: {}", msg),
            MetaDbError::EpochMismatch {
                shard_id,
                expected,
                actual,
            } => write!(
                f,
                "epoch mismatch on shard {}: expected {:?}, actual {:?}",
                shard_id, expected, actual
            ),
            MetaDbError::NotLeader {
                shard_id,
                leader_hint,
            } => write!(
                f,
                "not leader for shard {} (leader_hint: {:?})",
                shard_id, leader_hint
            ),
            MetaDbError::UnsplittableJumboShard { shard_id, pid } => write!(
                f,
                "shard {} is an unsplittable jumbo shard dominated by single PID {}",
                shard_id, pid
            ),
            MetaDbError::WriteIntentConflict {
                conflicting_txn_id, ..
            } => write!(
                f,
                "conflicting 2PC write intent held by txn {}",
                conflicting_txn_id
            ),
            MetaDbError::TransactionAborted(msg) => write!(f, "transaction aborted: {}", msg),
            MetaDbError::InvalidRenameCycle { dir_id, dst_pid } => write!(
                f,
                "invalid directory rename cycle moving dir {} under {}",
                dir_id, dst_pid
            ),
            MetaDbError::AuxOutOfOrderGap {
                origin_shard_id,
                uid,
                expected_after,
                got_prev,
            } => write!(
                f,
                "out-of-order quota delta gap from shard {} for uid {}: expected after {:?}, got {:?}",
                origin_shard_id, uid, expected_after, got_prev
            ),
            MetaDbError::StorageIoError(msg) => write!(f, "storage I/O error: {}", msg),
            MetaDbError::RpcError(msg) => write!(f, "RPC error: {}", msg),
            MetaDbError::ConsensusError(msg) => write!(f, "consensus error: {}", msg),
            MetaDbError::SqlParseError(msg) => write!(f, "SQL template parse error: {}", msg),
            MetaDbError::HlcSaturated => write!(
                f,
                "HLC saturated at the 48-bit physical ceiling; refusing to wrap the logical counter"
            ),
            MetaDbError::SplitDeferred(msg) => write!(f, "shard split deferred: {}", msg),
            MetaDbError::IdentityRejected(msg) => write!(f, "identity rejected: {}", msg),
            MetaDbError::RetryableConsensus {
                shard_id,
                leader_hint,
                kind,
            } => write!(
                f,
                "retryable consensus error on shard {} ({:?}, leader_hint: {:?})",
                shard_id, kind, leader_hint
            ),
        }
    }
}

impl MetaDbError {
    /// A write may be proposed again inside the existing routing budget.
    ///
    /// Application results such as `AlreadyExists` and `EpochMismatch` are not
    /// retryable here. `EpochMismatch` on a multi-key prepare still aborts that
    /// round and regroups; it is not proposed again as the same batch.
    pub fn is_retryable_write_consensus(&self) -> bool {
        matches!(
            self,
            MetaDbError::NotLeader { .. } | MetaDbError::RetryableConsensus { .. }
        )
    }
}

impl From<std::io::Error> for MetaDbError {
    fn from(err: std::io::Error) -> Self {
        MetaDbError::StorageIoError(err.to_string())
    }
}

impl std::error::Error for MetaDbError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_retry_covers_quorum_and_discard_not_application_results() {
        assert!(MetaDbError::NotLeader {
            shard_id: 1,
            leader_hint: Some(2),
        }
        .is_retryable_write_consensus());
        assert!(MetaDbError::RetryableConsensus {
            shard_id: 1,
            leader_hint: Some(2),
            kind: RetryableConsensusKind::LogDiscarded,
        }
        .is_retryable_write_consensus());
        assert!(MetaDbError::RetryableConsensus {
            shard_id: 1,
            leader_hint: None,
            kind: RetryableConsensusKind::QuorumUnavailable,
        }
        .is_retryable_write_consensus());
        assert!(!MetaDbError::AlreadyExists {
            pid: 1,
            name: "a".to_string(),
        }
        .is_retryable_write_consensus());
        assert!(!MetaDbError::EpochMismatch {
            shard_id: 1,
            expected: ShardEpoch::initial(),
            actual: ShardEpoch::initial().bump_conf_ver(),
        }
        .is_retryable_write_consensus());
        assert!(
            !MetaDbError::ConsensusError("raft stopped".to_string()).is_retryable_write_consensus()
        );
    }
}
