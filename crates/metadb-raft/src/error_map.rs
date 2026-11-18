//! Maps OpenRaft write and read errors onto MetaDB errors.
//!
//! `ForwardToLeader` stays `NotLeader`. A discarded log entry and a missing
//! quorum become `RetryableConsensus` so the write path can try again inside
//! its existing budget. Storage failure and a stopped node stay `ConsensusError`
//! because retrying them would hide a durable or fatal stop.

use openraft::error::ClientWriteError;
use openraft::error::Fatal;
use openraft::error::ForwardToLeader;
use openraft::error::LinearizableReadError;
use openraft::error::RaftError;

use metadb_core::{MetaDbError, RetryableConsensusKind, ShardId};

use crate::types::MetaDbTypeConfig;

pub(crate) fn map_client_write_error(
    shard_id: ShardId,
    error: RaftError<MetaDbTypeConfig, ClientWriteError<MetaDbTypeConfig>>,
) -> MetaDbError {
    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            not_leader(shard_id, forward)
        }
        RaftError::APIError(ClientWriteError::LogEntryDiscarded(forward)) => {
            MetaDbError::RetryableConsensus {
                shard_id,
                leader_hint: forward.leader_id,
                kind: RetryableConsensusKind::LogDiscarded,
            }
        }
        RaftError::APIError(other) => MetaDbError::ConsensusError(other.to_string()),
        RaftError::Fatal(fatal) => map_fatal(fatal),
    }
}

pub(crate) fn map_linearizable_read_error(
    shard_id: ShardId,
    error: RaftError<MetaDbTypeConfig, LinearizableReadError<MetaDbTypeConfig>>,
) -> MetaDbError {
    match error {
        RaftError::APIError(LinearizableReadError::ForwardToLeader(forward)) => {
            not_leader(shard_id, forward)
        }
        RaftError::APIError(LinearizableReadError::QuorumNotEnough(_)) => {
            MetaDbError::RetryableConsensus {
                shard_id,
                leader_hint: None,
                kind: RetryableConsensusKind::QuorumUnavailable,
            }
        }
        RaftError::Fatal(fatal) => map_fatal(fatal),
    }
}

fn not_leader(shard_id: ShardId, forward: ForwardToLeader<MetaDbTypeConfig>) -> MetaDbError {
    MetaDbError::NotLeader {
        shard_id,
        leader_hint: forward.leader_id,
    }
}

fn map_fatal(fatal: Fatal<MetaDbTypeConfig>) -> MetaDbError {
    MetaDbError::ConsensusError(fatal.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::error::ForwardReason;

    fn discarded(
        leader: Option<u64>,
    ) -> RaftError<MetaDbTypeConfig, ClientWriteError<MetaDbTypeConfig>> {
        RaftError::APIError(ClientWriteError::LogEntryDiscarded(ForwardToLeader {
            leader_id: leader,
            leader_node: None,
            reason: ForwardReason::NotLeader,
        }))
    }

    #[test]
    fn discarded_log_is_retryable_and_names_the_leader() {
        let error = map_client_write_error(7, discarded(Some(3)));
        assert!(error.is_retryable_write_consensus());
        assert_eq!(
            error,
            MetaDbError::RetryableConsensus {
                shard_id: 7,
                leader_hint: Some(3),
                kind: RetryableConsensusKind::LogDiscarded,
            }
        );
    }

    #[test]
    fn stopped_raft_is_not_retried() {
        let error = map_client_write_error(1, RaftError::Fatal(Fatal::Stopped));
        assert!(!error.is_retryable_write_consensus());
        assert!(matches!(error, MetaDbError::ConsensusError(_)));
    }

    #[test]
    fn missing_read_quorum_is_distinct_from_consensus_error() {
        use openraft::error::QuorumNotEnough;
        let error = map_linearizable_read_error(
            4,
            RaftError::APIError(LinearizableReadError::QuorumNotEnough(QuorumNotEnough {
                cluster: "{1,2,3}".to_string(),
                got: std::collections::BTreeSet::from([1]),
            })),
        );
        assert_eq!(
            error,
            MetaDbError::RetryableConsensus {
                shard_id: 4,
                leader_hint: None,
                kind: RetryableConsensusKind::QuorumUnavailable,
            }
        );
    }
}
