//! Online zero-downtime replica evolution (`1-Voter -> Learner -> 3-Voter`) via OpenRaft joint consensus.

use std::collections::BTreeSet;
use std::time::Duration;

use openraft::async_runtime::watch::WatchReceiver;
use openraft::BasicNode;

use metadb_core::{MetaDbError, NodeId, Result};

use crate::network::MetaDbRaft;

pub const DEFAULT_LEARNER_CATCHUP_THRESHOLD_ENTRIES: u64 = 64;

pub struct ConfChangeOrchestrator;

impl ConfChangeOrchestrator {
    /// Adds `(node_id, grpc_addr)` as a non-voting `Learner`, waits until its replicated log lag
    /// drops within `catchup_threshold` entries of the leader's `last_log_index` (streaming an
    /// SSTable snapshot first if needed), and promotes the new voter set via joint consensus.
    pub async fn add_learners_and_promote_voters(
        raft: &MetaDbRaft,
        learners_to_add: &[(NodeId, String)],
        final_voters: BTreeSet<NodeId>,
        catchup_threshold: u64,
        timeout: Duration,
    ) -> Result<()> {
        for (node_id, addr) in learners_to_add {
            raft.add_learner(*node_id, BasicNode::new(addr), false)
                .await
                .map_err(|e| MetaDbError::ConsensusError(format!("add_learner failed: {}", e)))?;
        }

        let start = tokio::time::Instant::now();
        let mut metrics_rx = raft.metrics();
        loop {
            {
                let m = metrics_rx.borrow_watched().clone();
                let last_idx = m.last_log_index.unwrap_or(0);
                let all_caught_up = if let Some(repl) = &m.replication {
                    learners_to_add.iter().all(|(nid, _)| {
                        let matched = repl
                            .get(nid)
                            .and_then(|opt| opt.map(|id| id.index))
                            .unwrap_or(0);
                        last_idx.saturating_sub(matched) <= catchup_threshold
                    })
                } else {
                    false
                };
                if all_caught_up {
                    break;
                }
            }

            if start.elapsed() > timeout {
                return Err(MetaDbError::ConsensusError(
                    "timed out waiting for learner replication catch-up".to_string(),
                ));
            }
            tokio::select! {
                _ = metrics_rx.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(25)) => {}
            }
        }

        tokio::time::timeout(timeout, raft.change_membership(final_voters, false))
            .await
            .map_err(|_| MetaDbError::ConsensusError("change_membership timed out".to_string()))?
            .map_err(|e| MetaDbError::ConsensusError(format!("change_membership failed: {}", e)))?;

        Ok(())
    }

    /// Replaces the voter set. Used to drop replicas; learners are added separately.
    pub async fn change_voters(raft: &MetaDbRaft, final_voters: BTreeSet<NodeId>) -> Result<()> {
        tokio::time::timeout(
            Duration::from_secs(20),
            raft.change_membership(final_voters, false),
        )
        .await
        .map_err(|_| MetaDbError::ConsensusError("change_membership timed out".to_string()))?
        .map_err(|e| MetaDbError::ConsensusError(format!("change_membership failed: {}", e)))?;
        Ok(())
    }
}
