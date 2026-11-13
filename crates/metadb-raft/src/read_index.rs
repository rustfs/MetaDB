//! Linearizable `ReadIndex` micro-batcher with post-wait `ShardEpoch` revalidation (`Resolution B16` / `G-C2`).

use std::sync::Arc;
use std::time::Duration;

use openraft::errors::{LinearizableReadError, RaftError};
use openraft::ReadPolicy;
use tokio::sync::{oneshot, watch, Mutex};

use metadb_core::{HlcTimestamp, MetaDbError, Result, ShardEpoch, ShardId};
use metadb_storage::{LifecycleStorageEngine, SnapshotGuard};

use crate::network::MetaDbRaft;
use crate::types::AppliedFrontier;

const MICRO_BATCH_WINDOW: Duration = Duration::from_micros(200);

/// Verified linearizable read token produced after `applied_index >= read_index`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadToken {
    pub read_index: u64,
    pub max_applied_hlc: HlcTimestamp,
    pub epoch: ShardEpoch,
}

type WaiterSender = oneshot::Sender<Result<u64>>;

/// Coalesces concurrent `ensure_linearizable(ReadPolicy::ReadIndex)` requests within `200µs`
/// so high-QPS `dirstat`/`lookup`/`listdir` calls share a single quorum heartbeat round.
pub struct ReadIndexBatcher {
    shard_id: ShardId,
    raft: MetaDbRaft,
    frontier_rx: watch::Receiver<AppliedFrontier>,
    pending_waiters: Arc<Mutex<Vec<WaiterSender>>>,
}

impl ReadIndexBatcher {
    pub fn new(
        shard_id: ShardId,
        raft: MetaDbRaft,
        frontier_rx: watch::Receiver<AppliedFrontier>,
    ) -> Self {
        Self {
            shard_id,
            raft,
            frontier_rx,
            pending_waiters: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn current_frontier(&self) -> AppliedFrontier {
        *self.frontier_rx.borrow()
    }

    /// Obtains a quorum-confirmed `ReadToken` and revalidates `expected_epoch` *after*
    /// `applied_index >= read_index` (`Resolution B16` / `G-C2`).
    pub async fn linearizable_token(&self, expected_epoch: ShardEpoch) -> Result<ReadToken> {
        // Fast pre-check before network heartbeat.
        let pre_frontier = *self.frontier_rx.borrow();
        if pre_frontier.epoch != expected_epoch {
            return Err(MetaDbError::EpochMismatch {
                shard_id: self.shard_id,
                expected: expected_epoch,
                actual: pre_frontier.epoch,
            });
        }

        let read_index = self.coalesced_read_index().await?;
        let post_frontier = self.wait_applied_at_least(read_index).await?;

        // Mandatory post-wait epoch revalidation: catches any `ExecuteSplit` or `ConfChange`
        // committed in `(pre_frontier.raft_index ..= read_index]`.
        if post_frontier.epoch != expected_epoch {
            return Err(MetaDbError::EpochMismatch {
                shard_id: self.shard_id,
                expected: expected_epoch,
                actual: post_frontier.epoch,
            });
        }

        Ok(ReadToken {
            read_index,
            max_applied_hlc: post_frontier.hlc,
            epoch: post_frontier.epoch,
        })
    }

    /// Acquires a linearizable `SnapshotGuard` fenced by `ReadIndex` and strict `ShardEpoch` equality.
    pub async fn acquire_linearizable_guard(
        &self,
        expected_epoch: ShardEpoch,
        engine: &LifecycleStorageEngine,
    ) -> Result<(ReadToken, SnapshotGuard)> {
        let token = self.linearizable_token(expected_epoch).await?;
        let snap = engine.acquire_linearizable_snapshot(token.read_index)?;
        Ok((token, snap))
    }

    async fn coalesced_read_index(&self) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        let is_driver = {
            let mut guard = self.pending_waiters.lock().await;
            let first = guard.is_empty();
            guard.push(tx);
            first
        };

        if is_driver {
            // If multiple callers arrive within 200µs, batch them into one quorum check;
            // in single-caller / 1-Voter mode, yield once so concurrent tasks on the executor can join.
            tokio::task::yield_now().await;
            if self.pending_waiters.lock().await.len() > 1 {
                tokio::time::sleep(MICRO_BATCH_WINDOW).await;
            }

            let waiters = {
                let mut guard = self.pending_waiters.lock().await;
                std::mem::take(&mut *guard)
            };

            let res = self.execute_ensure_linearizable().await;
            for w in waiters {
                let _ = w.send(res.clone());
            }
        }

        rx.await
            .map_err(|_| MetaDbError::ConsensusError("ReadIndex driver cancelled".to_string()))?
    }

    async fn execute_ensure_linearizable(&self) -> Result<u64> {
        match self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
            Ok(read_log_id) => Ok(read_log_id.index()),
            Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(fwd))) => {
                Err(MetaDbError::NotLeader {
                    shard_id: self.shard_id,
                    leader_hint: fwd.leader_id,
                })
            }
            Err(other) => Err(MetaDbError::ConsensusError(other.to_string())),
        }
    }

    async fn wait_applied_at_least(&self, target_index: u64) -> Result<AppliedFrontier> {
        let mut rx = self.frontier_rx.clone();
        loop {
            let cur = *rx.borrow_and_update();
            if cur.raft_index >= target_index {
                return Ok(cur);
            }
            rx.changed()
                .await
                .map_err(|_| MetaDbError::ConsensusError("state machine closed".to_string()))?;
        }
    }
}
