//! Linearizable `ReadIndex` micro-batcher with post-wait `ShardEpoch` revalidation (`Resolution B16` / `G-C2`).

use std::sync::Arc;
use std::time::Duration;

use openraft::ReadPolicy;
use tokio::sync::{oneshot, watch, Mutex};

use metadb_core::{HlcTimestamp, MetaDbError, Result, ShardEpoch, ShardId};
use metadb_storage::SnapshotGuard;

use crate::network::MetaDbRaft;
use crate::state_machine::ShardStateMachine;
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

#[derive(Default)]
struct BatcherState {
    driver_running: bool,
    pending_waiters: Vec<WaiterSender>,
}

/// Coalesces concurrent `ensure_linearizable(ReadPolicy::ReadIndex)` requests within `200µs`
/// and serializes follow-up waves while a quorum round is in-flight so high-QPS
/// `dirstat`/`lookup`/`listdir` calls share at most one active quorum heartbeat round per shard.
pub struct ReadIndexBatcher {
    shard_id: ShardId,
    raft: MetaDbRaft,
    frontier_rx: watch::Receiver<AppliedFrontier>,
    state: Arc<Mutex<BatcherState>>,
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
            state: Arc::new(Mutex::new(BatcherState::default())),
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
        state_machine: &ShardStateMachine,
    ) -> Result<(ReadToken, SnapshotGuard)> {
        let token = self.linearizable_token(expected_epoch).await?;
        let snap = state_machine.acquire_snapshot(expected_epoch, token.read_index, None)?;
        Ok((token, snap))
    }

    async fn coalesced_read_index(&self) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        let spawn_driver = {
            let mut guard = self.state.lock().await;
            guard.pending_waiters.push(tx);
            if guard.driver_running {
                false
            } else {
                guard.driver_running = true;
                true
            }
        };

        if spawn_driver {
            // Spawn the batch driver on an independent task so cancelling a single caller's
            // request future never strands peer waiters in `pending_waiters`, and any callers
            // arriving while `ensure_linearizable` is in-flight coalesce into one follow-up wave.
            let state = self.state.clone();
            let raft = self.raft.clone();
            let shard_id = self.shard_id;
            tokio::spawn(async move {
                loop {
                    tokio::task::yield_now().await;
                    if state.lock().await.pending_waiters.len() > 1
                        || metadb_core::buggify("raft.read_index_coalesce_yield")
                    {
                        tokio::time::sleep(MICRO_BATCH_WINDOW).await;
                    }

                    let waiters = {
                        let mut guard = state.lock().await;
                        if guard.pending_waiters.is_empty() {
                            guard.driver_running = false;
                            break;
                        }
                        std::mem::take(&mut guard.pending_waiters)
                    };
                    if waiters.len() > 1 {
                        metadb_core::code_probe("probe.raft.read_index_coalesced");
                    }

                    let res = match raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
                        Ok(read_log_id) => Ok(read_log_id.index()),
                        Err(error) => Err(crate::error_map::map_linearizable_read_error(
                            shard_id, error,
                        )),
                    };
                    for w in waiters {
                        let _ = w.send(res.clone());
                    }

                    let mut guard = state.lock().await;
                    if guard.pending_waiters.is_empty() {
                        guard.driver_running = false;
                        break;
                    }
                }
            });
        }

        rx.await
            .map_err(|_| MetaDbError::ConsensusError("ReadIndex driver cancelled".to_string()))?
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
