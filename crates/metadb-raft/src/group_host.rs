//! Per-shard `ShardRaftGroup` wrapper and multi-shard `ShardGroupHost` with `NODE_SHARD_REGISTRY`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use openraft::async_runtime::watch::WatchReceiver;
use openraft::errors::RaftError;
use openraft::storage::RaftStateMachine;
use openraft::{BasicNode, Config};
use serde::{Deserialize, Serialize};

use metadb_core::codec;
use metadb_core::{
    append_crc32c_le, verify_crc32c_le, HlcTimestamp, HybridLogicalClock, MetaDbError, NodeId,
    Result, ShardEpoch, ShardId,
};
use metadb_storage::sstable::sync_dir;
use metadb_storage::{LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache, SnapshotGuard};

use crate::log_store::SegmentedRaftLogStore;
use crate::network::{
    ChannelPool, LocalProposer, MetaDbRaft, MultiRaftRouter, TonicRaftNetworkFactory,
};
use crate::read_index::{ReadIndexBatcher, ReadToken};
use crate::state_machine::{ShardApplyHandler, ShardStateMachine};
use crate::types::{MetaDbRaftRequest, MetaDbRaftResponse, ShardWatermarkTracker};

/// Union of the voter configs in an applied membership.
///
/// A uniform config is a single set. A joint config contributes every voter that
/// Raft has already applied, so status does not keep the previous uniform set.
pub fn voter_ids_from_joint_configs<I, S>(configs: I) -> BTreeSet<NodeId>
where
    I: IntoIterator<Item = S>,
    S: IntoIterator<Item = NodeId>,
{
    configs.into_iter().flatten().collect()
}

/// Tuning parameters for OpenRaft timers and snapshot policies.
#[derive(Debug, Clone)]
pub struct RaftNodeOptions {
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub max_in_snapshot_log_to_keep: u64,
    pub max_segment_bytes: u64,
}

impl Default for RaftNodeOptions {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 150,
            election_timeout_min_ms: 450,
            election_timeout_max_ms: 900,
            max_in_snapshot_log_to_keep: 0,
            max_segment_bytes: crate::log_store::DEFAULT_MAX_SEGMENT_BYTES,
        }
    }
}

/// Parks `client_write` callers inside `propose` until a failure test releases them.
///
/// Production leaves the gate unarmed, so `enter` returns immediately. A test arms it,
/// waits until at least one proposal is blocked, and only then starts Raft shutdown.
#[derive(Debug, Default)]
pub struct ProposalShutdownGate {
    armed: AtomicBool,
    released: AtomicBool,
    depth: AtomicUsize,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl ProposalShutdownGate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    pub fn depth(&self) -> usize {
        self.depth.load(Ordering::Acquire)
    }

    /// Blocks inside `ShardRaftGroup::propose` immediately before `client_write` when armed.
    pub async fn enter(&self) {
        if !self.armed.load(Ordering::Acquire) {
            return;
        }
        self.depth.fetch_add(1, Ordering::AcqRel);
        self.arrived.notify_waiters();
        loop {
            let notified = self.release.notified();
            tokio::pin!(notified);
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    pub async fn wait_for_arrival(&self) {
        loop {
            if self.depth.load(Ordering::Acquire) >= 1 {
                return;
            }
            let notified = self.arrived.notified();
            tokio::pin!(notified);
            if self.depth.load(Ordering::Acquire) >= 1 {
                return;
            }
            notified.await;
        }
    }

    /// Records that shutdown is starting while `depth >= 1`, then lets the parked
    /// `client_write` run concurrently with Raft shutdown.
    pub fn release_for_shutdown(&self) {
        let depth = self.depth.load(Ordering::Acquire);
        assert!(
            depth >= 1,
            "Raft shutdown started with no proposal waiting inside propose()"
        );
        self.released.store(true, Ordering::Release);
        self.release.notify_waiters();
    }
}

/// Active Multi-Raft group hosting one shard's consensus log, state machine, and `ReadIndexBatcher`.
pub struct ShardRaftGroup {
    shard_id: ShardId,
    node_id: NodeId,
    raft: MetaDbRaft,
    log_store: SegmentedRaftLogStore,
    state_machine: ShardStateMachine,
    read_batcher: Arc<ReadIndexBatcher>,
    watermarks: Arc<ShardWatermarkTracker>,
    clock: Arc<HybridLogicalClock>,
    proposal_gate: RwLock<Option<Arc<ProposalShutdownGate>>>,
    channel_pool: ChannelPool,
    /// `(remaining failures, leader hint)` injected before `client_write`.
    testing_not_leader: std::sync::Mutex<(u32, Option<NodeId>)>,
    testing_epoch_mismatch: std::sync::atomic::AtomicU32,
    testing_log_discarded: std::sync::atomic::AtomicU32,
}

impl ShardRaftGroup {
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        shard_id: ShardId,
        node_id: NodeId,
        shard_dir: impl AsRef<Path>,
        block_cache: Arc<ShardedBlockCache>,
        lsm_config: LsmEngineConfig,
        raft_opts: RaftNodeOptions,
        handler: Arc<dyn ShardApplyHandler>,
        watermarks: Arc<ShardWatermarkTracker>,
        clock: Arc<HybridLogicalClock>,
        router: &MultiRaftRouter,
        channel_pool: ChannelPool,
    ) -> Result<Arc<Self>> {
        let shard_dir = shard_dir.as_ref().to_path_buf();
        let raft_dir = shard_dir.join("raft");

        let (state_machine, frontier_rx) = ShardStateMachine::open(
            shard_id,
            &shard_dir,
            block_cache,
            lsm_config,
            handler,
            watermarks.clone(),
        )?;

        let log_store = SegmentedRaftLogStore::open_with_segment_size(
            &raft_dir,
            raft_opts.max_segment_bytes,
            watermarks.clone(),
        )
        .map_err(|e| MetaDbError::StorageIoError(e.to_string()))?;

        let config = Arc::new(
            Config {
                cluster_name: format!("metadb-shard-{}", shard_id),
                heartbeat_interval: raft_opts.heartbeat_interval_ms,
                election_timeout_min: raft_opts.election_timeout_min_ms,
                election_timeout_max: raft_opts.election_timeout_max_ms,
                max_in_snapshot_log_to_keep: raft_opts.max_in_snapshot_log_to_keep,
                purge_batch_size: 256,
                ..Default::default()
            }
            .validate()
            .map_err(|e| MetaDbError::ConsensusError(e.to_string()))?,
        );

        let network = TonicRaftNetworkFactory::new(shard_id, channel_pool.clone());
        let raft = MetaDbRaft::new(
            node_id,
            config,
            network,
            log_store.clone(),
            state_machine.clone(),
        )
        .await
        .map_err(|e| MetaDbError::ConsensusError(e.to_string()))?;

        // Replay a committed tail into the WAL-less memtable before serving.
        // A fresh log has no committed entry, and the recovery condition stays
        // false until initialize, so waiting here only burns the timeout.
        if log_store.has_committed_entries() {
            let _ = raft.wait_for_recovery(Some(Duration::from_secs(5))).await;
        }

        router.register_group(shard_id, raft.clone());
        let read_batcher = Arc::new(ReadIndexBatcher::new(shard_id, raft.clone(), frontier_rx));

        let group = Arc::new(Self {
            shard_id,
            node_id,
            raft,
            log_store,
            state_machine,
            read_batcher,
            watermarks,
            clock,
            proposal_gate: RwLock::new(None),
            channel_pool,
            testing_not_leader: std::sync::Mutex::new((0, None)),
            testing_epoch_mismatch: std::sync::atomic::AtomicU32::new(0),
            testing_log_discarded: std::sync::atomic::AtomicU32::new(0),
        });
        router.register_local_proposer(shard_id, group.clone());
        Ok(group)
    }

    /// The next `count` proposals fail with `EpochMismatch` before reaching Raft.
    pub fn testing_fail_next_epoch_mismatch(&self, count: u32) {
        self.testing_epoch_mismatch
            .store(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// The next `count` `propose` calls return `NotLeader` before reaching Raft.
    pub fn testing_fail_next_proposals(&self, count: u32, leader_hint: Option<NodeId>) {
        *self.testing_not_leader.lock().unwrap() = (count, leader_hint);
    }

    /// The next `count` proposals fail as a discarded log entry before reaching Raft.
    pub fn testing_fail_next_log_discarded(&self, count: u32) {
        self.testing_log_discarded
            .store(count, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn current_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow_watched().current_leader
    }

    pub fn voter_count(&self) -> usize {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow_watched();
        snapshot.membership_config.membership().voter_ids().count()
    }

    pub fn peer_addr(&self, node_id: NodeId) -> Option<String> {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow_watched();
        let addr = snapshot
            .membership_config
            .membership()
            .get_node(&node_id)?
            .addr
            .clone();
        if addr.is_empty() {
            None
        } else {
            Some(addr)
        }
    }

    pub fn set_proposal_shutdown_gate(&self, gate: Option<Arc<ProposalShutdownGate>>) {
        *self.proposal_gate.write().unwrap() = gate;
    }

    /// Bootstraps initial cluster membership (e.g., `1-Voter` in Stage 1 or `3-Voter` on split child)
    /// and deterministically awaits initial leader election and membership application for single-voter groups.
    pub async fn initialize_if_needed(&self, members: BTreeMap<NodeId, String>) -> Result<()> {
        let should_wait_leader = members.len() == 1 && members.contains_key(&self.node_id);
        let nodes: BTreeMap<NodeId, BasicNode> = members
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();
        match self.raft.initialize(nodes).await {
            Ok(()) => {}
            Err(RaftError::APIError(openraft::errors::InitializeError::NotAllowed(_))) => {}
            Err(e) => return Err(MetaDbError::ConsensusError(e.to_string())),
        }
        if should_wait_leader {
            // Single-voter election is local, but a loaded test runner can
            // delay the metrics update past a 5 second budget.
            self.wait_for_leader(Duration::from_secs(15)).await?;
        }
        Ok(())
    }

    /// Waits up to `timeout` until OpenRaft elects a leader and applies the current membership log entry.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<NodeId> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut rx = self.raft.metrics();
        loop {
            {
                let m = rx.borrow_watched();
                if let Some(leader) = m.current_leader {
                    if m.last_applied.is_some() {
                        return Ok(leader);
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(MetaDbError::ConsensusError(format!(
                    "timed out waiting for leader election on shard {}",
                    self.shard_id
                )));
            }
            if tokio::time::timeout_at(deadline, rx.changed())
                .await
                .is_err()
            {
                return Err(MetaDbError::ConsensusError(format!(
                    "timed out waiting for leader metrics change on shard {}",
                    self.shard_id
                )));
            }
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn raft(&self) -> &MetaDbRaft {
        &self.raft
    }

    pub fn log_store(&self) -> &SegmentedRaftLogStore {
        &self.log_store
    }

    pub fn state_machine(&self) -> &ShardStateMachine {
        &self.state_machine
    }

    /// `Some` when this process must not be reported as a live voter.
    pub fn storage_unrecoverable(&self) -> Option<String> {
        self.state_machine.storage_unrecoverable()
    }

    /// Rewrite a published snapshot that disappeared, or refuse to serve.
    ///
    /// A missing in-use snapshot used to stop RaftCore while cluster status
    /// still answered from the cached voter set. Repair from the LSM when it
    /// can cover the purged log. Otherwise leave an explicit unrecoverable
    /// error so status and later reads do not pretend the voter is healthy.
    pub async fn repair_published_snapshot(&self) -> Result<()> {
        if let Some(reason) = self.storage_unrecoverable() {
            return Err(MetaDbError::StorageIoError(format!(
                "snapshot unrecoverable: {reason}"
            )));
        }
        if !self.state_machine.published_snapshot_missing() {
            return Ok(());
        }
        let mut state_machine = self.state_machine.clone();
        match state_machine.get_current_snapshot().await {
            Ok(_) => Ok(()),
            Err(error) => Err(MetaDbError::StorageIoError(error.to_string())),
        }
    }

    pub fn engine(&self) -> Arc<LifecycleStorageEngine> {
        self.state_machine.engine()
    }

    pub fn watermarks(&self) -> &Arc<ShardWatermarkTracker> {
        &self.watermarks
    }

    pub fn read_batcher(&self) -> &Arc<ReadIndexBatcher> {
        &self.read_batcher
    }

    pub fn current_epoch(&self) -> ShardEpoch {
        self.state_machine.handler().current_epoch()
    }

    /// True when the applied Raft membership lists `node_id` as a voter.
    pub fn membership_contains_voter(&self, node_id: NodeId) -> bool {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow_watched().clone();
        let is_voter = snapshot
            .membership_config
            .membership()
            .voter_ids()
            .any(|id| id == node_id);
        is_voter
    }

    /// Voter ids and dial addresses from a uniform Raft membership.
    ///
    /// A joint configuration is refused so a later add or remove is not planned from a
    /// half-applied voter set. `change_membership` can return before this metrics watch
    /// shows the uniform config, so callers retry for a short interval.
    pub fn uniform_voter_addrs(&self) -> Result<BTreeMap<NodeId, String>> {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow_watched().clone();
        let effective = snapshot
            .membership_config
            .membership()
            .get_joint_config()
            .clone();
        if effective.len() != 1 {
            return Err(MetaDbError::ConsensusError(format!(
                "shard {} membership change is still in joint consensus",
                self.shard_id
            )));
        }
        let mut out = BTreeMap::new();
        for id in &effective[0] {
            let addr = snapshot
                .membership_config
                .membership()
                .get_node(id)
                .map(|node| node.addr.clone())
                .unwrap_or_default();
            if addr.is_empty() {
                return Err(MetaDbError::ConsensusError(format!(
                    "voter {id} on shard {} has no raft address",
                    self.shard_id
                )));
            }
            out.insert(*id, addr);
        }
        Ok(out)
    }

    /// Voter ids in the applied Raft membership, including every config of a joint pair.
    ///
    /// Status and `GetRoutes` use this so a third voter that is already in the applied
    /// membership is visible before the uniform config commits. Membership changes are
    /// still planned from [`uniform_voter_addrs`](Self::uniform_voter_addrs).
    pub fn applied_voter_addrs(&self) -> Result<BTreeMap<NodeId, String>> {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow_watched().clone();
        let membership = snapshot.membership_config.membership();
        let ids = voter_ids_from_joint_configs(
            membership
                .get_joint_config()
                .iter()
                .map(|set| set.iter().copied()),
        );
        if ids.is_empty() {
            return Err(MetaDbError::ConsensusError(format!(
                "shard {} has no applied voters",
                self.shard_id
            )));
        }
        let mut out = BTreeMap::new();
        for id in ids {
            let addr = membership
                .get_node(&id)
                .map(|node| node.addr.clone())
                .unwrap_or_default();
            out.insert(id, addr);
        }
        Ok(out)
    }

    pub async fn wait_uniform_voter_addrs(&self) -> Result<BTreeMap<NodeId, String>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match self.uniform_voter_addrs() {
                Ok(voters) => return Ok(voters),
                Err(err) if tokio::time::Instant::now() >= deadline => return Err(err),
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    }

    /// Appends a blank entry, snapshots the state machine, and purges the log through that
    /// snapshot so a new replica must install the checkpoint instead of replaying an empty prefix.
    ///
    /// OpenRaft publishes the snapshot index before `purged` catches up, and the aux-outbox
    /// watermark that gates the purge is advanced by the maintenance tick. Retry the flush
    /// until those indexes meet instead of failing the first time they disagree.
    pub async fn prepare_learner_snapshot(&self) -> Result<u64> {
        self.raft.write_blank().await.map_err(|e| {
            MetaDbError::ConsensusError(format!(
                "blank barrier before learner snapshot on shard {} failed: {e}",
                self.shard_id
            ))
        })?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let _ = self.flush_snapshot_and_purge().await?;
            let metrics = self.raft.metrics();
            let snapshot = metrics.borrow_watched().clone();
            let snap_idx = snapshot.snapshot.as_ref().map(|id| id.index).unwrap_or(0);
            let purged = snapshot.purged.as_ref().map(|id| id.index).unwrap_or(0);
            if snap_idx > 0 && purged >= snap_idx {
                return Ok(snap_idx);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(MetaDbError::ConsensusError(format!(
                    "shard {} snapshot {snap_idx} was not purged (purged {purged}); refusing to promote a replica that could miss state seeded outside the log",
                    self.shard_id
                )));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Proposes a mutation to OpenRaft after pre-checking `expected_epoch` (`Resolution B16`)
    /// and stamping a drift-checked causal HLC timestamp.
    ///
    /// A known leader is forwarded at most once. The forward budget is five seconds.
    pub async fn propose(
        &self,
        expected_epoch: ShardEpoch,
        caller_hlc: HlcTimestamp,
        payload: Vec<u8>,
    ) -> Result<MetaDbRaftResponse> {
        self.propose_until(
            expected_epoch,
            caller_hlc,
            payload,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
    }

    /// Same as [`propose`](Self::propose), but the forwarded RPC cannot outlive `deadline`.
    pub async fn propose_until(
        &self,
        expected_epoch: ShardEpoch,
        caller_hlc: HlcTimestamp,
        payload: Vec<u8>,
        deadline: tokio::time::Instant,
    ) -> Result<MetaDbRaftResponse> {
        self.repair_published_snapshot().await?;
        {
            let mut hook = self.testing_not_leader.lock().unwrap();
            if hook.0 > 0 {
                hook.0 -= 1;
                return Err(MetaDbError::NotLeader {
                    shard_id: self.shard_id,
                    leader_hint: hook.1,
                });
            }
        }
        if self
            .testing_epoch_mismatch
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            self.testing_epoch_mismatch
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            return Err(MetaDbError::EpochMismatch {
                shard_id: self.shard_id,
                expected: expected_epoch,
                actual: expected_epoch.bump_version(),
            });
        }
        if self
            .testing_log_discarded
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            self.testing_log_discarded
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            return Err(MetaDbError::RetryableConsensus {
                shard_id: self.shard_id,
                leader_hint: Some(self.node_id),
                kind: metadb_core::RetryableConsensusKind::LogDiscarded,
            });
        }
        match self
            .propose_local(expected_epoch, caller_hlc, payload.clone())
            .await
        {
            Err(MetaDbError::NotLeader { leader_hint, .. }) => {
                let request = MetaDbRaftRequest {
                    expected_epoch,
                    hlc: caller_hlc,
                    payload,
                };
                self.forward_client_write(leader_hint, None, request, deadline)
                    .await
            }
            Err(MetaDbError::RetryableConsensus {
                leader_hint: Some(leader),
                ..
            }) if leader != self.node_id => {
                let request = MetaDbRaftRequest {
                    expected_epoch,
                    hlc: caller_hlc,
                    payload,
                };
                self.forward_client_write(Some(leader), None, request, deadline)
                    .await
            }
            other => other,
        }
    }

    /// Leader-local proposal. Validates this node's clock and does not forward.
    ///
    /// Forwarded `ClientWrite` RPCs enter here so a follower-adjusted timestamp is
    /// checked against the leader's drift limit, and application errors are returned
    /// to the caller instead of an outer success.
    pub async fn propose_local(
        &self,
        expected_epoch: ShardEpoch,
        caller_hlc: HlcTimestamp,
        payload: Vec<u8>,
    ) -> Result<MetaDbRaftResponse> {
        let local_epoch = self.current_epoch();
        if expected_epoch != local_epoch {
            return Err(MetaDbError::EpochMismatch {
                shard_id: self.shard_id,
                expected: expected_epoch,
                actual: local_epoch,
            });
        }

        let proposal_hlc = if caller_hlc == HlcTimestamp::ZERO {
            self.clock.now()
        } else {
            self.clock.update_checked(caller_hlc)?
        };

        let req = MetaDbRaftRequest {
            expected_epoch,
            hlc: proposal_hlc,
            payload,
        };

        let gate = self.proposal_gate.read().unwrap().clone();
        if let Some(gate) = gate {
            gate.enter().await;
        }

        let write_resp = match self.raft.client_write(req).await {
            Ok(response) => response,
            Err(error) => {
                return Err(crate::error_map::map_client_write_error(
                    self.shard_id,
                    error,
                ))
            }
        };
        let app_resp = write_resp.data;
        if let Err(err) = &app_resp.result {
            return Err(err.clone());
        }
        Ok(app_resp)
    }

    async fn forward_client_write(
        &self,
        leader_id: Option<NodeId>,
        leader_node: Option<&BasicNode>,
        request: MetaDbRaftRequest,
        deadline: tokio::time::Instant,
    ) -> Result<MetaDbRaftResponse> {
        let Some(leader_id) = leader_id else {
            return Err(MetaDbError::NotLeader {
                shard_id: self.shard_id,
                leader_hint: None,
            });
        };
        if leader_id == self.node_id {
            return Err(MetaDbError::NotLeader {
                shard_id: self.shard_id,
                leader_hint: Some(leader_id),
            });
        }
        let addr = leader_node
            .map(|node| node.addr.clone())
            .filter(|addr| !addr.is_empty())
            .or_else(|| self.peer_addr(leader_id));
        let Some(addr) = addr else {
            return Err(MetaDbError::NotLeader {
                shard_id: self.shard_id,
                leader_hint: Some(leader_id),
            });
        };
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(MetaDbError::RpcError(
                "forwarded proposal exceeded the routing deadline".to_string(),
            ));
        }
        let response = match tokio::time::timeout(
            remaining,
            self.channel_pool
                .client_write(&addr, self.shard_id, &request),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                return Err(MetaDbError::RpcError(
                    "forwarded proposal timed out".to_string(),
                ))
            }
        };
        response.result.clone()?;
        Ok(response)
    }

    /// Acquires a linearizable `SnapshotGuard` fenced by `ReadIndex` and post-wait `ShardEpoch` revalidation.
    pub async fn linearizable_read(
        &self,
        expected_epoch: ShardEpoch,
    ) -> Result<(ReadToken, SnapshotGuard)> {
        self.repair_published_snapshot().await?;
        self.read_batcher
            .acquire_linearizable_guard(expected_epoch, &self.state_machine)
            .await
    }

    /// Acquires a fixed-barrier `SnapshotGuard` for cross-shard `DualModeFineGrained` `dirstat` (`Resolution B1`):
    /// 1. Advances local HLC past `barrier_hlc` (`clock.update_checked(barrier_hlc)`), proving zero
    ///    future Raft proposals on this shard can receive `hlc <= barrier_hlc`.
    /// 2. Completes `ReadIndex` + post-wait `ShardEpoch` revalidation.
    /// 3. Acquires `SnapshotGuard` pinned strictly at `barrier_hlc`.
    pub async fn fixed_barrier_read(
        &self,
        expected_epoch: ShardEpoch,
        barrier_hlc: HlcTimestamp,
    ) -> Result<(ReadToken, SnapshotGuard)> {
        self.repair_published_snapshot().await?;
        let _ = self.clock.update_checked(barrier_hlc)?;
        let token = self.read_batcher.linearizable_token(expected_epoch).await?;
        let snap = self.state_machine.acquire_snapshot(
            expected_epoch,
            token.read_index,
            Some(barrier_hlc),
        )?;
        Ok((token, snap))
    }

    /// Flushes `MemMvccStore` to L0 SSTable, triggers an OpenRaft snapshot, and purges eligible `.raftlog` segments.
    pub async fn flush_snapshot_and_purge(&self) -> Result<u64> {
        let (flushed_idx, _) = self.state_machine.flush_to_lsm()?;
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| MetaDbError::ConsensusError(e.to_string()))?;

        // Give OpenRaft's async snapshot task a moment to complete and invoke `purge`.
        for _ in 0..40 {
            if self.watermarks.snapshot_values().2 >= flushed_idx {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let safe_idx = self.watermarks.safe_purge_index();
        let _ = self.raft.trigger().purge_log(safe_idx).await;
        // A hold can make that purge a no-op. OpenRaft may not ask again for the same
        // index after the hold is released, so apply the storage clamp directly.
        let _ = self.log_store.try_purge_to_safe_watermark();
        for _ in 0..40 {
            if self.log_store.retained_entry_count() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(safe_idx)
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|e| MetaDbError::ConsensusError(e.to_string()))
    }
}

#[tonic::async_trait]
impl LocalProposer for ShardRaftGroup {
    async fn propose_local_forwarded(
        &self,
        expected_epoch: ShardEpoch,
        caller_hlc: HlcTimestamp,
        payload: Vec<u8>,
    ) -> Result<MetaDbRaftResponse> {
        self.propose_local(expected_epoch, caller_hlc, payload)
            .await
    }
}

/// Durable entry in `<data_dir>/NODE_SHARD_REGISTRY` tracking split child bootstrap (`Resolution B15`).
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub enum RegistryShardState {
    /// Child shard materialized for a split that has not yet published routes.
    /// Consumed by `DurableCellManager` on startup so a crash after the parent fence
    /// can resume the child boot instead of dropping the right-half key range.
    PendingChildBoot {
        parent_shard_id: ShardId,
        peers: BTreeMap<NodeId, String>,
        split_key: Vec<u8>,
        target_version: u64,
        parent_end_key: Vec<u8>,
        dual_mode_pid: Option<u64>,
    },
    Active {
        peers: BTreeMap<NodeId, String>,
    },
}

/// Manages all `ShardRaftGroup` instances hosted on a Backend (`BE`) node.
pub struct ShardGroupHost {
    node_id: NodeId,
    data_dir: PathBuf,
    block_cache: Arc<ShardedBlockCache>,
    clock: Arc<HybridLogicalClock>,
    router: MultiRaftRouter,
    channel_pool: ChannelPool,
    groups: RwLock<HashMap<ShardId, Arc<ShardRaftGroup>>>,
    registry: RwLock<BTreeMap<ShardId, RegistryShardState>>,
}

impl ShardGroupHost {
    pub fn open(
        node_id: NodeId,
        data_dir: impl AsRef<Path>,
        block_cache: Arc<ShardedBlockCache>,
        clock: Arc<HybridLogicalClock>,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(data_dir.join("shards"))?;
        let registry = Self::load_registry(&data_dir)?;
        Ok(Self {
            node_id,
            data_dir,
            block_cache,
            clock,
            router: MultiRaftRouter::new(),
            channel_pool: ChannelPool::default(),
            groups: RwLock::new(HashMap::new()),
            registry: RwLock::new(registry),
        })
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn block_cache(&self) -> &Arc<ShardedBlockCache> {
        &self.block_cache
    }

    pub fn clock(&self) -> &Arc<HybridLogicalClock> {
        &self.clock
    }

    pub fn router(&self) -> &MultiRaftRouter {
        &self.router
    }

    pub fn channel_pool(&self) -> &ChannelPool {
        &self.channel_pool
    }

    pub fn set_shared_secret(&self, secret: Option<String>) {
        self.channel_pool.set_shared_secret(secret);
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn shard_dir(&self, shard_id: ShardId) -> PathBuf {
        self.data_dir
            .join("shards")
            .join(format!("{:020}", shard_id))
    }

    pub fn record_registry_state(
        &self,
        shard_id: ShardId,
        state: RegistryShardState,
    ) -> Result<()> {
        let mut guard = self.registry.write().unwrap();
        guard.insert(shard_id, state);
        Self::persist_registry(&self.data_dir, &guard)
    }

    pub fn registry_entries(&self) -> BTreeMap<ShardId, RegistryShardState> {
        self.registry.read().unwrap().clone()
    }

    pub fn register_running_group(&self, group: Arc<ShardRaftGroup>) {
        self.groups.write().unwrap().insert(group.shard_id(), group);
    }

    pub fn get_group(&self, shard_id: ShardId) -> Option<Arc<ShardRaftGroup>> {
        self.groups.read().unwrap().get(&shard_id).cloned()
    }

    pub fn all_groups(&self) -> Vec<Arc<ShardRaftGroup>> {
        self.groups.read().unwrap().values().cloned().collect()
    }

    pub async fn shutdown_all(&self) -> Result<()> {
        self.shutdown_groups(true).await
    }

    /// Stops Raft groups without flushing the memtable, so a crash-injection test can
    /// reopen the raft tail that had not yet reached an LSM checkpoint.
    pub async fn shutdown_all_no_flush(&self) -> Result<()> {
        self.shutdown_groups(false).await
    }

    async fn shutdown_groups(&self, flush: bool) -> Result<()> {
        self.router.clear();
        let groups: Vec<Arc<ShardRaftGroup>> = {
            let mut guard = self.groups.write().unwrap();
            guard.drain().map(|(_, g)| g).collect()
        };
        for g in &groups {
            if flush {
                let _ = g.state_machine().flush_to_lsm();
            }
            let _ = g.shutdown().await;
            self.router.remove_group(g.shard_id());
        }
        drop(groups);
        Ok(())
    }

    fn load_registry(data_dir: &Path) -> Result<BTreeMap<ShardId, RegistryShardState>> {
        let path = data_dir.join("NODE_SHARD_REGISTRY");
        if !path.exists() {
            return Ok(BTreeMap::new());
        }
        let mut raw = Vec::new();
        File::open(&path)?.read_to_end(&mut raw)?;
        let body = verify_crc32c_le(&raw, "NODE_SHARD_REGISTRY")?;
        decode_registry(body)
    }

    fn persist_registry(
        data_dir: &Path,
        reg: &BTreeMap<ShardId, RegistryShardState>,
    ) -> Result<()> {
        let mut raw = codec::serialize(reg).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        append_crc32c_le(&mut raw);
        let tmp = data_dir.join("NODE_SHARD_REGISTRY.tmp");
        let dst = data_dir.join("NODE_SHARD_REGISTRY");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(&raw)?;
            f.sync_data()?;
        }
        fs::rename(&tmp, &dst)?;
        sync_dir(data_dir)?;
        Ok(())
    }
}

/// `PendingChildBoot` originally stored only the parent id and peer map.
#[derive(Debug, Deserialize, wincode::SchemaRead)]
enum LegacyRegistryShardState {
    PendingChildBoot {
        parent_shard_id: ShardId,
        peers: BTreeMap<NodeId, String>,
    },
    Active {
        peers: BTreeMap<NodeId, String>,
    },
}

/// Shard ids named by a `NODE_SHARD_REGISTRY` file.
pub fn shard_ids_in_registry_file(path: &Path) -> Result<std::collections::BTreeSet<ShardId>> {
    use std::collections::BTreeSet;
    let mut raw = Vec::new();
    File::open(path)?.read_to_end(&mut raw)?;
    let body = verify_crc32c_le(&raw, "NODE_SHARD_REGISTRY")?;
    Ok(decode_registry(body)?.into_keys().collect::<BTreeSet<_>>())
}

fn decode_registry(body: &[u8]) -> Result<BTreeMap<ShardId, RegistryShardState>> {
    if let Ok(reg) = codec::deserialize::<BTreeMap<ShardId, RegistryShardState>>(body) {
        return Ok(reg);
    }
    let legacy: BTreeMap<ShardId, LegacyRegistryShardState> = codec::deserialize(body)
        .map_err(|e| MetaDbError::CodecError(format!("NODE_SHARD_REGISTRY: {e}")))?;
    Ok(legacy
        .into_iter()
        .map(|(id, state)| {
            let state = match state {
                LegacyRegistryShardState::PendingChildBoot {
                    parent_shard_id,
                    peers,
                } => RegistryShardState::PendingChildBoot {
                    parent_shard_id,
                    peers,
                    split_key: Vec::new(),
                    target_version: 0,
                    parent_end_key: Vec::new(),
                    dual_mode_pid: None,
                },
                LegacyRegistryShardState::Active { peers } => RegistryShardState::Active { peers },
            };
            (id, state)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joint_membership_reports_every_applied_voter() {
        let previous = BTreeSet::from([1, 2]);
        let incoming = BTreeSet::from([1, 2, 3]);
        let reported = voter_ids_from_joint_configs([previous.clone(), incoming]);
        assert_eq!(reported, BTreeSet::from([1, 2, 3]));
        assert_ne!(
            reported, previous,
            "a joint config must not keep the previous uniform voter set"
        );
        assert_eq!(
            voter_ids_from_joint_configs([BTreeSet::from([1, 2, 3])]),
            BTreeSet::from([1, 2, 3])
        );
    }
}
