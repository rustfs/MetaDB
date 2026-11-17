//! Per-shard `ShardRaftGroup` wrapper and multi-shard `ShardGroupHost` with `NODE_SHARD_REGISTRY`.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use openraft::async_runtime::watch::WatchReceiver;
use openraft::errors::{ClientWriteError, RaftError};
use openraft::{BasicNode, Config};
use serde::{Deserialize, Serialize};

use metadb_core::{
    append_crc32c_le, verify_crc32c_le, HlcTimestamp, HybridLogicalClock, MetaDbError, NodeId,
    Result, ShardEpoch, ShardId,
};
use metadb_storage::sstable::sync_dir;
use metadb_storage::{LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache, SnapshotGuard};

use crate::log_store::SegmentedRaftLogStore;
use crate::network::{ChannelPool, MetaDbRaft, MultiRaftRouter, TonicRaftNetworkFactory};
use crate::read_index::{ReadIndexBatcher, ReadToken};
use crate::state_machine::{ShardApplyHandler, ShardStateMachine};
use crate::types::{MetaDbRaftRequest, MetaDbRaftResponse, ShardWatermarkTracker};

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

        let network = TonicRaftNetworkFactory::new(shard_id, channel_pool);
        let raft = MetaDbRaft::new(
            node_id,
            config,
            network,
            log_store.clone(),
            state_machine.clone(),
        )
        .await
        .map_err(|e| MetaDbError::ConsensusError(e.to_string()))?;

        // Block until OpenRaft replays any committed tail entries `(flushed_raft_index ..= last_committed]`
        // into the WAL-less `MemMvccStore` before serving traffic (`Resolution B5`)!
        let _ = raft.wait_for_recovery(Some(Duration::from_secs(5))).await;

        router.register_group(shard_id, raft.clone());
        let read_batcher = Arc::new(ReadIndexBatcher::new(shard_id, raft.clone(), frontier_rx));

        Ok(Arc::new(Self {
            shard_id,
            node_id,
            raft,
            log_store,
            state_machine,
            read_batcher,
            watermarks,
            clock,
            proposal_gate: RwLock::new(None),
        }))
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
            self.wait_for_leader(Duration::from_secs(5)).await?;
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
    pub async fn propose(
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
            Ok(r) => r,
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                return Err(MetaDbError::NotLeader {
                    shard_id: self.shard_id,
                    leader_hint: fwd.leader_id,
                });
            }
            Err(e) => return Err(MetaDbError::ConsensusError(e.to_string())),
        };

        let app_resp = write_resp.data;
        if let Err(err) = &app_resp.result {
            return Err(err.clone());
        }
        Ok(app_resp)
    }

    /// Acquires a linearizable `SnapshotGuard` fenced by `ReadIndex` and post-wait `ShardEpoch` revalidation.
    pub async fn linearizable_read(
        &self,
        expected_epoch: ShardEpoch,
    ) -> Result<(ReadToken, SnapshotGuard)> {
        let engine = self.engine();
        self.read_batcher
            .acquire_linearizable_guard(expected_epoch, &engine)
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
        let _ = self.clock.update_checked(barrier_hlc)?;
        let token = self.read_batcher.linearizable_token(expected_epoch).await?;
        let engine = self.engine();
        let snap = engine.acquire_fixed_snapshot(barrier_hlc)?;
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

/// Durable entry in `<data_dir>/NODE_SHARD_REGISTRY` tracking split child bootstrap (`Resolution B15`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        let mut raw =
            bincode::serialize(reg).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
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
#[derive(Debug, Deserialize)]
enum LegacyRegistryShardState {
    PendingChildBoot {
        parent_shard_id: ShardId,
        peers: BTreeMap<NodeId, String>,
    },
    Active {
        peers: BTreeMap<NodeId, String>,
    },
}

fn decode_registry(body: &[u8]) -> Result<BTreeMap<ShardId, RegistryShardState>> {
    if let Ok(reg) = bincode::deserialize::<BTreeMap<ShardId, RegistryShardState>>(body) {
        return Ok(reg);
    }
    let legacy: BTreeMap<ShardId, LegacyRegistryShardState> = bincode::deserialize(body)
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
