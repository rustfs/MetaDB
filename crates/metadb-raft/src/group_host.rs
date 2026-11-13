//! Per-shard `ShardRaftGroup` wrapper and multi-shard `ShardGroupHost` with `NODE_SHARD_REGISTRY`.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

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
        }))
    }

    /// Bootstraps initial cluster membership (e.g., `1-Voter` in Stage 1 or `3-Voter` on split child).
    pub async fn initialize_if_needed(&self, members: BTreeMap<NodeId, String>) -> Result<()> {
        let nodes: BTreeMap<NodeId, BasicNode> = members
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();
        match self.raft.initialize(nodes).await {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(openraft::errors::InitializeError::NotAllowed(_))) => Ok(()),
            Err(e) => Err(MetaDbError::ConsensusError(e.to_string())),
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
    PendingChildBoot {
        parent_shard_id: ShardId,
        peers: BTreeMap<NodeId, String>,
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
        let groups = self.all_groups();
        for g in groups {
            let _ = g.state_machine().flush_to_lsm();
            let _ = g.shutdown().await;
            self.router.remove_group(g.shard_id());
        }
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
        bincode::deserialize(body).map_err(|e| MetaDbError::CodecError(e.to_string()))
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
