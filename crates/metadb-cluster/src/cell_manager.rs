//! Persistent `DurableCellManager` managing OpenRaft shard groups, 3-stage recoverable
//! `ExecuteSplit` (`SplitPlanRecord`), Jumbo Shard dedicated-node isolation, and `DualModeFineGrained`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use openraft::async_runtime::watch::WatchReceiver;
use openraft::{BasicNode, ChangeMembers, Precondition};
use serde::{Deserialize, Serialize};

use metadb_core::codec;
use metadb_core::{
    append_crc32c_le, verify_crc32c_le, CellId, CompositeKeyRange, InodeId, MetaDbError, NodeId,
    Result, ShardEpoch, ShardId,
};
use metadb_raft::{
    ConfChangeOrchestrator, RaftNodeOptions, RegistryShardState, ShardGroupHost, ShardRaftGroup,
    ShardWatermarkTracker,
};
use metadb_storage::sstable::sync_dir;
use metadb_storage::LsmEngineConfig;
use metadb_tx::{RoutedShardHandle, TxStateMachineHandler};
use tracing;

use crate::cell_directory::{shards_covering, union_route_range};
use crate::control_plane_state::{
    decide_heartbeat, decode_control_plane, normalize_ids, remote_state_change_is_too_soon,
    unix_ms, HeartbeatDecision, HeartbeatWrite, PersistedControlPlane, StoredHeartbeat,
    StoredPlacement, CONTROL_PLANE_FILE, CONTROL_PLANE_VERSION, PLACEMENT_RETRY_MS,
};
use crate::id_allocator::{AllocIdKind, DurableIdAllocator};
use crate::identity::{node_id_in_ha_range, random_id, ClusterIdentity};
use crate::jumbo::JumboIsolationPlanner;
use crate::jumbo_files::{
    copy_shard_tree, load_live_dir, refuse_dedicated_root, remove_tree, shard_move_dir,
    write_move_phase, MovePhaseError, MOVE_LIVE, MOVE_STAGED,
};
use crate::jumbo_placement::{
    automatic_leadership_transfer_allowed, executable_placement_actions,
    jumbo_membership_change_allowed, plan_jumbo_placement, CommittedMove, PlacementAction,
    PlacementObservation, PlacementPhase, ShardVoters, AUTOMATIC_TRANSFER_DISABLED_REASON,
};
use crate::meta_cache::{CompositeMetaCache, CompositeShardRoute};
use crate::shard::{ShardHealthState, ShardSplitMode};
use crate::split_worker::{RaftShardSplitWorker, SplitCandidate, SplitCrashPoint};

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub enum SplitPlanState {
    Planned,
    Activated,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct SplitPlanRecord {
    pub parent_shard_id: ShardId,
    pub child_shard_id: ShardId,
    pub split_key: Vec<u8>,
    pub target_version: u64,
    pub state: SplitPlanState,
    pub dual_mode_pid: Option<InodeId>,
}

#[derive(
    Debug, Clone, Default, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
struct PersistedCellState {
    nodes: BTreeMap<NodeId, String>,
    routes: BTreeMap<ShardId, CompositeShardRoute>,
    split_plans: BTreeMap<u128, SplitPlanRecord>,
    /// This process joined an existing Raft group. Restart must not call `Raft::initialize`,
    /// which would fork a new cluster if the local log were empty.
    #[serde(default)]
    joined_existing: bool,
    /// Promotion was requested, or had already committed, before `joined_existing` was published.
    /// Restart resumes this join instead of rejecting the node id as a fresh member.
    #[serde(default)]
    join_pending: bool,
    /// `node_id -> instance id`. A second instance cannot reuse the node id.
    #[serde(default)]
    node_instances: BTreeMap<NodeId, u128>,
}

/// On-disk layout after `join_pending` and before `node_instances`.
#[derive(Debug, Deserialize, wincode::SchemaRead)]
struct CellStateBeforeNodeInstances {
    nodes: BTreeMap<NodeId, String>,
    routes: BTreeMap<ShardId, CompositeShardRoute>,
    split_plans: BTreeMap<u128, SplitPlanRecord>,
    joined_existing: bool,
    join_pending: bool,
}

/// On-disk layout after `joined_existing` and before `join_pending`.
#[derive(Debug, Deserialize, wincode::SchemaRead)]
struct CellStateJoinedOnly {
    nodes: BTreeMap<NodeId, String>,
    routes: BTreeMap<ShardId, CompositeShardRoute>,
    split_plans: BTreeMap<u128, SplitPlanRecord>,
    joined_existing: bool,
}

/// On-disk layout before `joined_existing` was stored in the same record.
#[derive(Debug, Deserialize, wincode::SchemaRead)]
struct CellStateBeforeJoinFlag {
    nodes: BTreeMap<NodeId, String>,
    routes: BTreeMap<ShardId, CompositeShardRoute>,
    split_plans: BTreeMap<u128, SplitPlanRecord>,
}

/// What a cell directory already contains, before this process opens it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellDirectoryRole {
    /// No `CELL_MANAGER_STATE` file.
    Absent,
    /// State file exists but no shard routes were published.
    Empty,
    /// This directory bootstrapped its own cell.
    Bootstrapped,
    /// This directory joined an existing Raft group.
    Joined,
    /// Join was persisted but not finished. Resume it; do not bootstrap.
    Joining,
}

/// Cell-level manager coordinating OpenRaft shards, `ExecuteSplit`, Jumbo isolation, and `ConfChange`.
pub struct DurableCellManager {
    cell_id: CellId,
    state_dir: PathBuf,
    split_threshold_bytes: usize,
    lsm_config: LsmEngineConfig,
    raft_opts: RaftNodeOptions,
    id_allocator: Arc<DurableIdAllocator>,
    meta_cache: Arc<CompositeMetaCache>,
    host: Arc<ShardGroupHost>,
    handlers: RwLock<HashMap<ShardId, Arc<TxStateMachineHandler>>>,
    state: Mutex<PersistedCellState>,
    split_lock: tokio::sync::Mutex<()>,
    /// Test hook: the next `n` `CELL_MANAGER_STATE` persists fail before touching disk.
    fail_persists: AtomicU32,
    crash_point: Mutex<Option<SplitCrashPoint>>,
    identity: Mutex<Option<ClusterIdentity>>,
    /// Serializes `add_learner` / `change_membership` across concurrent join and scale RPCs.
    membership_lock: tokio::sync::Mutex<()>,
    /// Raft-applied node instance bindings. Shared with every shard handler.
    instance_bindings: Arc<Mutex<BTreeMap<NodeId, (u128, String)>>>,
    /// Heartbeats and jumbo placement records. Not a route table.
    control: Mutex<PersistedControlPlane>,
    /// Test hook: the next `n` `CELL_CONTROL_PLANE` persists fail before touching disk.
    fail_control_persists: AtomicU32,
    /// Test hook: the next `n` persists rename the file and then fail the directory sync.
    fail_control_dirsync: AtomicU32,
    /// Separate disk for this process's jumbo shard files. Empty means the shared `be` tree.
    jumbo_dedicated_root: Mutex<Option<PathBuf>>,
    /// Shard ids whose file move already failed in this process. Not retried until restart.
    jumbo_file_move_failed: Mutex<HashSet<ShardId>>,
    /// Test hook: return after `MOVE_LIVE` and the dest group are durable, before
    /// the old directory is removed.
    stop_before_source_removal: AtomicBool,
}

impl DurableCellManager {
    #[allow(clippy::too_many_arguments)]
    pub async fn open_or_bootstrap(
        cell_id: CellId,
        state_dir: impl AsRef<Path>,
        host: Arc<ShardGroupHost>,
        local_addr: String,
        bootstrap_table_shards: bool,
        split_threshold_bytes: usize,
        lsm_config: LsmEngineConfig,
        raft_opts: RaftNodeOptions,
        id_allocator: Arc<DurableIdAllocator>,
        meta_cache: Arc<CompositeMetaCache>,
    ) -> Result<Arc<Self>> {
        Self::open_or_bootstrap_in_range(
            cell_id,
            state_dir,
            host,
            local_addr,
            bootstrap_table_shards,
            split_threshold_bytes,
            lsm_config,
            raft_opts,
            id_allocator,
            meta_cache,
            None,
            None,
        )
        .await
    }

    /// `owned_range` `None` keeps the historical whole user key space.
    /// A configured range must already be the durable route union when the
    /// cell directory exists.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_or_bootstrap_in_range(
        cell_id: CellId,
        state_dir: impl AsRef<Path>,
        host: Arc<ShardGroupHost>,
        local_addr: String,
        bootstrap_table_shards: bool,
        split_threshold_bytes: usize,
        lsm_config: LsmEngineConfig,
        raft_opts: RaftNodeOptions,
        id_allocator: Arc<DurableIdAllocator>,
        meta_cache: Arc<CompositeMetaCache>,
        owned_range: Option<CompositeKeyRange>,
        jumbo_dedicated_root: Option<PathBuf>,
    ) -> Result<Arc<Self>> {
        node_id_in_ha_range(host.node_id())?;
        let state_dir = state_dir.as_ref().to_path_buf();
        fs::create_dir_all(&state_dir)?;
        let preexisting_identity = ClusterIdentity::load(&state_dir)?;
        let state_path = state_dir.join("CELL_MANAGER_STATE");

        let is_existing = state_path.try_exists()?;
        if !is_existing && state_dir.join("CELL_OWNER").try_exists()? {
            return Err(MetaDbError::StorageIoError(
                "CELL_MANAGER_STATE is missing from an existing cell directory".to_string(),
            ));
        }
        let persisted = if is_existing {
            let mut raw = Vec::new();
            File::open(&state_path)?.read_to_end(&mut raw)?;
            let body = verify_crc32c_le(&raw, "CELL_MANAGER_STATE")?;
            let mut state = decode_cell_state(body)?;
            let old_addr = state.nodes.get(&host.node_id()).ok_or_else(|| {
                MetaDbError::ShardRoutingError(
                    "data directory belongs to a different node ID".to_string(),
                )
            })?;
            if state.routes.values().any(|route| route.cell_id != cell_id) {
                return Err(MetaDbError::ShardRoutingError(
                    "data directory belongs to a different cell ID".to_string(),
                ));
            }
            if old_addr != &local_addr {
                for route in state
                    .routes
                    .values()
                    .filter(|route| route.peers.contains_key(&host.node_id()))
                {
                    let voters: Vec<_> = route
                        .peers
                        .iter()
                        .filter(|(_, (_, voter))| *voter)
                        .map(|(&id, _)| id)
                        .collect();
                    if voters != [host.node_id()] {
                        return Err(MetaDbError::ShardRoutingError(
                            "a replicated node must restart at its persisted address; change node addresses online before restarting".to_string()));
                    }
                }
                state.nodes.insert(host.node_id(), local_addr.clone());
                for route in state.routes.values_mut() {
                    if let Some((address, _)) = route.peers.get_mut(&host.node_id()) {
                        *address = local_addr.clone();
                    }
                }
            }
            state
        } else {
            let mut init = PersistedCellState::default();
            init.nodes.insert(host.node_id(), local_addr.clone());
            let mut init_peers = BTreeMap::new();
            init_peers.insert(host.node_id(), (local_addr.clone(), true));

            for (sid, range) in shards_covering(owned_range.as_ref(), bootstrap_table_shards)? {
                init.routes.insert(
                    sid,
                    CompositeShardRoute {
                        cell_id,
                        shard_id: sid,
                        range,
                        epoch: ShardEpoch::initial(),
                        leader_node: host.node_id(),
                        peers: init_peers.clone(),
                        split_mode: ShardSplitMode::DirectoryColocated,
                        health_state: ShardHealthState::Normal,
                        dual_mode_pid: None,
                    },
                );
            }
            Self::write_state_file(&state_dir, &init)?;
            init
        };
        if let Some(expected) = &owned_range {
            let actual = union_route_range(persisted.routes.values().map(|route| &route.range))?;
            if &actual != expected {
                return Err(MetaDbError::ShardRoutingError(
                    "configured cell range does not match the durable routes".to_string(),
                ));
            }
        }

        Self::verify_or_initialize_owner(
            &state_dir,
            host.node_id(),
            cell_id,
            &persisted,
            is_existing,
        )?;
        let control_plane = Self::load_control_plane(&state_dir)?;
        if let Some(root) = jumbo_dedicated_root.as_ref() {
            refuse_dedicated_root(host.data_dir(), root)?;
        }

        let mgr = Arc::new(Self {
            cell_id,
            state_dir,
            split_threshold_bytes,
            lsm_config,
            raft_opts,
            id_allocator,
            meta_cache,
            host,
            handlers: RwLock::new(HashMap::new()),
            state: Mutex::new(persisted),
            split_lock: tokio::sync::Mutex::new(()),
            fail_persists: AtomicU32::new(0),
            crash_point: Mutex::new(None),
            identity: Mutex::new(preexisting_identity),
            membership_lock: tokio::sync::Mutex::new(()),
            instance_bindings: Arc::new(Mutex::new(BTreeMap::new())),
            control: Mutex::new(control_plane),
            fail_control_persists: AtomicU32::new(0),
            fail_control_dirsync: AtomicU32::new(0),
            jumbo_dedicated_root: Mutex::new(jumbo_dedicated_root),
            jumbo_file_move_failed: Mutex::new(HashSet::new()),
            stop_before_source_removal: AtomicBool::new(false),
        });

        if let Err(error) = mgr.boot_local_shards().await {
            let _ = mgr.host.shutdown_all().await;
            return Err(error);
        }
        if mgr.cluster_id() == 0 && mgr.replicated_membership() {
            let _ = mgr.host.shutdown_all().await;
            return Err(MetaDbError::IdentityRejected(
                "legacy replicated cell directory has no shared CLUSTER_IDENTITY; refusing to invent a per-member cluster id"
                    .to_string(),
            ));
        }
        if let Err(error) = mgr.recover_incomplete_splits().await {
            let _ = mgr.host.shutdown_all().await;
            return Err(error);
        }
        if let Err(error) = mgr.ingest_engine_bindings() {
            let _ = mgr.host.shutdown_all().await;
            return Err(error);
        }
        if mgr.cluster_id() == 0 {
            // A restored log already names this node's instance. Minting a new id
            // would be rejected as a reused node identity.
            if mgr.bound_instance(mgr.host.node_id()).is_some() {
                let _ = mgr.host.shutdown_all().await;
                return Err(MetaDbError::IdentityRejected(
                    "CLUSTER_IDENTITY is missing but this node is already bound; refusing to mint a new instance id"
                        .to_string(),
                ));
            }
            match ClusterIdentity::load_or_create(&mgr.state_dir, mgr.host.node_id(), cell_id, None)
            {
                Ok(created) => *mgr.identity.lock().unwrap() = Some(created),
                Err(error) => {
                    let _ = mgr.host.shutdown_all().await;
                    return Err(error);
                }
            }
        }
        if let Err(error) = mgr.bind_local_instance().await {
            let _ = mgr.host.shutdown_all().await;
            return Err(error);
        }
        Ok(mgr)
    }

    /// Opens a cell directory that will join an existing group.
    ///
    /// Does not publish shard routes and does not call `Raft::initialize`. The caller starts
    /// uninitialized replica groups, then asks the current leader to add this node.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_joining(
        cell_id: CellId,
        state_dir: impl AsRef<Path>,
        host: Arc<ShardGroupHost>,
        local_addr: String,
        split_threshold_bytes: usize,
        lsm_config: LsmEngineConfig,
        raft_opts: RaftNodeOptions,
        id_allocator: Arc<DurableIdAllocator>,
        meta_cache: Arc<CompositeMetaCache>,
    ) -> Result<Arc<Self>> {
        node_id_in_ha_range(host.node_id())?;
        let state_dir = state_dir.as_ref().to_path_buf();
        fs::create_dir_all(&state_dir)?;
        let state_path = state_dir.join("CELL_MANAGER_STATE");
        let state_existed = state_path.exists();
        let persisted = if state_existed {
            let mut loaded = Self::load_state_file(&state_dir)?;
            loaded.nodes.insert(host.node_id(), local_addr);
            Self::write_state_file(&state_dir, &loaded)?;
            loaded
        } else {
            let mut init = PersistedCellState::default();
            init.nodes.insert(host.node_id(), local_addr);
            Self::write_state_file(&state_dir, &init)?;
            init
        };
        // Record the owner before membership adds peers. A later restart of this
        // directory goes through `open_or_bootstrap`, which rejects a multi-peer
        // cell directory that has no `CELL_OWNER`.
        Self::verify_or_initialize_owner(
            &state_dir,
            host.node_id(),
            cell_id,
            &persisted,
            state_existed,
        )?;
        let identity = ClusterIdentity::load(&state_dir)?;
        let control_plane = Self::load_control_plane(&state_dir)?;
        Ok(Arc::new(Self {
            cell_id,
            state_dir,
            split_threshold_bytes,
            lsm_config,
            raft_opts,
            id_allocator,
            meta_cache,
            host,
            handlers: RwLock::new(HashMap::new()),
            state: Mutex::new(persisted),
            split_lock: tokio::sync::Mutex::new(()),
            fail_persists: AtomicU32::new(0),
            crash_point: Mutex::new(None),
            identity: Mutex::new(identity),
            membership_lock: tokio::sync::Mutex::new(()),
            instance_bindings: Arc::new(Mutex::new(BTreeMap::new())),
            control: Mutex::new(control_plane),
            fail_control_persists: AtomicU32::new(0),
            fail_control_dirsync: AtomicU32::new(0),
            jumbo_dedicated_root: Mutex::new(None),
            jumbo_file_move_failed: Mutex::new(HashSet::new()),
            stop_before_source_removal: AtomicBool::new(false),
        }))
    }

    /// Classifies an existing cell directory without starting Raft.
    pub fn inspect_directory(state_dir: impl AsRef<Path>) -> Result<CellDirectoryRole> {
        let state_dir = state_dir.as_ref();
        let state_path = state_dir.join("CELL_MANAGER_STATE");
        if !state_path.exists() {
            return Ok(CellDirectoryRole::Absent);
        }
        let st = Self::load_state_file(state_dir)?;
        if st.joined_existing {
            Ok(CellDirectoryRole::Joined)
        } else if st.join_pending {
            Ok(CellDirectoryRole::Joining)
        } else if st.routes.is_empty() {
            Ok(CellDirectoryRole::Empty)
        } else {
            Ok(CellDirectoryRole::Bootstrapped)
        }
    }

    /// Address this node must keep listening on so existing Raft peers can dial it.
    pub fn persisted_advertise_addr(
        state_dir: impl AsRef<Path>,
        node_id: NodeId,
    ) -> Result<Option<String>> {
        let state_dir = state_dir.as_ref();
        if !state_dir.join("CELL_MANAGER_STATE").exists() {
            return Ok(None);
        }
        let st = Self::load_state_file(state_dir)?;
        let mut voter_addrs = BTreeSet::new();
        for route in st.routes.values() {
            if let Some((addr, is_voter)) = route.peers.get(&node_id) {
                if *is_voter && !addr.is_empty() {
                    voter_addrs.insert(addr.clone());
                }
            }
        }
        if voter_addrs.len() > 1 {
            return Err(MetaDbError::ConsensusError(format!(
                "node {node_id} has conflicting persisted raft addresses: {voter_addrs:?}"
            )));
        }
        if let Some(addr) = voter_addrs.into_iter().next() {
            return Ok(Some(addr));
        }
        Ok(st
            .nodes
            .get(&node_id)
            .filter(|addr| !addr.is_empty())
            .cloned())
    }

    pub fn joined_existing(&self) -> bool {
        self.state.lock().unwrap().joined_existing
    }

    pub fn join_pending(&self) -> bool {
        self.state.lock().unwrap().join_pending
    }

    pub fn cluster_id(&self) -> u128 {
        self.identity
            .lock()
            .unwrap()
            .map(|identity| identity.cluster_id)
            .unwrap_or(0)
    }

    pub fn node_instance_id(&self) -> u128 {
        self.identity
            .lock()
            .unwrap()
            .map(|identity| identity.node_instance_id)
            .unwrap_or(0)
    }

    /// Adopts `cluster_id` for a joiner and records this directory's instance id.
    pub fn adopt_cluster_identity(&self, cluster_id: u128) -> Result<ClusterIdentity> {
        let identity = ClusterIdentity::load_or_create(
            &self.state_dir,
            self.host.node_id(),
            self.cell_id,
            Some(cluster_id),
        )?;
        *self.identity.lock().unwrap() = Some(identity);
        Ok(identity)
    }

    pub fn bound_instance(&self, node_id: NodeId) -> Option<u128> {
        self.instance_bindings
            .lock()
            .unwrap()
            .get(&node_id)
            .map(|(instance, _)| *instance)
    }

    fn replicated_membership(&self) -> bool {
        self.local_shard_handles()
            .iter()
            .any(|(_, group, _)| group.voter_count() > 1)
    }

    async fn bind_local_instance(&self) -> Result<()> {
        let Some(identity) = *self.identity.lock().unwrap() else {
            return Ok(());
        };
        if self.bound_instance(identity.node_id) == Some(identity.node_instance_id) {
            return self.mirror_bindings_to_state();
        }
        let addr = self
            .state
            .lock()
            .unwrap()
            .nodes
            .get(&identity.node_id)
            .cloned()
            .unwrap_or_default();
        if addr.is_empty() {
            return Err(MetaDbError::IdentityRejected(format!(
                "node {} has no grpc address to bind its instance id",
                identity.node_id
            )));
        }
        self.register_node_instance(identity.node_id, addr, identity.node_instance_id)
            .await
    }

    /// Records the routes this process intends to join before membership is proposed.
    pub fn begin_join(&self, routes: &[CompositeShardRoute]) -> Result<()> {
        let mut guard = self.state.lock().unwrap();
        if guard.joined_existing {
            return Ok(());
        }
        let mut next = guard.clone();
        next.join_pending = true;
        for route in routes {
            next.routes.insert(route.shard_id, route.clone());
            self.meta_cache.upsert_route(route.clone());
        }
        self.persist_sync(&next)?;
        *guard = next;
        Ok(())
    }

    /// Test hook. Leaves Raft logs in place but clears the finished-join flag so reopen resumes.
    pub fn testing_mark_join_interrupted(&self) -> Result<()> {
        let mut guard = self.state.lock().unwrap();
        let mut next = guard.clone();
        next.joined_existing = false;
        next.join_pending = true;
        self.persist_sync(&next)?;
        *guard = next;
        Ok(())
    }

    /// Replaces cached peer sets with the committed Raft membership.
    pub fn routes_with_local_membership(&self) -> Result<Vec<CompositeShardRoute>> {
        let mut routes = self.list_routes();
        for route in &mut routes {
            let (_, group, _) = self.lookup_local_handle(route.shard_id)?;
            let voters = group.applied_voter_addrs()?;
            route.epoch = group.current_epoch();
            route.peers = voters
                .into_iter()
                .map(|(node_id, addr)| (node_id, (addr, true)))
                .collect();
        }
        Ok(routes)
    }

    /// Committed voter ids for `shard_id`, taken from Raft rather than the route cache.
    pub fn raft_voter_ids(&self, shard_id: ShardId) -> Result<BTreeSet<NodeId>> {
        let (_, group, _) = self.lookup_local_handle(shard_id)?;
        Ok(group.applied_voter_addrs()?.into_keys().collect())
    }

    /// Voter set `remove_voter` would submit, planned from Raft membership.
    pub fn voters_after_removal(
        &self,
        shard_id: ShardId,
        node_id: NodeId,
    ) -> Result<BTreeSet<NodeId>> {
        let mut voters = self.raft_voter_ids(shard_id)?;
        if !voters.contains(&node_id) {
            return Ok(voters);
        }
        if voters.len() <= 1 {
            return Err(MetaDbError::ConsensusError(format!(
                "refusing to remove the last voter of shard {shard_id}"
            )));
        }
        voters.remove(&node_id);
        Ok(voters)
    }

    /// Starts one uninitialized Raft replica per route. The leader's `ConfChange` replicates
    /// membership; this method must not call `Raft::initialize`.
    pub async fn start_joining_replicas(&self, routes: &[CompositeShardRoute]) -> Result<()> {
        for route in routes {
            if self.host.get_group(route.shard_id).is_some() {
                continue;
            }
            let wm = Arc::new(ShardWatermarkTracker::default());
            let handler = Arc::new(TxStateMachineHandler::new(
                route.shard_id,
                ShardEpoch::initial(),
                route.range.clone(),
                wm.clone(),
            ));
            handler.set_instance_bindings(self.instance_bindings.clone());
            let sdir = self.host.shard_dir(route.shard_id);
            let group = ShardRaftGroup::start(
                route.shard_id,
                self.host.node_id(),
                &sdir,
                self.host.block_cache().clone(),
                self.lsm_config.clone(),
                self.raft_opts.clone(),
                handler.clone(),
                wm,
                self.host.clock().clone(),
                self.host.router(),
                self.host.channel_pool().clone(),
            )
            .await?;
            self.host.register_running_group(group);
            self.handlers
                .write()
                .unwrap()
                .insert(route.shard_id, handler);
            self.meta_cache.upsert_route(route.clone());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    /// True when every local shard's applied membership includes `node_id` as a voter.
    pub fn local_voters_include(&self, node_id: NodeId) -> bool {
        let groups = self.local_shard_handles();
        if groups.is_empty() {
            return false;
        }
        groups
            .iter()
            .all(|(_, group, _)| group.membership_contains_voter(node_id))
    }

    /// Persists the leader's post-join routes and records that restart must not bootstrap.
    pub fn install_joined_membership(&self, mut routes: Vec<CompositeShardRoute>) -> Result<()> {
        for route in &mut routes {
            if let Ok((_, group, _)) = self.lookup_local_handle(route.shard_id) {
                route.epoch = group.current_epoch();
            }
        }
        let mut guard = self.state.lock().unwrap();
        let mut next = guard.clone();
        next.joined_existing = true;
        next.join_pending = false;
        for route in &routes {
            for (nid, (addr, _)) in &route.peers {
                next.nodes.insert(*nid, addr.clone());
            }
            next.routes.insert(route.shard_id, route.clone());
            let voter_addrs = route
                .peers
                .iter()
                .filter(|(_, (_, is_voter))| *is_voter)
                .map(|(&nid, (addr, _))| (nid, addr.clone()))
                .collect();
            self.host.record_registry_state(
                route.shard_id,
                RegistryShardState::Active { peers: voter_addrs },
            )?;
        }
        self.persist_sync(&next)?;
        *guard = next;
        drop(guard);
        for route in routes {
            self.meta_cache.upsert_route(route);
        }
        self.ingest_engine_bindings()?;
        Ok(())
    }

    /// Test hook. The next `n` cell-state persists return an I/O error and leave memory unchanged.
    pub fn testing_fail_next_persists(&self, n: u32) {
        self.fail_persists.store(n, Ordering::Relaxed);
    }

    /// Test hook. `execute_local_split` returns after the named durable step.
    pub fn set_split_crash_point(&self, point: Option<SplitCrashPoint>) {
        *self.crash_point.lock().unwrap() = point;
    }

    /// Reads `CELL_MANAGER_STATE` from disk (not the in-memory cache).
    pub fn durable_split_plan_on_disk(&self, request_uuid: u128) -> Result<bool> {
        let st = Self::load_state_file(&self.state_dir)?;
        Ok(st.split_plans.contains_key(&request_uuid))
    }

    async fn boot_local_shards(&self) -> Result<()> {
        let (routes, joined_existing): (Vec<CompositeShardRoute>, bool) = {
            let st = self.state.lock().unwrap();
            (st.routes.values().cloned().collect(), st.joined_existing)
        };

        for route in routes {
            let voter_addrs: BTreeMap<NodeId, String> = route
                .peers
                .iter()
                .filter(|(_, (_, is_voter))| *is_voter)
                .map(|(&nid, (addr, _))| (nid, addr.clone()))
                .collect();

            if route.peers.contains_key(&self.host.node_id()) {
                let wm = Arc::new(ShardWatermarkTracker::default());
                // A joined replica replays membership from the log (or restores it from the
                // flushed checkpoint). Starting at the persisted final epoch would bump again.
                let handler_epoch = if joined_existing {
                    ShardEpoch::initial()
                } else {
                    route.epoch
                };
                let handler = Arc::new(TxStateMachineHandler::new(
                    route.shard_id,
                    handler_epoch,
                    route.range.clone(),
                    wm.clone(),
                ));
                handler.set_instance_bindings(self.instance_bindings.clone());
                let sdir = self.resolved_shard_dir(route.shard_id)?;
                let group = ShardRaftGroup::start(
                    route.shard_id,
                    self.host.node_id(),
                    &sdir,
                    self.host.block_cache().clone(),
                    self.lsm_config.clone(),
                    self.raft_opts.clone(),
                    handler.clone(),
                    wm,
                    self.host.clock().clone(),
                    self.host.router(),
                    self.host.channel_pool().clone(),
                )
                .await?;
                if !joined_existing {
                    // Joined replicas already have membership in the replicated log.
                    // `initialize` on an empty log would fork a cluster with the same node ids.
                    group.initialize_if_needed(voter_addrs.clone()).await?;
                }
                self.host.register_running_group(group.clone());
                let membership = group
                    .raft()
                    .metrics()
                    .borrow_watched()
                    .membership_config
                    .clone();
                let local_addr = &route.peers[&self.host.node_id()].0;
                if membership
                    .membership()
                    .get_node(&self.host.node_id())
                    .is_some_and(|node| node.addr != *local_addr)
                {
                    let voters: Vec<_> = membership.membership().voter_ids().collect();
                    if voters != [self.host.node_id()]
                        || membership.membership().get_joint_config().len() != 1
                    {
                        return Err(MetaDbError::ShardRoutingError(
                            "persisted Raft peer address differs from the listener; a replicated address change requires an online membership update".to_string()));
                    }
                    // ReadIndex waits for a committed log from the recovered leader's
                    // current term before changing membership metadata.
                    let _ = group.linearizable_read(group.current_epoch()).await?;
                    group
                        .raft()
                        .change_membership_if(
                            ChangeMembers::SetNodes(BTreeMap::from([(
                                self.host.node_id(),
                                BasicNode::new(local_addr),
                            )])),
                            true,
                            [Precondition::LastMembershipLogId {
                                last_membership_log_id: *membership.log_id(),
                            }],
                        )
                        .await
                        .map_err(|error| {
                            MetaDbError::ConsensusError(format!(
                                "failed to update the local Raft peer address: {error}"
                            ))
                        })?;
                }
                self.host.record_registry_state(
                    route.shard_id,
                    RegistryShardState::Active { peers: voter_addrs },
                )?;
                self.handlers
                    .write()
                    .unwrap()
                    .insert(route.shard_id, handler);

                let mut synced_route = route.clone();
                synced_route.epoch = group.current_epoch();
                {
                    let mut state = self.state.lock().unwrap();
                    let mut next = state.clone();
                    next.routes
                        .insert(synced_route.shard_id, synced_route.clone());
                    // Persist refreshed node/route addresses only after the Raft
                    // membership update has committed. Repeating it is safe on restart.
                    self.persist_sync(&next)?;
                    *state = next;
                }
                self.meta_cache.upsert_route(synced_route);
            } else {
                self.meta_cache.upsert_route(route);
            }
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
        Ok(())
    }

    pub fn cell_id(&self) -> CellId {
        self.cell_id
    }

    pub fn local_node_id(&self) -> NodeId {
        self.host.node_id()
    }

    fn resolved_shard_dir(&self, shard_id: ShardId) -> Result<PathBuf> {
        let Some(root) = self.jumbo_dedicated_root.lock().unwrap().clone() else {
            return Ok(self.host.shard_dir(shard_id));
        };
        let Some(dest) = load_live_dir(&root, shard_id)? else {
            return Ok(self.host.shard_dir(shard_id));
        };
        let source = self.host.shard_dir(shard_id);
        if source.exists() {
            remove_tree(&source)?;
        }
        Ok(dest)
    }

    /// Copies this process's jumbo shard directory onto `dedicated_root`.
    ///
    /// Order: CRC the copy, fsync `MOVE_LIVE`, then open Raft on that directory,
    /// then delete the old directory. A restart that sees `MOVE_LIVE` opens the
    /// dedicated directory and does not open the old one. The dedicated directory
    /// is not deleted after that record exists. Another machine's disk is not
    /// written. `transfer_leader` is not called.
    pub async fn relocate_jumbo_shard_files(
        &self,
        shard_id: ShardId,
        dedicated_root: &Path,
    ) -> Result<PathBuf> {
        let _lock = self.membership_lock.lock().await;
        refuse_dedicated_root(self.host.data_dir(), dedicated_root)?;
        if self
            .jumbo_file_move_failed
            .lock()
            .unwrap()
            .contains(&shard_id)
        {
            return Err(MetaDbError::ShardRoutingError(format!(
                "jumbo shard {shard_id} file move already failed in this process; the source was left in place and transfer_leader was not called"
            )));
        }
        let route = {
            let state = self.state.lock().unwrap();
            state.routes.get(&shard_id).cloned()
        };
        let Some(route) = route else {
            return Err(MetaDbError::ShardRoutingError(format!(
                "shard {shard_id} has no route; files were not moved"
            )));
        };
        match route.health_state {
            ShardHealthState::JumboIsolated { dedicated_node, .. }
                if dedicated_node == self.host.node_id() => {}
            ShardHealthState::JumboIsolated { dedicated_node, .. } => {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "dedicated node {dedicated_node} is not this process; shard files stay on this disk. Another machine was not written and transfer_leader was not called"
                )));
            }
            ShardHealthState::Normal => {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "shard {shard_id} is not jumbo-isolated; a route flag was not treated as a file move"
                )));
            }
        }
        *self.jumbo_dedicated_root.lock().unwrap() = Some(dedicated_root.to_path_buf());
        let dest = shard_move_dir(dedicated_root, shard_id);
        if load_live_dir(dedicated_root, shard_id)?.is_some() {
            self.finish_live_move(shard_id, &route, &dest).await?;
            return Ok(dest);
        }
        let source = self.host.shard_dir(shard_id);
        // A destination without MOVE_LIVE has never had Raft opened on it.
        // A published destination is handled above and is not deleted.
        if dest.exists() {
            remove_tree(&dest)?;
        }
        if !source.exists() {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo shard {shard_id} directory {} is missing; nothing was published on {}",
                source.display(),
                dest.display()
            )));
        }
        let group = self.host.get_group(shard_id).ok_or_else(|| {
            MetaDbError::ShardRoutingError(format!(
                "shard {shard_id} is not running; files were not moved"
            ))
        })?;
        group.state_machine().flush_to_lsm().map_err(|error| {
            MetaDbError::StorageIoError(format!(
                "jumbo shard {shard_id} flush failed before the file move: {error}"
            ))
        })?;
        group.shutdown().await?;
        self.host.take_running_group(shard_id);
        drop(group);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let files = match copy_shard_tree(&source, &dest) {
            Ok(files) => files,
            Err(error) => {
                let _ = remove_tree(&dest);
                self.open_shard_group(&route, &source).await?;
                self.jumbo_file_move_failed.lock().unwrap().insert(shard_id);
                return Err(error);
            }
        };
        if let Err(error) = write_move_phase(&dest, shard_id, MOVE_STAGED, &files) {
            let _ = remove_tree(&dest);
            self.open_shard_group(&route, &source).await?;
            self.jumbo_file_move_failed.lock().unwrap().insert(shard_id);
            return Err(MetaDbError::StorageIoError(error.to_string()));
        }
        // MOVE_LIVE is durable before Raft opens dest. After the LIVE rename
        // is visible, a restart opens dest and must not delete it or reopen
        // source, including when the directory fsync fails.
        if let Err(error) = write_move_phase(&dest, shard_id, MOVE_LIVE, &files) {
            if matches!(error, MovePhaseError::LiveVisibleDirSyncFailed(_)) {
                return Err(MetaDbError::StorageIoError(format!(
                    "jumbo shard {shard_id} MOVE_LIVE is visible on {} but the directory fsync failed; the old directory was not reopened and the dedicated copy was not deleted: {error}",
                    dest.display()
                )));
            }
            let _ = remove_tree(&dest);
            self.open_shard_group(&route, &source).await?;
            self.jumbo_file_move_failed.lock().unwrap().insert(shard_id);
            return Err(MetaDbError::StorageIoError(error.to_string()));
        }
        if let Err(error) = self.open_shard_group(&route, &dest).await {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo shard {shard_id} is published on {} but did not finish opening; the old directory will not be used on restart: {error}",
                dest.display()
            )));
        }
        if self
            .stop_before_source_removal
            .swap(false, Ordering::Relaxed)
        {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo shard {shard_id} is published on {}; the old directory was left in place and will not be opened on restart",
                dest.display()
            )));
        }
        self.remove_source_after_live(shard_id, &dest)?;
        Ok(dest)
    }

    async fn finish_live_move(
        &self,
        shard_id: ShardId,
        route: &CompositeShardRoute,
        dest: &Path,
    ) -> Result<()> {
        if self.host.get_group(shard_id).is_none() {
            self.open_shard_group(route, dest).await?;
        }
        self.remove_source_after_live(shard_id, dest)
    }

    fn remove_source_after_live(&self, shard_id: ShardId, dest: &Path) -> Result<()> {
        let source = self.host.shard_dir(shard_id);
        if !source.exists() {
            return Ok(());
        }
        remove_tree(&source).map_err(|error| {
            MetaDbError::StorageIoError(format!(
                "jumbo shard {shard_id} is open on {} but the old directory could not be removed: {error}",
                dest.display()
            ))
        })
    }

    /// Moves every local jumbo shard whose dedicated node is this process.
    /// No dedicated root means this returns without copying. A failed shard is
    /// not retried until the process restarts. `transfer_leader` is not called.
    pub async fn relocate_configured_jumbo_files(&self) -> Result<()> {
        let Some(root) = self.jumbo_dedicated_root.lock().unwrap().clone() else {
            return Ok(());
        };
        let shard_ids: Vec<ShardId> = {
            let state = self.state.lock().unwrap();
            state
                .routes
                .values()
                .filter_map(|route| match route.health_state {
                    ShardHealthState::JumboIsolated { dedicated_node, .. }
                        if dedicated_node == self.host.node_id() =>
                    {
                        Some(route.shard_id)
                    }
                    _ => None,
                })
                .collect()
        };
        for shard_id in shard_ids {
            if self
                .jumbo_file_move_failed
                .lock()
                .unwrap()
                .contains(&shard_id)
            {
                continue;
            }
            if let Err(error) = self.relocate_jumbo_shard_files(shard_id, &root).await {
                tracing::warn!(%error, shard_id, "jumbo shard files were not moved");
            }
        }
        Ok(())
    }

    async fn open_shard_group(&self, route: &CompositeShardRoute, dir: &Path) -> Result<()> {
        let wm = Arc::new(ShardWatermarkTracker::default());
        let handler = Arc::new(TxStateMachineHandler::new(
            route.shard_id,
            route.epoch,
            route.range.clone(),
            wm.clone(),
        ));
        handler.set_instance_bindings(self.instance_bindings.clone());
        let voters: BTreeMap<NodeId, String> = route
            .peers
            .iter()
            .filter(|(_, (_, voter))| *voter)
            .map(|(&id, (addr, _))| (id, addr.clone()))
            .collect();
        let group = ShardRaftGroup::start(
            route.shard_id,
            self.host.node_id(),
            dir,
            self.host.block_cache().clone(),
            self.lsm_config.clone(),
            self.raft_opts.clone(),
            handler.clone(),
            wm,
            self.host.clock().clone(),
            self.host.router(),
            self.host.channel_pool().clone(),
        )
        .await?;
        group.initialize_if_needed(voters).await?;
        self.host.register_running_group(group);
        self.handlers
            .write()
            .unwrap()
            .insert(route.shard_id, handler);
        Ok(())
    }

    /// Test hook. The next move publishes `MOVE_LIVE` and opens the destination,
    /// then returns before deleting the old directory.
    pub fn testing_stop_before_jumbo_source_removal(&self) {
        self.stop_before_source_removal
            .store(true, Ordering::Relaxed);
    }

    /// Test hook. Persists the jumbo route flag without moving files.
    pub fn testing_mark_local_jumbo(&self, shard_id: ShardId, dominant_pid: InodeId) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let mut next = state.clone();
        let route = next.routes.get_mut(&shard_id).ok_or_else(|| {
            MetaDbError::ShardRoutingError(format!("shard {shard_id} has no route"))
        })?;
        route.health_state = ShardHealthState::JumboIsolated {
            dedicated_node: self.host.node_id(),
            dominant_pid,
        };
        let updated = route.clone();
        self.persist_sync(&next)?;
        *state = next;
        self.meta_cache.upsert_route(updated);
        Ok(())
    }

    pub fn host(&self) -> &Arc<ShardGroupHost> {
        &self.host
    }

    pub fn meta_cache(&self) -> &Arc<CompositeMetaCache> {
        &self.meta_cache
    }

    pub fn register_node(&self, node_id: NodeId, grpc_addr: String) -> Result<()> {
        node_id_in_ha_range(node_id)?;
        self.register_address_only(node_id, grpc_addr)
    }

    /// Binds `node_id` to `instance_id` and replicates that binding before returning.
    ///
    /// `instance_id == 0` is the legacy address-only register. A non-zero instance is
    /// proposed through Raft, so followers learn it before this call acknowledges success.
    /// Voter removal does not drop the binding.
    pub async fn register_node_instance(
        &self,
        node_id: NodeId,
        grpc_addr: String,
        instance_id: u128,
    ) -> Result<()> {
        node_id_in_ha_range(node_id)?;
        if instance_id == 0 {
            return self.register_address_only(node_id, grpc_addr);
        }
        if grpc_addr.trim().is_empty() {
            return Err(MetaDbError::IdentityRejected(
                "refusing to register an empty grpc address".to_string(),
            ));
        }
        let grpc_addr = grpc_addr.trim().to_string();
        if let Some((bound, _)) = self.instance_bindings.lock().unwrap().get(&node_id) {
            if *bound != instance_id {
                return Err(MetaDbError::IdentityRejected(format!(
                    "node {node_id} is already bound to instance {bound:#x}; refusing a reused node identity"
                )));
            }
        }
        if let Some((other, (bound, _))) = self
            .instance_bindings
            .lock()
            .unwrap()
            .iter()
            .find(|(id, (instance, _))| **id != node_id && *instance == instance_id)
        {
            return Err(MetaDbError::IdentityRejected(format!(
                "node instance {bound:#x} is already bound to node {other}"
            )));
        }
        self.propose_instance_binding(node_id, instance_id, grpc_addr.clone())
            .await?;
        self.mirror_bindings_to_state()
    }

    fn register_address_only(&self, node_id: NodeId, grpc_addr: String) -> Result<()> {
        if grpc_addr.trim().is_empty() {
            return Err(MetaDbError::IdentityRejected(
                "refusing to register an empty grpc address".to_string(),
            ));
        }
        let grpc_addr = grpc_addr.trim().to_string();
        let mut guard = self.state.lock().unwrap();
        if self
            .instance_bindings
            .lock()
            .unwrap()
            .contains_key(&node_id)
            || guard.node_instances.contains_key(&node_id)
        {
            return Err(MetaDbError::IdentityRejected(format!(
                "node {node_id} is already bound to an instance; refusing an anonymous register"
            )));
        }
        if let Some(existing) = guard.nodes.get(&node_id) {
            if existing != &grpc_addr {
                return Err(MetaDbError::IdentityRejected(format!(
                    "node {node_id} is already bound to {existing}; refusing {grpc_addr}"
                )));
            }
            return Ok(());
        }
        let mut next = guard.clone();
        next.nodes.insert(node_id, grpc_addr);
        self.persist_sync(&next)?;
        *guard = next;
        Ok(())
    }

    async fn propose_instance_binding(
        &self,
        node_id: NodeId,
        instance_id: u128,
        grpc_addr: String,
    ) -> Result<()> {
        let (_, group, _) = self.lookup_local_handle(1).or_else(|_| {
            self.local_shard_handles()
                .into_iter()
                .next()
                .ok_or_else(|| {
                    MetaDbError::ConsensusError(
                        "no local shard can replicate the node instance binding".to_string(),
                    )
                })
        })?;
        let payload = metadb_tx::command_payload(&metadb_tx::TxRaftCmd::BindNodeInstance {
            node_id,
            instance_id,
            grpc_addr,
        })?;
        let epoch = group.current_epoch();
        group
            .propose_until(
                epoch,
                metadb_core::HlcTimestamp::ZERO,
                payload,
                tokio::time::Instant::now() + std::time::Duration::from_secs(10),
            )
            .await?;
        Ok(())
    }

    fn ingest_engine_bindings(&self) -> Result<()> {
        let read_at = metadb_core::HlcTimestamp::new(metadb_core::HlcTimestamp::MAX_PHYSICAL_MS, 0);
        for (_, group, _) in self.local_shard_handles() {
            let rows = group.engine().scan_range(&[0x09], &[0x0A], read_at)?;
            for (key, value) in rows {
                if key.len() != 9 || key[0] != 0x09 {
                    continue;
                }
                let node_id = u64::from_be_bytes(key[1..9].try_into().unwrap());
                let record: metadb_tx::NodeInstanceRecord = metadb_core::codec::deserialize(&value)
                    .map_err(|error| MetaDbError::CodecError(error.to_string()))?;
                self.instance_bindings
                    .lock()
                    .unwrap()
                    .insert(node_id, (record.instance_id, record.grpc_addr));
            }
        }
        self.mirror_bindings_to_state()
    }

    fn mirror_bindings_to_state(&self) -> Result<()> {
        let bindings = self.instance_bindings.lock().unwrap().clone();
        let mut guard = self.state.lock().unwrap();
        let mut next = guard.clone();
        let mut changed = false;
        for (node_id, (instance_id, addr)) in bindings {
            if next.node_instances.get(&node_id) != Some(&instance_id) {
                next.node_instances.insert(node_id, instance_id);
                changed = true;
            }
            if !addr.is_empty() {
                next.nodes.entry(node_id).or_insert_with(|| {
                    changed = true;
                    addr
                });
            }
        }
        if changed {
            self.persist_sync(&next)?;
            *guard = next;
        }
        Ok(())
    }

    /// Routes for cluster status. A local shard that cannot rebuild its snapshot
    /// is not returned as a live voter; the caller gets an error instead.
    pub fn routes_for_live_status(&self) -> Result<Vec<CompositeShardRoute>> {
        let routes = self.list_routes();
        for route in &routes {
            if let Some(group) = self.host.get_group(route.shard_id) {
                // Probe the snapshot directory. The in-memory latch is still
                // empty until something attempts repair.
                group.ensure_published_snapshot_sync().map_err(|error| {
                    MetaDbError::StorageIoError(format!(
                        "shard {} storage is unrecoverable, so it is not a live voter: {error}",
                        route.shard_id
                    ))
                })?;
            }
        }
        Ok(routes)
    }

    pub fn list_routes(&self) -> Vec<CompositeShardRoute> {
        let mut routes: Vec<CompositeShardRoute> = {
            let guard = self.state.lock().unwrap();
            guard.routes.values().cloned().collect()
        };
        self.overlay_live_membership(&mut routes);
        routes
    }

    /// Reports the applied Raft membership and epoch. The persisted route cache can
    /// lag a follower that has already applied `{1,2,3}` while still storing two voters.
    fn overlay_live_membership(&self, routes: &mut [CompositeShardRoute]) {
        for route in routes.iter_mut() {
            let Ok((_, group, _)) = self.lookup_local_handle(route.shard_id) else {
                continue;
            };
            route.epoch = group.current_epoch();
            if let Some(leader) = group.current_leader() {
                route.leader_node = leader;
            }
            if let Ok(voters) = group.applied_voter_addrs() {
                route.peers = voters
                    .into_iter()
                    .map(|(node_id, addr)| {
                        let addr = if addr.is_empty() {
                            route
                                .peers
                                .get(&node_id)
                                .map(|(cached, _)| cached.clone())
                                .unwrap_or_default()
                        } else {
                            addr
                        };
                        (node_id, (addr, true))
                    })
                    .collect();
            }
        }
    }

    pub fn local_shard_handles(&self) -> Vec<RoutedShardHandle> {
        let handlers = self.handlers.read().unwrap();
        let mut out = Vec::new();
        for (&sid, h) in handlers.iter() {
            if let Some(g) = self.host.get_group(sid) {
                out.push((g.current_epoch(), g, h.clone()));
            }
        }
        out
    }

    pub fn lookup_local_handle(&self, shard_id: ShardId) -> Result<RoutedShardHandle> {
        let g = self.host.get_group(shard_id).ok_or_else(|| {
            MetaDbError::ShardRoutingError(format!("shard {} not hosted locally", shard_id))
        })?;
        let h = self
            .handlers
            .read()
            .unwrap()
            .get(&shard_id)
            .cloned()
            .ok_or_else(|| {
                MetaDbError::ShardRoutingError(format!("handler {} missing", shard_id))
            })?;
        Ok((g.current_epoch(), g, h))
    }

    /// Stage A of `ExecuteSplit` (`Resolution B15` & `B18`): durably allocates `child_shard_id`
    /// and records `SplitPlanRecord::Planned` before `S_parent` executes local split.
    pub fn alloc_split_plan(
        &self,
        request_uuid: u128,
        parent_shard_id: ShardId,
        split_key: Vec<u8>,
        target_version: u64,
        dual_mode_pid: Option<InodeId>,
    ) -> Result<ShardId> {
        {
            let guard = self.state.lock().unwrap();
            if request_uuid != 0 {
                if let Some(existing) = guard.split_plans.get(&request_uuid) {
                    return Ok(existing.child_shard_id);
                }
            }
        }
        let (child_shard_id, _) =
            self.id_allocator
                .allocate_block(request_uuid, AllocIdKind::ShardId, 1)?;

        let mut guard = self.state.lock().unwrap();
        if request_uuid != 0 {
            if let Some(existing) = guard.split_plans.get(&request_uuid) {
                return Ok(existing.child_shard_id);
            }
        }
        let mut next = guard.clone();
        next.split_plans.insert(
            request_uuid,
            SplitPlanRecord {
                parent_shard_id,
                child_shard_id,
                split_key,
                target_version,
                state: SplitPlanState::Planned,
                dual_mode_pid,
            },
        );
        self.persist_sync(&next)?;
        *guard = next;
        Ok(child_shard_id)
    }

    /// Stage C of `ExecuteSplit`: atomically publishes `(parent_route, child_route)` in `CELL_MANAGER_STATE` and `MetaCache`.
    pub fn activate_split(
        &self,
        request_uuid: u128,
        parent_route: CompositeShardRoute,
        child_route: CompositeShardRoute,
        child_handler: Option<Arc<TxStateMachineHandler>>,
    ) -> Result<()> {
        {
            let mut guard = self.state.lock().unwrap();
            let mut next = guard.clone();
            if let Some(plan) = next.split_plans.get_mut(&request_uuid) {
                plan.state = SplitPlanState::Activated;
            }
            next.routes
                .insert(parent_route.shard_id, parent_route.clone());
            next.routes
                .insert(child_route.shard_id, child_route.clone());
            self.persist_sync(&next)?;
            *guard = next;
        }
        if let Some(h) = child_handler {
            self.handlers
                .write()
                .unwrap()
                .insert(child_route.shard_id, h);
        }
        self.meta_cache
            .apply_split_routes(parent_route, child_route);
        Ok(())
    }

    /// Evaluates `shard_id` against `split_threshold_bytes` (or runs unconditionally when `force == true`):
    /// - Splits along `PID` boundary (or `(PID, name)` when `DualModeFineGrained` is enabled).
    /// - Isolates single-`PID` Jumbo Shards onto a dedicated BE node (`JumboIsolated`).
    pub async fn maybe_split_or_isolate_shard(
        &self,
        shard_id: ShardId,
        force: bool,
    ) -> Result<Option<ShardId>> {
        let _guard = self.split_lock.lock().await;
        // Finish or drop any plan left by a crash before discovering a new boundary.
        self.recover_incomplete_splits().await?;
        // Recovery can defer a pre-fence plan until transaction maintenance has
        // drained the original shard. Keep its durable boundary and child ID.
        if self
            .state
            .lock()
            .unwrap()
            .split_plans
            .values()
            .any(|plan| plan.parent_shard_id == shard_id && plan.state == SplitPlanState::Planned)
        {
            return Ok(None);
        }
        let route = {
            let st = self.state.lock().unwrap();
            st.routes.get(&shard_id).cloned().ok_or_else(|| {
                MetaDbError::ShardRoutingError(format!("unknown shard {}", shard_id))
            })?
        };
        let (ep, group, handler) = self.lookup_local_handle(shard_id)?;
        let mut live_route = route;
        live_route.epoch = ep;

        if !force {
            let approx = group
                .engine()
                .approximate_size_bytes(self.host.clock().now());
            if approx < self.split_threshold_bytes {
                return Ok(None);
            }
        }

        let Some(candidate) = RaftShardSplitWorker::discover_split_key(&live_route, &group).await?
        else {
            return Ok(None);
        };

        match candidate {
            SplitCandidate::Jumbo { dominant_pid } => {
                let dedicated_node = {
                    let mut st = self.state.lock().unwrap();
                    let nodes: Vec<NodeId> = st.nodes.keys().copied().collect();
                    let all_routes: Vec<CompositeShardRoute> =
                        st.routes.values().cloned().collect();
                    let new_health = JumboIsolationPlanner::select_dedicated_node(
                        shard_id,
                        dominant_pid,
                        live_route.leader_node,
                        &nodes,
                        &all_routes,
                    );
                    let ShardHealthState::JumboIsolated { dedicated_node, .. } = new_health else {
                        return Err(MetaDbError::ShardRoutingError(
                            "jumbo planner did not select a dedicated node".to_string(),
                        ));
                    };
                    let already = st
                        .routes
                        .get(&shard_id)
                        .is_some_and(|route| route.health_state == new_health);
                    if !already && st.routes.contains_key(&shard_id) {
                        let mut next = st.clone();
                        if let Some(route) = next.routes.get_mut(&shard_id) {
                            route.health_state = new_health;
                        }
                        let updated = next.routes.get(&shard_id).cloned();
                        self.persist_sync(&next)?;
                        *st = next;
                        if let Some(updated) = updated {
                            self.meta_cache.upsert_route(updated);
                        }
                    }
                    dedicated_node
                };
                // The route flag is detection only. Placement below either performs a
                // membership-safe move or records why it did not.
                self.reconcile_jumbo_placement(shard_id, dominant_pid, dedicated_node)
                    .await?;
                Ok(None)
            }
            SplitCandidate::SplitAt {
                split_key,
                dual_mode_pid,
            } => {
                let req_uuid = ((self.cell_id as u128) << 96)
                    | ((shard_id as u128) << 48)
                    | (live_route.epoch.version as u128 + 1);
                let child_shard_id = self.alloc_split_plan(
                    req_uuid,
                    shard_id,
                    split_key,
                    live_route.epoch.version + 1,
                    dual_mode_pid,
                )?;
                // A retry of the same epoch reuses the persisted boundary, not a newly
                // discovered one that was never copied into the child.
                let (split_key, dual_mode_pid, target_version) = {
                    let st = self.state.lock().unwrap();
                    let plan = st.split_plans.get(&req_uuid).cloned().ok_or_else(|| {
                        MetaDbError::ShardRoutingError(
                            "split plan missing after durable alloc".to_string(),
                        )
                    })?;
                    (plan.split_key, plan.dual_mode_pid, plan.target_version)
                };
                let crash_at = *self.crash_point.lock().unwrap();
                let (new_parent, new_child, _child_grp, child_handler) =
                    RaftShardSplitWorker::execute_local_split(
                        &self.host,
                        &live_route,
                        &group,
                        &handler,
                        child_shard_id,
                        split_key,
                        dual_mode_pid,
                        self.lsm_config.clone(),
                        self.raft_opts.clone(),
                        target_version,
                        crash_at,
                    )
                    .await?;
                self.activate_split(req_uuid, new_parent, new_child, Some(child_handler))?;
                Ok(Some(child_shard_id))
            }
        }
    }

    /// Switches a Jumbo Shard into `ShardSplitMode::DualModeFineGrained` for `dominant_pid`.
    pub fn enable_dual_mode_split(&self, shard_id: ShardId, dominant_pid: InodeId) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        if !st.routes.contains_key(&shard_id) {
            return Err(MetaDbError::ShardRoutingError(format!(
                "unknown shard {}",
                shard_id
            )));
        }
        let mut next = st.clone();
        let route = next
            .routes
            .get_mut(&shard_id)
            .ok_or_else(|| MetaDbError::ShardRoutingError(format!("unknown shard {}", shard_id)))?;
        route.split_mode = ShardSplitMode::DualModeFineGrained;
        route.dual_mode_pid = Some(dominant_pid);
        let updated = route.clone();
        self.persist_sync(&next)?;
        *st = next;
        self.meta_cache.upsert_route(updated);
        Ok(())
    }

    /// Grows or shrinks every local shard to `target_voters`.
    ///
    /// Growth adds the lowest registered node ids that are not yet voters.
    /// Shrink drops the highest voter ids until the target count remains.
    /// Existing voters are kept when the count already matches.
    pub async fn scale_out_replicas(&self, target_voters: usize) -> Result<usize> {
        self.scale_out_replicas_ensuring(target_voters, None).await
    }

    /// Like [`scale_out_replicas`](Self::scale_out_replicas), but `ensure_node` is
    /// added even when the live voter count already meets `target_voters`.
    ///
    /// A joiner that computed `target = cached_voters + 1` used to no-op once two
    /// voters existed, so the third node never became a voter.
    pub async fn scale_out_replicas_ensuring(
        &self,
        target_voters: usize,
        ensure_node: Option<NodeId>,
    ) -> Result<usize> {
        if target_voters == 0 && ensure_node.is_none() {
            return Err(MetaDbError::ConsensusError(
                "target_voters must be at least 1".to_string(),
            ));
        }
        let _membership = self.membership_lock.lock().await;
        let (nodes, shard_ids): (BTreeMap<NodeId, String>, Vec<ShardId>) = {
            let st = self.state.lock().unwrap();
            if ensure_node.is_none() && st.nodes.len() < target_voters {
                return Err(MetaDbError::ConsensusError(format!(
                    "need {} registered nodes in cell {}, have {}",
                    target_voters,
                    self.cell_id,
                    st.nodes.len()
                )));
            }
            (st.nodes.clone(), st.routes.keys().copied().collect())
        };
        let mut upgraded = 0usize;
        for sid in shard_ids {
            self.retarget_shard_voters(sid, target_voters, &nodes, ensure_node)
                .await?;
            upgraded += 1;
        }
        Ok(upgraded)
    }

    /// Removes `node_id` from every local shard voter set. Refuses to drop the last voter.
    pub async fn remove_voter(&self, node_id: NodeId) -> Result<usize> {
        let _membership = self.membership_lock.lock().await;
        let shard_ids: Vec<ShardId> = {
            let st = self.state.lock().unwrap();
            st.routes.keys().copied().collect()
        };
        let mut changed = 0usize;
        for sid in shard_ids {
            let voters = self.voters_after_removal(sid, node_id)?;
            let (_, group, _) = self.lookup_local_handle(sid)?;
            let current = group.uniform_voter_addrs()?;
            if current.len() == voters.len() && current.keys().all(|id| voters.contains(id)) {
                continue;
            }
            ConfChangeOrchestrator::change_voters(group.raft(), voters.clone()).await?;
            let committed = group.wait_uniform_voter_addrs().await?;
            self.persist_voter_set(sid, &committed, group.current_epoch())?;
            changed += 1;
        }
        Ok(changed)
    }

    async fn retarget_shard_voters(
        &self,
        shard_id: ShardId,
        target_voters: usize,
        nodes: &BTreeMap<NodeId, String>,
        ensure_node: Option<NodeId>,
    ) -> Result<()> {
        let (_, group, _) = self.lookup_local_handle(shard_id)?;
        let current = group.uniform_voter_addrs()?;
        let current_voters: BTreeSet<NodeId> = current.keys().copied().collect();
        let mut registered: BTreeSet<NodeId> = nodes.keys().copied().collect();
        registered.extend(current_voters.iter().copied());
        let final_voters =
            plan_membership_voters(&current_voters, &registered, target_voters, ensure_node)?;

        let mut addrs = nodes.clone();
        for (nid, addr) in &current {
            addrs.entry(*nid).or_insert_with(|| addr.clone());
        }
        for nid in final_voters.difference(&current_voters) {
            if addrs.get(nid).map(|addr| addr.is_empty()).unwrap_or(true) {
                return Err(MetaDbError::ConsensusError(format!(
                    "missing grpc address for node {nid} on shard {shard_id}"
                )));
            }
        }
        let learners_to_add: Vec<(NodeId, String)> = final_voters
            .iter()
            .filter(|nid| !current_voters.contains(nid))
            .filter_map(|nid| addrs.get(nid).map(|addr| (*nid, addr.clone())))
            .collect();
        if !learners_to_add.is_empty() {
            // Split children seed inherited keys into SSTables at Raft index 0. Purge a snapshot
            // first so the learner cannot become a voter by replaying a log that never contained them.
            let min_matched = group.prepare_learner_snapshot().await?;
            ConfChangeOrchestrator::add_learners_and_promote_voters(
                group.raft(),
                &learners_to_add,
                final_voters.clone(),
                64,
                Duration::from_secs(20),
                min_matched,
            )
            .await?;
        } else if current_voters != final_voters {
            ConfChangeOrchestrator::change_voters(group.raft(), final_voters.clone()).await?;
        }
        let committed = group.wait_uniform_voter_addrs().await?;
        self.persist_voter_set(shard_id, &committed, group.current_epoch())?;
        Ok(())
    }

    fn persist_voter_set(
        &self,
        shard_id: ShardId,
        voters: &BTreeMap<NodeId, String>,
        epoch: metadb_core::ShardEpoch,
    ) -> Result<()> {
        let mut st = self.state.lock().unwrap();
        if !st.routes.contains_key(&shard_id) {
            return Ok(());
        }
        let mut next = st.clone();
        if let Some(route) = next.routes.get_mut(&shard_id) {
            let mut peers = BTreeMap::new();
            for (nid, addr) in voters {
                if addr.is_empty() {
                    return Err(MetaDbError::ConsensusError(format!(
                        "missing grpc address for voter {nid} on shard {shard_id}"
                    )));
                }
                next.nodes.insert(*nid, addr.clone());
                peers.insert(*nid, (addr.clone(), true));
            }
            route.peers = peers;
            route.epoch = epoch;
        }
        let updated = next.routes.get(&shard_id).cloned();
        self.persist_sync(&next)?;
        *st = next;
        if let Some(updated) = updated {
            self.meta_cache.upsert_route(updated);
        }
        Ok(())
    }

    /// Shard ids named by `CELL_MANAGER_STATE` bytes: published routes and split plans.
    ///
    /// The bytes are the on-disk frame, including its CRC32C trailer. This does not open a path.
    pub fn shard_ids_in_cell_state_bytes(raw: &[u8]) -> Result<BTreeSet<ShardId>> {
        let body = verify_crc32c_le(raw, "CELL_MANAGER_STATE")?;
        let state = decode_cell_state(body)?;
        let mut ids = BTreeSet::new();
        ids.extend(state.routes.keys().copied());
        for plan in state.split_plans.values() {
            ids.insert(plan.parent_shard_id);
            ids.insert(plan.child_shard_id);
        }
        Ok(ids)
    }

    /// Shard ids named by a `CELL_MANAGER_STATE` file: published routes and split plans.
    pub fn shard_ids_in_cell_state_file(path: &Path) -> Result<BTreeSet<ShardId>> {
        let mut raw = Vec::new();
        File::open(path)?.read_to_end(&mut raw)?;
        Self::shard_ids_in_cell_state_bytes(&raw)
    }

    fn load_state_file(dir: &Path) -> Result<PersistedCellState> {
        let state_path = dir.join("CELL_MANAGER_STATE");
        if !state_path.exists() {
            return Ok(PersistedCellState::default());
        }
        let mut raw = Vec::new();
        File::open(&state_path)?.read_to_end(&mut raw)?;
        let body = verify_crc32c_le(&raw, "CELL_MANAGER_STATE")?;
        decode_cell_state(body)
    }

    /// Resumes splits that fenced the parent (or deleted its right-half keys) without publishing
    /// child routes, adopts orphan child directories, and restores a parent range when a fence
    /// cannot be completed.
    async fn recover_incomplete_splits(&self) -> Result<()> {
        self.discard_incomplete_splitting_dirs()?;

        let plans: Vec<(u128, SplitPlanRecord)> = {
            let st = self.state.lock().unwrap();
            st.split_plans
                .iter()
                .filter(|(_, plan)| plan.state == SplitPlanState::Planned)
                .map(|(uuid, plan)| (*uuid, plan.clone()))
                .collect()
        };
        let mut resumed_children: HashSet<ShardId> = HashSet::new();
        for (uuid, plan) in plans {
            match self.resume_planned_split(uuid, &plan).await {
                Ok(()) => {}
                Err(MetaDbError::WriteIntentConflict { .. } | MetaDbError::SplitDeferred(_)) => {
                    // Transaction recovery starts after Cell opening. A plan that
                    // has not fenced its parent must leave its original route
                    // available so that recovery can drain it, then retry on the
                    // next maintenance pass. Storage/codec/consensus errors still
                    // abort opening instead of being mistaken for deferrals.
                }
                Err(error) => return Err(error),
            }
            resumed_children.insert(plan.child_shard_id);
        }

        self.resume_registry_pending(&resumed_children).await?;
        self.adopt_orphan_shard_dirs().await?;
        self.restore_unpublished_fences().await?;
        Ok(())
    }

    fn discard_incomplete_splitting_dirs(&self) -> Result<()> {
        let shards_root = self.host.data_dir().join("shards");
        if !shards_root.exists() {
            return Ok(());
        }
        for ent in fs::read_dir(&shards_root)? {
            let ent = ent?;
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".splitting") {
                let _ = fs::remove_dir_all(ent.path());
            }
        }
        Ok(())
    }

    async fn resume_planned_split(&self, request_uuid: u128, plan: &SplitPlanRecord) -> Result<()> {
        let already_published = {
            let st = self.state.lock().unwrap();
            st.routes.contains_key(&plan.child_shard_id)
        };
        if already_published {
            self.mark_plan_activated(request_uuid)?;
            return Ok(());
        }
        let Some(parent_route) = ({
            let st = self.state.lock().unwrap();
            st.routes.get(&plan.parent_shard_id).cloned()
        }) else {
            return Ok(());
        };
        if self.host.get_group(plan.parent_shard_id).is_none() {
            return Ok(());
        }
        let interior = plan.split_key.as_slice() > parent_route.range.start_key.as_slice()
            && plan.split_key.as_slice() < parent_route.range.end_key.as_slice();
        if !interior {
            self.drop_split_plan(request_uuid)?;
            return Ok(());
        }
        let current = self
            .host
            .get_group(plan.parent_shard_id)
            .map(|g| g.current_epoch().version)
            .unwrap_or(0);
        if current > plan.target_version {
            self.drop_split_plan(request_uuid)?;
            return Ok(());
        }
        self.finish_split(
            request_uuid,
            &parent_route,
            plan.child_shard_id,
            plan.split_key.clone(),
            plan.dual_mode_pid,
            plan.target_version,
        )
        .await
    }

    async fn resume_registry_pending(&self, already: &HashSet<ShardId>) -> Result<()> {
        let pending: Vec<(ShardId, RegistryShardState)> = self
            .host
            .registry_entries()
            .into_iter()
            .filter(|(id, state)| {
                !already.contains(id)
                    && matches!(state, RegistryShardState::PendingChildBoot { .. })
            })
            .collect();
        for (child_id, state) in pending {
            let RegistryShardState::PendingChildBoot {
                parent_shard_id,
                split_key,
                target_version,
                dual_mode_pid,
                ..
            } = state
            else {
                continue;
            };
            if self.state.lock().unwrap().routes.contains_key(&child_id) {
                continue;
            }
            let Some(parent_route) = ({
                let st = self.state.lock().unwrap();
                st.routes.get(&parent_shard_id).cloned()
            }) else {
                continue;
            };
            if self.host.get_group(parent_shard_id).is_none() {
                continue;
            }
            if split_key.is_empty() || target_version == 0 {
                continue;
            }
            if split_key.as_slice() <= parent_route.range.start_key.as_slice()
                || split_key.as_slice() >= parent_route.range.end_key.as_slice()
            {
                continue;
            }
            let current = self
                .host
                .get_group(parent_shard_id)
                .map(|g| g.current_epoch().version)
                .unwrap_or(0);
            if current > target_version {
                continue;
            }
            let req_uuid = (child_id as u128) << 64 | target_version as u128;
            self.finish_split(
                req_uuid,
                &parent_route,
                child_id,
                split_key,
                dual_mode_pid,
                target_version,
            )
            .await?;
        }
        Ok(())
    }

    async fn adopt_orphan_shard_dirs(&self) -> Result<()> {
        let known: HashSet<ShardId> = self.state.lock().unwrap().routes.keys().copied().collect();
        let mut orphans = Vec::new();
        let shards_root = self.host.data_dir().join("shards");
        if shards_root.exists() {
            for ent in fs::read_dir(&shards_root)? {
                let ent = ent?;
                if !ent.path().is_dir() {
                    continue;
                }
                let name = ent.file_name().to_string_lossy().to_string();
                if name.ends_with(".splitting") {
                    continue;
                }
                let Ok(id) = name.parse::<ShardId>() else {
                    continue;
                };
                if known.contains(&id) {
                    continue;
                }
                if ent.path().join("lsm").exists() {
                    orphans.push(id);
                }
            }
        }
        if orphans.is_empty() {
            return Ok(());
        }

        let parents: Vec<CompositeShardRoute> = self
            .state
            .lock()
            .unwrap()
            .routes
            .values()
            .cloned()
            .collect();
        for parent_route in parents {
            let Some(handler) = self
                .handlers
                .read()
                .unwrap()
                .get(&parent_route.shard_id)
                .cloned()
            else {
                continue;
            };
            let fenced = handler.key_range();
            if fenced.end_key.as_slice() >= parent_route.range.end_key.as_slice() {
                continue;
            }
            let reg = self.host.registry_entries();
            let child_id = reg
                .iter()
                .find_map(|(cid, st)| {
                    if let RegistryShardState::PendingChildBoot {
                        parent_shard_id, ..
                    } = st
                    {
                        if *parent_shard_id == parent_route.shard_id && orphans.contains(cid) {
                            return Some(*cid);
                        }
                    }
                    None
                })
                .or_else(|| {
                    if orphans.len() == 1 {
                        Some(orphans[0])
                    } else {
                        None
                    }
                });
            let Some(child_id) = child_id else {
                continue;
            };
            if self.host.get_group(parent_route.shard_id).is_none() {
                continue;
            }
            let req_uuid = (child_id as u128) << 64;
            let target_version = self
                .host
                .get_group(parent_route.shard_id)
                .map(|g| g.current_epoch().version)
                .unwrap_or(parent_route.epoch.version);
            self.finish_split(
                req_uuid,
                &parent_route,
                child_id,
                fenced.end_key.clone(),
                parent_route.dual_mode_pid,
                target_version,
            )
            .await?;
            orphans.retain(|id| *id != child_id);
        }
        Ok(())
    }

    /// Parent LSM/handler range was fenced but no child plan or directory exists.
    /// Publish a restore command so right-half keys that are still in the parent become readable.
    async fn restore_unpublished_fences(&self) -> Result<()> {
        // A child directory that is not in the route table may be the only copy of the
        // right-half keys. Widening the parent fence in that case would hide the loss.
        if self.has_unpublished_child_dir() {
            return Ok(());
        }
        let routes: Vec<CompositeShardRoute> = self
            .state
            .lock()
            .unwrap()
            .routes
            .values()
            .cloned()
            .collect();
        for route in routes {
            let Ok((epoch, group, handler)) = self.lookup_local_handle(route.shard_id) else {
                continue;
            };
            if handler.key_range() == route.range {
                continue;
            }
            let cmd = metadb_tx::TxRaftCmd::RestoreShardRange {
                restored_range: route.range.clone(),
                new_epoch: epoch,
            };
            let payload = metadb_tx::command_payload(&cmd)?;
            group
                .propose(epoch, self.host.clock().now(), payload)
                .await?;
            group.state_machine().flush_to_lsm()?;
        }
        Ok(())
    }

    fn has_unpublished_child_dir(&self) -> bool {
        let known: HashSet<ShardId> = self.state.lock().unwrap().routes.keys().copied().collect();
        let shards_root = self.host.data_dir().join("shards");
        let Ok(rd) = fs::read_dir(&shards_root) else {
            return false;
        };
        for ent in rd.flatten() {
            if !ent.path().is_dir() {
                continue;
            }
            let name = ent.file_name().to_string_lossy().to_string();
            if name.ends_with(".splitting") {
                continue;
            }
            if let Ok(id) = name.parse::<ShardId>() {
                if !known.contains(&id) && ent.path().join("lsm").exists() {
                    return true;
                }
            }
        }
        false
    }

    async fn finish_split(
        &self,
        request_uuid: u128,
        parent_route: &CompositeShardRoute,
        child_shard_id: ShardId,
        split_key: Vec<u8>,
        dual_mode_pid: Option<InodeId>,
        target_version: u64,
    ) -> Result<()> {
        let (_epoch, group, handler) = self.lookup_local_handle(parent_route.shard_id)?;
        let live_route = parent_route.clone();
        let (new_parent, new_child, _child_grp, child_handler) =
            RaftShardSplitWorker::execute_local_split(
                &self.host,
                &live_route,
                &group,
                &handler,
                child_shard_id,
                split_key,
                dual_mode_pid,
                self.lsm_config.clone(),
                self.raft_opts.clone(),
                target_version,
                None,
            )
            .await?;
        self.activate_split(request_uuid, new_parent, new_child, Some(child_handler))
    }

    fn drop_split_plan(&self, request_uuid: u128) -> Result<()> {
        let mut guard = self.state.lock().unwrap();
        if !guard.split_plans.contains_key(&request_uuid) {
            return Ok(());
        }
        let mut next = guard.clone();
        next.split_plans.remove(&request_uuid);
        self.persist_sync(&next)?;
        *guard = next;
        Ok(())
    }

    fn mark_plan_activated(&self, request_uuid: u128) -> Result<()> {
        let mut guard = self.state.lock().unwrap();
        let Some(plan) = guard.split_plans.get(&request_uuid) else {
            return Ok(());
        };
        if plan.state == SplitPlanState::Activated {
            return Ok(());
        }
        let mut next = guard.clone();
        if let Some(plan) = next.split_plans.get_mut(&request_uuid) {
            plan.state = SplitPlanState::Activated;
        }
        self.persist_sync(&next)?;
        *guard = next;
        Ok(())
    }

    fn persist_sync(&self, st: &PersistedCellState) -> Result<()> {
        if self.fail_persists.load(Ordering::Relaxed) > 0 {
            self.fail_persists.fetch_sub(1, Ordering::Relaxed);
            return Err(MetaDbError::StorageIoError(
                "injected persist_sync failure".to_string(),
            ));
        }
        Self::write_state_file(&self.state_dir, st)
    }

    fn write_state_file(dir: &Path, st: &PersistedCellState) -> Result<()> {
        let _shard_set = if dir
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("cell_"))
        {
            let root = dir.parent().unwrap_or_else(|| Path::new("."));
            Some(metadb_raft::backup::lock_be_shard_set(&root.join("be"))?)
        } else {
            None
        };
        let mut raw = codec::serialize(st).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        append_crc32c_le(&mut raw);
        let tmp = dir.join("CELL_MANAGER_STATE.tmp");
        let dst = dir.join("CELL_MANAGER_STATE");
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
        sync_dir(dir)?;
        Ok(())
    }

    /// The next `n` `CELL_CONTROL_PLANE` persists fail before the file is replaced.
    pub fn testing_fail_next_control_persists(&self, n: u32) {
        self.fail_control_persists.store(n, Ordering::Relaxed);
    }

    /// The next `n` control-plane writes rename the file and then fail `sync_dir`.
    pub fn testing_fail_next_control_dirsync(&self, n: u32) {
        self.fail_control_dirsync.store(n, Ordering::Relaxed);
    }

    /// Moves one stored heartbeat's observation time backward in memory only.
    /// The sequence is unchanged. Tests use this to open the remote rate window.
    pub fn testing_age_stored_heartbeat(&self, node_id: NodeId, earlier_by_ms: u64) {
        if let Some(heartbeat) = self.control.lock().unwrap().heartbeats.get_mut(&node_id) {
            heartbeat.observed_unix_ms = heartbeat.observed_unix_ms.saturating_sub(earlier_by_ms);
        }
    }

    pub fn stored_heartbeat(&self, node_id: NodeId) -> Option<StoredHeartbeat> {
        self.control
            .lock()
            .unwrap()
            .heartbeats
            .get(&node_id)
            .cloned()
    }

    pub fn stored_placement(&self, shard_id: ShardId) -> Option<StoredPlacement> {
        self.control
            .lock()
            .unwrap()
            .placements
            .get(&shard_id)
            .cloned()
    }

    pub fn local_heartbeat_is_fresh(&self, now_ms: u64, interval_ms: u64) -> bool {
        self.control
            .lock()
            .unwrap()
            .heartbeats
            .get(&self.host.node_id())
            .is_some_and(|heartbeat| {
                now_ms.saturating_sub(heartbeat.observed_unix_ms) < interval_ms
            })
    }

    /// gRPC addresses of Raft leaders of local shards when this process is not that leader.
    pub fn remote_leader_addrs(&self) -> Vec<String> {
        let me = self.host.node_id();
        let shard_ids: Vec<ShardId> = self.state.lock().unwrap().routes.keys().copied().collect();
        let mut addrs = BTreeSet::new();
        for shard_id in shard_ids {
            let Some(group) = self.host.get_group(shard_id) else {
                continue;
            };
            let Some(leader) = group.current_leader() else {
                continue;
            };
            if leader == me {
                continue;
            }
            if let Some(addr) = group.peer_addr(leader) {
                if !addr.is_empty() {
                    addrs.insert(addr);
                }
            }
        }
        addrs.into_iter().collect()
    }

    /// Records this process's own heartbeat. Shard descriptors from clients are not an input.
    /// Disk free space is left unknown; a zero is not stored as a measurement.
    pub fn publish_local_heartbeat(&self) -> Result<StoredHeartbeat> {
        let me = self.host.node_id();
        node_id_in_ha_range(me)?;
        let hosted = {
            let state = self.state.lock().unwrap();
            let mut ids: Vec<ShardId> = state
                .routes
                .iter()
                .filter(|(_, route)| route.peers.contains_key(&me))
                .map(|(id, _)| *id)
                .collect();
            ids.sort_unstable();
            ids
        };
        let hosted = normalize_ids(hosted).map_err(MetaDbError::ShardRoutingError)?;
        let mut guard = self.control.lock().unwrap();
        let mut next = guard.clone();
        if next.observation_generation == 0 {
            next.observation_generation = random_id()?;
        }
        let seq = next.next_local_seq.max(1);
        let write = HeartbeatWrite {
            node_id: me,
            seq,
            observed_unix_ms: unix_ms(),
            disk_free_bytes: None,
            reported_shard_ids: Vec::new(),
            local_hosted_shard_ids: hosted,
            observation_generation: next.observation_generation,
        };
        let decision = decide_heartbeat(guard.heartbeats.get(&me), &write)
            .map_err(MetaDbError::ShardRoutingError)?;
        match decision {
            HeartbeatDecision::Replay(existing) => {
                if guard.observation_generation == 0 {
                    self.store_control(&mut guard, next)?;
                }
                Ok(existing)
            }
            HeartbeatDecision::Store(heartbeat) => {
                next.heartbeats.insert(me, heartbeat.clone());
                next.next_local_seq = seq.saturating_add(1);
                self.store_control(&mut guard, next)?;
                Ok(heartbeat)
            }
        }
    }

    /// Persists a registered peer's liveness. `reported_shard_ids` are stored as an annotation
    /// and are not installed into `CELL_MANAGER_STATE` or `CompositeMetaCache`.
    pub fn accept_remote_heartbeat(
        &self,
        node_id: NodeId,
        seq: u64,
        disk_free_bytes: Option<u64>,
        reported_shard_ids: Vec<ShardId>,
        instance_id: Option<u128>,
        observation_generation: u128,
    ) -> Result<()> {
        node_id_in_ha_range(node_id)?;
        if node_id == self.host.node_id() {
            return Err(MetaDbError::ShardRoutingError(
                "refusing a remote heartbeat for the local node; this process writes its own heartbeat. Client shard descriptors are not applied to routes"
                    .to_string(),
            ));
        }
        {
            let state = self.state.lock().unwrap();
            if !state.nodes.contains_key(&node_id) {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "node {node_id} is not registered; client shard descriptors are not applied to routes"
                )));
            }
            if let Some(bound) = state.node_instances.get(&node_id).copied() {
                if instance_id != Some(bound) {
                    return Err(MetaDbError::ShardRoutingError(format!(
                        "heartbeat for node {node_id} does not carry the bound instance; registration is not a TransferLeader capability and this record is not a shard epoch"
                    )));
                }
            }
        }
        let reported = normalize_ids(reported_shard_ids).map_err(MetaDbError::ShardRoutingError)?;
        let write = HeartbeatWrite {
            node_id,
            seq,
            observed_unix_ms: unix_ms(),
            disk_free_bytes,
            reported_shard_ids: reported,
            local_hosted_shard_ids: Vec::new(),
            observation_generation,
        };
        let mut guard = self.control.lock().unwrap();
        let decision = decide_heartbeat(guard.heartbeats.get(&node_id), &write)
            .map_err(MetaDbError::ShardRoutingError)?;
        match decision {
            HeartbeatDecision::Replay(_) => Ok(()),
            HeartbeatDecision::Store(heartbeat) => {
                if let Some(previous) = guard.heartbeats.get(&node_id) {
                    if remote_state_change_is_too_soon(
                        previous.observed_unix_ms,
                        write.observed_unix_ms,
                    ) {
                        return Err(MetaDbError::ShardRoutingError(
                            "heartbeat rate limit; the stored record was kept, including a generation change. A heartbeat is not a scheduling epoch"
                                .to_string(),
                        ));
                    }
                }
                let mut next = guard.clone();
                next.heartbeats.insert(node_id, heartbeat);
                self.store_control(&mut guard, next)
            }
        }
    }

    /// Plans jumbo placement from live Raft voters and attempts only a safe move.
    ///
    /// A blocked plan is `Ok` with `PlacementPhase::Blocked` or `LeaderOnDedicated`.
    /// That is not eviction. `Err` is a failed fsync of this record or a voter-set
    /// persist after a committed membership change.
    pub async fn reconcile_jumbo_placement(
        &self,
        shard_id: ShardId,
        dominant_pid: InodeId,
        dedicated_node: NodeId,
    ) -> Result<StoredPlacement> {
        let _membership = self.membership_lock.lock().await;
        let previous = self.stored_placement(shard_id);
        let mut committed_moves = previous
            .as_ref()
            .map(|record| record.committed_moves.clone())
            .unwrap_or_default();
        let transfer_attempt = previous
            .as_ref()
            .map(|record| record.last_transfer_attempt_unix_ms)
            .unwrap_or(0);
        let mut replace_attempt = previous
            .as_ref()
            .map(|record| record.last_replace_attempt_unix_ms)
            .unwrap_or(0);

        for _ in 0..8 {
            let observation =
                match self.placement_observation(shard_id, dominant_pid, dedicated_node) {
                    Ok(observation) => observation,
                    Err(error) => {
                        return self.finish_placement(placement_record(
                            shard_id,
                            dominant_pid,
                            dedicated_node,
                            PlacementPhase::Blocked,
                            format!("{error}; no membership change was proposed"),
                            committed_moves,
                            transfer_attempt,
                            replace_attempt,
                        ));
                    }
                };
            for shard in &observation.shards {
                self.sync_route_voters_from_raft(shard.shard_id)?;
            }
            let decision = plan_jumbo_placement(&observation);
            let actions = executable_placement_actions(&decision.actions);
            if !automatic_leadership_transfer_allowed()
                && decision
                    .actions
                    .iter()
                    .any(|action| matches!(action, PlacementAction::TransferLeadership { .. }))
            {
                let reason =
                    append_reason(&decision.blocked_reason, AUTOMATIC_TRANSFER_DISABLED_REASON);
                return self.finish_placement(placement_record(
                    shard_id,
                    dominant_pid,
                    dedicated_node,
                    PlacementPhase::Blocked,
                    reason,
                    committed_moves,
                    transfer_attempt,
                    replace_attempt,
                ));
            }
            let Some(action) = actions.first().cloned() else {
                return self.finish_placement(placement_record(
                    shard_id,
                    dominant_pid,
                    dedicated_node,
                    decision.phase,
                    decision.blocked_reason,
                    committed_moves,
                    transfer_attempt,
                    replace_attempt,
                ));
            };
            if !action_runs_on(self.host.node_id(), &observation, &action) {
                return self.finish_placement(placement_record(
                    shard_id,
                    dominant_pid,
                    dedicated_node,
                    decision.phase,
                    decision.blocked_reason,
                    committed_moves,
                    transfer_attempt,
                    replace_attempt,
                ));
            }
            match action {
                PlacementAction::TransferLeadership { .. } => {
                    // Unreachable while automatic transfer is disabled. The filter
                    // above drops the variant. Do not call `trigger().transfer_leader`.
                    let reason =
                        append_reason(&decision.blocked_reason, AUTOMATIC_TRANSFER_DISABLED_REASON);
                    return self.finish_placement(placement_record(
                        shard_id,
                        dominant_pid,
                        dedicated_node,
                        PlacementPhase::Blocked,
                        reason,
                        committed_moves,
                        transfer_attempt,
                        replace_attempt,
                    ));
                }
                PlacementAction::ReplaceDedicatedOnPeer {
                    shard_id: target,
                    remove,
                    add,
                } => {
                    let now = unix_ms();
                    if replace_attempt != 0
                        && now.saturating_sub(replace_attempt) < PLACEMENT_RETRY_MS
                    {
                        let reason = append_reason(
                            &decision.blocked_reason,
                            "peer voter replacement is in cooldown and was not submitted again",
                        );
                        return self.finish_placement(placement_record(
                            shard_id,
                            dominant_pid,
                            dedicated_node,
                            decision.phase,
                            reason,
                            committed_moves,
                            transfer_attempt,
                            replace_attempt,
                        ));
                    }
                    replace_attempt = now;
                    self.save_placement(&placement_record(
                        shard_id,
                        dominant_pid,
                        dedicated_node,
                        decision.phase,
                        decision.blocked_reason.clone(),
                        committed_moves.clone(),
                        transfer_attempt,
                        replace_attempt,
                    ))?;
                    match self.replace_dedicated_voter(target, remove, add).await {
                        ReplaceOutcome::NotProposed => {}
                        ReplaceOutcome::Committed {
                            route_persist_error,
                        } => {
                            push_move(
                                &mut committed_moves,
                                CommittedMove::VoterReplaced {
                                    shard_id: target,
                                    removed: remove,
                                    added: add,
                                },
                            );
                            let reason = match &route_persist_error {
                                Some(error) => format!(
                                    "membership committed but the route record failed to persist: {error}; committed_moves keeps VoterReplaced"
                                ),
                                None => decision.blocked_reason.clone(),
                            };
                            self.save_placement(&placement_record(
                                shard_id,
                                dominant_pid,
                                dedicated_node,
                                decision.phase,
                                reason,
                                committed_moves.clone(),
                                transfer_attempt,
                                replace_attempt,
                            ))?;
                            if let Some(error) = route_persist_error {
                                return Err(error);
                            }
                        }
                        ReplaceOutcome::Rejected(error) => {
                            let reason = append_reason(
                                &decision.blocked_reason,
                                &format!(
                                    "peer voter replacement was not committed: {error}; no VoterReplaced was recorded"
                                ),
                            );
                            return self.finish_placement(placement_record(
                                shard_id,
                                dominant_pid,
                                dedicated_node,
                                PlacementPhase::Blocked,
                                reason,
                                committed_moves,
                                transfer_attempt,
                                replace_attempt,
                            ));
                        }
                    }
                }
            }
        }
        Err(MetaDbError::ConsensusError(
            "jumbo placement did not converge before the attempt limit".to_string(),
        ))
    }

    fn placement_observation(
        &self,
        shard_id: ShardId,
        dominant_pid: InodeId,
        dedicated_node: NodeId,
    ) -> Result<PlacementObservation> {
        let routes: Vec<CompositeShardRoute> = self
            .state
            .lock()
            .unwrap()
            .routes
            .values()
            .cloned()
            .collect();
        let mut shards = Vec::new();
        let mut proven = BTreeSet::new();
        let mut reserved = BTreeSet::new();
        for route in routes {
            if route.shard_id != shard_id {
                if let ShardHealthState::JumboIsolated {
                    dedicated_node: other,
                    ..
                } = route.health_state
                {
                    reserved.insert(other);
                }
            }
            let Some(group) = self.host.get_group(route.shard_id) else {
                continue;
            };
            let voters_map = group.uniform_voter_addrs()?;
            let voters = voters_map.keys().copied().collect();
            proven.extend(voters_map.keys().copied());
            shards.push(ShardVoters {
                shard_id: route.shard_id,
                voters,
                leader: group.current_leader(),
            });
        }
        Ok(PlacementObservation {
            jumbo_shard_id: shard_id,
            dominant_pid,
            dedicated_node,
            local_node: self.host.node_id(),
            shards,
            proven_voters: proven,
            reserved_dedicated_nodes: reserved,
        })
    }

    fn sync_route_voters_from_raft(&self, shard_id: ShardId) -> Result<()> {
        let (_, group, _) = self.lookup_local_handle(shard_id)?;
        let live = group.uniform_voter_addrs()?;
        let stored = {
            let state = self.state.lock().unwrap();
            state.routes.get(&shard_id).map(|route| {
                route
                    .peers
                    .iter()
                    .filter(|(_, (_, voter))| *voter)
                    .map(|(id, (addr, _))| (*id, addr.clone()))
                    .collect::<BTreeMap<_, _>>()
            })
        };
        if stored.as_ref() == Some(&live) {
            return Ok(());
        }
        self.persist_voter_set(shard_id, &live, group.current_epoch())
    }

    /// Does not call `trigger().transfer_leader`. An already-correct leader is not a
    /// committed move. A matching voter set that this call did not propose is
    /// [`ReplaceOutcome::NotProposed`].
    async fn replace_dedicated_voter(
        &self,
        shard_id: ShardId,
        remove: NodeId,
        add: NodeId,
    ) -> ReplaceOutcome {
        let (_, group, _) = match self.lookup_local_handle(shard_id) {
            Ok(handle) => handle,
            Err(error) => return ReplaceOutcome::Rejected(error),
        };
        if group.current_leader() != Some(self.host.node_id()) {
            return ReplaceOutcome::NotProposed;
        }
        let current = match group.uniform_voter_addrs() {
            Ok(voters) => voters,
            Err(error) => return ReplaceOutcome::Rejected(error),
        };
        let old: BTreeSet<NodeId> = current.keys().copied().collect();
        if !old.contains(&remove) {
            return ReplaceOutcome::NotProposed;
        }
        let mut proposed = old.clone();
        proposed.remove(&remove);
        proposed.insert(add);
        if let Err(refusal) = jumbo_membership_change_allowed(&old, &proposed) {
            return ReplaceOutcome::Rejected(MetaDbError::ConsensusError(format!(
                "refusing jumbo voter change on shard {shard_id}: {refusal:?}"
            )));
        }
        let proven = match self.node_is_proven_voter(add) {
            Ok(proven) => proven,
            Err(error) => return ReplaceOutcome::Rejected(error),
        };
        if !proven {
            return ReplaceOutcome::Rejected(MetaDbError::ConsensusError(format!(
                "refusing to add node {add} on shard {shard_id}; it is not already a voter of a local shard"
            )));
        }
        let proposed_change = if current.contains_key(&add) {
            ConfChangeOrchestrator::change_voters(group.raft(), proposed.clone()).await
        } else {
            match self.proven_voter_addr(add) {
                Ok(addr) => match group.prepare_learner_snapshot().await {
                    Ok(min_matched) => {
                        ConfChangeOrchestrator::add_learners_and_promote_voters(
                            group.raft(),
                            &[(add, addr)],
                            proposed.clone(),
                            64,
                            Duration::from_secs(20),
                            min_matched,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            }
        };
        if let Err(error) = proposed_change {
            return self.finish_replace_after_proposal(group.as_ref(), &proposed, error);
        }
        match group.wait_uniform_voter_addrs().await {
            Ok(committed) => self.committed_replace(shard_id, group.as_ref(), &proposed, committed),
            Err(error) => self.finish_replace_after_proposal(group.as_ref(), &proposed, error),
        }
    }

    fn finish_replace_after_proposal(
        &self,
        group: &metadb_raft::ShardRaftGroup,
        proposed: &BTreeSet<NodeId>,
        error: MetaDbError,
    ) -> ReplaceOutcome {
        match group.uniform_voter_addrs() {
            Ok(live) if live.keys().copied().collect::<BTreeSet<_>>() == *proposed => {
                self.committed_replace(group.shard_id(), group, proposed, live)
            }
            _ => ReplaceOutcome::Rejected(error),
        }
    }

    fn committed_replace(
        &self,
        shard_id: ShardId,
        group: &metadb_raft::ShardRaftGroup,
        proposed: &BTreeSet<NodeId>,
        committed: BTreeMap<NodeId, String>,
    ) -> ReplaceOutcome {
        let committed_ids: BTreeSet<NodeId> = committed.keys().copied().collect();
        if committed_ids != *proposed {
            return ReplaceOutcome::Rejected(MetaDbError::ConsensusError(format!(
                "shard {shard_id} voters {committed_ids:?} are not the planned set {proposed:?}; no VoterReplaced was recorded"
            )));
        }
        match self.persist_voter_set(shard_id, &committed, group.current_epoch()) {
            Ok(()) => ReplaceOutcome::Committed {
                route_persist_error: None,
            },
            Err(error) => ReplaceOutcome::Committed {
                route_persist_error: Some(error),
            },
        }
    }

    fn node_is_proven_voter(&self, node_id: NodeId) -> Result<bool> {
        let shard_ids: Vec<ShardId> = self.state.lock().unwrap().routes.keys().copied().collect();
        for shard_id in shard_ids {
            let Some(group) = self.host.get_group(shard_id) else {
                continue;
            };
            if group.uniform_voter_addrs()?.contains_key(&node_id) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn proven_voter_addr(&self, node_id: NodeId) -> Result<String> {
        let shard_ids: Vec<ShardId> = self.state.lock().unwrap().routes.keys().copied().collect();
        for shard_id in shard_ids {
            let Some(group) = self.host.get_group(shard_id) else {
                continue;
            };
            if let Some(addr) = group.uniform_voter_addrs()?.get(&node_id) {
                if !addr.is_empty() {
                    return Ok(addr.clone());
                }
            }
        }
        Err(MetaDbError::ConsensusError(format!(
            "node {node_id} has no raft address on a local shard"
        )))
    }

    fn finish_placement(&self, record: StoredPlacement) -> Result<StoredPlacement> {
        self.save_placement(&record)?;
        Ok(self.stored_placement(record.shard_id).unwrap_or(record))
    }

    fn save_placement(&self, record: &StoredPlacement) -> Result<()> {
        let mut guard = self.control.lock().unwrap();
        if guard
            .placements
            .get(&record.shard_id)
            .is_some_and(|current| current.same_outcome(record))
        {
            return Ok(());
        }
        let mut next = guard.clone();
        next.placements.insert(record.shard_id, record.clone());
        self.store_control(&mut guard, next)
    }

    /// Installs `next` when the new file is the visible directory entry, including
    /// when the directory fsync fails after rename. A later persist then continues
    /// from that state instead of writing the previous sequence back over it.
    fn store_control(
        &self,
        guard: &mut PersistedControlPlane,
        next: PersistedControlPlane,
    ) -> Result<()> {
        match self.persist_control(&next) {
            Ok(()) => {
                *guard = next;
                Ok(())
            }
            Err(ControlPersistError::NotVisible(error)) => Err(error),
            Err(ControlPersistError::VisibleDirSyncFailed(error)) => {
                *guard = next;
                Err(error)
            }
        }
    }

    fn persist_control(
        &self,
        state: &PersistedControlPlane,
    ) -> std::result::Result<(), ControlPersistError> {
        if state.version != CONTROL_PLANE_VERSION {
            return Err(ControlPersistError::NotVisible(MetaDbError::CodecError(
                format!(
                    "{CONTROL_PLANE_FILE} version {} is not supported",
                    state.version
                ),
            )));
        }
        if self.fail_control_persists.load(Ordering::Relaxed) > 0 {
            self.fail_control_persists.fetch_sub(1, Ordering::Relaxed);
            return Err(ControlPersistError::NotVisible(
                MetaDbError::StorageIoError(
                    "injected CELL_CONTROL_PLANE persist failure".to_string(),
                ),
            ));
        }
        self.write_control_file(&self.state_dir, state)
    }

    fn load_control_plane(dir: &Path) -> Result<PersistedControlPlane> {
        let path = dir.join(CONTROL_PLANE_FILE);
        if !path.exists() {
            return Ok(PersistedControlPlane::default());
        }
        let mut raw = Vec::new();
        File::open(&path)?.read_to_end(&mut raw)?;
        let body = verify_crc32c_le(&raw, CONTROL_PLANE_FILE)?;
        decode_control_plane(body)
    }

    fn write_control_file(
        &self,
        dir: &Path,
        state: &PersistedControlPlane,
    ) -> std::result::Result<(), ControlPersistError> {
        let _shard_set = if dir
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("cell_"))
        {
            let root = dir.parent().unwrap_or_else(|| Path::new("."));
            metadb_raft::backup::lock_be_shard_set(&root.join("be"))
                .map(Some)
                .map_err(ControlPersistError::NotVisible)?
        } else {
            None
        };
        let mut raw = codec::serialize(state).map_err(|error| {
            ControlPersistError::NotVisible(MetaDbError::CodecError(error.to_string()))
        })?;
        append_crc32c_le(&mut raw);
        let tmp = dir.join(format!("{CONTROL_PLANE_FILE}.tmp"));
        let dst = dir.join(CONTROL_PLANE_FILE);
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|error| ControlPersistError::NotVisible(error.into()))?;
            file.write_all(&raw)
                .map_err(|error| ControlPersistError::NotVisible(error.into()))?;
            file.sync_data()
                .map_err(|error| ControlPersistError::NotVisible(error.into()))?;
        }
        fs::rename(&tmp, &dst).map_err(|error| ControlPersistError::NotVisible(error.into()))?;
        if self.fail_control_dirsync.load(Ordering::Relaxed) > 0 {
            self.fail_control_dirsync.fetch_sub(1, Ordering::Relaxed);
            return Err(ControlPersistError::VisibleDirSyncFailed(
                MetaDbError::StorageIoError(
                    "injected CELL_CONTROL_PLANE directory sync failure after rename".to_string(),
                ),
            ));
        }
        sync_dir(dir).map_err(|error| {
            ControlPersistError::VisibleDirSyncFailed(MetaDbError::StorageIoError(format!(
                "CELL_CONTROL_PLANE directory sync failed after rename: {error}"
            )))
        })?;
        Ok(())
    }

    fn verify_or_initialize_owner(
        dir: &Path,
        node_id: NodeId,
        cell_id: CellId,
        state: &PersistedCellState,
        legacy: bool,
    ) -> Result<()> {
        let path = dir.join("CELL_OWNER");
        if path.try_exists()? {
            let raw = fs::read(path)?;
            let body = verify_crc32c_le(&raw, "CELL_OWNER")?;
            if body.len() != 16
                || body[..8] != node_id.to_le_bytes()
                || body[8..] != cell_id.to_le_bytes()
            {
                return Err(MetaDbError::ShardRoutingError(
                    "cell directory owner does not match the configured node and cell IDs"
                        .to_string(),
                ));
            }
            return Ok(());
        }
        // Legacy singleton routes identify the sole owner unambiguously. A
        // cluster-wide nodes map does not: it also contains registered peers.
        if legacy
            && state
                .routes
                .values()
                .any(|route| route.peers.len() != 1 || !route.peers.contains_key(&node_id))
        {
            return Err(MetaDbError::ShardRoutingError(
                "legacy replicated cell directory has no owner marker; migrate ownership offline before startup".to_string(),
            ));
        }
        let mut raw = node_id.to_le_bytes().to_vec();
        raw.extend_from_slice(&cell_id.to_le_bytes());
        append_crc32c_le(&mut raw);
        let tmp = dir.join("CELL_OWNER.tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(&raw)?;
        file.sync_data()?;
        drop(file);
        fs::rename(tmp, path)?;
        sync_dir(dir)?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
enum ControlPersistError {
    /// The previous file is still the visible record. Memory must stay unchanged.
    NotVisible(MetaDbError),
    /// `rename` has published the new file. Memory must keep that state so a later
    /// persist cannot write an older sequence over it. The directory fsync did not finish.
    VisibleDirSyncFailed(MetaDbError),
}

enum ReplaceOutcome {
    /// This call did not propose a voter change. An already-matching set is observation.
    NotProposed,
    /// Raft's uniform voter set matches the plan. `route_persist_error` is set when
    /// that fact did not reach `CELL_MANAGER_STATE`.
    Committed {
        route_persist_error: Option<MetaDbError>,
    },
    /// No matching voter set was observed. Do not record `VoterReplaced`.
    Rejected(MetaDbError),
}

#[allow(clippy::too_many_arguments)]
fn placement_record(
    shard_id: ShardId,
    dominant_pid: InodeId,
    dedicated_node: NodeId,
    phase: PlacementPhase,
    blocked_reason: String,
    committed_moves: Vec<CommittedMove>,
    last_transfer_attempt_unix_ms: u64,
    last_replace_attempt_unix_ms: u64,
) -> StoredPlacement {
    StoredPlacement {
        shard_id,
        dominant_pid,
        dedicated_node,
        phase,
        blocked_reason,
        committed_moves,
        last_transfer_attempt_unix_ms,
        last_replace_attempt_unix_ms,
        updated_unix_ms: unix_ms(),
    }
}

fn append_reason(existing: &str, extra: &str) -> String {
    if existing.is_empty() {
        extra.to_string()
    } else {
        format!("{existing}; {extra}")
    }
}

fn action_runs_on(
    local: NodeId,
    observation: &PlacementObservation,
    action: &PlacementAction,
) -> bool {
    let shard_id = match action {
        PlacementAction::TransferLeadership { shard_id, .. }
        | PlacementAction::ReplaceDedicatedOnPeer { shard_id, .. } => *shard_id,
    };
    observation
        .shards
        .iter()
        .any(|shard| shard.shard_id == shard_id && shard.leader == Some(local))
}

fn push_move(moves: &mut Vec<CommittedMove>, movement: CommittedMove) {
    if !moves.contains(&movement) {
        moves.push(movement);
    }
}

/// `SplitPlanRecord` gained `dual_mode_pid` after the first on-disk layout.
/// Old `CELL_MANAGER_STATE` files still decode; missing mode is `None`.
#[derive(Debug, Deserialize, wincode::SchemaRead)]
struct LegacySplitPlanRecord {
    parent_shard_id: ShardId,
    child_shard_id: ShardId,
    split_key: Vec<u8>,
    target_version: u64,
    state: SplitPlanState,
}

#[derive(Debug, Deserialize, wincode::SchemaRead)]
struct LegacyPersistedCellState {
    nodes: BTreeMap<NodeId, String>,
    routes: BTreeMap<ShardId, CompositeShardRoute>,
    split_plans: BTreeMap<u128, LegacySplitPlanRecord>,
}

fn decode_cell_state(body: &[u8]) -> Result<PersistedCellState> {
    if let Ok(state) = codec::deserialize::<PersistedCellState>(body) {
        return Ok(state);
    }
    if let Ok(prior) = codec::deserialize::<CellStateBeforeNodeInstances>(body) {
        return Ok(PersistedCellState {
            nodes: prior.nodes,
            routes: prior.routes,
            split_plans: prior.split_plans,
            joined_existing: prior.joined_existing,
            join_pending: prior.join_pending,
            node_instances: BTreeMap::new(),
        });
    }
    if let Ok(prior) = codec::deserialize::<CellStateJoinedOnly>(body) {
        return Ok(PersistedCellState {
            nodes: prior.nodes,
            routes: prior.routes,
            split_plans: prior.split_plans,
            joined_existing: prior.joined_existing,
            join_pending: false,
            node_instances: BTreeMap::new(),
        });
    }
    if let Ok(prior) = codec::deserialize::<CellStateBeforeJoinFlag>(body) {
        return Ok(PersistedCellState {
            nodes: prior.nodes,
            routes: prior.routes,
            split_plans: prior.split_plans,
            joined_existing: false,
            join_pending: false,
            node_instances: BTreeMap::new(),
        });
    }
    let legacy: LegacyPersistedCellState = codec::deserialize(body)
        .map_err(|e| MetaDbError::CodecError(format!("CELL_MANAGER_STATE: {e}")))?;
    Ok(PersistedCellState {
        nodes: legacy.nodes,
        routes: legacy.routes,
        split_plans: legacy
            .split_plans
            .into_iter()
            .map(|(uuid, plan)| {
                (
                    uuid,
                    SplitPlanRecord {
                        parent_shard_id: plan.parent_shard_id,
                        child_shard_id: plan.child_shard_id,
                        split_key: plan.split_key,
                        target_version: plan.target_version,
                        state: plan.state,
                        dual_mode_pid: None,
                    },
                )
            })
            .collect(),
        joined_existing: false,
        join_pending: false,
        node_instances: BTreeMap::new(),
    })
}

/// Voter target for a join. `our_id` is included once even when some shards already list it.
pub fn join_scale_target(reported_voters: &BTreeSet<NodeId>, our_id: NodeId) -> usize {
    let mut voters = reported_voters.clone();
    voters.insert(our_id);
    voters.len()
}

/// Plans the voter set for one shard.
///
/// When `ensure_node` is set, that node is included even if `target_voters` is
/// already satisfied by the current set. Count-only changes still grow from the
/// lowest registered ids and shrink from the highest.
pub fn plan_membership_voters(
    current: &BTreeSet<NodeId>,
    registered: &BTreeSet<NodeId>,
    target_voters: usize,
    ensure_node: Option<NodeId>,
) -> Result<BTreeSet<NodeId>> {
    if let Some(node) = ensure_node {
        if node == 0 {
            return Err(MetaDbError::IdentityRejected(
                "refusing to ensure node id 0".to_string(),
            ));
        }
        if !current.contains(&node) && !registered.contains(&node) {
            return Err(MetaDbError::ConsensusError(format!(
                "node {node} is not registered"
            )));
        }
        let mut final_voters = current.clone();
        final_voters.insert(node);
        let goal = target_voters.max(final_voters.len());
        for nid in registered {
            if final_voters.len() >= goal {
                break;
            }
            final_voters.insert(*nid);
        }
        if target_voters > final_voters.len() {
            return Err(MetaDbError::ConsensusError(format!(
                "need {target_voters} voters, can form {}",
                final_voters.len()
            )));
        }
        return Ok(final_voters);
    }

    if target_voters == 0 {
        return Err(MetaDbError::ConsensusError(
            "target_voters must be at least 1".to_string(),
        ));
    }
    let mut final_voters = current.clone();
    if final_voters.len() > target_voters {
        while final_voters.len() > target_voters {
            let Some(drop_id) = final_voters.iter().next_back().copied() else {
                break;
            };
            final_voters.remove(&drop_id);
        }
    } else {
        for nid in registered {
            if final_voters.len() >= target_voters {
                break;
            }
            final_voters.insert(*nid);
        }
        if final_voters.len() < target_voters {
            return Err(MetaDbError::ConsensusError(format!(
                "need {target_voters} voters, can form {}",
                final_voters.len()
            )));
        }
    }
    if final_voters.is_empty() {
        return Err(MetaDbError::ConsensusError(
            "refusing to remove every voter".to_string(),
        ));
    }
    Ok(final_voters)
}
