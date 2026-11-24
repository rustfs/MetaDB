//! Persistent `DurableCellManager` managing OpenRaft shard groups, 3-stage recoverable
//! `ExecuteSplit` (`SplitPlanRecord`), Jumbo Shard dedicated-node isolation, and `DualModeFineGrained`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
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

use crate::cell_directory::{encode_directory_body, shards_covering, union_route_range};
use crate::control_plane_state::{
    decide_heartbeat, decode_control_plane, normalize_ids, remote_state_change_is_too_soon,
    unix_ms, HeartbeatDecision, HeartbeatWrite, PersistedControlPlane, StoredHeartbeat,
    StoredPlacement, CONTROL_PLANE_FILE, CONTROL_PLANE_VERSION, PLACEMENT_RETRY_MS,
};
use crate::heartbeat_scheduler::{
    capture_mono_ns, HeartbeatScheduler, LivenessDecision, MeasuredSample, MeasurementReject,
    ScheduleNow,
};
use crate::id_allocator::{AllocIdKind, DurableIdAllocator};
use crate::identity::{node_id_in_ha_range, random_id, ClusterIdentity};
use crate::jumbo::JumboIsolationPlanner;
use crate::jumbo_files::{
    copy_shard_tree, load_live_dir, refuse_dedicated_root, remove_tree, shard_move_dir,
    visible_move_phase, write_move_phase, MovePhaseError, MOVE_LIVE, MOVE_STAGED,
};
use crate::jumbo_placement::{
    automatic_leadership_transfer_allowed, executable_placement_actions,
    jumbo_membership_change_allowed, plan_jumbo_placement, CommittedMove, PlacementAction,
    PlacementObservation, PlacementPhase, ShardVoters, SpareDrainPlan, SpareDrainRequest,
    AUTOMATIC_TRANSFER_DISABLED_REASON,
};
use crate::jumbo_remote::{
    append_handoff, append_spare_drain, destination_live_cut, drain_record_for,
    handoff_blocks_local_open, load_handoffs, load_spare_drains, outcome_from_drain,
    push_jumbo_shard, refuse_non_loopback, DestinationCut, PushResult, RemoteHandoffRecord,
    RemoteMoveOutcome, SpareDrainRecord,
};
use crate::leadership_transfer::{
    run_automatic_leadership_transfer, LeadershipTransferAttempt, LeadershipTransferInput,
    LeadershipTransferOutcome, TransferObservation,
};
use crate::meta_cache::{CompositeMetaCache, CompositeShardRoute};
use crate::replicated_control::validate_replicated_record;
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

/// Membership read from an exact `CELL_MANAGER_STATE` body.
///
/// Legacy layouts are not accepted here. A replicated snapshot is always the
/// current layout; a body that only decodes as an older layout is corrupt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManagerSnapshot {
    pub nodes: BTreeMap<NodeId, String>,
    pub voters: BTreeMap<ShardId, BTreeMap<NodeId, String>>,
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
    /// Measured recv and round-trip samples. Not reloaded from `CELL_CONTROL_PLANE`.
    liveness: Mutex<HeartbeatScheduler>,
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
    /// Test hook: the next control-plane propose reports that this process is
    /// not the leader and writes nothing.
    control_propose_not_leader: AtomicBool,
    /// Times the leadership gate entered its callback. Not an OpenRaft call.
    leadership_transfer_requests: AtomicU64,
    /// Opt-in flag enabling automatic leadership transfer after `ProbeTransferLeader` verification.
    /// Defaults to `AUTOMATIC_LEADERSHIP_TRANSFER_ENABLED` (`false`).
    allow_automatic_transfer: AtomicBool,
    /// Optional operator-supplied child-name split boundary hints for `DualModeFineGrained` shards.
    dual_mode_split_hints: Mutex<HashMap<ShardId, String>>,
}

struct LeadershipGroupFacts {
    open: bool,
    leader: Option<NodeId>,
    voters: BTreeSet<NodeId>,
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
            liveness: Mutex::new(HeartbeatScheduler::default()),
            fail_control_persists: AtomicU32::new(0),
            fail_control_dirsync: AtomicU32::new(0),
            jumbo_dedicated_root: Mutex::new(jumbo_dedicated_root),
            jumbo_file_move_failed: Mutex::new(HashSet::new()),
            stop_before_source_removal: AtomicBool::new(false),
            control_propose_not_leader: AtomicBool::new(false),
            leadership_transfer_requests: AtomicU64::new(0),
            allow_automatic_transfer: AtomicBool::new(automatic_leadership_transfer_allowed()),
            dual_mode_split_hints: Mutex::new(HashMap::new()),
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
        if let Err(error) = mgr.adopt_instance_binding_from_snapshot() {
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
        if let Err(error) = mgr.reconcile_replicated_membership() {
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
            liveness: Mutex::new(HeartbeatScheduler::default()),
            fail_control_persists: AtomicU32::new(0),
            fail_control_dirsync: AtomicU32::new(0),
            jumbo_dedicated_root: Mutex::new(None),
            jumbo_file_move_failed: Mutex::new(HashSet::new()),
            stop_before_source_removal: AtomicBool::new(false),
            control_propose_not_leader: AtomicBool::new(false),
            leadership_transfer_requests: AtomicU64::new(0),
            allow_automatic_transfer: AtomicBool::new(automatic_leadership_transfer_allowed()),
            dual_mode_split_hints: Mutex::new(HashMap::new()),
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

    /// Keeps a committed manager snapshot when its instance binding was not on
    /// this shard's log.
    ///
    /// A jumbo handoff copies `CELL_MANAGER_STATE` onto another shard. That
    /// copy does not include the `0x09` binding row. Republishing the local
    /// file would replace the committed manager body. The snapshot's instance
    /// is enough to skip that publish.
    fn adopt_instance_binding_from_snapshot(&self) -> Result<()> {
        let Some(identity) = *self.identity.lock().unwrap() else {
            return Ok(());
        };
        if self.bound_instance(identity.node_id) == Some(identity.node_instance_id) {
            return Ok(());
        }
        let Some(record) = self.replicated_control()? else {
            return Ok(());
        };
        let Ok(state) = decode_cell_state(&record.manager_body) else {
            return Ok(());
        };
        if state.node_instances.get(&identity.node_id) != Some(&identity.node_instance_id) {
            return Ok(());
        }
        let address = state
            .nodes
            .get(&identity.node_id)
            .cloned()
            .unwrap_or_default();
        self.instance_bindings
            .lock()
            .unwrap()
            .insert(identity.node_id, (identity.node_instance_id, address));
        Ok(())
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
            handler.set_control_plane_validator(std::sync::Arc::new(validate_replicated_record));
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

    /// Starts an uninitialized replica group for a split child shard or spare-voter onboarding
    /// before the current leader streams a Raft snapshot and promotes this node to voter.
    pub async fn open_joining_shard(
        &self,
        shard_id: ShardId,
        range: CompositeKeyRange,
        initial_epoch: ShardEpoch,
        use_jumbo_dedicated_root: bool,
    ) -> Result<()> {
        node_id_in_ha_range(self.host.node_id())?;
        if handoff_blocks_local_open(&self.state_dir, shard_id)? {
            return Err(MetaDbError::ShardRoutingError(format!(
                "shard {shard_id} has a recorded remote handoff on node {}; refusing to open a joining replica",
                self.host.node_id()
            )));
        }
        if self.host.get_group(shard_id).is_some() {
            return Ok(());
        }
        let sdir = if use_jumbo_dedicated_root {
            if let Some(root) = self.jumbo_dedicated_root.lock().unwrap().clone() {
                shard_move_dir(&root, shard_id)
            } else {
                self.host.shard_dir(shard_id)
            }
        } else {
            self.host.shard_dir(shard_id)
        };
        let wm = Arc::new(ShardWatermarkTracker::default());
        let handler = Arc::new(TxStateMachineHandler::new(
            shard_id,
            initial_epoch,
            range,
            wm.clone(),
        ));
        handler.set_instance_bindings(self.instance_bindings.clone());
        handler.set_control_plane_validator(Arc::new(validate_replicated_record));
        let group = ShardRaftGroup::start(
            shard_id,
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
        self.host.record_registry_state(
            shard_id,
            RegistryShardState::Active {
                peers: BTreeMap::new(),
            },
        )?;
        self.handlers.write().unwrap().insert(shard_id, handler);
        Ok(())
    }

    /// Stops a drained local shard replica (and deletes its shard files when `delete_files == true`),
    /// refusing if this node is still an applied voter of `shard_id`.
    pub async fn close_removed_shard(&self, shard_id: ShardId, delete_files: bool) -> Result<()> {
        let _membership = self.membership_lock.lock().await;
        if let Some(group) = self.host.get_group(shard_id) {
            if group
                .applied_voter_addrs()?
                .contains_key(&self.host.node_id())
            {
                return Err(MetaDbError::ConsensusError(format!(
                    "refusing to close shard {shard_id}: node {} is still an applied voter",
                    self.host.node_id()
                )));
            }
        }
        self.handlers.write().unwrap().remove(&shard_id);
        if delete_files {
            self.host.remove_and_gc_shard(shard_id).await?;
        } else if let Some(group) = self.host.take_running_group(shard_id) {
            group.shutdown().await?;
            self.host.router().remove_group(shard_id);
            self.host.router().remove_local_proposer(shard_id);
        }
        Ok(())
    }

    /// Returns an `Arc<dyn LocalShardControlHook>` backed by a `Weak<DurableCellManager>` for
    /// in-process multi-voter tests where `ControlPlaneServiceServer` is not mounted.
    pub fn as_local_control_hook(self: &Arc<Self>) -> Arc<dyn metadb_raft::LocalShardControlHook> {
        Arc::new(CellLocalControlHook {
            cell: Arc::downgrade(self),
        })
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
                if handoff_blocks_local_open(&self.state_dir, route.shard_id)? {
                    // The directory was handed to another listener after MOVE_LIVE.
                    // Do not create an empty Raft group in its place. The route row
                    // stays as it was. This process does not forward the shard.
                    self.meta_cache.upsert_route(route);
                    continue;
                }
                handler.set_instance_bindings(self.instance_bindings.clone());
                handler
                    .set_control_plane_validator(std::sync::Arc::new(validate_replicated_record));
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

    /// Places this process's jumbo shard directory on a loopback listener.
    ///
    /// `MOVE_STAGED` then `MOVE_LIVE` run on the destination disk. The destination
    /// does not serve placed-file reads until the live directory fsync succeeds.
    /// The source directory is removed only after that cut, the spare-drain record,
    /// and the handoff record are durable. A failure before `MOVE_LIVE` is visible
    /// reopens the source and does not record the spare drain. `transfer_leader`
    /// is not called. Raft is not opened on the destination. `CELL_CONTROL_PLANE`
    /// rows are not deleted. The maintenance loop does not call this method.
    pub async fn relocate_jumbo_shard_to_remote(
        &self,
        shard_id: ShardId,
        dest_node: NodeId,
        dest_addr: SocketAddr,
        drain: &SpareDrainRequest,
    ) -> Result<RemoteMoveOutcome> {
        let _lock = self.membership_lock.lock().await;
        refuse_non_loopback(dest_addr)?;
        node_id_in_ha_range(dest_node)?;
        if drain.dedicated_node != dest_node {
            return Err(MetaDbError::ShardRoutingError(format!(
                "spare drain names dedicated node {} but the destination is {dest_node}; no files were copied and transfer_leader was not called",
                drain.dedicated_node
            )));
        }
        let plan = drain.plan().map_err(|error| {
            MetaDbError::ConsensusError(format!("{error}; no files were copied"))
        })?;
        if let Some(existing) = load_handoffs(&self.state_dir)?
            .into_iter()
            .find(|record| record.shard_id == shard_id)
        {
            if existing.dest_node != dest_node {
                return Err(MetaDbError::StorageIoError(format!(
                    "jumbo shard {shard_id} is already handed off to node {}; the source was not sent to {dest_node}",
                    existing.dest_node
                )));
            }
            return self
                .finish_if_live_dirsync_succeeded(shard_id, dest_node, dest_addr, &plan)
                .await;
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
                if dedicated_node == dest_node && dedicated_node != self.host.node_id() => {}
            ShardHealthState::JumboIsolated { dedicated_node, .. }
                if dedicated_node == self.host.node_id() =>
            {
                return Err(MetaDbError::ShardRoutingError(
                    "dedicated node is this process; the local dedicated-disk move was not replaced and transfer_leader was not called"
                        .to_string(),
                ));
            }
            ShardHealthState::JumboIsolated { dedicated_node, .. } => {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "dedicated node {dedicated_node} is not destination {dest_node}; files were not moved and transfer_leader was not called"
                )));
            }
            ShardHealthState::Normal => {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "shard {shard_id} is not jumbo-isolated; a route flag was not treated as a remote file move"
                )));
            }
        }
        let source = self.host.shard_dir(shard_id);
        if !source.exists() {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo shard {shard_id} directory {} is missing; nothing was published",
                source.display()
            )));
        }
        if self.host.get_group(shard_id).is_none() {
            return self
                .finish_if_live_dirsync_succeeded(shard_id, dest_node, dest_addr, &plan)
                .await;
        }
        self.preserve_control_snapshot_before_handoff(shard_id)
            .await?;
        let group = self.host.get_group(shard_id).ok_or_else(|| {
            MetaDbError::ShardRoutingError(format!(
                "shard {shard_id} is not running; files were not moved"
            ))
        })?;
        group.state_machine().flush_to_lsm().map_err(|error| {
            MetaDbError::StorageIoError(format!(
                "jumbo shard {shard_id} flush failed before the remote move: {error}"
            ))
        })?;
        let shutdown_result = group.shutdown().await;
        drop(group);
        self.release_stopped_shard(shard_id);
        if let Err(error) = shutdown_result {
            let _ = self.reopen_source_group(&route, &source).await;
            return Err(error);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        match push_jumbo_shard(
            &source,
            shard_id,
            self.host.node_id(),
            dest_node,
            dest_addr,
        )
        .await
        {
            Ok(PushResult::LiveCut) => {
                self.finish_remote_after_live(shard_id, dest_node, &plan)
                    .await
            }
            Err(failure) if failure.live_visible => Err(MetaDbError::StorageIoError(format!(
                "{}; the source directory was not reopened and was not deleted; the spare voter was not replaced; transfer_leader was not called",
                failure.message
            ))),
            Err(failure) => {
                if let Err(reopen) = self.reopen_source_group(&route, &source).await {
                    return Err(MetaDbError::StorageIoError(format!(
                        "{}; the source copy is still at {} but reopening it failed: {reopen}; the spare voter was not replaced; transfer_leader was not called",
                        failure.message,
                        source.display()
                    )));
                }
                Err(MetaDbError::StorageIoError(format!(
                    "{}; the source copy was reopened; the spare voter was not replaced; transfer_leader was not called",
                    failure.message
                )))
            }
        }
    }

    fn release_stopped_shard(&self, shard_id: ShardId) {
        drop(self.host.take_running_group(shard_id));
        self.host.router().remove_group(shard_id);
        self.host.router().remove_local_proposer(shard_id);
    }

    async fn reopen_source_group(&self, route: &CompositeShardRoute, source: &Path) -> Result<()> {
        let mut last = None;
        for _ in 0..20 {
            match self.open_shard_group(route, source).await {
                Ok(()) => return Ok(()),
                Err(error) if error.to_string().contains("would block") => {
                    last = Some(error);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last.unwrap_or_else(|| {
            MetaDbError::StorageIoError(
                "reopening the jumbo source did not finish; the directory was not deleted"
                    .to_string(),
            )
        }))
    }

    async fn finish_if_live_dirsync_succeeded(
        &self,
        shard_id: ShardId,
        dest_node: NodeId,
        dest_addr: SocketAddr,
        plan: &SpareDrainPlan,
    ) -> Result<RemoteMoveOutcome> {
        match destination_live_cut(shard_id, self.host.node_id(), dest_node, dest_addr).await? {
            DestinationCut::Durable => {
                self.finish_remote_after_live(shard_id, dest_node, plan)
                    .await
            }
            DestinationCut::VisibleNotDurable => {
                match push_jumbo_shard(
                    &self.host.shard_dir(shard_id),
                    shard_id,
                    self.host.node_id(),
                    dest_node,
                    dest_addr,
                )
                .await
                {
                    Ok(PushResult::LiveCut) => {
                        self.finish_remote_after_live(shard_id, dest_node, plan)
                            .await
                    }
                    Err(failure) => Err(MetaDbError::StorageIoError(format!(
                        "{}; the source directory was not deleted; the spare voter was not replaced; transfer_leader was not called",
                        failure.message
                    ))),
                }
            }
            DestinationCut::Absent => Err(MetaDbError::ShardRoutingError(format!(
                "shard {shard_id} is not running and the destination directory fsync has not succeeded; files were not moved and the source was not deleted"
            ))),
        }
    }

    async fn finish_remote_after_live(
        &self,
        shard_id: ShardId,
        dest_node: NodeId,
        plan: &SpareDrainPlan,
    ) -> Result<RemoteMoveOutcome> {
        // The source group may already be closed. Copy both snapshots before
        // the directory is removed. A non-leader target or a different manager
        // body leaves the source in place.
        self.preserve_control_snapshot_before_handoff(shard_id)
            .await?;
        let raft_membership_changed = self.try_raft_spare_drain(plan).await?;
        append_spare_drain(
            &self.state_dir,
            drain_record_for(shard_id, plan, raft_membership_changed),
        )?;
        append_handoff(
            &self.state_dir,
            RemoteHandoffRecord {
                shard_id,
                dest_node,
                destination_raft_opened: false,
                transfer_leader_called: false,
            },
        )?;
        let source = self.host.shard_dir(shard_id);
        if source.exists() {
            remove_tree(&source).map_err(|error| {
                MetaDbError::StorageIoError(format!(
                    "jumbo shard {shard_id} is published on the destination but the old directory could not be removed: {error}; it will not be opened on restart"
                ))
            })?;
        }
        let stored = load_spare_drains(&self.state_dir)?;
        let record = stored
            .iter()
            .find(|stored| {
                stored.jumbo_shard_id == shard_id
                    && stored.drain_shard_id == plan.shard_id
                    && stored.removed == plan.remove
                    && stored.added == plan.add
            })
            .cloned()
            .ok_or_else(|| {
                MetaDbError::StorageIoError(
                    "spare drain record was not readable after it was written".to_string(),
                )
            })?;
        Ok(outcome_from_drain(&record))
    }

    /// Changes Raft only when this process is already the leader and the live
    /// voter set equals `voters_before`. A closed shard, a different live set,
    /// or a set that already equals `voters_after` does not set
    /// `raft_membership_changed`.
    async fn try_raft_spare_drain(&self, plan: &SpareDrainPlan) -> Result<bool> {
        let Some(group) = self.host.get_group(plan.shard_id) else {
            return Ok(false);
        };
        if group.current_leader() != Some(self.host.node_id()) {
            return Ok(false);
        }
        let live: BTreeSet<NodeId> = group.uniform_voter_addrs()?.into_keys().collect();
        if live != plan.voters_before {
            return Ok(false);
        }
        match self
            .replace_dedicated_voter(plan.shard_id, plan.remove, plan.add)
            .await
        {
            ReplaceOutcome::Committed {
                route_persist_error: None,
            } => Ok(true),
            ReplaceOutcome::Committed {
                route_persist_error: Some(error),
            } => Err(MetaDbError::StorageIoError(format!(
                "spare replacement reached the planned voter set but the route record failed: {error}; the source copy was kept; transfer_leader was not called"
            ))),
            ReplaceOutcome::NotProposed => Ok(false),
            ReplaceOutcome::Rejected(error) => Err(MetaDbError::ConsensusError(format!(
                "spare voter replacement was not committed: {error}; the source copy was kept; transfer_leader was not called"
            ))),
        }
    }

    pub fn stored_spare_drains(&self) -> Result<Vec<SpareDrainRecord>> {
        load_spare_drains(&self.state_dir)
    }

    pub fn stored_remote_handoffs(&self) -> Result<Vec<RemoteHandoffRecord>> {
        load_handoffs(&self.state_dir)
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
        handler.set_control_plane_validator(std::sync::Arc::new(validate_replicated_record));
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
        self.testing_mark_jumbo(shard_id, self.host.node_id(), dominant_pid)
    }

    /// Test hook. Persists `JumboIsolated` for `dedicated_node` without moving files.
    pub fn testing_mark_jumbo(
        &self,
        shard_id: ShardId,
        dedicated_node: NodeId,
        dominant_pid: InodeId,
    ) -> Result<()> {
        node_id_in_ha_range(dedicated_node)?;
        let mut state = self.state.lock().unwrap();
        let mut next = state.clone();
        let route = next.routes.get_mut(&shard_id).ok_or_else(|| {
            MetaDbError::ShardRoutingError(format!("shard {shard_id} has no route"))
        })?;
        route.health_state = ShardHealthState::JumboIsolated {
            dedicated_node,
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
            self.register_address_only(node_id, grpc_addr)?;
            return self.publish_manager_snapshot().await;
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
        self.mirror_bindings_to_state()?;
        self.publish_manager_snapshot().await
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
        let _ = self.reconcile_follower_split_routes();
        let mut routes: Vec<CompositeShardRoute> = {
            let guard = self.state.lock().unwrap();
            guard.routes.values().cloned().collect()
        };
        self.overlay_live_membership(&mut routes);
        routes
    }

    /// Reconciles split child routes onto followers (or newly elected leaders after split failover)
    /// from the replicated `CELL_MANAGER_STATE` snapshot or contiguous local handler ranges.
    fn reconcile_follower_split_routes(&self) -> Result<()> {
        let has_planned_split = self
            .state
            .lock()
            .unwrap()
            .split_plans
            .values()
            .any(|plan| plan.state == SplitPlanState::Planned);
        if has_planned_split {
            return Ok(());
        }

        if let Ok(Some(record)) = self.replicated_control() {
            if let Ok(rep_state) = wincode::config::deserialize_exact::<PersistedCellState, _>(
                &record.manager_body,
                wincode::config::DefaultConfig::default(),
            ) {
                let mut guard = self.state.lock().unwrap();
                let missing_running_child = rep_state.routes.keys().any(|sid| {
                    !guard.routes.contains_key(sid) && self.host.get_group(*sid).is_some()
                });
                if missing_running_child {
                    let mut next = guard.clone();
                    let mut upserts = Vec::new();
                    for (&sid, rep_route) in &rep_state.routes {
                        let accept = match next.routes.get(&sid) {
                            None => self.host.get_group(sid).is_some(),
                            Some(cur) => rep_route.epoch.version > cur.epoch.version,
                        };
                        if accept {
                            next.routes.insert(sid, rep_route.clone());
                            upserts.push(rep_route.clone());
                        }
                    }
                    if !upserts.is_empty() {
                        let _ = self.persist_sync(&next);
                        *guard = next;
                        drop(guard);
                        for r in upserts {
                            self.meta_cache.upsert_route(r);
                        }
                        return Ok(());
                    }
                }
            }
        }

        // Fallback when the split leader crashed after `UpdateShardRangeOnSplit` committed on Raft
        // and before `PublishControlPlane` ran: pair narrowed parent handler with running child handler.
        let orphan_handlers: Vec<(ShardId, Arc<TxStateMachineHandler>)> = {
            let known: HashSet<ShardId> =
                self.state.lock().unwrap().routes.keys().copied().collect();
            let handlers = self.handlers.read().unwrap();
            handlers
                .iter()
                .filter(|(&sid, _)| !known.contains(&sid) && self.host.get_group(sid).is_some())
                .map(|(&sid, h)| (sid, h.clone()))
                .collect()
        };
        if orphan_handlers.is_empty() {
            return Ok(());
        }
        let parent_routes: Vec<CompositeShardRoute> = self
            .state
            .lock()
            .unwrap()
            .routes
            .values()
            .cloned()
            .collect();
        for parent_route in parent_routes {
            let Ok((p_epoch, p_group, p_handler)) = self.lookup_local_handle(parent_route.shard_id)
            else {
                continue;
            };
            let p_range = p_handler.key_range();
            if p_range == parent_route.range
                || p_range.start_key != parent_route.range.start_key
                || p_range.end_key >= parent_route.range.end_key
            {
                continue;
            }
            for &(child_sid, ref c_handler) in &orphan_handlers {
                let c_range = c_handler.key_range();
                if c_range.start_key == p_range.end_key
                    && c_range.end_key == parent_route.range.end_key
                {
                    let Some(c_group) = self.host.get_group(child_sid) else {
                        continue;
                    };
                    let mut updated_parent = parent_route.clone();
                    updated_parent.range = p_range.clone();
                    updated_parent.epoch = p_epoch;
                    if let Some(l) = p_group.current_leader() {
                        updated_parent.leader_node = l;
                    }
                    let child_peers = c_group
                        .applied_voter_addrs()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(nid, addr)| (nid, (addr, true)))
                        .collect::<BTreeMap<_, _>>();
                    let child_route = CompositeShardRoute {
                        cell_id: parent_route.cell_id,
                        shard_id: child_sid,
                        range: c_range,
                        epoch: c_group.current_epoch(),
                        leader_node: c_group
                            .current_leader()
                            .unwrap_or(updated_parent.leader_node),
                        peers: if child_peers.is_empty() {
                            parent_route.peers.clone()
                        } else {
                            child_peers
                        },
                        split_mode: parent_route.split_mode,
                        health_state: if parent_route.dual_mode_pid.is_some() {
                            ShardHealthState::Normal
                        } else {
                            parent_route.health_state
                        },
                        dual_mode_pid: parent_route.dual_mode_pid,
                    };
                    if parent_route.dual_mode_pid.is_some() {
                        updated_parent.health_state = ShardHealthState::Normal;
                    }
                    let mut guard = self.state.lock().unwrap();
                    let mut next = guard.clone();
                    next.routes
                        .insert(updated_parent.shard_id, updated_parent.clone());
                    next.routes.insert(child_sid, child_route.clone());
                    let _ = self.persist_sync(&next);
                    *guard = next;
                    drop(guard);
                    self.meta_cache
                        .apply_split_routes(updated_parent, child_route);
                    break;
                }
            }
        }
        Ok(())
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
        let mut live_routes = [route];
        self.overlay_live_membership(&mut live_routes);
        let mut live_route = live_routes.into_iter().next().unwrap();
        live_route.epoch = ep;

        if !force {
            let approx = group
                .engine()
                .approximate_size_bytes(self.host.clock().now());
            if approx < self.split_threshold_bytes {
                return Ok(None);
            }
        }

        let explicit_candidate = if live_route.split_mode == ShardSplitMode::DualModeFineGrained {
            let dpid = live_route.dual_mode_pid;
            let hint = self
                .dual_mode_split_hints
                .lock()
                .unwrap()
                .get(&shard_id)
                .cloned();
            match (dpid, hint) {
                (Some(pid), Some(name)) => {
                    let key = metadb_core::MetaKey::child(pid, &name).encode();
                    if key > live_route.range.start_key && key < live_route.range.end_key {
                        Some(SplitCandidate::SplitAt {
                            split_key: key,
                            dual_mode_pid: Some(pid),
                        })
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        };

        let candidate = match explicit_candidate {
            Some(c) => Some(c),
            None => RaftShardSplitWorker::discover_split_key(&live_route, &group).await?,
        };
        let Some(candidate) = candidate else {
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
                if dual_mode_pid.is_some() {
                    self.dual_mode_split_hints.lock().unwrap().remove(&shard_id);
                    let mut ctrl = self.control.lock().unwrap();
                    if ctrl.placements.contains_key(&shard_id) {
                        let mut next_ctrl = ctrl.clone();
                        next_ctrl.placements.remove(&shard_id);
                        let _ = self.store_control(&mut ctrl, next_ctrl);
                    }
                }
                let _ = self.publish_manager_snapshot().await;
                Ok(Some(child_shard_id))
            }
        }
    }

    /// Switches a Jumbo Shard into `ShardSplitMode::DualModeFineGrained` for `dominant_pid`.
    pub fn enable_dual_mode_split(&self, shard_id: ShardId, dominant_pid: InodeId) -> Result<()> {
        self.enable_dual_mode_split_with_hint(shard_id, dominant_pid, None)
    }

    /// Switches a Jumbo Shard into `ShardSplitMode::DualModeFineGrained` for `dominant_pid` with
    /// an optional explicit child-name split boundary hint.
    pub fn enable_dual_mode_split_with_hint(
        &self,
        shard_id: ShardId,
        dominant_pid: InodeId,
        split_name_hint: Option<String>,
    ) -> Result<()> {
        let cleaned_hint = match split_name_hint {
            Some(s) if !s.is_empty() => {
                metadb_core::schema::validate_user_entry_name(&s)?;
                Some(s)
            }
            _ => None,
        };
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
        drop(st);
        if let Some(hint) = cleaned_hint {
            self.dual_mode_split_hints
                .lock()
                .unwrap()
                .insert(shard_id, hint);
        } else {
            self.dual_mode_split_hints.lock().unwrap().remove(&shard_id);
        }
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
        self.publish_manager_snapshot().await?;
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
        self.publish_manager_snapshot().await?;
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
        let mut live_routes = [parent_route.clone()];
        self.overlay_live_membership(&mut live_routes);
        let live_route = live_routes.into_iter().next().unwrap();
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
        self.activate_split(request_uuid, new_parent, new_child, Some(child_handler))?;
        let _ = self.publish_manager_snapshot().await;
        Ok(())
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

    fn control_group(&self) -> Result<std::sync::Arc<metadb_raft::ShardRaftGroup>> {
        let running = self.running_shard_ids();
        let Some(lowest_running) = running.first().copied() else {
            let lowest = self.route_shard_ids().first().copied();
            return Err(match lowest {
                Some(shard_id) => MetaDbError::ShardRoutingError(format!(
                    "shard {shard_id} is not running; the replicated directory was not published"
                )),
                None => MetaDbError::ShardRoutingError(
                    "cell has no shard that can store the replicated directory".to_string(),
                ),
            });
        };
        if let Some(shard_id) = self.running_carrier_with_record()? {
            return self.host.get_group(shard_id).ok_or_else(|| {
                MetaDbError::ShardRoutingError(format!(
                    "shard {shard_id} dropped its control snapshot while it was being read"
                ))
            });
        }
        if self.committed_snapshot_unreadable_after_handoff()? {
            return Err(MetaDbError::ShardRoutingError(
                "committed cell directory is not readable after jumbo handoff; refusing to publish an empty directory on another shard"
                    .to_string(),
            ));
        }
        self.host.get_group(lowest_running).ok_or_else(|| {
            MetaDbError::ShardRoutingError(format!(
                "shard {lowest_running} is not running; the replicated directory was not published"
            ))
        })
    }

    fn route_shard_ids(&self) -> Vec<ShardId> {
        let mut ids: Vec<ShardId> = self.state.lock().unwrap().routes.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    fn running_shard_ids(&self) -> Vec<ShardId> {
        self.route_shard_ids()
            .into_iter()
            .filter(|shard_id| self.host.get_group(*shard_id).is_some())
            .collect()
    }

    /// Lowest running shard that holds the highest committed control sequence.
    fn running_carrier_with_record(&self) -> Result<Option<ShardId>> {
        let mut best: Option<(ShardId, u64)> = None;
        for shard_id in self.running_shard_ids() {
            let Some(group) = self.host.get_group(shard_id) else {
                continue;
            };
            let Some(record) = metadb_tx::read_replicated_control_record(group.engine().as_ref())?
            else {
                continue;
            };
            let replace = match best {
                None => true,
                Some((_, sequence)) if record.sequence > sequence => true,
                Some((id, sequence)) if record.sequence == sequence && shard_id < id => true,
                _ => false,
            };
            if replace {
                best = Some((shard_id, record.sequence));
            }
        }
        Ok(best.map(|(shard_id, _)| shard_id))
    }

    fn committed_snapshot_unreadable_after_handoff(&self) -> Result<bool> {
        if self.running_carrier_with_record()?.is_some() {
            return Ok(false);
        }
        if self.read_handed_off_control_record()?.is_some() {
            return Ok(false);
        }
        let running = self.running_shard_ids();
        let Some(lowest_running) = running.first().copied() else {
            return Ok(false);
        };
        Ok(load_handoffs(&self.state_dir)?
            .iter()
            .any(|record| record.shard_id < lowest_running))
    }

    fn record_outside_group(
        &self,
        except: ShardId,
    ) -> Result<Option<metadb_tx::ReplicatedControlRecord>> {
        let mut best: Option<metadb_tx::ReplicatedControlRecord> = None;
        for shard_id in self.running_shard_ids() {
            if shard_id == except {
                continue;
            }
            let Some(group) = self.host.get_group(shard_id) else {
                continue;
            };
            let Some(record) = metadb_tx::read_replicated_control_record(group.engine().as_ref())?
            else {
                continue;
            };
            if best
                .as_ref()
                .is_none_or(|current| record.sequence >= current.sequence)
            {
                best = Some(record);
            }
        }
        if let Some(record) = self.read_handed_off_control_record()? {
            if best
                .as_ref()
                .is_none_or(|current| record.sequence >= current.sequence)
            {
                best = Some(record);
            }
        }
        Ok(best)
    }

    fn read_handed_off_control_record(&self) -> Result<Option<metadb_tx::ReplicatedControlRecord>> {
        let mut best: Option<metadb_tx::ReplicatedControlRecord> = None;
        for handoff in load_handoffs(&self.state_dir)? {
            if self.host.get_group(handoff.shard_id).is_some() {
                continue;
            }
            let lsm = self.host.shard_dir(handoff.shard_id).join("lsm");
            if !lsm.exists() {
                continue;
            }
            let engine = metadb_storage::LifecycleStorageEngine::open_lsm_existing(
                handoff.shard_id,
                &lsm,
                self.host.block_cache().clone(),
                self.lsm_config.clone(),
            )?;
            let Some(record) = metadb_tx::read_replicated_control_record(&engine)? else {
                continue;
            };
            let replace = match &best {
                None => true,
                Some(current) if record.sequence > current.sequence => true,
                _ => false,
            };
            if replace {
                best = Some(record);
            }
        }
        Ok(best)
    }

    /// Copies both committed snapshots off `leaving` before that directory is removed.
    ///
    /// `Ok(false)` from a target that is not led by this process is a failure.
    /// The open shard must then contain the same directory and the same
    /// `CELL_MANAGER_STATE` body. A closed source log is read from disk. If it
    /// cannot be moved, the source directory stays.
    async fn preserve_control_snapshot_before_handoff(&self, leaving: ShardId) -> Result<()> {
        let Some(record) = self.committed_record_for_shard(leaving)? else {
            return Ok(());
        };
        if self.open_shard_has_exact_snapshot(leaving, &record)? {
            return Ok(());
        }
        let next_id = self
            .running_shard_ids()
            .into_iter()
            .find(|shard_id| *shard_id != leaving);
        let Some(next_id) = next_id else {
            return Err(MetaDbError::ShardRoutingError(format!(
                "shard {leaving} holds the committed cell directory and CELL_MANAGER_STATE and no other shard group is open; refusing to hand it off or publish an empty directory"
            )));
        };
        let Some(next) = self.host.get_group(next_id) else {
            return Err(MetaDbError::ShardRoutingError(format!(
                "shard {next_id} stopped before the committed directory and CELL_MANAGER_STATE could be copied; the source directory was not deleted"
            )));
        };
        let proposed = self
            .propose_control_on_group(
                next.as_ref(),
                record.directory_cell_ids.clone(),
                record.directory_body.clone(),
                record.manager_body.clone(),
                Some(record.sequence),
            )
            .await?;
        if !proposed {
            return Err(MetaDbError::ShardRoutingError(format!(
                "shard {next_id} is not led by this process; the committed directory and CELL_MANAGER_STATE were not proposed and the source directory was not deleted"
            )));
        }
        let copied = metadb_tx::read_replicated_control_record(next.engine().as_ref())?
            .ok_or_else(|| {
                MetaDbError::ShardRoutingError(format!(
                    "shard {next_id} did not store the committed directory and CELL_MANAGER_STATE; the source directory was not deleted"
                ))
            })?;
        if !control_snapshots_match(&copied, &record) {
            return Err(MetaDbError::ShardRoutingError(format!(
                "shard {next_id} stored a different directory or CELL_MANAGER_STATE; refusing to drop the committed snapshot and the source directory was not deleted"
            )));
        }
        Ok(())
    }

    fn committed_record_for_shard(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<metadb_tx::ReplicatedControlRecord>> {
        if let Some(group) = self.host.get_group(shard_id) {
            return metadb_tx::read_replicated_control_record(group.engine().as_ref());
        }
        self.read_stopped_shard_control_record(shard_id)
    }

    fn open_shard_has_exact_snapshot(
        &self,
        leaving: ShardId,
        record: &metadb_tx::ReplicatedControlRecord,
    ) -> Result<bool> {
        for shard_id in self.running_shard_ids() {
            if shard_id == leaving {
                continue;
            }
            let Some(group) = self.host.get_group(shard_id) else {
                continue;
            };
            let Some(existing) =
                metadb_tx::read_replicated_control_record(group.engine().as_ref())?
            else {
                continue;
            };
            if control_snapshots_match(&existing, record) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn read_stopped_shard_control_record(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<metadb_tx::ReplicatedControlRecord>> {
        if self.host.get_group(shard_id).is_some() {
            return Ok(None);
        }
        let lsm = self.host.shard_dir(shard_id).join("lsm");
        if !lsm.exists() {
            return Ok(None);
        }
        let engine = metadb_storage::LifecycleStorageEngine::open_lsm_existing(
            shard_id,
            &lsm,
            self.host.block_cache().clone(),
            self.lsm_config.clone(),
        )?;
        metadb_tx::read_replicated_control_record(&engine)
    }

    /// The next control-plane propose reports that this process is not the leader.
    pub fn testing_next_control_propose_not_leader(&self) {
        self.control_propose_not_leader
            .store(true, Ordering::Relaxed);
    }

    /// Writes `manager_body` onto `shard_id` with the current committed directory.
    ///
    /// The directory bytes stay the same. The manager body is what differs.
    pub async fn testing_publish_manager_body_on_shard(
        &self,
        shard_id: ShardId,
        manager_body: Vec<u8>,
    ) -> Result<()> {
        let record = self.replicated_control()?.ok_or_else(|| {
            MetaDbError::ShardRoutingError("no committed control snapshot to diverge".to_string())
        })?;
        let Some(group) = self.host.get_group(shard_id) else {
            return Err(MetaDbError::ShardRoutingError(format!(
                "shard {shard_id} is not running"
            )));
        };
        let proposed = self
            .propose_control_on_group(
                group.as_ref(),
                record.directory_cell_ids,
                record.directory_body,
                manager_body,
                Some(record.sequence),
            )
            .await?;
        if !proposed {
            return Err(MetaDbError::ShardRoutingError(format!(
                "shard {shard_id} did not accept the divergent CELL_MANAGER_STATE"
            )));
        }
        Ok(())
    }

    pub fn testing_control_record_on_shard(
        &self,
        shard_id: ShardId,
    ) -> Result<Option<metadb_tx::ReplicatedControlRecord>> {
        self.committed_record_for_shard(shard_id)
    }

    /// Publishes the committed directory with a different, still-valid manager body.
    ///
    /// The address of this process gains a suffix. Nodes, voters, and addresses
    /// stay non-empty, so the record still decodes.
    pub async fn testing_diverge_manager_on_shard(&self, shard_id: ShardId) -> Result<Vec<u8>> {
        let record = self.replicated_control()?.ok_or_else(|| {
            MetaDbError::ShardRoutingError("no committed control snapshot to diverge".to_string())
        })?;
        let mut state: PersistedCellState =
            codec::deserialize(&record.manager_body).map_err(|error| {
                MetaDbError::CodecError(format!(
                    "CELL_MANAGER_STATE could not be diverged: {error}"
                ))
            })?;
        let node = self.host.node_id();
        let address = state.nodes.get_mut(&node).ok_or_else(|| {
            MetaDbError::ShardRoutingError(format!(
                "CELL_MANAGER_STATE has no address for node {node}"
            ))
        })?;
        address.push_str("-diverged");
        for route in state.routes.values_mut() {
            if let Some((peer, _)) = route.peers.get_mut(&node) {
                peer.push_str("-diverged");
            }
        }
        let body = codec::serialize(&state).map_err(|error| {
            MetaDbError::CodecError(format!("CELL_MANAGER_STATE could not be diverged: {error}"))
        })?;
        self.testing_publish_manager_body_on_shard(shard_id, body.clone())
            .await?;
        Ok(body)
    }

    /// Snapshot committed on this cell's existing shard Raft group, if any.
    ///
    /// A value that does not decode fails closed. This does not create cells.
    pub fn replicated_control(&self) -> Result<Option<metadb_tx::ReplicatedControlRecord>> {
        if let Some(shard_id) = self.running_carrier_with_record()? {
            let group = self.host.get_group(shard_id).ok_or_else(|| {
                MetaDbError::ShardRoutingError(format!(
                    "shard {shard_id} dropped its control snapshot while it was being read"
                ))
            })?;
            return metadb_tx::read_replicated_control_record(group.engine().as_ref());
        }
        if let Some(record) = self.read_handed_off_control_record()? {
            return Ok(Some(record));
        }
        if self.committed_snapshot_unreadable_after_handoff()? {
            return Err(MetaDbError::ShardRoutingError(
                "committed cell directory is not readable after jumbo handoff; refusing to publish an empty directory"
                    .to_string(),
            ));
        }
        Ok(None)
    }

    /// Fail closed when the replicated manager snapshot does not decode.
    ///
    /// A committed scale-in can leave this file behind the applied Raft
    /// membership, or the other way around. That lag is not a dropped cell.
    /// A body that does not decode is corrupt and refuses to open.
    pub fn reconcile_replicated_membership(&self) -> Result<()> {
        let Some(record) = self.replicated_control()? else {
            return Ok(());
        };
        decode_manager_body_exact(&record.manager_body)?;
        Ok(())
    }

    /// Propose `records` and this process's cell-manager snapshot.
    ///
    /// Returns `Ok(false)` when this process is not the shard leader or it joined
    /// an existing group. The caller decides whether that is an error. An identical
    /// snapshot is not rewritten and returns `Ok(true)`.
    pub async fn publish_control_plane(
        &self,
        records: &BTreeMap<CellId, crate::cell_directory::DirectoryCell>,
    ) -> Result<bool> {
        if self.joined_existing() {
            return Ok(false);
        }
        let directory_body = encode_directory_body(records)?;
        let directory_cell_ids: Vec<CellId> = records.keys().copied().collect();
        let manager_body = {
            let state = self.state.lock().unwrap();
            codec::serialize(&*state)
                .map_err(|error| MetaDbError::CodecError(format!("CELL_MANAGER_STATE: {error}")))?
        };
        let floor = self.replicated_control()?.map(|record| record.sequence);
        self.propose_control_plane(directory_cell_ids, directory_body, manager_body, floor)
            .await
    }

    /// Republish membership while keeping the directory body already committed.
    ///
    /// Not being the leader leaves the previous snapshot in place.
    async fn publish_manager_snapshot(&self) -> Result<()> {
        if self.joined_existing() {
            return Ok(());
        }
        let manager_body = {
            let state = self.state.lock().unwrap();
            codec::serialize(&*state)
                .map_err(|error| MetaDbError::CodecError(format!("CELL_MANAGER_STATE: {error}")))?
        };
        let (directory_cell_ids, directory_body, floor) = match self.replicated_control()? {
            Some(current) => (
                current.directory_cell_ids,
                current.directory_body,
                Some(current.sequence),
            ),
            None => {
                if self.committed_snapshot_unreadable_after_handoff()? {
                    return Err(MetaDbError::ShardRoutingError(
                        "committed cell directory is not readable after jumbo handoff; refusing to publish an empty directory"
                            .to_string(),
                    ));
                }
                (Vec::new(), encode_directory_body(&BTreeMap::new())?, None)
            }
        };
        self.propose_control_plane(directory_cell_ids, directory_body, manager_body, floor)
            .await?;
        Ok(())
    }

    async fn propose_control_plane(
        &self,
        directory_cell_ids: Vec<CellId>,
        directory_body: Vec<u8>,
        manager_body: Vec<u8>,
        floor_sequence: Option<u64>,
    ) -> Result<bool> {
        let group = self.control_group()?;
        self.propose_control_on_group(
            group.as_ref(),
            directory_cell_ids,
            directory_body,
            manager_body,
            floor_sequence,
        )
        .await
    }

    async fn propose_control_on_group(
        &self,
        group: &metadb_raft::ShardRaftGroup,
        directory_cell_ids: Vec<CellId>,
        directory_body: Vec<u8>,
        manager_body: Vec<u8>,
        floor_sequence: Option<u64>,
    ) -> Result<bool> {
        if self
            .control_propose_not_leader
            .swap(false, Ordering::Relaxed)
        {
            return Ok(false);
        }
        let leader = group.wait_for_leader(Duration::from_secs(15)).await?;
        if leader != self.host.node_id() {
            return Ok(false);
        }
        if let Some(current) = metadb_tx::read_replicated_control_record(group.engine().as_ref())? {
            for cell_id in &current.directory_cell_ids {
                if !directory_cell_ids.contains(cell_id) {
                    return Err(MetaDbError::ShardRoutingError(format!(
                        "refusing to drop replicated cell {cell_id}"
                    )));
                }
            }
            if current.directory_cell_ids == directory_cell_ids
                && current.directory_body == directory_body
                && current.manager_body == manager_body
            {
                return Ok(true);
            }
        }
        let outside = self.record_outside_group(group.shard_id())?;
        if let Some(prior) = &outside {
            for cell_id in &prior.directory_cell_ids {
                if !directory_cell_ids.contains(cell_id) {
                    return Err(MetaDbError::ShardRoutingError(format!(
                        "refusing to drop replicated cell {cell_id} while moving the committed directory"
                    )));
                }
            }
        }
        let sequence = match metadb_tx::read_replicated_control_record(group.engine().as_ref())? {
            Some(current) => current.sequence.saturating_add(1),
            None => {
                let floor = outside
                    .as_ref()
                    .map(|record| record.sequence)
                    .or(floor_sequence)
                    .unwrap_or(1);
                let same = outside.as_ref().is_some_and(|record| {
                    record.directory_cell_ids == directory_cell_ids
                        && record.directory_body == directory_body
                        && record.manager_body == manager_body
                });
                if same {
                    floor
                } else if outside.is_some() {
                    floor.saturating_add(1)
                } else {
                    floor
                }
            }
        };
        if sequence == 0 {
            return Err(MetaDbError::CodecError(
                "control-plane sequence overflow; refusing to publish".to_string(),
            ));
        }
        let payload = metadb_tx::command_payload(&metadb_tx::TxRaftCmd::PublishControlPlane {
            sequence,
            directory_cell_ids,
            directory_body,
            manager_body,
        })?;
        let epoch = group.current_epoch();
        group
            .propose_until(
                epoch,
                self.host.clock().now(),
                payload,
                tokio::time::Instant::now() + Duration::from_secs(15),
            )
            .await?;
        Ok(true)
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

    /// Local placement rows in shard-id order. This is not a route table.
    pub fn stored_placements(&self) -> Vec<StoredPlacement> {
        self.control
            .lock()
            .unwrap()
            .placements
            .values()
            .cloned()
            .collect()
    }

    pub fn local_heartbeat_is_fresh(&self, now_ms: u64, interval_ms: u64) -> bool {
        if !crate::heartbeat_scheduler::wall_unix_ms_is_trusted(now_ms) {
            return false;
        }
        self.control
            .lock()
            .unwrap()
            .heartbeats
            .get(&self.host.node_id())
            .is_some_and(|heartbeat| {
                now_ms >= heartbeat.observed_unix_ms
                    && now_ms - heartbeat.observed_unix_ms < interval_ms
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
    /// The wall stamp is not a measured heartbeat and is not recorded for the scheduler.
    pub fn publish_local_heartbeat(&self) -> Result<StoredHeartbeat> {
        let me = self.host.node_id();
        node_id_in_ha_range(me)?;
        let observed_unix_ms = self.trusted_heartbeat_wall()?;
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
            observed_unix_ms,
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
        let observed_unix_ms = self.trusted_heartbeat_wall()?;
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
            observed_unix_ms,
            disk_free_bytes,
            reported_shard_ids: reported,
            local_hosted_shard_ids: Vec::new(),
            observation_generation,
        };
        let mut guard = self.control.lock().unwrap();
        let decision = decide_heartbeat(guard.heartbeats.get(&node_id), &write)
            .map_err(MetaDbError::ShardRoutingError)?;
        match decision {
            HeartbeatDecision::Replay(_) => {}
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
                self.store_control(&mut guard, next)?;
            }
        }
        drop(guard);
        self.note_measured_recv(node_id, seq, observed_unix_ms);
        Ok(())
    }

    fn trusted_heartbeat_wall(&self) -> Result<u64> {
        self.liveness
            .lock()
            .unwrap()
            .effective_wall_unix_ms()
            .ok_or_else(|| {
                MetaDbError::ShardRoutingError(
                    "untrusted wall clock; the heartbeat was not stored and is not a scheduling input"
                        .to_string(),
                )
            })
    }

    fn note_measured_recv(&self, node_id: NodeId, seq: u64, wall_unix_ms: u64) {
        let sample = MeasuredSample {
            node_id,
            seq,
            sent_mono_ns: None,
            recv_mono_ns: capture_mono_ns(),
            wall_unix_ms,
        };
        if let Err(error) = self.liveness.lock().unwrap().record(sample) {
            tracing::debug!(
                node_id,
                seq,
                ?error,
                "recv heartbeat was not a trusted measurement"
            );
        }
    }

    /// Records a send/ack round trip for `node_id`. A zero or backwards gap is ignored.
    /// An untrusted wall clock records nothing. This does not change routes.
    pub fn record_acknowledged_round_trip(
        &self,
        node_id: NodeId,
        seq: u64,
        sent_mono_ns: u64,
        recv_mono_ns: u64,
    ) {
        let mut scheduler = self.liveness.lock().unwrap();
        let Some(wall_unix_ms) = scheduler.effective_wall_unix_ms() else {
            return;
        };
        let sample = MeasuredSample {
            node_id,
            seq,
            sent_mono_ns: Some(sent_mono_ns),
            recv_mono_ns,
            wall_unix_ms,
        };
        let _ = scheduler.record(sample);
    }

    pub fn control_heartbeat_publish_is_due(&self, now_mono_ns: u64) -> bool {
        self.liveness.lock().unwrap().publish_is_due(now_mono_ns)
    }

    pub fn note_control_heartbeat_published(&self, now_mono_ns: u64) {
        self.liveness.lock().unwrap().note_published(now_mono_ns);
    }

    /// Liveness hint for one node. A missing, expired, or untrusted input is a refusal.
    /// The result does not transfer leadership or move a Jumbo shard.
    pub fn liveness_decision(&self, node_id: NodeId) -> LivenessDecision {
        let scheduler = self.liveness.lock().unwrap();
        let now = scheduler.schedule_now();
        scheduler.decide(node_id, &now)
    }

    pub fn liveness_decision_at(&self, node_id: NodeId, now: ScheduleNow) -> LivenessDecision {
        self.liveness.lock().unwrap().decide(node_id, &now)
    }

    /// Computes a hint for every measured sample and does not apply it.
    pub fn observe_liveness_hints(&self) {
        let scheduler = self.liveness.lock().unwrap();
        let now = scheduler.schedule_now();
        for decision in scheduler.decide_all(&now) {
            match decision {
                LivenessDecision::HintLive { node_id, seq, .. } => {
                    tracing::trace!(
                        node_id,
                        seq,
                        "liveness hint recorded; no transfer_leader and no jumbo move"
                    );
                }
                LivenessDecision::Refuse { node_id, reason } => {
                    tracing::trace!(node_id, ?reason, "liveness scheduler refused");
                }
            }
        }
    }

    /// Test hook. `Some(1)` and `None` are the untrusted-clock path.
    pub fn testing_set_heartbeat_wall(&self, wall: Option<u64>) {
        self.liveness.lock().unwrap().set_wall_override(wall);
    }

    pub fn testing_record_measured_recv(
        &self,
        node_id: NodeId,
        seq: u64,
        recv_mono_ns: u64,
        wall_unix_ms: u64,
    ) -> std::result::Result<(), MeasurementReject> {
        self.liveness.lock().unwrap().record(MeasuredSample {
            node_id,
            seq,
            sent_mono_ns: None,
            recv_mono_ns,
            wall_unix_ms,
        })
    }

    /// Test hook. Points jumbo move detection at a dedicated root without copying.
    pub fn testing_set_jumbo_dedicated_root(&self, root: Option<PathBuf>) {
        *self.jumbo_dedicated_root.lock().unwrap() = root;
    }

    /// Returns `"live"`, `"staged"`, `"error"`, or `""` for `shard_id` on `jumbo_dedicated_root`.
    pub fn jumbo_move_phase_name(&self, shard_id: ShardId) -> &'static str {
        let Some(root) = self.jumbo_dedicated_root.lock().unwrap().clone() else {
            return "";
        };
        match visible_move_phase(&shard_move_dir(&root, shard_id), shard_id) {
            Ok(Some(MOVE_LIVE)) => "live",
            Ok(Some(MOVE_STAGED)) => "staged",
            Ok(Some(_)) => "unknown",
            Ok(None) => "",
            Err(_) => "error",
        }
    }

    /// Count of nodes currently holding a fresh measured `HintLive` heartbeat sample.
    pub fn live_heartbeat_node_count(&self) -> u64 {
        let scheduler = self.liveness.lock().unwrap();
        let now = scheduler.schedule_now();
        scheduler
            .decide_all(&now)
            .into_iter()
            .filter(|d| matches!(d, LivenessDecision::HintLive { .. }))
            .count() as u64
    }

    /// How many times the leadership gate entered its callback.
    pub fn leadership_transfer_requests(&self) -> u64 {
        self.leadership_transfer_requests.load(Ordering::Relaxed)
    }

    /// True when automatic leadership transfer is enabled on this cell (`false` by default).
    pub fn automatic_leadership_transfer_enabled(&self) -> bool {
        self.allow_automatic_transfer.load(Ordering::Relaxed)
    }

    /// Enables or disables opt-in automatic leadership transfer on this cell.
    pub fn set_automatic_leadership_transfer_enabled(&self, enabled: bool) {
        self.allow_automatic_transfer
            .store(enabled, Ordering::Relaxed);
    }

    /// Safely transfers Raft leadership of `shard_id` from this process to `target`:
    /// 1. Validates all fail-closed gate preconditions (`leadership_transfer_refusal`) and ensures
    ///    any recorded liveness sample for `target` has not expired.
    /// 2. Dials `ProbeTransferLeader` on `target` BEFORE touching OpenRaft's leader lease so an
    ///    offline, lagging, or older peer never disables the current leader's lease.
    /// 3. Calls `group.raft().trigger().transfer_leader(target)` and polls until `target` is
    ///    observed as leader over a window covering `election_timeout_max_ms`.
    pub async fn transfer_shard_leader(
        &self,
        shard_id: ShardId,
        target: NodeId,
    ) -> Result<LeadershipTransferOutcome> {
        let _membership = self.membership_lock.lock().await;
        let facts = self.leadership_group_facts(shard_id);
        let shard_dir = self.host.shard_dir(shard_id);
        let shard_existed = shard_dir.exists();
        let mut live_voters = facts.voters.clone();
        if matches!(
            self.liveness_decision(target),
            LivenessDecision::Refuse {
                reason: crate::heartbeat_scheduler::LivenessRefusal::ExpiredHeartbeat,
                ..
            }
        ) {
            live_voters.remove(&target);
        }
        let input = LeadershipTransferInput {
            shard_id,
            local_node: self.host.node_id(),
            current_leader: facts.leader,
            live_voters,
            intended_voters: facts.voters,
            target,
            group_transfer_allowed: true,
            clock_trusted: self.leadership_clock_trusted(),
            jumbo_move_in_progress: self.jumbo_move_blocks_transfer(shard_id),
            remote_handoff: self.remote_handoff_blocks_transfer(shard_id),
            source_group_open: facts.open,
            hint_live_only: false,
        };
        if let Some(_refusal) = crate::leadership_transfer::leadership_transfer_refusal(&input) {
            return Ok(run_automatic_leadership_transfer(
                &input,
                &shard_dir,
                |_| TransferObservation {
                    openraft_called: false,
                    target_became_leader: false,
                },
            ));
        }

        let Some(group) = self.host.get_group(shard_id) else {
            return Ok(LeadershipTransferOutcome {
                transfer_requested: false,
                openraft_called: false,
                membership_changed: false,
                source_directory_deleted: shard_existed && !shard_dir.exists(),
                invented_leader: false,
                leader: input.current_leader,
                refusal: Some(crate::leadership_transfer::TransferRefusal::ClosedSourceGroup),
                committed: None,
            });
        };
        let voters_before = group.uniform_voter_addrs()?;
        let Some(target_addr) = voters_before.get(&target).cloned() else {
            return Ok(LeadershipTransferOutcome {
                transfer_requested: false,
                openraft_called: false,
                membership_changed: false,
                source_directory_deleted: shard_existed && !shard_dir.exists(),
                invented_leader: false,
                leader: input.current_leader,
                refusal: Some(crate::leadership_transfer::TransferRefusal::TargetNotLiveVoter),
                committed: None,
            });
        };

        let min_matched = group
            .raft()
            .metrics()
            .borrow_watched()
            .last_applied
            .map(|id| id.index)
            .unwrap_or(0);
        self.leadership_transfer_requests
            .fetch_add(1, Ordering::Relaxed);
        let probe = self
            .host
            .channel_pool()
            .probe_transfer_leader(&target_addr, shard_id, target, min_matched)
            .await;
        match probe {
            Ok(resp) if resp.eligible => {}
            Ok(_) | Err(_) => {
                return Ok(LeadershipTransferOutcome {
                    transfer_requested: true,
                    openraft_called: false,
                    membership_changed: false,
                    source_directory_deleted: shard_existed && !shard_dir.exists(),
                    invented_leader: false,
                    leader: input.current_leader,
                    refusal: None,
                    committed: None,
                });
            }
        }

        group
            .raft()
            .trigger()
            .transfer_leader(target)
            .await
            .map_err(|error| {
                MetaDbError::ConsensusError(format!(
                    "openraft transfer_leader to node {target} on shard {shard_id} failed: {error}"
                ))
            })?;

        let poll_ms = self
            .raft_opts
            .election_timeout_max_ms
            .saturating_mul(2)
            .max(3_000);
        debug_assert!(crate::jumbo_placement::transfer_poll_covers_election(
            poll_ms,
            self.raft_opts.election_timeout_max_ms.max(1)
        ));
        let deadline = tokio::time::Instant::now() + Duration::from_millis(poll_ms);
        let mut target_became_leader = false;
        while tokio::time::Instant::now() < deadline {
            if group.current_leader() == Some(target) {
                target_became_leader = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let committed = target_became_leader.then_some(CommittedMove::LeadershipTransferred {
            shard_id,
            to: target,
        });
        if target_became_leader {
            {
                let mut st = self.state.lock().unwrap();
                if st.routes.contains_key(&shard_id) {
                    let mut next = st.clone();
                    if let Some(r) = next.routes.get_mut(&shard_id) {
                        r.leader_node = target;
                    }
                    let updated = next.routes.get(&shard_id).cloned();
                    let _ = self.persist_sync(&next);
                    *st = next;
                    if let Some(updated) = updated {
                        self.meta_cache.upsert_route(updated);
                    }
                }
            }
            if let Some(mut existing) = self.stored_placement(shard_id) {
                push_move(
                    &mut existing.committed_moves,
                    CommittedMove::LeadershipTransferred {
                        shard_id,
                        to: target,
                    },
                );
                existing.last_transfer_attempt_unix_ms = unix_ms();
                existing.updated_unix_ms = existing.last_transfer_attempt_unix_ms;
                let _ = self.save_placement(&existing);
            }
        }

        let voters_after = group.uniform_voter_addrs().ok();
        Ok(LeadershipTransferOutcome {
            transfer_requested: true,
            openraft_called: true,
            membership_changed: Some(voters_before) != voters_after,
            source_directory_deleted: shard_existed && !shard_dir.exists(),
            invented_leader: false,
            leader: if target_became_leader {
                Some(target)
            } else {
                group.current_leader().or(input.current_leader)
            },
            refusal: None,
            committed,
        })
    }

    /// Async maintenance hook executing safe `ProbeTransferLeader`-verified leadership transfer
    /// for Jumbo-isolated shards when `automatic_leadership_transfer_enabled()` is opted in.
    pub async fn consider_automatic_leadership_transfer_async(&self) {
        if !self.automatic_leadership_transfer_enabled() {
            self.consider_automatic_leadership_transfer();
            return;
        }
        for group in self.host.all_groups() {
            let shard_id = group.shard_id();
            let target = self.jumbo_transfer_target(shard_id);
            if target == 0 {
                continue;
            }
            if let Some(stored) = self.stored_placement(shard_id) {
                if stored.last_transfer_attempt_unix_ms != 0
                    && unix_ms().saturating_sub(stored.last_transfer_attempt_unix_ms)
                        < PLACEMENT_RETRY_MS
                {
                    continue;
                }
            }
            let _ = self.transfer_shard_leader(shard_id, target).await;
        }
    }

    /// Evaluates every local group. A `HintLive` sample is not read.
    ///
    /// `group_transfer_allowed` is [`automatic_leadership_transfer_allowed`],
    /// which is false. This does not call `trigger().transfer_leader`, does not
    /// change membership, does not delete a shard directory, and does not write
    /// a leader.
    pub fn consider_automatic_leadership_transfer(&self) {
        for group in self.host.all_groups() {
            let shard_id = group.shard_id();
            let facts = self.leadership_group_facts(shard_id);
            let input = LeadershipTransferInput {
                shard_id,
                local_node: self.host.node_id(),
                current_leader: facts.leader,
                live_voters: facts.voters.clone(),
                intended_voters: facts.voters,
                target: self.jumbo_transfer_target(shard_id),
                group_transfer_allowed: automatic_leadership_transfer_allowed(),
                clock_trusted: self.leadership_clock_trusted(),
                jumbo_move_in_progress: self.jumbo_move_blocks_transfer(shard_id),
                remote_handoff: self.remote_handoff_blocks_transfer(shard_id),
                source_group_open: facts.open,
                hint_live_only: false,
            };
            let _ = self.finish_leadership_transfer(&input, &self.host.shard_dir(shard_id));
        }
    }

    /// Same gate as [`Self::consider_automatic_leadership_transfer`], with the
    /// caller's observations. Production maintenance does not call this.
    pub fn testing_attempt_leadership_transfer(
        &self,
        attempt: &LeadershipTransferAttempt,
    ) -> LeadershipTransferOutcome {
        let facts = self.leadership_group_facts(attempt.shard_id);
        let live_voters = attempt.observed_live_voters.clone().unwrap_or(facts.voters);
        let current_leader = match attempt.observed_leader {
            Some(leader) => leader,
            None => facts.leader,
        };
        let input = LeadershipTransferInput {
            shard_id: attempt.shard_id,
            local_node: self.host.node_id(),
            current_leader,
            live_voters,
            intended_voters: attempt.intended_voters.clone(),
            target: attempt.target,
            group_transfer_allowed: attempt.group_transfer_allowed,
            clock_trusted: self.leadership_clock_trusted(),
            jumbo_move_in_progress: self.jumbo_move_blocks_transfer(attempt.shard_id),
            remote_handoff: self.remote_handoff_blocks_transfer(attempt.shard_id),
            source_group_open: facts.open,
            hint_live_only: attempt.hint_live_only,
        };
        self.finish_leadership_transfer(&input, &attempt.source_directory)
    }

    fn finish_leadership_transfer(
        &self,
        input: &LeadershipTransferInput,
        source_directory: &Path,
    ) -> LeadershipTransferOutcome {
        let leader_before = self.raft_leader(input.shard_id);
        let voters_before = self.raft_voter_ids(input.shard_id).ok();
        let shard_dir = self.host.shard_dir(input.shard_id);
        let shard_existed = shard_dir.exists();
        let mut outcome = run_automatic_leadership_transfer(input, source_directory, |_target| {
            self.leadership_transfer_requests
                .fetch_add(1, Ordering::Relaxed);
            // OpenRaft disables the leader lease before any peer answers.
            // Capability is still unknown, so this callback does not call
            // `trigger().transfer_leader` and does not report a new leader.
            TransferObservation {
                openraft_called: false,
                target_became_leader: false,
            }
        });
        if voters_before != self.raft_voter_ids(input.shard_id).ok() {
            outcome.membership_changed = true;
        }
        if shard_existed && !shard_dir.exists() {
            outcome.source_directory_deleted = true;
        }
        if leader_before != self.raft_leader(input.shard_id) {
            outcome.invented_leader = true;
        }
        outcome
    }

    fn raft_leader(&self, shard_id: ShardId) -> Option<NodeId> {
        self.host
            .get_group(shard_id)
            .and_then(|group| group.current_leader())
    }

    fn leadership_clock_trusted(&self) -> bool {
        self.liveness
            .lock()
            .unwrap()
            .effective_wall_unix_ms()
            .is_some()
    }

    fn leadership_group_facts(&self, shard_id: ShardId) -> LeadershipGroupFacts {
        let Some(group) = self.host.get_group(shard_id) else {
            return LeadershipGroupFacts {
                open: false,
                leader: None,
                voters: BTreeSet::new(),
            };
        };
        if group.serve_view().stopped {
            return LeadershipGroupFacts {
                open: false,
                leader: group.current_leader(),
                voters: BTreeSet::new(),
            };
        }
        match group.uniform_voter_addrs() {
            Ok(voters) => LeadershipGroupFacts {
                open: true,
                leader: group.current_leader(),
                voters: voters.into_keys().collect(),
            },
            Err(_) => LeadershipGroupFacts {
                open: false,
                leader: group.current_leader(),
                voters: BTreeSet::new(),
            },
        }
    }

    /// Any visible move phase blocks. A missing dedicated root does not.
    /// An unreadable record fails closed.
    fn jumbo_move_blocks_transfer(&self, shard_id: ShardId) -> bool {
        let Some(root) = self.jumbo_dedicated_root.lock().unwrap().clone() else {
            return false;
        };
        let dest = shard_move_dir(&root, shard_id);
        match visible_move_phase(&dest, shard_id) {
            Ok(None) => false,
            Ok(Some(_)) | Err(_) => true,
        }
    }

    fn remote_handoff_blocks_transfer(&self, shard_id: ShardId) -> bool {
        handoff_blocks_local_open(&self.state_dir, shard_id).unwrap_or(true)
    }

    /// Dedicated node of a jumbo route, when it is someone else. Otherwise no
    /// target: node 0 is not a live voter, so the gate refuses.
    fn jumbo_transfer_target(&self, shard_id: ShardId) -> NodeId {
        let state = self.state.lock().unwrap();
        let Some(route) = state.routes.get(&shard_id) else {
            return 0;
        };
        match route.health_state {
            ShardHealthState::JumboIsolated { dedicated_node, .. }
                if dedicated_node != self.host.node_id() =>
            {
                dedicated_node
            }
            _ => 0,
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
        let outcome = match group.wait_uniform_voter_addrs().await {
            Ok(committed) => self.committed_replace(shard_id, group.as_ref(), &proposed, committed),
            Err(error) => self.finish_replace_after_proposal(group.as_ref(), &proposed, error),
        };
        if matches!(outcome, ReplaceOutcome::Committed { .. }) {
            if let Some(removed_addr) = current.get(&remove).cloned() {
                if !removed_addr.is_empty() && remove != self.host.node_id() {
                    let pool = self.host.channel_pool().clone();
                    let cell_id = self.cell_id;
                    tokio::spawn(async move {
                        let _ = pool
                            .close_removed_shard(
                                &removed_addr,
                                metadb_proto::CloseRemovedShardRequest {
                                    cell_id,
                                    shard_id,
                                    delete_files: true,
                                },
                            )
                            .await;
                    });
                }
            }
        }
        outcome
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

fn control_snapshots_match(
    copied: &metadb_tx::ReplicatedControlRecord,
    record: &metadb_tx::ReplicatedControlRecord,
) -> bool {
    copied.directory_cell_ids == record.directory_cell_ids
        && copied.directory_body == record.directory_body
        && copied.manager_body == record.manager_body
}

pub fn manager_voters_from_body(
    body: &[u8],
) -> Result<BTreeMap<ShardId, BTreeMap<NodeId, String>>> {
    Ok(decode_manager_body_exact(body)?.voters)
}

pub fn manager_nodes_from_body(body: &[u8]) -> Result<BTreeMap<NodeId, String>> {
    Ok(decode_manager_body_exact(body)?.nodes)
}

pub(crate) fn decode_manager_body_exact(body: &[u8]) -> Result<ManagerSnapshot> {
    let state: PersistedCellState =
        wincode::config::deserialize_exact(body, wincode::config::DefaultConfig::default())
            .map_err(|error| {
                MetaDbError::CodecError(format!(
                    "CELL_MANAGER_STATE publication is corrupt or inconsistent: {error}"
                ))
            })?;
    Ok(ManagerSnapshot {
        nodes: state.nodes,
        voters: voter_sets(&state.routes),
    })
}

fn voter_sets(
    routes: &BTreeMap<ShardId, CompositeShardRoute>,
) -> BTreeMap<ShardId, BTreeMap<NodeId, String>> {
    routes
        .iter()
        .map(|(shard_id, route)| {
            let voters = route
                .peers
                .iter()
                .filter(|(_, (_, is_voter))| *is_voter)
                .map(|(node_id, (address, _))| (*node_id, address.clone()))
                .collect();
            (*shard_id, voters)
        })
        .collect()
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

struct CellLocalControlHook {
    cell: std::sync::Weak<DurableCellManager>,
}

#[tonic::async_trait]
impl metadb_raft::LocalShardControlHook for CellLocalControlHook {
    async fn open_joining_shard(&self, req: metadb_proto::OpenJoiningShardRequest) -> Result<()> {
        let cell = self.cell.upgrade().ok_or_else(|| {
            MetaDbError::ShardRoutingError("target cell manager has been dropped".to_string())
        })?;
        if req.cell_id != 0 && req.cell_id != cell.cell_id() {
            return Err(MetaDbError::ShardRoutingError(format!(
                "cell id mismatch: request targets {}, local cell is {}",
                req.cell_id,
                cell.cell_id()
            )));
        }
        let range = CompositeKeyRange::new(req.start_key, req.end_key)?;
        let initial_epoch = req
            .initial_epoch
            .map(ShardEpoch::from)
            .unwrap_or_else(ShardEpoch::initial);
        cell.open_joining_shard(
            req.shard_id,
            range,
            initial_epoch,
            req.use_jumbo_dedicated_root,
        )
        .await
    }

    async fn close_removed_shard(&self, req: metadb_proto::CloseRemovedShardRequest) -> Result<()> {
        let cell = self.cell.upgrade().ok_or_else(|| {
            MetaDbError::ShardRoutingError("target cell manager has been dropped".to_string())
        })?;
        if req.cell_id != 0 && req.cell_id != cell.cell_id() {
            return Err(MetaDbError::ShardRoutingError(format!(
                "cell id mismatch: request targets {}, local cell is {}",
                req.cell_id,
                cell.cell_id()
            )));
        }
        cell.close_removed_shard(req.shard_id, req.delete_files)
            .await
    }
}
