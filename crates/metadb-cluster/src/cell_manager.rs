//! Persistent `DurableCellManager` managing OpenRaft shard groups, 3-stage recoverable
//! `ExecuteSplit` (`SplitPlanRecord`), Jumbo Shard dedicated-node isolation, and `DualModeFineGrained`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use openraft::async_runtime::watch::WatchReceiver;
use openraft::{BasicNode, ChangeMembers, Precondition};
use serde::{Deserialize, Serialize};

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

use crate::id_allocator::{AllocIdKind, DurableIdAllocator};
use crate::jumbo::JumboIsolationPlanner;
use crate::meta_cache::{CompositeMetaCache, CompositeShardRoute};
use crate::shard::{ShardHealthState, ShardSplitMode};
use crate::split_worker::{RaftShardSplitWorker, SplitCandidate, SplitCrashPoint};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SplitPlanState {
    Planned,
    Activated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitPlanRecord {
    pub parent_shard_id: ShardId,
    pub child_shard_id: ShardId,
    pub split_key: Vec<u8>,
    pub target_version: u64,
    pub state: SplitPlanState,
    pub dual_mode_pid: Option<InodeId>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
}

/// On-disk layout after `joined_existing` and before `join_pending`.
#[derive(Debug, Deserialize)]
struct CellStateJoinedOnly {
    nodes: BTreeMap<NodeId, String>,
    routes: BTreeMap<ShardId, CompositeShardRoute>,
    split_plans: BTreeMap<u128, SplitPlanRecord>,
    joined_existing: bool,
}

/// On-disk layout before `joined_existing` was stored in the same record.
#[derive(Debug, Deserialize)]
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
        let state_dir = state_dir.as_ref().to_path_buf();
        fs::create_dir_all(&state_dir)?;
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

            if bootstrap_table_shards {
                for (sid, range) in [
                    (1u64, CompositeKeyRange::meta_table_root()),
                    (2u64, CompositeKeyRange::quota_table_root()),
                    (3u64, CompositeKeyRange::secondary_index_table_root()),
                ] {
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
            } else {
                init.routes.insert(
                    1,
                    CompositeShardRoute {
                        cell_id,
                        shard_id: 1,
                        range: CompositeKeyRange::all_user_tables(),
                        epoch: ShardEpoch::initial(),
                        leader_node: host.node_id(),
                        peers: init_peers,
                        split_mode: ShardSplitMode::DirectoryColocated,
                        health_state: ShardHealthState::Normal,
                        dual_mode_pid: None,
                    },
                );
            }
            Self::write_state_file(&state_dir, &init)?;
            init
        };

        Self::verify_or_initialize_owner(
            &state_dir,
            host.node_id(),
            cell_id,
            &persisted,
            is_existing,
        )?;

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
        });

        if let Err(error) = mgr.boot_local_shards().await {
            let _ = mgr.host.shutdown_all().await;
            return Err(error);
        }
        if let Err(error) = mgr.recover_incomplete_splits().await {
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
        let state_dir = state_dir.as_ref().to_path_buf();
        fs::create_dir_all(&state_dir)?;
        let state_path = state_dir.join("CELL_MANAGER_STATE");
        let persisted = if state_path.exists() {
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
            let voters = group.uniform_voter_addrs()?;
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
        Ok(group.uniform_voter_addrs()?.into_keys().collect())
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
        for route in routes {
            self.meta_cache.upsert_route(route);
        }
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

    pub fn host(&self) -> &Arc<ShardGroupHost> {
        &self.host
    }

    pub fn meta_cache(&self) -> &Arc<CompositeMetaCache> {
        &self.meta_cache
    }

    pub fn register_node(&self, node_id: NodeId, grpc_addr: String) -> Result<()> {
        let mut guard = self.state.lock().unwrap();
        let mut next = guard.clone();
        next.nodes.insert(node_id, grpc_addr);
        self.persist_sync(&next)?;
        *guard = next;
        Ok(())
    }

    pub fn list_routes(&self) -> Vec<CompositeShardRoute> {
        let guard = self.state.lock().unwrap();
        guard.routes.values().cloned().collect()
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
                let mut st = self.state.lock().unwrap();
                let nodes: Vec<NodeId> = st.nodes.keys().copied().collect();
                let all_routes: Vec<CompositeShardRoute> = st.routes.values().cloned().collect();
                let new_health = JumboIsolationPlanner::select_dedicated_node(
                    shard_id,
                    dominant_pid,
                    live_route.leader_node,
                    &nodes,
                    &all_routes,
                );
                if st.routes.contains_key(&shard_id) {
                    let mut next = st.clone();
                    if let Some(r) = next.routes.get_mut(&shard_id) {
                        r.health_state = new_health;
                    }
                    let updated = next.routes.get(&shard_id).cloned();
                    self.persist_sync(&next)?;
                    *st = next;
                    if let Some(updated) = updated {
                        self.meta_cache.upsert_route(updated);
                    }
                }
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
        if target_voters == 0 {
            return Err(MetaDbError::ConsensusError(
                "target_voters must be at least 1".to_string(),
            ));
        }
        let (nodes, shard_ids): (BTreeMap<NodeId, String>, Vec<ShardId>) = {
            let st = self.state.lock().unwrap();
            if st.nodes.len() < target_voters {
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
            self.retarget_shard_voters(sid, target_voters, &nodes)
                .await?;
            upgraded += 1;
        }
        Ok(upgraded)
    }

    /// Removes `node_id` from every local shard voter set. Refuses to drop the last voter.
    pub async fn remove_voter(&self, node_id: NodeId) -> Result<usize> {
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
    ) -> Result<()> {
        let (_, group, _) = self.lookup_local_handle(shard_id)?;
        let current = group.uniform_voter_addrs()?;
        let current_voters: BTreeSet<NodeId> = current.keys().copied().collect();
        let mut final_voters = current_voters.clone();
        if final_voters.len() > target_voters {
            while final_voters.len() > target_voters {
                let Some(drop_id) = final_voters.iter().next_back().copied() else {
                    break;
                };
                final_voters.remove(&drop_id);
            }
        } else {
            for nid in nodes.keys().copied() {
                if final_voters.len() >= target_voters {
                    break;
                }
                final_voters.insert(nid);
            }
            if final_voters.len() < target_voters {
                return Err(MetaDbError::ConsensusError(format!(
                    "need {target_voters} voters for shard {shard_id}, can form {}",
                    final_voters.len()
                )));
            }
        }
        if final_voters.is_empty() {
            return Err(MetaDbError::ConsensusError(
                "refusing to remove every voter".to_string(),
            ));
        }

        let mut addrs = nodes.clone();
        for (nid, addr) in &current {
            addrs.entry(*nid).or_insert_with(|| addr.clone());
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
            let payload =
                bincode::serialize(&cmd).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
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
        let mut raw = bincode::serialize(st).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
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

/// `SplitPlanRecord` gained `dual_mode_pid` after the first on-disk layout.
/// Old `CELL_MANAGER_STATE` files still decode; missing mode is `None`.
#[derive(Debug, Deserialize)]
struct LegacySplitPlanRecord {
    parent_shard_id: ShardId,
    child_shard_id: ShardId,
    split_key: Vec<u8>,
    target_version: u64,
    state: SplitPlanState,
}

#[derive(Debug, Deserialize)]
struct LegacyPersistedCellState {
    nodes: BTreeMap<NodeId, String>,
    routes: BTreeMap<ShardId, CompositeShardRoute>,
    split_plans: BTreeMap<u128, LegacySplitPlanRecord>,
}

fn decode_cell_state(body: &[u8]) -> Result<PersistedCellState> {
    if let Ok(state) = bincode::deserialize::<PersistedCellState>(body) {
        return Ok(state);
    }
    if let Ok(prior) = bincode::deserialize::<CellStateJoinedOnly>(body) {
        return Ok(PersistedCellState {
            nodes: prior.nodes,
            routes: prior.routes,
            split_plans: prior.split_plans,
            joined_existing: prior.joined_existing,
            join_pending: false,
        });
    }
    if let Ok(prior) = bincode::deserialize::<CellStateBeforeJoinFlag>(body) {
        return Ok(PersistedCellState {
            nodes: prior.nodes,
            routes: prior.routes,
            split_plans: prior.split_plans,
            joined_existing: false,
            join_pending: false,
        });
    }
    let legacy: LegacyPersistedCellState = bincode::deserialize(body)
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
    })
}
