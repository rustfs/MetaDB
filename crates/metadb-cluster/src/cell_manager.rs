//! Persistent `DurableCellManager` managing OpenRaft shard groups, 3-stage recoverable
//! `ExecuteSplit` (`SplitPlanRecord`), Jumbo Shard dedicated-node isolation, and `DualModeFineGrained`.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

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
use crate::split_worker::{RaftShardSplitWorker, SplitCandidate};

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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedCellState {
    nodes: BTreeMap<NodeId, String>,
    routes: BTreeMap<ShardId, CompositeShardRoute>,
    split_plans: BTreeMap<u128, SplitPlanRecord>,
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

        let is_existing = state_path.exists();
        let persisted = if is_existing {
            let mut raw = Vec::new();
            File::open(&state_path)?.read_to_end(&mut raw)?;
            let body = verify_crc32c_le(&raw, "CELL_MANAGER_STATE")?;
            bincode::deserialize::<PersistedCellState>(body)
                .map_err(|e| MetaDbError::CodecError(e.to_string()))?
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
            Self::persist_sync(&state_dir, &init)?;
            init
        };

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
        });

        mgr.boot_local_shards().await?;
        Ok(mgr)
    }

    async fn boot_local_shards(&self) -> Result<()> {
        let routes: Vec<CompositeShardRoute> = {
            let st = self.state.lock().unwrap();
            st.routes.values().cloned().collect()
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
                let handler = Arc::new(TxStateMachineHandler::new(
                    route.shard_id,
                    route.epoch,
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
                group.initialize_if_needed(voter_addrs.clone()).await?;
                self.host.register_running_group(group.clone());
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
        guard.nodes.insert(node_id, grpc_addr);
        Self::persist_sync(&self.state_dir, &guard)
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
        guard.split_plans.insert(
            request_uuid,
            SplitPlanRecord {
                parent_shard_id,
                child_shard_id,
                split_key,
                target_version,
                state: SplitPlanState::Planned,
            },
        );
        Self::persist_sync(&self.state_dir, &guard)?;
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
        if let Some(h) = child_handler {
            self.handlers
                .write()
                .unwrap()
                .insert(child_route.shard_id, h);
        }
        {
            let mut guard = self.state.lock().unwrap();
            if let Some(plan) = guard.split_plans.get_mut(&request_uuid) {
                plan.state = SplitPlanState::Activated;
            }
            guard
                .routes
                .insert(parent_route.shard_id, parent_route.clone());
            guard
                .routes
                .insert(child_route.shard_id, child_route.clone());
            Self::persist_sync(&self.state_dir, &guard)?;
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
                if let Some(r) = st.routes.get_mut(&shard_id) {
                    r.health_state = new_health;
                    let updated = r.clone();
                    Self::persist_sync(&self.state_dir, &st)?;
                    self.meta_cache.upsert_route(updated);
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
                    split_key.clone(),
                    live_route.epoch.version + 1,
                )?;
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
        let route = st
            .routes
            .get_mut(&shard_id)
            .ok_or_else(|| MetaDbError::ShardRoutingError(format!("unknown shard {}", shard_id)))?;
        route.split_mode = ShardSplitMode::DualModeFineGrained;
        route.dual_mode_pid = Some(dominant_pid);
        let updated = route.clone();
        Self::persist_sync(&self.state_dir, &st)?;
        self.meta_cache.upsert_route(updated);
        Ok(())
    }

    /// Upgrades local shard Raft groups in this Cell to `target_voters` via `ConfChangeOrchestrator`.
    pub async fn scale_out_replicas(&self, target_voters: usize) -> Result<usize> {
        let (candidates, shard_ids): (Vec<(NodeId, String)>, Vec<ShardId>) = {
            let st = self.state.lock().unwrap();
            if st.nodes.len() < target_voters {
                return Err(MetaDbError::ConsensusError(format!(
                    "need {} registered nodes in cell {}, have {}",
                    target_voters,
                    self.cell_id,
                    st.nodes.len()
                )));
            }
            let cands = st
                .nodes
                .iter()
                .take(target_voters)
                .map(|(&nid, addr)| (nid, addr.clone()))
                .collect();
            let sids = st.routes.keys().copied().collect();
            (cands, sids)
        };

        let final_voters: std::collections::BTreeSet<NodeId> =
            candidates.iter().map(|(nid, _)| *nid).collect();
        let mut upgraded = 0usize;

        for sid in shard_ids {
            let (_, group, _) = self.lookup_local_handle(sid)?;
            let existing_peers: BTreeMap<NodeId, (String, bool)> = {
                let st = self.state.lock().unwrap();
                st.routes
                    .get(&sid)
                    .map(|r| r.peers.clone())
                    .unwrap_or_default()
            };
            let learners_to_add: Vec<(NodeId, String)> = candidates
                .iter()
                .filter(|(nid, _)| !existing_peers.contains_key(nid))
                .cloned()
                .collect();

            if !learners_to_add.is_empty() {
                ConfChangeOrchestrator::add_learners_and_promote_voters(
                    group.raft(),
                    &learners_to_add,
                    final_voters.clone(),
                    64,
                    Duration::from_secs(10),
                )
                .await?;
            }

            let new_epoch = group.current_epoch();
            let mut st = self.state.lock().unwrap();
            if let Some(r) = st.routes.get_mut(&sid) {
                for (nid, addr) in &candidates {
                    r.peers.insert(*nid, (addr.clone(), true));
                }
                r.epoch = new_epoch;
                let updated = r.clone();
                Self::persist_sync(&self.state_dir, &st)?;
                self.meta_cache.upsert_route(updated);
            }
            upgraded += 1;
        }
        Ok(upgraded)
    }

    fn persist_sync(dir: &Path, st: &PersistedCellState) -> Result<()> {
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
}
