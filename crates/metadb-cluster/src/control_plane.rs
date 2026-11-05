use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use metadb_core::{HlcTimestamp, InodeId, MetaDbError, MetaKey, NodeId, Result, ShardId};
use metadb_storage::LifecycleStorageEngine;

use crate::shard::{PolicyShardSplitter, ShardDescriptor, ShardHealthState, ShardSplitMode};

/// Identifier of an independently managed cluster Cell (§8 Two-Tier Management).
pub type CellId = u64;

/// Cached routing entry stored in `MetaCache`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardRoute {
    pub cell_id: CellId,
    pub shard_id: ShardId,
    pub start_pid: InodeId,
    pub end_pid: InodeId,
    pub leader_node: NodeId,
    pub conf_epoch: u64,
}

/// Scalable routing cache tier (`MetaCache`, §3.2 & §8) placed in front of the Managers
/// to prevent proxy restart thundering herds and serve shard lookups in memory.
pub struct MetaCache {
    routes_by_start_pid: RwLock<BTreeMap<InodeId, ShardRoute>>,
}

impl Default for MetaCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaCache {
    pub fn new() -> Self {
        Self {
            routes_by_start_pid: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn upsert_route(&self, route: ShardRoute) {
        self.routes_by_start_pid
            .write()
            .unwrap()
            .insert(route.start_pid, route);
    }

    pub fn invalidate_all(&self) {
        self.routes_by_start_pid.write().unwrap().clear();
    }

    /// Resolves the `ShardRoute` covering `pid` in $O(\log N)$ time.
    pub fn lookup_pid(&self, pid: InodeId) -> Option<ShardRoute> {
        let guard = self.routes_by_start_pid.read().unwrap();
        let (_, route) = guard.range(..=pid).next_back()?;
        if pid >= route.start_pid && pid < route.end_pid {
            Some(route.clone())
        } else {
            None
        }
    }
}

struct CellManagerInner {
    next_shard_id: ShardId,
    shards: BTreeMap<ShardId, ShardDescriptor>,
    engines: BTreeMap<ShardId, Arc<LifecycleStorageEngine>>,
    available_nodes: Vec<NodeId>,
}

/// Cell-level Local Manager (§8 Single Manager vs. Two-Tier Management).
///
/// Manages per-shard heartbeats, dynamic `group_key = PID` splitting, Jumbo Shard
/// isolation scheduling, and Raft replica membership within a single Cell.
pub struct CellLocalManager {
    cell_id: CellId,
    split_threshold_bytes: usize,
    mvcc_retention_ms: u64,
    tombstone_density_threshold: usize,
    inner: RwLock<CellManagerInner>,
}

impl CellLocalManager {
    pub fn new(
        cell_id: CellId,
        initial_node: NodeId,
        initial_shard_id: ShardId,
        start_pid: InodeId,
        end_pid: InodeId,
        split_threshold_bytes: usize,
        mvcc_retention_ms: u64,
        tombstone_density_threshold: usize,
    ) -> Self {
        let mut root_desc = ShardDescriptor::new_root(initial_shard_id, initial_node);
        root_desc.start_pid = start_pid;
        root_desc.end_pid = end_pid;

        let root_engine = Arc::new(LifecycleStorageEngine::new(
            initial_shard_id,
            mvcc_retention_ms,
            tombstone_density_threshold,
            true,
        ));

        let mut shards = BTreeMap::new();
        shards.insert(initial_shard_id, root_desc);
        let mut engines = BTreeMap::new();
        engines.insert(initial_shard_id, root_engine);

        Self {
            cell_id,
            split_threshold_bytes,
            mvcc_retention_ms,
            tombstone_density_threshold,
            inner: RwLock::new(CellManagerInner {
                next_shard_id: initial_shard_id + 1,
                shards,
                engines,
                available_nodes: vec![initial_node],
            }),
        }
    }

    pub fn cell_id(&self) -> CellId {
        self.cell_id
    }

    /// Registers a new BE storage node in this Cell for horizontal scale-out.
    pub fn register_node(&self, node_id: NodeId) {
        let mut guard = self.inner.write().unwrap();
        if !guard.available_nodes.contains(&node_id) {
            guard.available_nodes.push(node_id);
        }
    }

    /// Upgrades all shards in this Cell from `1-Voter` Single-Node mode to `3-Voter` HA mode
    /// via Raft `add_learner` + `promote_learner_to_voter` when at least 3 nodes are registered.
    pub fn scale_out_raft_replicas(&self, target_replicas: usize) -> Result<usize> {
        let mut guard = self.inner.write().unwrap();
        if guard.available_nodes.len() < target_replicas {
            return Err(MetaDbError::ConsensusError(format!(
                "need at least {} registered nodes in cell {}, have {}",
                target_replicas,
                self.cell_id,
                guard.available_nodes.len()
            )));
        }
        let candidate_nodes: Vec<NodeId> = guard
            .available_nodes
            .iter()
            .copied()
            .take(target_replicas)
            .collect();

        let mut upgraded_shards = 0usize;
        for desc in guard.shards.values_mut() {
            for &node in &candidate_nodes {
                if !desc.membership.members.contains_key(&node) {
                    desc.membership.add_learner(node);
                    desc.membership.promote_learner_to_voter(node)?;
                }
            }
            upgraded_shards += 1;
        }
        Ok(upgraded_shards)
    }

    /// Finds the shard descriptor and storage engine responsible for `pid`.
    pub fn route_pid(
        &self,
        pid: InodeId,
    ) -> Result<(ShardDescriptor, Arc<LifecycleStorageEngine>)> {
        let guard = self.inner.read().unwrap();
        for (shard_id, desc) in &guard.shards {
            if desc.contains_pid(pid) {
                let engine = guard.engines.get(shard_id).unwrap().clone();
                return Ok((desc.clone(), engine));
            }
        }
        Err(MetaDbError::ShardRoutingError(format!(
            "no shard in cell {} covers PID {}",
            self.cell_id, pid
        )))
    }

    /// Inspects `shard_id` against `split_threshold_bytes`:
    /// - If over threshold and multiple `PID`s exist, splits the shard cleanly at the
    ///   `group_key = PID` boundary and moves right-half records into a new shard.
    /// - If over threshold and dominated by a single `PID` (Jumbo Shard), isolates the
    ///   shard onto a dedicated node (`ShardHealthState::JumboIsolated`).
    pub fn maybe_split_or_isolate_shard(
        &self,
        shard_id: ShardId,
        hlc: HlcTimestamp,
    ) -> Result<Option<ShardId>> {
        let (desc, engine) = {
            let guard = self.inner.read().unwrap();
            let desc = guard
                .shards
                .get(&shard_id)
                .cloned()
                .ok_or_else(|| MetaDbError::ShardRoutingError(format!("unknown shard {}", shard_id)))?;
            let engine = guard.engines.get(&shard_id).unwrap().clone();
            (desc, engine)
        };

        let approx_bytes = engine.approximate_size_bytes(hlc);
        if approx_bytes < self.split_threshold_bytes {
            return Ok(None);
        }

        match PolicyShardSplitter::find_pid_split_boundary(&desc, &engine, hlc) {
            Ok(split_pid) => {
                let mut guard = self.inner.write().unwrap();
                let new_shard_id = guard.next_shard_id;
                guard.next_shard_id += 1;

                // Pick a target leader node round-robin from available nodes.
                let node_idx = (new_shard_id as usize) % guard.available_nodes.len();
                let leader_node = guard.available_nodes[node_idx];

                let new_engine = Arc::new(LifecycleStorageEngine::new(
                    new_shard_id,
                    self.mvcc_retention_ms,
                    self.tombstone_density_threshold,
                    true,
                ));

                // Migrate keys with `pid >= split_pid` from `engine` to `new_engine`.
                let move_start = MetaKey::encode_pid_prefix_start(split_pid);
                let move_end = MetaKey::encode_pid_prefix_end(desc.end_pid.saturating_sub(1));
                let rows = engine.scan_range(&move_start, &move_end, hlc)?;
                for (k, v) in rows {
                    new_engine.put(k.clone(), v, hlc);
                    engine.delete(k, hlc);
                }

                let mut right_desc = ShardDescriptor::new_root(new_shard_id, leader_node);
                right_desc.start_pid = split_pid;
                right_desc.end_pid = desc.end_pid;
                right_desc.membership = desc.membership.clone();
                right_desc.membership.leader = leader_node;

                if let Some(left_desc) = guard.shards.get_mut(&shard_id) {
                    left_desc.end_pid = split_pid;
                }
                guard.shards.insert(new_shard_id, right_desc);
                guard.engines.insert(new_shard_id, new_engine);

                Ok(Some(new_shard_id))
            }
            Err(MetaDbError::UnsplittableJumboShard { pid, .. }) => {
                // Jumbo-shard-aware scheduling and isolation (§4.1):
                let mut guard = self.inner.write().unwrap();
                let dedicated_node = *guard.available_nodes.last().unwrap_or(&desc.membership.leader);
                if let Some(target_desc) = guard.shards.get_mut(&shard_id) {
                    target_desc.health_state = ShardHealthState::JumboIsolated {
                        dedicated_node,
                        dominant_pid: pid,
                    };
                    target_desc.membership.leader = dedicated_node;
                }
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Operator fallback: switches an isolated Jumbo Shard into `DualModeFineGrained` (§4.1).
    pub fn enable_dual_mode_split(&self, shard_id: ShardId, split_name: String) -> Result<()> {
        let mut guard = self.inner.write().unwrap();
        let desc = guard
            .shards
            .get_mut(&shard_id)
            .ok_or_else(|| MetaDbError::ShardRoutingError(format!("unknown shard {}", shard_id)))?;
        desc.split_mode = ShardSplitMode::DualModeFineGrained;
        desc.fine_grained_name_split = Some(split_name);
        Ok(())
    }

    pub fn list_shards(&self) -> Vec<ShardDescriptor> {
        self.inner.read().unwrap().shards.values().cloned().collect()
    }

    pub fn all_engines(&self) -> Vec<Arc<LifecycleStorageEngine>> {
        self.inner.read().unwrap().engines.values().cloned().collect()
    }
}

struct GlobalRangeMapping {
    start_pid: InodeId,
    end_pid: InodeId,
    cell: Arc<CellLocalManager>,
}

/// Top-tier Global Manager (§8 Single Manager vs. Two-Tier Management).
///
/// Maintains coarse-grained contiguous key-range mappings to independent `CellLocalManager`
/// instances so per-shard heartbeats and splits stay inside each Cell, enabling the cluster
/// to scale past 1,000+ nodes and millions of shards (100B–10T+ records).
pub struct GlobalManager {
    cells: RwLock<Vec<GlobalRangeMapping>>,
    meta_cache: Arc<MetaCache>,
}

impl GlobalManager {
    pub fn new(meta_cache: Arc<MetaCache>) -> Self {
        Self {
            cells: RwLock::new(Vec::new()),
            meta_cache,
        }
    }

    pub fn meta_cache(&self) -> &Arc<MetaCache> {
        &self.meta_cache
    }

    /// Registers a `CellLocalManager` responsible for `[start_pid, end_pid)`.
    pub fn register_cell(
        &self,
        start_pid: InodeId,
        end_pid: InodeId,
        cell: Arc<CellLocalManager>,
    ) {
        let mut guard = self.cells.write().unwrap();
        guard.push(GlobalRangeMapping {
            start_pid,
            end_pid,
            cell: cell.clone(),
        });
        guard.sort_by_key(|m| m.start_pid);
        drop(guard);
        self.refresh_meta_cache();
    }

    /// Resolves the `CellLocalManager` and `ShardDescriptor` for `pid`, populating `MetaCache`.
    pub fn route_pid(
        &self,
        pid: InodeId,
    ) -> Result<(Arc<CellLocalManager>, ShardDescriptor, Arc<LifecycleStorageEngine>)> {
        let guard = self.cells.read().unwrap();
        for mapping in guard.iter() {
            if pid >= mapping.start_pid && pid < mapping.end_pid {
                let (desc, engine) = mapping.cell.route_pid(pid)?;
                self.meta_cache.upsert_route(ShardRoute {
                    cell_id: mapping.cell.cell_id(),
                    shard_id: desc.shard_id,
                    start_pid: desc.start_pid,
                    end_pid: desc.end_pid,
                    leader_node: desc.membership.leader,
                    conf_epoch: desc.membership.conf_epoch,
                });
                return Ok((mapping.cell.clone(), desc, engine));
            }
        }
        Err(MetaDbError::ShardRoutingError(format!(
            "no cell registered for PID {}",
            pid
        )))
    }

    /// Refreshes `MetaCache` with all current shard routes across all Cells.
    pub fn refresh_meta_cache(&self) {
        let guard = self.cells.read().unwrap();
        for mapping in guard.iter() {
            for desc in mapping.cell.list_shards() {
                self.meta_cache.upsert_route(ShardRoute {
                    cell_id: mapping.cell.cell_id(),
                    shard_id: desc.shard_id,
                    start_pid: desc.start_pid,
                    end_pid: desc.end_pid,
                    leader_node: desc.membership.leader,
                    conf_epoch: desc.membership.conf_epoch,
                });
            }
        }
    }

    pub fn cells(&self) -> Vec<Arc<CellLocalManager>> {
        self.cells
            .read()
            .unwrap()
            .iter()
            .map(|m| m.cell.clone())
            .collect()
    }
}
