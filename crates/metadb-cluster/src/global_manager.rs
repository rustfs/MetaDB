//! Top-tier `DurableGlobalManager` implementing `ShardDirectory` (`metadb-tx`) across Cells,
//! `CompositeMetaCache` routing, `DurableIdAllocator`, and fenced `DirRenameLease`s.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use metadb_core::{CellId, CompositeKeyRange, HybridLogicalClock, InodeId, MetaDbError, Result};
use metadb_tx::{DirRenameLease, RoutedShardHandle, ShardDirectory};

use crate::cell_manager::DurableCellManager;
use crate::id_allocator::DurableIdAllocator;
use crate::meta_cache::CompositeMetaCache;

/// Top-tier Two-Tier Control Plane manager (`GlobalManager` + `MetaCache` + `DurableIdAllocator`).
pub struct DurableGlobalManager {
    clock: Arc<HybridLogicalClock>,
    id_allocator: Arc<DurableIdAllocator>,
    meta_cache: Arc<CompositeMetaCache>,
    cells: RwLock<BTreeMap<CellId, (CompositeKeyRange, Arc<DurableCellManager>)>>,
}

impl DurableGlobalManager {
    pub fn new(
        clock: Arc<HybridLogicalClock>,
        id_allocator: Arc<DurableIdAllocator>,
        meta_cache: Arc<CompositeMetaCache>,
    ) -> Self {
        Self {
            clock,
            id_allocator,
            meta_cache,
            cells: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn id_allocator(&self) -> &Arc<DurableIdAllocator> {
        &self.id_allocator
    }

    pub fn meta_cache(&self) -> &Arc<CompositeMetaCache> {
        &self.meta_cache
    }

    pub fn register_cell(&self, range: CompositeKeyRange, cell: Arc<DurableCellManager>) {
        for r in cell.list_routes() {
            self.meta_cache.upsert_route(r);
        }
        self.cells
            .write()
            .unwrap()
            .insert(cell.cell_id(), (range, cell));
    }

    pub fn get_cell(&self, cell_id: CellId) -> Option<Arc<DurableCellManager>> {
        self.cells
            .read()
            .unwrap()
            .get(&cell_id)
            .map(|(_, c)| c.clone())
    }

    pub fn all_cells(&self) -> Vec<Arc<DurableCellManager>> {
        self.cells
            .read()
            .unwrap()
            .values()
            .map(|(_, c)| c.clone())
            .collect()
    }

    pub fn refresh_meta_cache(&self) {
        for cell in self.all_cells() {
            for r in cell.list_routes() {
                self.meta_cache.upsert_route(r);
            }
        }
    }

    fn try_route_user_key(&self, user_key: &[u8]) -> Result<RoutedShardHandle> {
        let route = self.meta_cache.lookup_key(user_key).ok_or_else(|| {
            MetaDbError::ShardRoutingError("no published route covering key".to_string())
        })?;
        let cell = self.get_cell(route.cell_id).ok_or_else(|| {
            MetaDbError::ShardRoutingError(format!("unknown cell {}", route.cell_id))
        })?;
        let (_, group, handler) = cell.lookup_local_handle(route.shard_id)?;
        let actual = group.current_epoch();
        let actual_range = handler.key_range();
        // A split fence updates the handler range and epoch together, before the
        // child route is published. A range mismatch, or a key that the fenced
        // parent no longer contains, must wait. Membership changes only the
        // epoch and leaves the range in place; those reads stay on this shard.
        if actual_range != route.range || !actual_range.contains_key(user_key) {
            return Err(MetaDbError::EpochMismatch {
                shard_id: route.shard_id,
                expected: route.epoch,
                actual,
            });
        }
        Ok((actual, group, handler))
    }

    fn try_route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> Result<Vec<RoutedShardHandle>> {
        if start_user_key >= end_user_key {
            return Ok(Vec::new());
        }
        let routes = self.meta_cache.lookup_range(start_user_key, end_user_key);
        let mut covered_until = start_user_key.to_vec();
        let mut out = Vec::with_capacity(routes.len());
        for route in routes {
            let cell = self.get_cell(route.cell_id).ok_or_else(|| {
                MetaDbError::ShardRoutingError(format!("unknown cell {}", route.cell_id))
            })?;
            let (_, group, handler) = cell.lookup_local_handle(route.shard_id)?;
            let actual = group.current_epoch();
            if handler.key_range() != route.range {
                return Err(MetaDbError::EpochMismatch {
                    shard_id: route.shard_id,
                    expected: route.epoch,
                    actual,
                });
            }
            let overlap_start = route.range.start_key.as_slice().max(start_user_key);
            let overlap_end = route.range.end_key.as_slice().min(end_user_key);
            if overlap_start != covered_until.as_slice() || overlap_end <= overlap_start {
                return Err(MetaDbError::ShardRoutingError(
                    "published routes do not provide contiguous range coverage".to_string(),
                ));
            }
            covered_until = overlap_end.to_vec();
            out.push((actual, group, handler));
        }
        if covered_until.as_slice() != end_user_key {
            return Err(MetaDbError::ShardRoutingError(
                "published routes do not cover the complete requested range".to_string(),
            ));
        }
        Ok(out)
    }
}

#[tonic::async_trait]
impl ShardDirectory for DurableGlobalManager {
    fn allocate_txn_id(&self, coordinator_node_id: u64) -> Result<metadb_core::TxnId> {
        // Share the persisted sequence with FileId grants. Gaps in that sequence
        // are harmless; resetting a process-local counter can alias live 2PC records.
        let (seq, _) =
            self.id_allocator
                .allocate_block(0, crate::id_allocator::AllocIdKind::FileId, 1)?;
        Ok(((coordinator_node_id as u128) << 64) | seq as u128)
    }

    fn route_user_key(&self, user_key: &[u8]) -> Result<RoutedShardHandle> {
        match self.try_route_user_key(user_key) {
            Ok(handle) => Ok(handle),
            Err(_) => {
                self.refresh_meta_cache();
                self.try_route_user_key(user_key)
            }
        }
    }

    fn route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> Result<Vec<RoutedShardHandle>> {
        match self.try_route_user_range(start_user_key, end_user_key) {
            Ok(handles) => Ok(handles),
            Err(_) => {
                self.refresh_meta_cache();
                self.try_route_user_range(start_user_key, end_user_key)
            }
        }
    }

    async fn acquire_dir_rename_lease(
        &self,
        _moved_dir_id: InodeId,
        ttl_ms: u64,
    ) -> Result<DirRenameLease> {
        let (token, expire_hlc) = self
            .id_allocator
            .acquire_dir_rename_lease(self.clock.now(), ttl_ms)?;
        Ok(DirRenameLease {
            rename_fence_token: token,
            expire_hlc,
        })
    }

    async fn release_dir_rename_lease(&self, token: u64) {
        let _ = self.id_allocator.release_dir_rename_lease(token);
    }

    fn raft_peer_addr(&self, shard_id: u64, node_id: u64) -> Option<String> {
        for cell in self.all_cells() {
            if let Ok((_, group, _)) = cell.lookup_local_handle(shard_id) {
                return group.peer_addr(node_id);
            }
        }
        None
    }

    fn local_shard(&self, shard_id: u64) -> Result<RoutedShardHandle> {
        for cell in self.all_cells() {
            if let Ok(handle) = cell.lookup_local_handle(shard_id) {
                return Ok(handle);
            }
        }
        Err(MetaDbError::ShardRoutingError(format!(
            "shard {shard_id} is not hosted locally"
        )))
    }
}
