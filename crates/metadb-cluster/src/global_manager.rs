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
}

#[tonic::async_trait]
impl ShardDirectory for DurableGlobalManager {
    fn route_user_key(&self, user_key: &[u8]) -> Result<RoutedShardHandle> {
        let route = match self.meta_cache.lookup_key(user_key) {
            Some(r) => r,
            None => {
                self.refresh_meta_cache();
                self.meta_cache.lookup_key(user_key).ok_or_else(|| {
                    MetaDbError::ShardRoutingError("no route covering key".to_string())
                })?
            }
        };
        let cell = self.get_cell(route.cell_id).ok_or_else(|| {
            MetaDbError::ShardRoutingError(format!("unknown cell {}", route.cell_id))
        })?;
        let (_, grp, handler) = cell.lookup_local_handle(route.shard_id)?;
        // Return the route's cached epoch so if a caller had a stale `MetaCache` entry,
        // `grp.propose(route.epoch, ...)` fences it via `EpochMismatch` (`Resolution B16`)
        // and self-heals on retry!
        if !handler.key_range().contains_key(user_key) || grp.current_epoch() != route.epoch {
            self.refresh_meta_cache();
            let refreshed = self.meta_cache.lookup_key(user_key).ok_or_else(|| {
                MetaDbError::ShardRoutingError("no route after refresh".to_string())
            })?;
            let r_cell = self.get_cell(refreshed.cell_id).ok_or_else(|| {
                MetaDbError::ShardRoutingError(format!("unknown cell {}", refreshed.cell_id))
            })?;
            let (ep, g2, h2) = r_cell.lookup_local_handle(refreshed.shard_id)?;
            return Ok((ep, g2, h2));
        }
        Ok((route.epoch, grp, handler))
    }

    fn route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> Result<Vec<RoutedShardHandle>> {
        let mut routes = self.meta_cache.lookup_range(start_user_key, end_user_key);
        if routes.is_empty() {
            self.refresh_meta_cache();
            routes = self.meta_cache.lookup_range(start_user_key, end_user_key);
        }
        let mut out = Vec::with_capacity(routes.len());
        for r in routes {
            let cell = self.get_cell(r.cell_id).ok_or_else(|| {
                MetaDbError::ShardRoutingError(format!("unknown cell {}", r.cell_id))
            })?;
            let (ep, grp, handler) = cell.lookup_local_handle(r.shard_id)?;
            out.push((ep, grp, handler));
        }
        Ok(out)
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
}
