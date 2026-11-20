//! Top-tier `DurableGlobalManager` implementing `ShardDirectory` (`metadb-tx`) across Cells,
//! `CompositeMetaCache` routing, `DurableIdAllocator`, and fenced `DirRenameLease`s.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

use metadb_core::{
    CellId, CompositeKeyRange, HybridLogicalClock, InodeId, MetaDbError, NodeId, Result,
};
use metadb_tx::{DirRenameLease, RoutedShardHandle, ShardDirectory, MAX_2PC_LEASE_LIFETIME_MS};

use crate::cell_directory::{
    load_directory, precheck_remote, publication_from_probe, store_directory, union_route_range,
    DirectoryCell, RemotePrecheck, RemotePublication,
};
use crate::cell_manager::DurableCellManager;
use crate::id_allocator::DurableIdAllocator;
use crate::meta_cache::CompositeMetaCache;

struct CellDirectory {
    path: Option<std::path::PathBuf>,
    records: BTreeMap<CellId, DirectoryCell>,
    /// False when no file existed. A missing file can be restored from the
    /// replicated snapshot. A file that was loaded is not rewritten to add cells.
    loaded_from_file: bool,
}

/// Which process should apply a metadata or transaction request.
///
/// `Local` is not a claim that this process owns the key. It means no
/// registered remote cell contains it. The local router still fails closed
/// when this process has no shard for that key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellPlacement {
    Local,
    Remote {
        cell_id: CellId,
        endpoints: Vec<String>,
    },
}

/// Top-tier Two-Tier Control Plane manager (`GlobalManager` + `MetaCache` + `DurableIdAllocator`).
pub struct DurableGlobalManager {
    clock: Arc<HybridLogicalClock>,
    id_allocator: Arc<DurableIdAllocator>,
    meta_cache: Arc<CompositeMetaCache>,
    cells: RwLock<BTreeMap<CellId, (CompositeKeyRange, Arc<DurableCellManager>)>>,
    directory: Mutex<CellDirectory>,
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
            directory: Mutex::new(CellDirectory {
                path: None,
                records: BTreeMap::new(),
                loaded_from_file: false,
            }),
        }
    }

    pub fn load_or_create_directory(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let loaded_from_file = path.exists();
        let records = if loaded_from_file {
            load_directory(path)?
        } else {
            BTreeMap::new()
        };
        *self.directory.lock().unwrap() = CellDirectory {
            path: Some(path.to_path_buf()),
            records,
            loaded_from_file,
        };
        Ok(())
    }

    /// Compare the loaded file with the snapshot committed on `cell`.
    ///
    /// A corrupt snapshot fails closed. A loaded file that is missing a
    /// replicated cell, or that disagrees on a remote cell, fails closed.
    /// A file that was never created is filled from the snapshot. The local
    /// cell's listen address may differ. This does not forward requests.
    pub fn align_directory_with_replication(&self, cell: &DurableCellManager) -> Result<()> {
        let Some(record) = cell.replicated_control()? else {
            return Ok(());
        };
        let replicated = crate::cell_directory::decode_directory_body(&record.directory_body)?;
        let mut directory = self.directory.lock().unwrap();
        if !directory.loaded_from_file {
            if replicated.is_empty() {
                return Ok(());
            }
            let Some(path) = directory.path.clone() else {
                return Err(MetaDbError::ShardRoutingError(
                    "cell directory is not loaded".to_string(),
                ));
            };
            store_directory(&path, &replicated)?;
            directory.records = replicated;
            directory.loaded_from_file = true;
            return Ok(());
        }
        for cell_record in replicated.values() {
            let Some(stored) = directory.records.get(&cell_record.cell_id) else {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "GLOBAL_CELL_DIRECTORY is missing replicated cell {}; refusing to drop it",
                    cell_record.cell_id
                )));
            };
            if !directory_cell_matches(stored, cell_record) {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "GLOBAL_CELL_DIRECTORY cell {} is inconsistent with the replicated snapshot; refusing to drop or invent cells",
                    cell_record.cell_id
                )));
            }
        }
        if cell.joined_existing() {
            for stored in directory.records.values() {
                if !replicated.contains_key(&stored.cell_id) {
                    return Err(MetaDbError::ShardRoutingError(format!(
                        "GLOBAL_CELL_DIRECTORY cell {} is not in the replicated snapshot; refusing to invent it",
                        stored.cell_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Propose the loaded directory and the cell's manager snapshot.
    ///
    /// `Ok(false)` means this process did not commit a new snapshot because it is
    /// not the shard leader. The previous snapshot is unchanged.
    pub async fn publish_replicated_directory(&self, cell: &DurableCellManager) -> Result<bool> {
        let records = self.directory.lock().unwrap().records.clone();
        cell.publish_control_plane(&records).await
    }

    /// Publishes this process's cell into the durable directory and the local map.
    ///
    /// Remote rows already in the file are kept. A local range that overlaps one
    /// of them, or that disagrees with the stored local range, refuses to start.
    pub fn install_local_cell(
        &self,
        cell: Arc<DurableCellManager>,
        endpoint: String,
    ) -> Result<()> {
        let endpoint = endpoint.trim().to_string();
        if endpoint.is_empty() {
            return Err(MetaDbError::ShardRoutingError(
                "local cell endpoint is empty".to_string(),
            ));
        }
        let routes = cell.list_routes();
        let range = union_route_range(routes.iter().map(|route| &route.range))?;
        let cluster_id = cell.cluster_id();
        if cluster_id == 0 {
            return Err(MetaDbError::ShardRoutingError(
                "local cell has no cluster identity".to_string(),
            ));
        }
        {
            let mut directory = self.directory.lock().unwrap();
            let Some(path) = directory.path.clone() else {
                return Err(MetaDbError::ShardRoutingError(
                    "cell directory is not loaded".to_string(),
                ));
            };
            for record in directory.records.values() {
                if record.cell_id == cell.cell_id() {
                    if !record.local {
                        return Err(MetaDbError::ShardRoutingError(format!(
                            "cell {} is already a remote registration",
                            cell.cell_id()
                        )));
                    }
                    if record.range != range {
                        return Err(MetaDbError::ShardRoutingError(
                            "durable local cell range does not match this process's routes"
                                .to_string(),
                        ));
                    }
                    continue;
                }
                if record.range.overlaps(&range.start_key, &range.end_key) {
                    return Err(MetaDbError::ShardRoutingError(format!(
                        "local cell {} overlaps cell {}",
                        cell.cell_id(),
                        record.cell_id
                    )));
                }
            }
            let mut next = directory.records.clone();
            next.insert(
                cell.cell_id(),
                DirectoryCell {
                    cell_id: cell.cell_id(),
                    range: range.clone(),
                    endpoints: vec![endpoint],
                    local: true,
                    cluster_id,
                    routes: Vec::new(),
                },
            );
            store_directory(&path, &next)?;
            directory.records = next;
            directory.loaded_from_file = true;
        }
        self.register_cell(range, cell);
        Ok(())
    }

    pub fn precheck_remote_cell(
        &self,
        cell_id: CellId,
        range: &CompositeKeyRange,
        endpoints: &[String],
    ) -> Result<RemotePrecheck> {
        let directory = self.directory.lock().unwrap();
        if directory.path.is_none() {
            return Err(MetaDbError::ShardRoutingError(
                "cell directory is not loaded".to_string(),
            ));
        }
        precheck_remote(&directory.records, cell_id, range, endpoints)
    }

    /// Stores a probed remote cell. `Ok(false)` means the stored publication
    /// was already identical, so nothing was rewritten.
    pub fn commit_remote_cell(&self, publication: RemotePublication) -> Result<bool> {
        let mut directory = self.directory.lock().unwrap();
        let Some(path) = directory.path.clone() else {
            return Err(MetaDbError::ShardRoutingError(
                "cell directory is not loaded".to_string(),
            ));
        };
        let node_ids = self.node_ids_in_use_except(publication.cell_id, &directory.records);
        let Some(stored) = publication_from_probe(&directory.records, publication, &node_ids)?
        else {
            return Ok(false);
        };
        let mut next = directory.records.clone();
        next.insert(stored.cell_id, stored);
        store_directory(&path, &next)?;
        directory.records = next;
        directory.loaded_from_file = true;
        Ok(true)
    }

    pub fn remote_publication(&self, cell_id: CellId) -> Option<DirectoryCell> {
        self.directory
            .lock()
            .unwrap()
            .records
            .get(&cell_id)
            .filter(|record| !record.local)
            .cloned()
    }

    pub fn remote_publications(&self) -> Vec<DirectoryCell> {
        self.directory
            .lock()
            .unwrap()
            .records
            .values()
            .filter(|record| !record.local)
            .cloned()
            .collect()
    }

    pub fn published_cell_count(&self) -> usize {
        let directory = self.directory.lock().unwrap();
        if directory.path.is_some() {
            directory.records.len()
        } else {
            drop(directory);
            self.cells.read().unwrap().len()
        }
    }

    fn node_ids_in_use_except(
        &self,
        except_cell: CellId,
        records: &BTreeMap<CellId, DirectoryCell>,
    ) -> BTreeSet<NodeId> {
        let mut ids = BTreeSet::new();
        for cell in self.all_cells() {
            if cell.cell_id() == except_cell {
                continue;
            }
            for route in cell.list_routes() {
                ids.extend(route.peers.keys().copied());
            }
        }
        for record in records.values() {
            if record.local || record.cell_id == except_cell {
                continue;
            }
            for route in &record.routes {
                ids.extend(route.peers.keys().copied());
            }
        }
        ids
    }

    fn remote_owning_key(&self, key: &[u8]) -> Option<DirectoryCell> {
        self.directory
            .lock()
            .unwrap()
            .records
            .values()
            .find(|record| !record.local && record.range.contains_key(key))
            .cloned()
    }

    fn remote_overlapping_range(&self, start: &[u8], end: &[u8]) -> Option<DirectoryCell> {
        self.directory
            .lock()
            .unwrap()
            .records
            .values()
            .find(|record| !record.local && record.range.overlaps(start, end))
            .cloned()
    }

    /// Where a set of user keys must be applied.
    ///
    /// `Local` means no registered remote cell contains any of the keys. The
    /// caller serves them on this process. `Remote` means one registered remote
    /// cell contains every key. A mix of cells, or a mix of a remote cell and a
    /// key it does not contain, is an error. This method does not dial anyone
    /// and does not write.
    pub fn placement_for_keys(&self, keys: &[&[u8]]) -> Result<CellPlacement> {
        let mut owner: Option<DirectoryCell> = None;
        for key in keys {
            if let Some(found) = self.remote_owning_key(key) {
                if let Some(existing) = &owner {
                    if existing.cell_id != found.cell_id {
                        return Err(MetaDbError::ShardRoutingError(
                            "request keys are owned by more than one remote cell; this process will not apply any of them locally"
                                .to_string(),
                        ));
                    }
                } else {
                    owner = Some(found);
                }
            }
        }
        let Some(owner) = owner else {
            return Ok(CellPlacement::Local);
        };
        for key in keys {
            if !owner.range.contains_key(key) {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "request is split between this process and cell {} at {}; this process will not apply a partial request locally",
                    owner.cell_id,
                    owner.endpoints.join(",")
                )));
            }
        }
        if owner.endpoints.is_empty() {
            return Err(MetaDbError::ShardRoutingError(format!(
                "cell {} has no endpoint; this process will not apply the request locally",
                owner.cell_id
            )));
        }
        Ok(CellPlacement::Remote {
            cell_id: owner.cell_id,
            endpoints: owner.endpoints,
        })
    }

    /// Where a half-open user-key range must be applied.
    ///
    /// A remote cell that overlaps the range but does not contain it is an
    /// error. The caller must not serve the uncovered part locally.
    pub fn placement_for_range(&self, start: &[u8], end: &[u8]) -> Result<CellPlacement> {
        if start >= end {
            return Ok(CellPlacement::Local);
        }
        let mut owner: Option<DirectoryCell> = None;
        let records = self.directory.lock().unwrap().records.clone();
        for record in records.values() {
            if record.local || !record.range.overlaps(start, end) {
                continue;
            }
            if owner.is_some() {
                return Err(MetaDbError::ShardRoutingError(
                    "requested range overlaps more than one remote cell; this process will not apply any of it locally"
                        .to_string(),
                ));
            }
            owner = Some(record.clone());
        }
        let Some(owner) = owner else {
            return Ok(CellPlacement::Local);
        };
        let contained =
            start >= owner.range.start_key.as_slice() && end <= owner.range.end_key.as_slice();
        if !contained {
            return Err(MetaDbError::ShardRoutingError(format!(
                "requested range overlaps cell {} at {} and is not contained by that cell; this process will not apply a partial range locally",
                owner.cell_id,
                owner.endpoints.join(",")
            )));
        }
        if owner.endpoints.is_empty() {
            return Err(MetaDbError::ShardRoutingError(format!(
                "cell {} has no endpoint; this process will not apply the request locally",
                owner.cell_id
            )));
        }
        Ok(CellPlacement::Remote {
            cell_id: owner.cell_id,
            endpoints: owner.endpoints,
        })
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
        if let Some(owner) = self.remote_owning_key(user_key) {
            return Err(MetaDbError::ShardRoutingError(format!(
                "key is owned by cell {} at {}; this process will not apply that key on a local shard",
                owner.cell_id,
                owner.endpoints.join(",")
            )));
        }
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
        if let Some(owner) = self.remote_overlapping_range(start_user_key, end_user_key) {
            return Err(MetaDbError::ShardRoutingError(format!(
                "requested range overlaps cell {} at {}; this process will not apply a partial range on a local shard",
                owner.cell_id,
                owner.endpoints.join(",")
            )));
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

fn directory_cell_matches(stored: &DirectoryCell, replicated: &DirectoryCell) -> bool {
    stored.cell_id == replicated.cell_id
        && stored.range == replicated.range
        && stored.local == replicated.local
        && stored.cluster_id == replicated.cluster_id
        && (replicated.local
            || (stored.endpoints == replicated.endpoints && stored.routes == replicated.routes))
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
        let (token, expire_hlc) = self.id_allocator.acquire_dir_rename_lease(
            self.clock.now(),
            ttl_ms,
            MAX_2PC_LEASE_LIFETIME_MS,
        )?;
        Ok(DirRenameLease {
            rename_fence_token: token,
            expire_hlc,
        })
    }

    async fn extend_dir_rename_lease(
        &self,
        token: u64,
        ttl_ms: u64,
    ) -> Result<Option<metadb_core::HlcTimestamp>> {
        let expire = self
            .id_allocator
            .extend_dir_rename_lease(token, self.clock.now(), ttl_ms)?;
        Ok(Some(expire))
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
