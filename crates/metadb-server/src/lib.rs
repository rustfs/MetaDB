//! Unified MetaDB Server & Cluster Engine (`metadb-server`).
//!
//! Supports seamless three-stage evolution using a single unified codebase:
//! - **Stage 1 (Single-Node All-in-One)**: 1 BE node, 1-Voter Raft, embedded `CellLocalManager` and `MetaCache`.
//! - **Stage 2 (Single-Node Multi-Shard)**: Dynamic `group_key = PID` boundary shard splitting as metadata grows.
//! - **Stage 3 (Distributed Multi-Cell Cluster)**: Zero-downtime Raft `ConfChange` (`1-Voter -> 3-Voter`),
//!   Jumbo Shard dedicated-node isolation, and multi-Cell federation under `GlobalManager`.

pub mod config;
pub mod grpc_services;
pub mod metrics;
pub mod simulation;
pub mod stress;

mod metrics_http;
mod operability;
mod rbac_layer;

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use metadb_security::{InstalledRoles, RoleGrantSpec, RolePresentation, TransportSecurity};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Interceptor;
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status};

/// Server interceptor for metadata, transaction, control-plane, and Raft RPCs.
///
/// An empty secret allows the call. `open_and_serve` only reaches that state when
/// `allow_unauthenticated` is set. A whitespace-only secret rejects every call.
/// Any other secret requires `authorization: Bearer <secret>` exactly.
#[derive(Clone)]
pub struct BearerAuth {
    secret: std::sync::Arc<str>,
}

impl BearerAuth {
    pub fn new(secret: impl AsRef<str>) -> Self {
        Self {
            secret: std::sync::Arc::from(secret.as_ref()),
        }
    }
}

pub(crate) fn bearer_header_matches(presented: &str, secret: &str) -> bool {
    let mut expected = Vec::with_capacity(7 + secret.len());
    expected.extend_from_slice(b"Bearer ");
    expected.extend_from_slice(secret.as_bytes());
    let got = presented.as_bytes();
    let mut diff = got.len() ^ expected.len();
    let n = got.len().max(expected.len());
    for i in 0..n {
        let x = got.get(i).copied().unwrap_or(0);
        let y = expected.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

pub(crate) enum SecretMaterial {
    Absent,
    WhitespaceOnly,
    Configured,
}

pub(crate) fn classify_secret(secret: &str) -> SecretMaterial {
    if secret.is_empty() {
        SecretMaterial::Absent
    } else if secret.trim().is_empty() {
        SecretMaterial::WhitespaceOnly
    } else {
        SecretMaterial::Configured
    }
}

impl Interceptor for BearerAuth {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        match classify_secret(&self.secret) {
            SecretMaterial::Absent => Ok(request),
            SecretMaterial::WhitespaceOnly => Err(Status::unauthenticated(
                "whitespace-only shared secret is not accepted",
            )),
            SecretMaterial::Configured => {
                let presented = request
                    .metadata()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("");
                if bearer_header_matches(presented, &self.secret) {
                    Ok(request)
                } else {
                    Err(Status::unauthenticated(
                        "missing or invalid authorization bearer token",
                    ))
                }
            }
        }
    }
}

/// Client interceptor. Attaches `authorization: Bearer <secret>` to outgoing RPCs.
/// An empty secret leaves the request unchanged. A whitespace-only secret is rejected.
/// An installed role, when present, is sent in addition to the bearer.
#[derive(Clone)]
pub struct BearerAuthClient {
    secret: std::sync::Arc<str>,
    role: Option<RolePresentation>,
}

impl BearerAuthClient {
    pub fn new(secret: impl AsRef<str>) -> Self {
        Self {
            secret: std::sync::Arc::from(secret.as_ref()),
            role: None,
        }
    }

    /// Presents an installed principal in addition to the shared bearer.
    pub fn with_role(mut self, role: RolePresentation) -> Self {
        self.role = Some(role);
        self
    }
}

impl Interceptor for BearerAuthClient {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        match classify_secret(&self.secret) {
            SecretMaterial::Absent => {}
            SecretMaterial::WhitespaceOnly => {
                return Err(Status::unauthenticated(
                    "whitespace-only shared secret is not a credential",
                ));
            }
            SecretMaterial::Configured => {
                let mut token = String::from("Bearer ");
                token.push_str(&self.secret);
                let mut value = tonic::metadata::MetadataValue::try_from(token).map_err(|_| {
                    Status::internal("shared secret is not a valid authorization header value")
                })?;
                value.set_sensitive(true);
                request.metadata_mut().insert("authorization", value);
            }
        }
        if let Some(role) = &self.role {
            role.apply_metadata(request.metadata_mut())
                .map_err(|_| Status::internal("RBAC principal headers could not be attached"))?;
        }
        Ok(request)
    }
}

use metadb_cluster::{
    capture_mono_ns, CellDirectoryRole, CellId, CellLocalManager, CompositeMetaCache,
    DurableCellManager, DurableGlobalManager, DurableIdAllocator, GlobalManager, LocalIdBuffer,
    MetaCache, ShardDescriptor,
};
use metadb_core::{
    EntryType, FileId, HlcTimestamp, HybridLogicalClock, InodeAttr, InodeId, MetaDbError,
    MetaEntry, NodeId, QuotaRecord, Result, SecondaryIndexRecord, ShardId, UserId, ROOT_INODE_ID,
};
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::control_plane_service_server::ControlPlaneServiceServer;
use metadb_proto::metadata_service_server::MetadataServiceServer;
use metadb_proto::raft_transport_service_server::{
    RaftTransportService, RaftTransportServiceServer,
};
use metadb_proto::transaction_service_server::TransactionServiceServer;
use metadb_proto::{
    GetRoutesRequest, RegisterNodeRequest, ScaleShardReplicasRequest, StoreHeartbeatRequest,
};
use metadb_proto::{RaftRpcEnvelope, RaftRpcReply, SnapshotChunkRequest, SnapshotChunkResponse};
use metadb_proxy::{MetadataSqlCommand, SqlTemplateParser, StatelessProxyRouter};
use metadb_raft::backup::{self, OnlineShard};
use metadb_raft::{
    MultiRaftRouter, MultiRaftTransportServer, ProposalShutdownGate, ShardGroupHost,
};
use metadb_storage::mvcc::DEFAULT_MVCC_RETENTION_MS;
use metadb_storage::{LifecycleStorageEngine, ShardedBlockCache};
use metadb_tx::{
    AsyncAttrAggregator, AsyncAuxiliarySink, AuxDispatcher, DistributedTxRouter, PushdownExecutor,
    TwoPhaseCommitCoordinator, TxnRecoveryWorker,
};

pub use config::{ServerRole, ServerTomlConfig};
pub use grpc_services::MetaDbGrpcService;
pub use metrics::{collect_cluster_metrics, render_prometheus_metrics, ClusterMetricsSnapshot};
pub use operability::{evaluate_readiness, ProcessPhase, ReadinessReport, ShardGate};

/// Result returned from executing a lightweight metadata SQL statement through `MetaDbEngine`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlQueryResult {
    Entry(MetaEntry),
    OptionalEntry(Option<MetaEntry>),
    Attr(InodeAttr),
    Entries(Vec<MetaEntry>),
}

/// Configuration for initializing a `MetaDbEngine`.
#[derive(Debug, Clone)]
pub struct MetaDbConfig {
    pub initial_node_id: NodeId,
    pub shard_split_threshold_bytes: usize,
    pub mvcc_retention_ms: u64,
    pub tombstone_density_threshold: usize,
}

impl Default for MetaDbConfig {
    fn default() -> Self {
        Self {
            initial_node_id: 1,
            shard_split_threshold_bytes: 4 * 1024 * 1024 * 1024, // 4 GiB default in production
            mvcc_retention_ms: DEFAULT_MVCC_RETENTION_MS,
            tombstone_density_threshold: 128,
        }
    }
}

impl MetaDbConfig {
    /// Parses a persistent `ServerTomlConfig` from disk for `MetaDbEngine::open(...).await`.
    pub fn from_toml_file(path: impl AsRef<std::path::Path>) -> Result<ServerTomlConfig> {
        ServerTomlConfig::from_toml_file(path)
    }
}

/// Top-level MetaDB engine coordinating the Proxy, MetaCache, Two-Tier Control Plane,
/// Single-Shard Pushdown Executor, 2PC Coordinator, Off-Critical-Path Queues, and
/// Lifecycle-Aware Storage Engines.
pub struct MetaDbEngine {
    clock: Arc<HybridLogicalClock>,
    global_manager: Arc<GlobalManager>,
    pushdown: Arc<PushdownExecutor>,
    two_pc: Arc<TwoPhaseCommitCoordinator>,
}

impl MetaDbEngine {
    /// Opens or bootstraps an asynchronous, persistent WAL-less LSM + OpenRaft MetaDB instance
    /// from `ServerTomlConfig` (convenience wrapper over `PersistentMetaDbCluster::open_and_serve`).
    pub async fn open(config: ServerTomlConfig) -> Result<Arc<PersistentMetaDbCluster>> {
        let (cluster, _addr) = PersistentMetaDbCluster::open_and_serve(config).await?;
        Ok(cluster)
    }

    /// In-memory simulator used by synchronous API-shape tests.
    ///
    /// This does **not** open a data directory, OpenRaft group, or WAL-less LSM.
    /// It is not crash-safe and is not the engine verified by the persistent test
    /// suite. Use [`MetaDbEngine::open`] or [`PersistentMetaDbCluster::open_and_serve`].
    pub fn new_single_node(config: MetaDbConfig) -> Self {
        let clock = Arc::new(HybridLogicalClock::new());
        let meta_cache = Arc::new(MetaCache::new());
        let global_manager = Arc::new(GlobalManager::new(meta_cache));

        let cell_0 = Arc::new(CellLocalManager::new(
            1,
            config.initial_node_id,
            1,
            0,
            u64::MAX,
            config.shard_split_threshold_bytes,
            config.mvcc_retention_ms,
            config.tombstone_density_threshold,
        ));
        global_manager.register_cell(0, u64::MAX, cell_0);

        let attr_aggregator = Arc::new(AsyncAttrAggregator::new());
        let aux_sink = Arc::new(AsyncAuxiliarySink::new());
        let pushdown = Arc::new(PushdownExecutor::new(attr_aggregator, aux_sink));
        let two_pc = Arc::new(TwoPhaseCommitCoordinator::new(pushdown.clone()));

        let engine = Self {
            clock,
            global_manager,
            pushdown,
            two_pc,
        };

        // Initialize root directory `/` (`ROOT_INODE_ID = 1`).
        let hlc = engine.clock.now();
        let (_, _, root_storage) = engine
            .global_manager
            .route_pid(ROOT_INODE_ID)
            .expect("root shard must exist");
        engine
            .pushdown
            .init_directory_attr(&root_storage, ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755, hlc)
            .expect("root directory attribute");

        engine
    }

    pub fn clock(&self) -> &Arc<HybridLogicalClock> {
        &self.clock
    }

    pub fn global_manager(&self) -> &Arc<GlobalManager> {
        &self.global_manager
    }

    pub fn pushdown(&self) -> &Arc<PushdownExecutor> {
        &self.pushdown
    }

    /// Creates a file in `pid` via single-shard pushdown (`1 RTT`) and triggers automatic
    /// policy/size-based shard evaluation.
    #[allow(clippy::too_many_arguments)]
    pub fn create_file(
        &self,
        pid: InodeId,
        name: &str,
        inode_id: InodeId,
        fid: FileId,
        uid: UserId,
        mode: u32,
        size_bytes: i64,
    ) -> Result<MetaEntry> {
        let hlc = self.clock.now();
        let (cell, desc, storage) = self.global_manager.route_pid(pid)?;
        let entry = self.pushdown.create_file(
            &storage, pid, name, inode_id, fid, uid, mode, size_bytes, hlc,
        )?;
        cell.maybe_split_or_isolate_shard(desc.shard_id, hlc)?;
        self.global_manager.refresh_meta_cache();
        Ok(entry)
    }

    /// Creates a subdirectory `name` with `new_dir_id` under `pid`.
    pub fn mkdir(
        &self,
        pid: InodeId,
        name: &str,
        new_dir_id: InodeId,
        uid: UserId,
        mode: u32,
    ) -> Result<MetaEntry> {
        let hlc = self.clock.now();
        let (parent_cell, parent_desc, parent_storage) = self.global_manager.route_pid(pid)?;
        let (_, _, new_dir_storage) = self.global_manager.route_pid(new_dir_id)?;
        let entry = self.pushdown.mkdir(
            &parent_storage,
            &new_dir_storage,
            pid,
            name,
            new_dir_id,
            uid,
            mode,
            hlc,
        )?;
        parent_cell.maybe_split_or_isolate_shard(parent_desc.shard_id, hlc)?;
        self.global_manager.refresh_meta_cache();
        Ok(entry)
    }

    /// Unlinks a file `(pid, name)`.
    pub fn unlink(&self, pid: InodeId, name: &str) -> Result<InodeAttr> {
        let hlc = self.clock.now();
        let (_, _, storage) = self.global_manager.route_pid(pid)?;
        self.pushdown.unlink(&storage, pid, name, hlc)
    }

    /// Removes an empty directory `(pid, name)` after verifying zero live children exist.
    pub fn rmdir(&self, pid: InodeId, name: &str) -> Result<InodeAttr> {
        let hlc = self.clock.now();
        let (_, _, parent_storage) = self.global_manager.route_pid(pid)?;
        let Some(entry) = self.pushdown.lookup(&parent_storage, pid, name, hlc)? else {
            return Err(metadb_core::MetaDbError::NotFound {
                pid,
                name: name.to_string(),
            });
        };
        let (_, _, dir_storage) = self.global_manager.route_pid(entry.attr.inode_id)?;
        self.pushdown
            .rmdir(&parent_storage, &dir_storage, pid, name, hlc)
    }

    /// Atomically renames `(src_pid, src_name)` to `(dst_pid, dst_name)` (within a single shard
    /// or across shards via 2PC).
    pub fn rename(
        &self,
        src_pid: InodeId,
        src_name: &str,
        dst_pid: InodeId,
        dst_name: &str,
    ) -> Result<MetaEntry> {
        let hlc = self.clock.now();
        let (_, _, src_storage) = self.global_manager.route_pid(src_pid)?;
        let (_, _, dst_storage) = self.global_manager.route_pid(dst_pid)?;
        let inode_storage = match self.pushdown.lookup(&src_storage, src_pid, src_name, hlc)? {
            Some(entry) if entry.attr.entry_type == EntryType::Directory => {
                self.global_manager.route_pid(entry.attr.inode_id)?.2
            }
            _ => src_storage.clone(),
        };
        self.two_pc.rename(
            &src_storage,
            &dst_storage,
            &inode_storage,
            src_pid,
            src_name,
            dst_pid,
            dst_name,
            hlc,
        )
    }

    /// Point lookup for `(pid, name)`.
    pub fn lookup(&self, pid: InodeId, name: &str) -> Result<Option<MetaEntry>> {
        let hlc = self.clock.now();
        let (_, _, storage) = self.global_manager.route_pid(pid)?;
        self.pushdown.lookup(&storage, pid, name, hlc)
    }

    /// Returns linearizable directory attributes `(pid, "_attr_")` via Read-Path Reconciliation.
    pub fn dirstat(&self, pid: InodeId) -> Result<InodeAttr> {
        let hlc = self.clock.now();
        let (_, _, storage) = self.global_manager.route_pid(pid)?;
        self.pushdown.dirstat(&storage, pid, hlc)
    }

    /// Lists all live child entries in directory `pid`.
    pub fn listdir(&self, pid: InodeId) -> Result<Vec<MetaEntry>> {
        self.listdir_limited(pid, None, None, usize::MAX)
    }

    /// Pinned directory page. `limit == usize::MAX` reads the whole directory.
    pub fn listdir_limited(
        &self,
        pid: InodeId,
        after_name: Option<&str>,
        name_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MetaEntry>> {
        let hlc = self.clock.now();
        let (_, _, storage) = self.global_manager.route_pid(pid)?;
        self.pushdown
            .listdir_limited(&storage, pid, after_name, name_prefix, limit, hlc)
    }

    /// Executes a lightweight metadata SQL template statement directly without full SQL optimizer overhead.
    pub fn execute_sql(&self, sql: &str) -> Result<SqlQueryResult> {
        match SqlTemplateParser::parse(sql)? {
            MetadataSqlCommand::CreateFile {
                pid,
                name,
                inode_id,
                fid,
                uid,
                mode,
                size_bytes,
            } => Ok(SqlQueryResult::Entry(self.create_file(
                pid, &name, inode_id, fid, uid, mode, size_bytes,
            )?)),
            MetadataSqlCommand::Mkdir {
                pid,
                name,
                dir_id,
                uid,
                mode,
            } => Ok(SqlQueryResult::Entry(
                self.mkdir(pid, &name, dir_id, uid, mode)?,
            )),
            MetadataSqlCommand::Unlink { pid, name } => {
                Ok(SqlQueryResult::Attr(self.unlink(pid, &name)?))
            }
            MetadataSqlCommand::Rmdir { pid, name } => {
                Ok(SqlQueryResult::Attr(self.rmdir(pid, &name)?))
            }
            MetadataSqlCommand::Lookup { pid, name } => {
                Ok(SqlQueryResult::OptionalEntry(self.lookup(pid, &name)?))
            }
            MetadataSqlCommand::DirStat { pid } => Ok(SqlQueryResult::Attr(self.dirstat(pid)?)),
            MetadataSqlCommand::ListDir {
                pid,
                after_name,
                name_prefix,
                limit,
            } => {
                let page_limit = if limit == 0 { usize::MAX } else { limit };
                let rows = self.listdir_limited(
                    pid,
                    after_name.as_deref(),
                    name_prefix.as_deref(),
                    page_limit,
                )?;
                Ok(SqlQueryResult::Entries(rows))
            }
            MetadataSqlCommand::Rename {
                src_pid,
                src_name,
                dst_pid,
                dst_name,
                ..
            } => Ok(SqlQueryResult::Entry(
                self.rename(src_pid, &src_name, dst_pid, &dst_name)?,
            )),
            other => Err(metadb_core::MetaDbError::SqlParseError(format!(
                "use async ClusterEngine / StatelessProxyRouter for template {:?}",
                other
            ))),
        }
    }

    /// Flushes asynchronous attribute aggregations, drains the asynchronous auxiliary sink
    /// (`QuotaTable` and `SecondaryIndexTable`), and flushes `In-Memory MVCC` across all shards.
    pub fn run_background_maintenance(&self) -> Result<()> {
        let hlc: HlcTimestamp = self.clock.now();
        let mut all_engines: Vec<Arc<LifecycleStorageEngine>> = Vec::new();
        for cell in self.global_manager.cells() {
            for engine in cell.all_engines() {
                self.pushdown
                    .attr_aggregator()
                    .flush_aggregated_deltas(&engine, hlc)?;
                all_engines.push(engine);
            }
        }

        let engine_refs: Vec<&LifecycleStorageEngine> =
            all_engines.iter().map(|e| e.as_ref()).collect();
        self.pushdown.aux_sink().drain_and_apply(&engine_refs);

        for engine in &all_engines {
            engine.flush_mvcc_to_persistent();
        }
        Ok(())
    }

    pub fn get_user_quota(&self, uid: UserId) -> Option<QuotaRecord> {
        self.pushdown.aux_sink().get_quota(uid)
    }

    pub fn get_secondary_index(&self, fid: FileId) -> Option<SecondaryIndexRecord> {
        self.pushdown.aux_sink().get_secondary_index(fid)
    }

    pub fn list_all_shards(&self) -> Vec<(CellId, ShardDescriptor)> {
        let mut out = Vec::new();
        for cell in self.global_manager.cells() {
            for shard in cell.list_shards() {
                out.push((cell.cell_id(), shard));
            }
        }
        out
    }
}

/// Production asynchronous, disk-persistent MetaDB server & embedded SDK (`PersistentMetaDbCluster`).
///
/// Orchestrates:
/// - `DurableGlobalManager` + `DurableCellManager` + `CompositeMetaCache` + `DurableIdAllocator`
/// - `ShardGroupHost` (`openraft` Multi-Raft + WAL-less `LsmStorageBackend`)
/// - `DistributedTxRouter` + `StatelessProxyRouter`
/// - Background `AuxDispatcher` & `TxnRecoveryWorker` loops
/// - Multiplexed `tonic` gRPC server (`MetadataService`, `TransactionService`, `ControlPlaneService`, `RaftTransportService`)
pub struct PersistentMetaDbCluster {
    config: ServerTomlConfig,
    clock: Arc<HybridLogicalClock>,
    id_buffer: Arc<LocalIdBuffer>,
    host: Option<Arc<ShardGroupHost>>,
    cell_manager: Option<Arc<DurableCellManager>>,
    /// `global-manager` publishes the cell directory and does not host Raft groups.
    directory_only: bool,
    /// `proxy` forwards metadata to one upstream and does not host Raft groups.
    proxy_gateway: bool,
    global_manager: Arc<DurableGlobalManager>,
    tx_router: Arc<DistributedTxRouter>,
    proxy_router: Arc<StatelessProxyRouter>,
    shutdown_tx: watch::Sender<bool>,
    task_handles: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    metrics_addr: OnceLock<String>,
    operability: Arc<operability::Operability>,
    transport_security: Arc<TransportSecurity>,
    roles: Option<Arc<InstalledRoles>>,
}

async fn serve_global_directory(
    config: ServerTomlConfig,
    listener: TcpListener,
    bound_addr: String,
    transport_security: Arc<TransportSecurity>,
    roles: Option<Arc<InstalledRoles>>,
) -> Result<(Arc<PersistentMetaDbCluster>, String)> {
    let clock = Arc::new(HybridLogicalClock::with_max_drift_ms(
        config.clock.max_clock_drift_ms,
    ));
    let id_allocator = Arc::new(DurableIdAllocator::open_for_node(
        config.node.data_dir.join("global_allocator"),
        config.node.node_id,
    )?);
    let id_buffer = Arc::new(LocalIdBuffer::new(
        id_allocator.clone(),
        config.node.node_id,
    ));
    let meta_cache = Arc::new(CompositeMetaCache::new());
    let global_manager = Arc::new(DurableGlobalManager::new(
        clock.clone(),
        id_allocator,
        meta_cache,
    ));
    global_manager.load_or_create_directory(
        config
            .node
            .data_dir
            .join(metadb_cluster::CELL_DIRECTORY_FILE),
    )?;
    let tx_router = Arc::new(
        DistributedTxRouter::new(config.node.node_id, clock.clone(), global_manager.clone())
            .with_forward_secret(config.security.shared_secret.clone())
            .with_transport_security(Arc::clone(&transport_security))
            .with_role_presentation(roles.as_ref().map(|roles| roles.local().clone())),
    );
    let proxy_router = Arc::new(StatelessProxyRouter::new(tx_router.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let operability = operability::Operability::starting();
    let cluster = Arc::new(PersistentMetaDbCluster {
        config,
        clock,
        id_buffer,
        host: None,
        cell_manager: None,
        directory_only: true,
        proxy_gateway: false,
        global_manager,
        tx_router,
        proxy_router,
        shutdown_tx,
        task_handles: tokio::sync::Mutex::new(Vec::new()),
        metrics_addr: OnceLock::new(),
        operability,
        transport_security: Arc::clone(&transport_security),
        roles: roles.clone(),
    });
    let shared_secret = cluster.config.security.shared_secret.clone();
    let grpc_svc = MetaDbGrpcService::new(
        cluster.global_manager.clone(),
        cluster.tx_router.clone(),
        cluster.proxy_router.clone(),
    )
    .with_forward_secret(shared_secret.clone())
    .with_transport_security(Arc::clone(&transport_security))
    .with_role_presentation(roles.as_ref().map(|roles| roles.local().clone()))
    .with_directory_only(true);
    let grpc_handle = spawn_grpc_server(
        listener,
        grpc_svc,
        shared_secret,
        RaftIngress::Groups(MultiRaftTransportServer::new(MultiRaftRouter::new())),
        shutdown_rx,
        Arc::clone(&transport_security),
        roles,
    );
    cluster.task_handles.lock().await.push(grpc_handle);
    if let Err(error) = spawn_metrics_listener(&cluster).await {
        cluster.operability.fail();
        let _ = cluster.shutdown_without_flush().await;
        return Err(error);
    }
    cluster.operability.mark_startup_finished();
    std::fs::write(
        cluster.config().node.data_dir.join("GRPC_ADDR"),
        &bound_addr,
    )?;
    Ok((cluster, bound_addr))
}

async fn register_cell_with_global_manager(
    cluster: &PersistentMetaDbCluster,
    global_addr: &str,
    bound_addr: &str,
) -> Result<()> {
    let cell = cluster.cell_manager();
    let routes = cell.list_routes();
    if routes.is_empty() {
        return Err(MetaDbError::ShardRoutingError(
            "cell-manager has no local routes to publish".to_string(),
        ));
    }
    let start_key = routes
        .iter()
        .map(|route| route.range.start_key.clone())
        .min()
        .expect("route");
    let end_key = routes
        .iter()
        .map(|route| route.range.end_key.clone())
        .max()
        .expect("route");
    wait_until_accepting(bound_addr).await;
    let endpoint = cluster
        .transport_security
        .prepare_url(global_addr)
        .map_err(|error| {
            MetaDbError::ShardRoutingError(format!(
                "global-manager address {global_addr} is not usable with the configured transport: {error}"
            ))
        })?;
    let mut client = control_plane_client(
        &endpoint,
        &cluster.config.security.shared_secret,
        &cluster.transport_security,
        cluster.roles().map(|roles| roles.local()),
    )
    .await?;
    let response = client
        .register_cell(metadb_proto::RegisterCellRequest {
            cell_id: cell.cell_id(),
            start_key,
            end_key,
            cell_manager_endpoints: vec![bound_addr.to_string()],
        })
        .await
        .map_err(|error| {
            MetaDbError::ShardRoutingError(format!(
                "global-manager at {endpoint} did not accept this cell: {error}"
            ))
        })?
        .into_inner();
    if let Some(error) = rpc_app_error(response.error) {
        return Err(MetaDbError::ShardRoutingError(format!(
            "global-manager at {endpoint} rejected this cell: {error}"
        )));
    }
    Ok(())
}

fn validate_role_topology(config: &ServerTomlConfig, has_cell_range: bool) -> Result<()> {
    let join = !config.cluster.join.trim().is_empty();
    let global = !config.cluster.global_manager.trim().is_empty();
    let upstream = config.cluster.upstream.trim();
    let jumbo = !config.sharding.jumbo_dedicated_root.trim().is_empty();
    match config.node.role {
        ServerRole::Proxy => {
            if upstream.is_empty() {
                return Err(MetaDbError::CodecError(
                    "proxy requires [cluster].upstream, the host:port of one shard host. This process does not open Raft groups and does not choose a cell by itself"
                        .to_string(),
                ));
            }
            if upstream.starts_with("https://") {
                return Err(MetaDbError::CodecError(
                    "proxy does not accept an https upstream; this process does not terminate TLS"
                        .to_string(),
                ));
            }
            if join || global || has_cell_range || jumbo {
                return Err(MetaDbError::CodecError(
                    "proxy forwards to one upstream; it does not join a shard, publish a cell, own a cell range, or place jumbo files"
                        .to_string(),
                ));
            }
        }
        ServerRole::Be => {
            if global {
                return Err(MetaDbError::CodecError(
                    "be hosts local shard replicas and does not publish a cell; use role cell-manager"
                        .to_string(),
                ));
            }
            if !upstream.is_empty() {
                return Err(MetaDbError::CodecError(
                    "be does not forward to an upstream; [cluster].upstream is only for role proxy"
                        .to_string(),
                ));
            }
        }
        ServerRole::GlobalManager => {
            if join || global || has_cell_range || jumbo || !upstream.is_empty() {
                return Err(MetaDbError::CodecError(
                    "global-manager stores the cell directory only; it does not join a shard, own a cell range, place jumbo files, or forward to an upstream"
                        .to_string(),
                ));
            }
        }
        ServerRole::AllInOne => {
            if global {
                return Err(MetaDbError::CodecError(
                    "all-in-one does not register with a separate global-manager; use role cell-manager"
                        .to_string(),
                ));
            }
            if !upstream.is_empty() {
                return Err(MetaDbError::CodecError(
                    "all-in-one does not forward to an upstream; [cluster].upstream is only for role proxy"
                        .to_string(),
                ));
            }
        }
        ServerRole::CellManager => {
            if !upstream.is_empty() {
                return Err(MetaDbError::CodecError(
                    "cell-manager does not forward to an upstream; [cluster].upstream is only for role proxy"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

async fn serve_proxy(
    config: ServerTomlConfig,
    transport_security: Arc<TransportSecurity>,
    roles: Option<Arc<InstalledRoles>>,
) -> Result<(Arc<PersistentMetaDbCluster>, String)> {
    let upstream = config.cluster.upstream.trim().to_string();
    if config.security.shared_secret.trim().is_empty()
        && !config::is_literal_loopback_addr(&upstream)
    {
        return Err(MetaDbError::CodecError(
            "unauthenticated proxy only forwards to a literal loopback upstream. Set a shared secret before using a non-loopback upstream, or point upstream at 127.0.0.0/8 or ::1. This process does not terminate TLS"
                .to_string(),
        ));
    }
    let shard_tree = config.node.data_dir.join("be");
    let cell_state = config
        .node
        .data_dir
        .join(format!("cell_{}", config.node.cell_id))
        .join("CELL_MANAGER_STATE");
    if shard_tree.exists() || cell_state.exists() {
        return Err(MetaDbError::CodecError(
            "proxy does not host shard files; this data directory already has shard or cell state"
                .to_string(),
        ));
    }
    let listener = TcpListener::bind(&config.node.grpc_listen_addr).await?;
    let bound = listener.local_addr().map_err(|error| {
        MetaDbError::StorageIoError(format!("proxy listener has no local address: {error}"))
    })?;
    let bound_addr = bound.to_string();
    config.reject_unauthenticated_bind(&bound_addr)?;

    std::fs::create_dir_all(&config.node.data_dir)?;
    let clock = Arc::new(HybridLogicalClock::with_max_drift_ms(
        config.clock.max_clock_drift_ms,
    ));
    let id_allocator = Arc::new(DurableIdAllocator::open_for_node(
        config.node.data_dir.join("global_allocator"),
        config.node.node_id,
    )?);
    let id_buffer = Arc::new(LocalIdBuffer::new(
        id_allocator.clone(),
        config.node.node_id,
    ));
    let meta_cache = Arc::new(CompositeMetaCache::new());
    let global_manager = Arc::new(DurableGlobalManager::new(
        clock.clone(),
        id_allocator,
        meta_cache,
    ));
    let tx_router = Arc::new(
        DistributedTxRouter::new(config.node.node_id, clock.clone(), global_manager.clone())
            .with_forward_secret(config.security.shared_secret.clone())
            .with_transport_security(Arc::clone(&transport_security))
            .with_role_presentation(roles.as_ref().map(|roles| roles.local().clone())),
    );
    let proxy_router = Arc::new(StatelessProxyRouter::new(tx_router.clone()));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let operability = operability::Operability::starting();
    let shared_secret = config.security.shared_secret.clone();
    let cluster = Arc::new(PersistentMetaDbCluster {
        config,
        clock,
        id_buffer,
        host: None,
        cell_manager: None,
        directory_only: false,
        proxy_gateway: true,
        global_manager,
        tx_router,
        proxy_router,
        shutdown_tx,
        task_handles: tokio::sync::Mutex::new(Vec::new()),
        metrics_addr: OnceLock::new(),
        operability,
        transport_security: Arc::clone(&transport_security),
        roles: roles.clone(),
    });
    let grpc_svc = MetaDbGrpcService::new(
        cluster.global_manager.clone(),
        cluster.tx_router.clone(),
        cluster.proxy_router.clone(),
    )
    .with_forward_secret(shared_secret.clone())
    .with_transport_security(Arc::clone(&transport_security))
    .with_role_presentation(roles.as_ref().map(|roles| roles.local().clone()))
    .with_upstream(upstream)
    .with_remote_cell_registration(false)
    .with_shard_host_only(false);
    let grpc_handle = spawn_grpc_server(
        listener,
        grpc_svc,
        shared_secret,
        RaftIngress::Proxy,
        shutdown_rx,
        transport_security,
        roles,
    );
    cluster.task_handles.lock().await.push(grpc_handle);
    if let Err(error) = spawn_metrics_listener(&cluster).await {
        cluster.operability.fail();
        let _ = cluster.shutdown_without_flush().await;
        return Err(error);
    }
    cluster.operability.mark_startup_finished();
    std::fs::write(
        cluster.config().node.data_dir.join("GRPC_ADDR"),
        &bound_addr,
    )?;
    Ok((cluster, bound_addr))
}

impl PersistentMetaDbCluster {
    /// Opens or bootstraps a persistent MetaDB instance from `config` and starts its gRPC listener
    /// (binding to `config.node.grpc_listen_addr`, e.g., `"127.0.0.1:0"` for OS-assigned ephemeral ports).
    ///
    /// When `config.cluster.join` is set and this data directory does not already own a cell,
    /// the process registers with that leader and joins every shard Raft group before returning.
    ///
    /// Transport resolution runs before this function creates a data directory or binds a
    /// socket. The carried transport is tonic gRPC over TCP. `mode = "rdma"` returns an
    /// error. `mode = "prefer-rdma"` logs an explicit fallback and still serves TCP/gRPC.
    pub async fn open_and_serve(config: ServerTomlConfig) -> Result<(Arc<Self>, String)> {
        let transport = config.resolve_transport()?;
        if transport.explicit_rdma_fallback() {
            tracing::warn!(
                selected = "tcp-grpc",
                rejection = "backend-not-implemented",
                "RDMA was requested and is not implemented; carrying traffic on tonic gRPC over TCP. No RDMA resources were allocated"
            );
        }
        metadb_cluster::node_id_in_ha_range(config.node.node_id)?;
        let owned_range = config.configured_cell_range()?;
        validate_role_topology(&config, owned_range.is_some())?;
        let directory_only = config.node.role == ServerRole::GlobalManager;
        config.raft_options.validate()?;
        config.reject_unauthenticated_bind(&config.node.grpc_listen_addr)?;
        if let Some(metrics_addr) = &config.node.metrics_listen_addr {
            config.reject_unauthenticated_bind(metrics_addr)?;
        }
        let tls_spec = config.security.tls.to_spec()?;
        tls_spec
            .reject_listen_host(&config.node.grpc_listen_addr)
            .map_err(security_startup_error)?;
        if let Some(metrics_addr) = &config.node.metrics_listen_addr {
            tls_spec
                .reject_listen_host(metrics_addr)
                .map_err(security_startup_error)?;
        }
        if !config.cluster.join.trim().is_empty() {
            tls_spec
                .prepare_url(&config.cluster.join)
                .map_err(security_startup_error)?;
        }
        if !config.cluster.global_manager.trim().is_empty() {
            tls_spec
                .prepare_url(&config.cluster.global_manager)
                .map_err(security_startup_error)?;
        }
        if config.node.role == ServerRole::Proxy {
            tls_spec
                .prepare_url(config.cluster.upstream.trim())
                .map_err(security_startup_error)?;
        }
        // TLS mode is rejected here, before a data directory or listener exists.
        // Certificate files are not opened.
        let transport_security =
            Arc::new(TransportSecurity::load(&tls_spec).map_err(security_startup_error)?);
        let roles = install_configured_roles(&config)?;
        if config.node.role == ServerRole::Proxy {
            return serve_proxy(config, transport_security, roles).await;
        }
        std::fs::create_dir_all(&config.node.data_dir)?;
        if directory_only {
            let shard_tree = config.node.data_dir.join("be");
            let cell_state = config
                .node
                .data_dir
                .join(format!("cell_{}", config.node.cell_id))
                .join("CELL_MANAGER_STATE");
            if shard_tree.exists() || cell_state.exists() {
                return Err(MetaDbError::CodecError(
                    "global-manager stores the cell directory only; this data directory already has shard or cell state"
                        .to_string(),
                ));
            }
        }
        let cell_dir = config
            .node
            .data_dir
            .join(format!("cell_{}", config.node.cell_id));
        let persisted_addr = if cell_dir.join("CELL_MANAGER_STATE").exists() {
            DurableCellManager::persisted_advertise_addr(&cell_dir, config.node.node_id)?
        } else {
            None
        };
        let listen_addr = if listen_port_is_ephemeral(&config.node.grpc_listen_addr) {
            persisted_addr
                .clone()
                .unwrap_or_else(|| config.node.grpc_listen_addr.clone())
        } else {
            config.node.grpc_listen_addr.clone()
        };
        config.reject_unauthenticated_bind(&listen_addr)?;
        tls_spec
            .reject_listen_host(&listen_addr)
            .map_err(security_startup_error)?;
        let listener = match TcpListener::bind(&listen_addr).await {
            Ok(listener) => listener,
            Err(error)
                if error.kind() == std::io::ErrorKind::AddrInUse && persisted_addr.is_some() =>
            {
                // The saved port is busy. A sole voter can move; several voters cannot.
                // `boot_local_shards` checks that after this bind. Re-check the fallback
                // string before binding it; do not resolve a hostname a second time.
                config.reject_unauthenticated_bind(&config.node.grpc_listen_addr)?;
                TcpListener::bind(&config.node.grpc_listen_addr).await?
            }
            Err(error) => return Err(error.into()),
        };
        let bound_addr = listener.local_addr()?.to_string();
        // The socket is already the address that was validated. This rejects a
        // bound IP that is not literal loopback when no bearer is configured.
        config.reject_unauthenticated_bind(&bound_addr)?;
        tls_spec
            .reject_listen_host(&bound_addr)
            .map_err(security_startup_error)?;
        if directory_only {
            return serve_global_directory(config, listener, bound_addr, transport_security, roles)
                .await;
        }

        let clock = Arc::new(HybridLogicalClock::with_max_drift_ms(
            config.clock.max_clock_drift_ms,
        ));
        let block_cache = Arc::new(ShardedBlockCache::new(config.storage.block_cache_bytes));
        let id_allocator = Arc::new(DurableIdAllocator::open_for_node(
            config.node.data_dir.join("global_allocator"),
            config.node.node_id,
        )?);
        let id_buffer = Arc::new(LocalIdBuffer::new(
            id_allocator.clone(),
            config.node.node_id,
        ));
        let meta_cache = Arc::new(CompositeMetaCache::new());
        let host = Arc::new(ShardGroupHost::open(
            config.node.node_id,
            config.node.data_dir.join("be"),
            block_cache,
            clock.clone(),
        )?);
        backup::register_runtime(&config.node.data_dir)?;
        host.set_shared_secret(Some(config.security.shared_secret.clone()));
        host.set_transport_security((*transport_security).clone());
        host.set_role_presentation(roles.as_ref().map(|roles| roles.local().clone()));
        let raft_router = host.router().clone();
        let directory_role = if cell_dir.join("CELL_MANAGER_STATE").exists() {
            DurableCellManager::inspect_directory(&cell_dir)?
        } else {
            CellDirectoryRole::Absent
        };
        let join_addr = config.cluster.join.trim().to_string();
        let join_fresh = !join_addr.is_empty()
            && matches!(
                directory_role,
                CellDirectoryRole::Absent | CellDirectoryRole::Empty | CellDirectoryRole::Joining
            );
        if !join_addr.is_empty() && !join_fresh {
            tracing::info!(
                join = %join_addr,
                "data directory already owns a cell; reopening local Raft state"
            );
        }

        let lsm_config = config.to_lsm_config()?;
        let cell_manager = if join_fresh {
            DurableCellManager::open_joining(
                config.node.cell_id,
                &cell_dir,
                host.clone(),
                bound_addr.clone(),
                config.sharding.shard_split_threshold_bytes,
                lsm_config,
                config.raft_options.clone(),
                id_allocator.clone(),
                meta_cache.clone(),
            )
            .await?
        } else {
            DurableCellManager::open_or_bootstrap_in_range(
                config.node.cell_id,
                &cell_dir,
                host.clone(),
                bound_addr.clone(),
                config.sharding.bootstrap_table_shards,
                config.sharding.shard_split_threshold_bytes,
                lsm_config,
                config.raft_options.clone(),
                id_allocator.clone(),
                meta_cache.clone(),
                owned_range,
                {
                    let text = config.sharding.jumbo_dedicated_root.trim();
                    if text.is_empty() {
                        None
                    } else {
                        Some(std::path::PathBuf::from(text))
                    }
                },
            )
            .await?
        };

        let global_manager = Arc::new(DurableGlobalManager::new(
            clock.clone(),
            id_allocator,
            meta_cache,
        ));
        global_manager.load_or_create_directory(
            config
                .node
                .data_dir
                .join(metadb_cluster::CELL_DIRECTORY_FILE),
        )?;
        if !join_fresh {
            global_manager.align_directory_with_replication(&cell_manager)?;
            global_manager.install_local_cell(cell_manager.clone(), bound_addr.clone())?;
            global_manager
                .publish_replicated_directory(&cell_manager)
                .await?;
        }
        let tx_router = Arc::new(
            DistributedTxRouter::new(config.node.node_id, clock.clone(), global_manager.clone())
                .with_forward_secret(config.security.shared_secret.clone())
                .with_transport_security(Arc::clone(&transport_security))
                .with_role_presentation(roles.as_ref().map(|roles| roles.local().clone())),
        );
        let proxy_router = Arc::new(StatelessProxyRouter::new(tx_router.clone()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let operability = operability::Operability::starting();
        let cluster = Arc::new(Self {
            config,
            clock,
            id_buffer,
            host: Some(host),
            cell_manager: Some(cell_manager),
            directory_only: false,
            proxy_gateway: false,
            global_manager,
            tx_router,
            proxy_router,
            shutdown_tx,
            task_handles: tokio::sync::Mutex::new(Vec::new()),
            metrics_addr: OnceLock::new(),
            operability,
            transport_security: Arc::clone(&transport_security),
            roles: roles.clone(),
        });
        let shared_secret = cluster.config.security.shared_secret.clone();
        if shared_secret.is_empty() {
            tracing::warn!(
                listen = %bound_addr,
                tls_mode = %transport_security.mode(),
                "allow_unauthenticated is set. Loopback gRPC and metrics listeners have no bearer. \
                 TLS mode does not assign roles. Non-loopback binds are still refused."
            );
        } else if !transport_security.is_plaintext() {
            tracing::info!(
                tls_mode = %transport_security.mode(),
                server_name = transport_security.server_name().unwrap_or(""),
                "tls transport enabled"
            );
        }
        let grpc_svc = MetaDbGrpcService::new(
            cluster.global_manager.clone(),
            cluster.tx_router.clone(),
            cluster.proxy_router.clone(),
        )
        .with_forward_secret(cluster.config.security.shared_secret.clone())
        .with_transport_security(Arc::clone(&transport_security))
        .with_role_presentation(roles.as_ref().map(|roles| roles.local().clone()))
        .with_remote_cell_registration(
            cluster.config.node.role != ServerRole::CellManager
                && cluster.config.node.role != ServerRole::Be,
        )
        .with_shard_host_only(cluster.config.node.role == ServerRole::Be);
        let grpc_handle = spawn_grpc_server(
            listener,
            grpc_svc,
            shared_secret,
            RaftIngress::Groups(MultiRaftTransportServer::new(raft_router)),
            shutdown_rx,
            Arc::clone(&transport_security),
            roles,
        );
        cluster.task_handles.lock().await.push(grpc_handle);
        // The probe listener is up before join so a scrape observes `starting`
        // instead of a later 200 that only means the socket is bound.
        if let Err(error) = spawn_metrics_listener(&cluster).await {
            cluster.operability.fail();
            let _ = cluster.shutdown_without_flush().await;
            return Err(error);
        }

        if join_fresh {
            // Raft RPCs from the leader arrive on this listener while membership is proposed.
            wait_until_accepting(&bound_addr).await;
            if let Err(error) = join_existing_raft_group(
                cluster.config(),
                cluster.cell_manager(),
                &bound_addr,
                &cluster.transport_security,
                cluster.roles().map(|roles| roles.local()),
            )
            .await
            {
                cluster.operability.fail();
                let _ = cluster.shutdown_without_flush().await;
                return Err(error);
            }
            if let Err(error) = cluster
                .cell_manager()
                .reconcile_replicated_membership()
                .and_then(|()| {
                    cluster
                        .global_manager
                        .align_directory_with_replication(cluster.cell_manager())
                })
                .and_then(|()| {
                    cluster
                        .global_manager
                        .install_local_cell(cluster.cell_manager().clone(), bound_addr.clone())
                })
            {
                cluster.operability.fail();
                let _ = cluster.shutdown_without_flush().await;
                return Err(error);
            }
            if let Err(error) = cluster
                .global_manager
                .publish_replicated_directory(cluster.cell_manager())
                .await
            {
                cluster.operability.fail();
                let _ = cluster.shutdown_without_flush().await;
                return Err(error);
            }
        }

        // A joined replica already has the root directory in the replicated state machine.
        if !cluster.cell_manager().joined_existing()
            && cluster.tx_router().dirstat(ROOT_INODE_ID).await.is_err()
        {
            let _ = cluster
                .tx_router()
                .init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
                .await;
        }
        if cluster.config.node.role == ServerRole::CellManager {
            let global = cluster.config.cluster.global_manager.trim().to_string();
            if !global.is_empty() {
                if let Err(error) =
                    register_cell_with_global_manager(&cluster, &global, &bound_addr).await
                {
                    cluster.operability.fail();
                    let _ = cluster.shutdown_without_flush().await;
                    return Err(error);
                }
            }
        }
        cluster.operability.mark_startup_finished();
        let raft = &cluster.config.raft_options;
        tracing::info!(
            heartbeat_ms = raft.heartbeat_interval_ms,
            election_min_ms = raft.election_timeout_min_ms,
            election_max_ms = raft.election_timeout_max_ms,
            max_in_snapshot_log_to_keep = raft.max_in_snapshot_log_to_keep,
            max_segment_bytes = raft.max_segment_bytes,
            "applied raft options"
        );

        // Background maintenance task (`AuxDispatcher` + `TxnRecoveryWorker` every 250ms).
        let bg_cluster = Arc::downgrade(&cluster);
        let mut bg_rx = cluster.shutdown_tx.subscribe();
        let bg_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = bg_rx.changed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                }
                if *bg_rx.borrow() {
                    break;
                }
                let Some(c) = bg_cluster.upgrade() else {
                    break;
                };
                // Drop an in-flight tick when shutdown is signaled. The tick proposes
                // through Raft, and a missing voter can leave that proposal unable to commit.
                tokio::select! {
                    biased;
                    _ = bg_rx.changed() => break,
                    _ = c.run_maintenance_tick_inner(false) => {}
                }
            }
        });
        cluster.task_handles.lock().await.push(bg_handle);

        // Publish only after bootstrap or join has finished. The file means the
        // gRPC port is accepting; `/readyz` is the traffic gate.
        std::fs::write(
            cluster.config().node.data_dir.join("GRPC_ADDR"),
            &bound_addr,
        )?;
        Ok((cluster, bound_addr))
    }

    pub fn config(&self) -> &ServerTomlConfig {
        &self.config
    }

    pub fn tx_router(&self) -> &Arc<DistributedTxRouter> {
        &self.tx_router
    }

    pub fn proxy_router(&self) -> &Arc<StatelessProxyRouter> {
        &self.proxy_router
    }

    pub fn cell_manager(&self) -> &Arc<DurableCellManager> {
        self.cell_manager
            .as_ref()
            .expect("global-manager has no local cell")
    }

    pub fn global_manager(&self) -> &Arc<DurableGlobalManager> {
        &self.global_manager
    }

    pub fn id_buffer(&self) -> &Arc<LocalIdBuffer> {
        &self.id_buffer
    }

    /// Creates a file under `pid` automatically allocating bit-63 `InodeId` and backing `FileId`.
    pub async fn create_file_auto_id(
        &self,
        pid: InodeId,
        name: &str,
        uid: UserId,
        mode: u32,
        size_bytes: i64,
    ) -> Result<MetaEntry> {
        let inode_id = self.id_buffer.next_file_inode_id()?;
        let fid = self.id_buffer.next_file_id()?;
        self.tx_router
            .create_file(pid, name, inode_id, fid, uid, mode, size_bytes)
            .await
    }

    /// Creates a subdirectory under `pid` automatically allocating a directory `PID` (bit 63 = `0`).
    pub async fn mkdir_auto_id(
        &self,
        pid: InodeId,
        name: &str,
        uid: UserId,
        mode: u32,
    ) -> Result<MetaEntry> {
        let dir_id = self.id_buffer.next_dir_inode_id()?;
        self.tx_router.mkdir(pid, name, dir_id, uid, mode).await
    }

    /// Runs one deterministic pass of `AuxDispatcher`, `TxnRecoveryWorker`, and split checks.
    pub async fn run_maintenance_tick(&self) -> Result<()> {
        self.run_maintenance_tick_inner(true).await
    }

    /// One background-loop pass. It skips a new snapshot when the log tail is small, but still
    /// applies the storage purge clamp so a released backup hold can discard that tail.
    pub async fn run_background_maintenance_tick(&self) -> Result<()> {
        self.run_maintenance_tick_inner(false).await
    }

    async fn run_maintenance_tick_inner(&self, force_snapshot: bool) -> Result<()> {
        // Operator backup jobs and pending holds are latched on this loop so a
        // `metadb-ctl` process can ask the running server to copy under its log lock.
        let _ = self.poll_backup_requests();
        for cell in self.global_manager.all_cells() {
            for (_, grp, handler) in cell.local_shard_handles() {
                let _ = AuxDispatcher::drain_shard_outbox(
                    &grp,
                    &handler,
                    self.global_manager.as_ref(),
                    &self.clock,
                )
                .await;
                let _ = TxnRecoveryWorker::run_recovery_pass_on_shard(
                    &grp,
                    &handler,
                    self.global_manager.as_ref(),
                    &self.clock,
                )
                .await;
                let _ = cell
                    .maybe_split_or_isolate_shard(grp.shard_id(), false)
                    .await;
            }
            let _ = cell.relocate_configured_jumbo_files().await;
        }
        // Snapshot + purge after auxiliary drain so `last_snapshot_index` advances and
        // `.raftlog` retention follows the 3-watermark gate in production (metadbd's loop).
        for cell in self.global_manager.all_cells() {
            for (_, grp, _) in cell.local_shard_handles() {
                // Rewrite a missing snapshot from the LSM without waiting for a
                // client read. A purged log has no other copy.
                let _ = grp.repair_published_snapshot().await;
                let retained = grp.log_store().retained_entry_count();
                if retained == 0 {
                    continue;
                }
                let (state_flushed, _, last_snap) = grp.watermarks().snapshot_values();
                // First snapshot (watermark still 0) or a large unpurged tail. Skip the
                // full-dataset snapshot on every 250ms tick while the log is still small.
                if !force_snapshot && last_snap > 0 && last_snap >= state_flushed && retained < 64 {
                    // A hold can block purge while a snapshot is already current. Releasing
                    // or expiring it must not wait for the next full snapshot.
                    let _ = grp.log_store().try_purge_to_safe_watermark();
                    continue;
                }
                grp.flush_snapshot_and_purge().await?;
            }
        }
        if let Err(error) = self.publish_control_heartbeat_if_due().await {
            tracing::warn!(%error, "control heartbeat was not published");
        }
        if let Some(cell) = &self.cell_manager {
            // Hints are computed here and not applied. This does not call
            // transfer_leader, move a Jumbo shard, change membership, or
            // replicate the cell directory.
            cell.observe_liveness_hints();
        }
        Ok(())
    }

    async fn publish_control_heartbeat_if_due(&self) -> Result<()> {
        let Some(cell) = &self.cell_manager else {
            return Ok(());
        };
        let now = capture_mono_ns();
        if !cell.control_heartbeat_publish_is_due(now) {
            return Ok(());
        }
        self.publish_control_heartbeat().await?;
        cell.note_control_heartbeat_published(capture_mono_ns());
        Ok(())
    }

    /// Persists this process's heartbeat, then sends that same sequence to the current
    /// Raft leader when this process is not the leader. The sender does not include
    /// shard descriptors. A failed send does not roll the local sequence back: the
    /// next interval uses a new sequence, and a replay of this sequence is idempotent.
    pub async fn publish_control_heartbeat(&self) -> Result<()> {
        let Some(cell) = &self.cell_manager else {
            return Ok(());
        };
        let heartbeat = cell.publish_local_heartbeat()?;
        let leaders = cell.remote_leader_addrs();
        for addr in leaders {
            if let Err(error) = self.send_heartbeat_with_retry(&addr, &heartbeat).await {
                tracing::warn!(%error, %addr, "leader did not accept the control heartbeat");
            }
        }
        Ok(())
    }

    async fn send_heartbeat_with_retry(
        &self,
        addr: &str,
        heartbeat: &metadb_cluster::StoredHeartbeat,
    ) -> Result<()> {
        let first = self.send_heartbeat_once(addr, heartbeat).await;
        if first.is_ok() {
            return Ok(());
        }
        self.send_heartbeat_once(addr, heartbeat).await
    }

    async fn send_heartbeat_once(
        &self,
        addr: &str,
        heartbeat: &metadb_cluster::StoredHeartbeat,
    ) -> Result<()> {
        let endpoint = self.transport_security.prepare_url(addr).map_err(|error| {
            MetaDbError::ConsensusError(format!("invalid leader address {addr}: {error}"))
        })?;
        let mut client = control_plane_client(
            &endpoint,
            &self.config.security.shared_secret,
            &self.transport_security,
            self.roles().map(|roles| roles.local()),
        )
        .await?;
        let sent_mono_ns = capture_mono_ns();
        let response = client
            .store_heartbeat(StoreHeartbeatRequest {
                cell_id: self.cell_manager().cell_id(),
                node_id: heartbeat.node_id,
                disk_free_bytes: heartbeat.disk_free_bytes.unwrap_or(0),
                disk_free_known: heartbeat.disk_free_bytes.is_some(),
                monotonic_seq: heartbeat.seq,
                shards: Vec::new(),
                instance_id: {
                    let instance = self.cell_manager().node_instance_id();
                    if instance == 0 {
                        Vec::new()
                    } else {
                        metadb_proto::convert::encode_u128_be(instance).to_vec()
                    }
                },
                observation_generation: metadb_proto::convert::encode_u128_be(
                    heartbeat.observation_generation,
                )
                .to_vec(),
            })
            .await
            .map_err(|error| {
                MetaDbError::ConsensusError(format!("store_heartbeat at {endpoint}: {error}"))
            })?
            .into_inner();
        if let Some(error) = rpc_app_error(response.error) {
            return Err(error);
        }
        let recv_mono_ns = capture_mono_ns();
        if let Some(cell) = &self.cell_manager {
            cell.record_acknowledged_round_trip(
                heartbeat.node_id,
                heartbeat.seq,
                sent_mono_ns,
                recv_mono_ns,
            );
        }
        Ok(())
    }

    /// Flushes all local shard MemTables to SSTables and drains auxiliary outboxes.
    pub async fn flush_all(&self) -> Result<()> {
        self.run_maintenance_tick().await?;
        for cell in self.global_manager.all_cells() {
            for (_, grp, _) in cell.local_shard_handles() {
                grp.state_machine().flush_to_lsm()?;
            }
        }
        Ok(())
    }

    pub fn readiness(&self) -> ReadinessReport {
        operability::evaluate_process_readiness(
            self.operability.phase(),
            &self.shard_gates(),
            self.directory_only,
            self.proxy_gateway,
        )
    }

    fn shard_gates(&self) -> Vec<ShardGate> {
        let Some(host) = &self.host else {
            return Vec::new();
        };
        host.all_groups()
            .iter()
            .map(|group| {
                let view = group.serve_view();
                ShardGate {
                    leader_known: view.leader_known,
                    applied: view.applied,
                    voter: view.voter,
                    recovery_pending: view.recovery_pending,
                    storage_unrecoverable: view.storage_unrecoverable,
                    stopped: view.stopped,
                    fatal: view.fatal,
                }
            })
            .collect()
    }

    pub fn metrics_snapshot(&self) -> ClusterMetricsSnapshot {
        let gates = self.shard_gates();
        let report = operability::evaluate_process_readiness(
            self.operability.phase(),
            &gates,
            self.directory_only,
            self.proxy_gateway,
        );
        let mut snap = collect_cluster_metrics(&self.global_manager);
        snap.apply_operability(&report, &gates, &self.config.raft_options);
        snap
    }

    /// Bound Prometheus address, when `node.metrics_listen_addr` was set.
    pub fn metrics_addr(&self) -> Option<&str> {
        self.metrics_addr.get().map(String::as_str)
    }

    /// TLS policy for outbound admin and control clients in this process.
    pub fn transport_security(&self) -> &TransportSecurity {
        &self.transport_security
    }

    /// Installed method grants. `None` means method RBAC is not installed.
    pub fn roles(&self) -> Option<&InstalledRoles> {
        self.roles.as_deref()
    }

    /// Stops gRPC and maintenance, drains auxiliary work, flushes memtables, and stops OpenRaft.
    ///
    /// The drain proposes through Raft. After a peer is gone that proposal cannot commit,
    /// so a stuck drain cannot pin process exit. Committed entries that are not in the
    /// memtable flush stay in the Raft log and replay on the next open.
    pub async fn shutdown(&self) -> Result<()> {
        self.operability.begin_shutdown();
        self.stop_tasks().await;
        match tokio::time::timeout(Duration::from_secs(10), self.flush_all()).await {
            Ok(result) => result?,
            Err(_) => {
                if let Some(host) = &self.host {
                    for group in host.all_groups() {
                        group.state_machine().flush_to_lsm()?;
                    }
                }
            }
        }
        let result = if let Some(host) = &self.host {
            host.shutdown_all().await
        } else {
            Ok(())
        };
        backup::unregister_runtime(&self.config.node.data_dir);
        result
    }

    /// Installs `gate` on every local Raft group so a test can park `client_write`.
    pub fn install_proposal_shutdown_gate(&self, gate: &Arc<ProposalShutdownGate>) {
        if let Some(host) = &self.host {
            for group in host.all_groups() {
                group.set_proposal_shutdown_gate(Some(gate.clone()));
            }
        }
    }

    /// Stops gRPC and maintenance, then shuts Raft down without flushing memtables.
    ///
    /// Committed entries that have not reached an LSM checkpoint stay in `.raftlog` and are
    /// replayed by the next [`open_and_serve`](Self::open_and_serve) on the same `data_dir`.
    /// This is the in-process crash used by failure-under-load tests: the LSM `LOCK` is
    /// released, but the memtable is not checkpointed first.
    pub async fn shutdown_without_flush(&self) -> Result<()> {
        self.operability.begin_shutdown();
        self.stop_tasks().await;
        let result = if let Some(host) = &self.host {
            host.shutdown_all_no_flush().await
        } else {
            Ok(())
        };
        backup::unregister_runtime(&self.config.node.data_dir);
        result
    }

    /// Installs a backup hold. Purge and snapshot deletion stay at the latched floors until
    /// [`release_backup_hold`](Self::release_backup_hold) or the hold's wall-clock expiry.
    pub fn acquire_backup_hold(&self, ttl: Duration) -> Result<backup::BackupHoldRecord> {
        match backup::begin_hold(&self.config.node.data_dir, ttl)? {
            backup::BeginHold::AlreadyActive(record) => Ok(record),
            backup::BeginHold::Pending => {
                let floors = self.collect_backup_floors()?;
                backup::finish_hold(&self.config.node.data_dir, floors)
            }
        }
    }

    pub fn release_backup_hold(&self) -> Result<()> {
        backup::release_hold(&self.config.node.data_dir)
    }

    /// Copies a restorable image while a backup hold is active.
    pub fn create_backup(&self, destination: &Path) -> Result<backup::BackupSummary> {
        let Some(host) = &self.host else {
            return Err(MetaDbError::ShardRoutingError(
                "global-manager has no shard files to back up".to_string(),
            ));
        };
        let groups = host.all_groups();
        let mut raft_dirs = Vec::with_capacity(groups.len());
        let mut engines = Vec::with_capacity(groups.len());
        for group in &groups {
            raft_dirs.push(host.shard_dir(group.shard_id()).join("raft"));
            engines.push(group.engine());
        }
        let shards: Vec<OnlineShard<'_>> = groups
            .iter()
            .enumerate()
            .map(|(index, group)| OnlineShard {
                shard_id: group.shard_id(),
                raft_dir: &raft_dirs[index],
                log_store: group.log_store(),
                engine: engines[index].as_ref(),
            })
            .collect();
        backup::create_online_backup(
            &self.config.node.data_dir,
            destination,
            &shards,
            &check_backup_tree,
        )
    }

    /// Services an operator hold latch or `BACKUP_JOB` left by `metadb-ctl`.
    pub fn poll_backup_requests(&self) -> Result<()> {
        let data_dir = self.config.node.data_dir.clone();
        let lock_path = data_dir.join("BACKUP_JOB.lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        if lock_file.try_lock().is_err() {
            return Ok(());
        }
        if matches!(
            backup::read_hold(&data_dir)?,
            Some(record) if record.phase == backup::HOLD_PHASE_PENDING
        ) {
            let floors = self.collect_backup_floors()?;
            let _ = backup::finish_hold(&data_dir, floors)?;
        }
        let Some(job) = backup::claim_backup_job(&data_dir)? else {
            return Ok(());
        };
        let outcome = self.create_backup(Path::new(&job.output_dir));
        backup::complete_backup_job(
            &data_dir,
            &job.output_dir,
            outcome.map_err(|error| error.to_string()),
        )?;
        Ok(())
    }

    fn collect_backup_floors(&self) -> Result<Vec<backup::ShardHoldFloor>> {
        let mut by_id = std::collections::BTreeMap::new();
        let Some(host) = &self.host else {
            return Ok(Vec::new());
        };
        for group in host.all_groups() {
            let raft_dir = host.shard_dir(group.shard_id()).join("raft");
            by_id.insert(
                group.shard_id(),
                backup::floor_for_open_log(
                    group.shard_id(),
                    &raft_dir,
                    group.log_store().last_purged_index(),
                ),
            );
        }
        for shard_id in backup::list_shard_ids(&self.config.node.data_dir)? {
            if by_id.contains_key(&shard_id) {
                continue;
            }
            let Some(host) = &self.host else {
                continue;
            };
            let raft_dir = host.shard_dir(shard_id).join("raft");
            by_id.insert(shard_id, backup::disk_floor(shard_id, &raft_dir)?);
        }
        Ok(by_id.into_values().collect())
    }

    async fn stop_tasks(&self) {
        let _ = self.shutdown_tx.send(true);
        let handles: Vec<_> = {
            let mut guard = self.task_handles.lock().await;
            std::mem::take(&mut *guard)
        };
        for h in handles {
            // Do not abandon the task: it may still hold the shard LSM lock.
            let _ = h.await;
        }
    }
}

/// Rejects a backup tree whose cell state names a shard the tree does not contain.
///
/// `shard_ids` and `cell_states` are the shard directories and `CELL_MANAGER_STATE`
/// frames already read through the anchored directory handle. This function does not
/// open a path.
pub fn check_backup_tree(shard_ids: &[ShardId], cell_states: &[(String, Vec<u8>)]) -> Result<()> {
    let copied: BTreeSet<ShardId> = shard_ids.iter().copied().collect();
    for (name, bytes) in cell_states {
        let referenced = metadb_cluster::DurableCellManager::shard_ids_in_cell_state_bytes(bytes)?;
        for shard_id in referenced {
            if !copied.contains(&shard_id) {
                return Err(MetaDbError::StorageIoError(format!(
                    "backup rejected: {name}/CELL_MANAGER_STATE references shard {shard_id} which was not copied"
                )));
            }
        }
    }
    Ok(())
}

async fn spawn_metrics_listener(cluster: &Arc<PersistentMetaDbCluster>) -> Result<()> {
    let Some(metrics_listen) = cluster.config.node.metrics_listen_addr.clone() else {
        return Ok(());
    };
    let listener = TcpListener::bind(&metrics_listen).await?;
    let bound = listener.local_addr()?.to_string();
    // Recheck the address the socket actually bound, including port 0.
    cluster.config.reject_unauthenticated_bind(&bound)?;
    let _ = cluster.metrics_addr.set(bound.clone());
    tracing::info!(
        metrics_addr = %bound,
        "Prometheus metrics endpoint listening"
    );
    let handle = tokio::spawn(metrics_http::serve(
        listener,
        Arc::downgrade(cluster),
        cluster.config.security.shared_secret.clone(),
        cluster.shutdown_tx.subscribe(),
        Arc::clone(&cluster.transport_security),
    ));
    cluster.task_handles.lock().await.push(handle);
    Ok(())
}

const PROXY_RAFT_REJECTION: &str = "proxy does not host raft groups";

#[derive(Clone)]
enum RaftIngress {
    Groups(MultiRaftTransportServer),
    Proxy,
}

#[tonic::async_trait]
impl RaftTransportService for RaftIngress {
    async fn append_entries(
        &self,
        request: Request<RaftRpcEnvelope>,
    ) -> std::result::Result<Response<RaftRpcReply>, Status> {
        match self {
            Self::Groups(server) => server.append_entries(request).await,
            Self::Proxy => Ok(Response::new(proxy_raft_reply())),
        }
    }

    async fn vote(
        &self,
        request: Request<RaftRpcEnvelope>,
    ) -> std::result::Result<Response<RaftRpcReply>, Status> {
        match self {
            Self::Groups(server) => server.vote(request).await,
            Self::Proxy => Ok(Response::new(proxy_raft_reply())),
        }
    }

    async fn transfer_leader(
        &self,
        request: Request<RaftRpcEnvelope>,
    ) -> std::result::Result<Response<RaftRpcReply>, Status> {
        match self {
            Self::Groups(server) => server.transfer_leader(request).await,
            Self::Proxy => Ok(Response::new(proxy_raft_reply())),
        }
    }

    async fn install_snapshot(
        &self,
        request: Request<tonic::Streaming<SnapshotChunkRequest>>,
    ) -> std::result::Result<Response<SnapshotChunkResponse>, Status> {
        match self {
            Self::Groups(server) => server.install_snapshot(request).await,
            Self::Proxy => Err(Status::failed_precondition(PROXY_RAFT_REJECTION)),
        }
    }

    async fn client_write(
        &self,
        request: Request<RaftRpcEnvelope>,
    ) -> std::result::Result<Response<RaftRpcReply>, Status> {
        match self {
            Self::Groups(server) => server.client_write(request).await,
            Self::Proxy => Ok(Response::new(proxy_raft_reply())),
        }
    }
}

fn proxy_raft_reply() -> RaftRpcReply {
    RaftRpcReply {
        bincode_payload: Vec::new(),
        error_message: PROXY_RAFT_REJECTION.to_string(),
    }
}

fn spawn_grpc_server(
    listener: TcpListener,
    grpc_svc: MetaDbGrpcService,
    shared_secret: String,
    raft: RaftIngress,
    mut shutdown_rx: watch::Receiver<bool>,
    security: Arc<TransportSecurity>,
    roles: Option<Arc<InstalledRoles>>,
) -> tokio::task::JoinHandle<()> {
    let auth = BearerAuth::new(shared_secret.clone());
    tokio::spawn(async move {
        let router = Server::builder()
            .layer(rbac_layer::RbacLayer::new(shared_secret.clone(), roles))
            .add_service(MetadataServiceServer::with_interceptor(
                grpc_svc.clone(),
                auth.clone(),
            ))
            .add_service(TransactionServiceServer::with_interceptor(
                grpc_svc.clone(),
                auth.clone(),
            ))
            .add_service(ControlPlaneServiceServer::with_interceptor(
                grpc_svc,
                auth.clone(),
            ))
            .add_service(RaftTransportServiceServer::with_interceptor(raft, auth));
        if !security.is_plaintext() {
            tracing::error!("refusing to serve: TLS termination is not available");
            return;
        }
        let _ = router
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.changed().await;
            })
            .await;
    })
}

fn security_startup_error(error: metadb_security::SecurityError) -> MetaDbError {
    MetaDbError::CodecError(error.to_string())
}

async fn wait_until_accepting(addr: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn rpc_message(err: Option<metadb_proto::ErrorDetailProto>) -> Option<String> {
    rpc_app_error(err).map(|error| error.to_string())
}

fn rpc_app_error(err: Option<metadb_proto::ErrorDetailProto>) -> Option<MetaDbError> {
    let err = err?;
    if err.message.is_empty() && err.error_kind.is_empty() && err.serialized_core_error.is_empty() {
        return None;
    }
    Some(MetaDbError::from(err))
}

fn join_failure_is_permanent(error: &MetaDbError) -> bool {
    matches!(error, MetaDbError::IdentityRejected(_))
}

pub(crate) async fn control_plane_client(
    addr: &str,
    secret: &str,
    security: &TransportSecurity,
    role: Option<&RolePresentation>,
) -> Result<
    ControlPlaneServiceClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            BearerAuthClient,
        >,
    >,
> {
    let url = security.prepare_url(addr).map_err(|error| {
        MetaDbError::ConsensusError(format!("invalid seed address {addr}: {error}"))
    })?;
    let endpoint = Endpoint::from_shared(url)
        .map_err(|e| MetaDbError::ConsensusError(format!("invalid seed address {addr}: {e}")))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(20));
    let channel = security.connect(endpoint).await.map_err(|e| {
        MetaDbError::ConsensusError(format!("connect to raft leader {addr} failed: {e}"))
    })?;
    let mut auth = BearerAuthClient::new(secret);
    if let Some(role) = role {
        auth = auth.with_role(role.clone());
    }
    Ok(ControlPlaneServiceClient::with_interceptor(channel, auth))
}

fn install_configured_roles(config: &ServerTomlConfig) -> Result<Option<Arc<InstalledRoles>>> {
    let Some(rbac) = &config.security.rbac else {
        return Ok(None);
    };
    let grants = rbac
        .grants
        .iter()
        .map(|grant| RoleGrantSpec {
            principal: grant.principal.clone(),
            credential: grant.credential.clone(),
            allow: grant.allow.clone(),
        })
        .collect::<Vec<_>>();
    InstalledRoles::install(
        &rbac.local_principal,
        &grants,
        &config.security.shared_secret,
    )
    .map(|roles| Some(Arc::new(roles)))
    .map_err(security_startup_error)
}

fn listen_port_is_ephemeral(addr: &str) -> bool {
    addr.rsplit_once(':')
        .map(|(_, port)| port == "0")
        .unwrap_or(false)
}

/// Registers this process with `config.cluster.join` and waits until every local shard
/// has applied a voter membership that includes this node.
///
/// Transient membership and transport errors are retried. A conflicting cluster or node
/// identity is returned immediately. A failed attempt does not abort the process by itself;
/// the caller marks startup failed, shuts the listeners down, and returns the error.
async fn join_existing_raft_group(
    config: &ServerTomlConfig,
    cell_manager: &DurableCellManager,
    bound_addr: &str,
    security: &TransportSecurity,
    role: Option<&RolePresentation>,
) -> Result<()> {
    let budget = config.join_timeout.unwrap_or(Duration::from_secs(45));
    let deadline = tokio::time::Instant::now() + budget;
    let mut last_error = None;
    while tokio::time::Instant::now() < deadline {
        match join_existing_raft_group_once(config, cell_manager, bound_addr, security, role).await
        {
            Ok(()) => return Ok(()),
            Err(error) if join_failure_is_permanent(&error) => return Err(error),
            Err(error) => {
                tracing::warn!(%error, "join attempt failed; retrying");
                last_error = Some(error);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        MetaDbError::ConsensusError("join failed before an attempt completed".to_string())
    }))
}

async fn join_existing_raft_group_once(
    config: &ServerTomlConfig,
    cell_manager: &DurableCellManager,
    bound_addr: &str,
    security: &TransportSecurity,
    role: Option<&RolePresentation>,
) -> Result<()> {
    let seed = security
        .prepare_url(&config.cluster.join)
        .map_err(|error| {
            MetaDbError::ConsensusError(format!(
                "invalid join address {}: {error}",
                config.cluster.join
            ))
        })?;
    let mut client =
        control_plane_client(&seed, &config.security.shared_secret, security, role).await?;
    let listed = client
        .get_routes(GetRoutesRequest {
            cell_id: config.node.cell_id,
        })
        .await
        .map_err(|e| MetaDbError::ConsensusError(format!("get_routes from {seed}: {e}")))?
        .into_inner();
    if let Some(error) = rpc_app_error(listed.error) {
        return Err(match error {
            MetaDbError::IdentityRejected(message) => MetaDbError::IdentityRejected(message),
            other => MetaDbError::ConsensusError(format!("get_routes from {seed}: {other}")),
        });
    }
    let cluster_id =
        metadb_proto::convert::decode_optional_u128_be(&listed.cluster_id).map_err(|error| {
            MetaDbError::IdentityRejected(format!("seed {seed} cluster id is invalid: {error}"))
        })?;
    if cluster_id == 0 {
        return Err(MetaDbError::IdentityRejected(format!(
            "seed {seed} has no cluster identity"
        )));
    }
    cell_manager.adopt_cluster_identity(cluster_id)?;
    let mut routes = Vec::with_capacity(listed.shards.len());
    for shard in listed.shards {
        routes.push(grpc_services::composite_route_from_proto(shard)?);
    }
    if routes.is_empty() {
        return Err(MetaDbError::ConsensusError(format!(
            "seed {seed} has no shard routes in cell {}",
            config.node.cell_id
        )));
    }
    let our_id = config.node.node_id;
    let on_every_shard = routes.iter().all(|route| {
        route
            .peers
            .get(&our_id)
            .map(|(_, is_voter)| *is_voter)
            .unwrap_or(false)
    });
    if on_every_shard && !cell_manager.join_pending() {
        return Err(MetaDbError::IdentityRejected(format!(
            "node_id {our_id} is already a voter; reopen that node's data directory instead of joining a fresh one"
        )));
    }
    // Persist intent before promotion. A crash after the leader commits membership can resume
    // from this flag instead of coming back as an empty node the leader still counts.
    cell_manager.begin_join(&routes)?;
    let mut voter_ids: BTreeSet<NodeId> = routes
        .iter()
        .flat_map(|route| {
            route
                .peers
                .iter()
                .filter(|(_, (_, is_voter))| *is_voter)
                .map(|(node_id, _)| *node_id)
        })
        .collect();
    cell_manager.start_joining_replicas(&routes).await?;
    if !on_every_shard {
        let instance_id = cell_manager.node_instance_id();
        if instance_id == 0 {
            return Err(MetaDbError::IdentityRejected(
                "joiner has no node instance id".to_string(),
            ));
        }
        let registered = client
            .register_node(RegisterNodeRequest {
                cell_id: config.node.cell_id,
                node_id: config.node.node_id,
                grpc_addr: bound_addr.to_string(),
                instance_id: metadb_proto::convert::encode_u128_be(instance_id).to_vec(),
            })
            .await
            .map_err(|e| MetaDbError::ConsensusError(format!("register_node at {seed}: {e}")))?
            .into_inner();
        if let Some(error) = rpc_app_error(registered.error) {
            return Err(error);
        }
        // Include this node once. A partial promotion already lists it on some shards.
        voter_ids.insert(our_id);
        let target_voters = u32::try_from(metadb_cluster::join_scale_target(&voter_ids, our_id))
            .unwrap_or(u32::MAX);
        let scaled = client
            .scale_shard_replicas(ScaleShardReplicasRequest {
                cell_id: config.node.cell_id,
                target_voters,
                ensure_node_id: our_id,
            })
            .await
            .map_err(|e| {
                MetaDbError::ConsensusError(format!("scale_shard_replicas at {seed}: {e}"))
            })?
            .into_inner();
        if let Some(error) = rpc_app_error(scaled.error) {
            if !cell_manager.local_voters_include(config.node.node_id) {
                return Err(match error {
                    MetaDbError::IdentityRejected(message) => {
                        MetaDbError::IdentityRejected(message)
                    }
                    other => MetaDbError::ConsensusError(format!(
                        "scale_shard_replicas at {seed}: {other}"
                    )),
                });
            }
        }
    }

    finish_resumed_join(config, cell_manager, &mut client, &seed, routes).await?;
    tracing::info!(
        node_id = our_id,
        seed = %seed,
        voters = voter_ids.len(),
        "joined existing raft group"
    );
    Ok(())
}

async fn finish_resumed_join(
    config: &ServerTomlConfig,
    cell_manager: &DurableCellManager,
    client: &mut ControlPlaneServiceClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            BearerAuthClient,
        >,
    >,
    seed: &str,
    fallback_routes: Vec<metadb_cluster::CompositeShardRoute>,
) -> Result<()> {
    let our_id = config.node.node_id;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !cell_manager.local_voters_include(our_id) {
        if tokio::time::Instant::now() >= deadline {
            return Err(MetaDbError::ConsensusError(format!(
                "timed out joining cell {} via {seed} as node {our_id}",
                config.node.cell_id
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let mut joined_routes = Vec::new();
    if let Ok(response) = client
        .get_routes(GetRoutesRequest {
            cell_id: config.node.cell_id,
        })
        .await
    {
        let refreshed = response.into_inner();
        if rpc_message(refreshed.error).is_none() {
            for shard in refreshed.shards {
                if let Ok(route) = grpc_services::composite_route_from_proto(shard) {
                    joined_routes.push(route);
                }
            }
        }
    }
    let seed_lists_us = !joined_routes.is_empty()
        && joined_routes.iter().all(|route| {
            route
                .peers
                .get(&our_id)
                .map(|(_, is_voter)| *is_voter)
                .unwrap_or(false)
        });
    if !seed_lists_us {
        joined_routes = match cell_manager.routes_with_local_membership() {
            Ok(routes) if !routes.is_empty() => routes,
            _ => fallback_routes,
        };
    }
    if joined_routes.is_empty()
        || joined_routes.iter().any(|route| {
            !route
                .peers
                .get(&our_id)
                .map(|(_, is_voter)| *is_voter)
                .unwrap_or(false)
        })
    {
        return Err(MetaDbError::ConsensusError(format!(
            "node {our_id} is not a voter on every shard after joining {seed}; join with the next node id in ascending order"
        )));
    }
    cell_manager.install_joined_membership(joined_routes)?;
    Ok(())
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn authorization(req: &Request<()>) -> Option<String> {
        req.metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }

    #[test]
    fn server_requires_exact_bearer_and_client_attaches_it() {
        let mut server = BearerAuth::new("s3cr3t");
        assert!(server.call(Request::new(())).is_err());

        let mut prefix = Request::new(());
        prefix.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer s3cr3").unwrap(),
        );
        assert!(server.call(prefix).is_err());

        let mut extra = Request::new(());
        extra.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from("Bearer s3cr3t-extra").unwrap(),
        );
        assert!(server.call(extra).is_err());

        let mut client = BearerAuthClient::new("s3cr3t");
        let outgoing = client.call(Request::new(())).unwrap();
        assert_eq!(authorization(&outgoing).as_deref(), Some("Bearer s3cr3t"));

        let mut inbound = Request::new(());
        inbound.metadata_mut().insert(
            "authorization",
            outgoing.metadata().get("authorization").unwrap().clone(),
        );
        assert!(server.call(inbound).is_ok());
    }

    #[test]
    fn empty_secret_allows_calls_and_adds_no_header() {
        let mut server = BearerAuth::new("");
        assert!(server.call(Request::new(())).is_ok());
        let mut client = BearerAuthClient::new("");
        let outgoing = client.call(Request::new(())).unwrap();
        assert!(authorization(&outgoing).is_none());
    }

    #[test]
    fn whitespace_secret_is_rejected_by_client_and_server() {
        let mut server = BearerAuth::new("   ");
        assert!(server.call(Request::new(())).is_err());
        let mut client = BearerAuthClient::new(" \t ");
        assert!(client.call(Request::new(())).is_err());
    }
}
