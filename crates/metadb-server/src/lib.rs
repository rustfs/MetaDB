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
pub mod stress;

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Interceptor;
use tonic::transport::Server;
use tonic::{Request, Status};

/// Optional shared-secret check for metadata, transaction, and control-plane RPCs.
/// An empty secret allows the call (loopback deployments without auth).
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

impl Interceptor for BearerAuth {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        if self.secret.is_empty() {
            return Ok(request);
        }
        let presented = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        let mut expected = String::from("Bearer ");
        expected.push_str(&self.secret);
        if presented == expected {
            Ok(request)
        } else {
            Err(Status::unauthenticated(
                "missing or invalid authorization bearer token",
            ))
        }
    }
}

use metadb_cluster::{
    CellId, CellLocalManager, CompositeMetaCache, DurableCellManager, DurableGlobalManager,
    DurableIdAllocator, GlobalManager, LocalIdBuffer, MetaCache, ShardDescriptor,
};
use metadb_core::{
    CompositeKeyRange, FileId, HlcTimestamp, HybridLogicalClock, InodeAttr, InodeId, MetaEntry,
    NodeId, QuotaRecord, Result, SecondaryIndexRecord, UserId, ROOT_INODE_ID,
};
use metadb_proto::control_plane_service_server::ControlPlaneServiceServer;
use metadb_proto::metadata_service_server::MetadataServiceServer;
use metadb_proto::raft_transport_service_server::RaftTransportServiceServer;
use metadb_proto::transaction_service_server::TransactionServiceServer;
use metadb_proxy::{MetadataSqlCommand, SqlTemplateParser, StatelessProxyRouter};
use metadb_raft::{MultiRaftTransportServer, ShardGroupHost};
use metadb_storage::mvcc::DEFAULT_MVCC_RETENTION_MS;
use metadb_storage::{LifecycleStorageEngine, ShardedBlockCache};
use metadb_tx::{
    AsyncAttrAggregator, AsyncAuxiliarySink, AuxDispatcher, DistributedTxRouter, PushdownExecutor,
    TwoPhaseCommitCoordinator, TxnRecoveryWorker,
};

pub use config::{ServerRole, ServerTomlConfig};
pub use grpc_services::MetaDbGrpcService;
pub use metrics::{collect_cluster_metrics, render_prometheus_metrics, ClusterMetricsSnapshot};

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
        engine.pushdown.init_directory_attr(
            &root_storage,
            ROOT_INODE_ID,
            ROOT_INODE_ID,
            0,
            0o755,
            hlc,
        );

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
        self.two_pc.rename(
            &src_storage,
            &dst_storage,
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
        let hlc = self.clock.now();
        let (_, _, storage) = self.global_manager.route_pid(pid)?;
        self.pushdown.listdir(&storage, pid, hlc)
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
                let mut rows = self.listdir(pid)?;
                if let Some(after) = after_name {
                    rows.retain(|e| e.key.name > after);
                }
                if let Some(pfx) = name_prefix {
                    rows.retain(|e| e.key.name.starts_with(&pfx));
                }
                if limit > 0 && rows.len() > limit {
                    rows.truncate(limit);
                }
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
    host: Arc<ShardGroupHost>,
    cell_manager: Arc<DurableCellManager>,
    global_manager: Arc<DurableGlobalManager>,
    tx_router: Arc<DistributedTxRouter>,
    proxy_router: Arc<StatelessProxyRouter>,
    shutdown_tx: watch::Sender<bool>,
    task_handles: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl PersistentMetaDbCluster {
    /// Opens or bootstraps a persistent MetaDB instance from `config` and starts its gRPC listener
    /// (binding to `config.node.grpc_listen_addr`, e.g., `"127.0.0.1:0"` for OS-assigned ephemeral ports).
    pub async fn open_and_serve(config: ServerTomlConfig) -> Result<(Arc<Self>, String)> {
        if config.node.role != ServerRole::AllInOne {
            return Err(metadb_core::MetaDbError::CodecError(format!(
                "server role {:?} is not implemented; only all-in-one runs a process. \
                 be, proxy, cell-manager, and global-manager are not role-trimmed and must not be treated as disaggregated deployments",
                config.node.role
            )));
        }
        let listener = TcpListener::bind(&config.node.grpc_listen_addr).await?;
        let bound_addr = listener.local_addr()?.to_string();

        let clock = Arc::new(HybridLogicalClock::with_max_drift_ms(
            config.clock.max_clock_drift_ms,
        ));
        let block_cache = Arc::new(ShardedBlockCache::new(config.storage.block_cache_bytes));
        let id_allocator = Arc::new(DurableIdAllocator::open(
            config.node.data_dir.join("global_allocator"),
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

        // Start RaftTransportService BEFORE initializing Multi-Raft groups so multi-node RPCs resolve immediately!
        let raft_router = host.router().clone();
        let cell_manager = DurableCellManager::open_or_bootstrap(
            config.node.cell_id,
            config
                .node
                .data_dir
                .join(format!("cell_{}", config.node.cell_id)),
            host.clone(),
            bound_addr.clone(),
            config.sharding.bootstrap_table_shards,
            config.sharding.shard_split_threshold_bytes,
            config.to_lsm_config(),
            config.raft_options.clone(),
            id_allocator.clone(),
            meta_cache.clone(),
        )
        .await?;

        let global_manager = Arc::new(DurableGlobalManager::new(
            clock.clone(),
            id_allocator,
            meta_cache,
        ));
        global_manager.register_cell(CompositeKeyRange::all_user_tables(), cell_manager.clone());

        let tx_router = Arc::new(DistributedTxRouter::new(
            config.node.node_id,
            clock.clone(),
            global_manager.clone(),
        ));
        let proxy_router = Arc::new(StatelessProxyRouter::new(tx_router.clone()));

        // Ensure root directory `/` (`ROOT_INODE_ID = 1`) exists idempotently on bootstrap!
        if tx_router.dirstat(ROOT_INODE_ID).await.is_err() {
            let _ = tx_router
                .init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
                .await;
        }

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let grpc_svc = MetaDbGrpcService::new(
            global_manager.clone(),
            tx_router.clone(),
            proxy_router.clone(),
        );

        let shared_secret = config.security.shared_secret.clone();
        if shared_secret.is_empty() {
            tracing::warn!(
                listen = %bound_addr,
                "gRPC metadata, transaction, and control-plane services have no authentication or TLS. \
                 Default bind is 127.0.0.1. Do not expose this process to untrusted networks until \
                 [security].shared_secret or an external TLS terminator is configured. \
                 RaftTransport on the same listener is also unauthenticated."
            );
        }
        let auth = BearerAuth::new(shared_secret);

        let grpc_handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(MetadataServiceServer::with_interceptor(
                    grpc_svc.clone(),
                    auth.clone(),
                ))
                .add_service(TransactionServiceServer::with_interceptor(
                    grpc_svc.clone(),
                    auth.clone(),
                ))
                .add_service(ControlPlaneServiceServer::with_interceptor(grpc_svc, auth))
                .add_service(RaftTransportServiceServer::new(
                    MultiRaftTransportServer::new(raft_router),
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_rx.changed().await;
                })
                .await;
        });

        let cluster = Arc::new(Self {
            config,
            clock,
            id_buffer,
            host,
            cell_manager,
            global_manager,
            tx_router,
            proxy_router,
            shutdown_tx,
            task_handles: tokio::sync::Mutex::new(vec![grpc_handle]),
        });

        // Background maintenance task (`AuxDispatcher` + `TxnRecoveryWorker` every 250ms).
        let bg_cluster = Arc::downgrade(&cluster);
        let mut bg_rx = cluster.shutdown_tx.subscribe();
        let bg_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = bg_rx.changed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {
                        let Some(c) = bg_cluster.upgrade() else { break; };
                        let _ = c.run_maintenance_tick().await;
                    }
                }
            }
        });
        cluster.task_handles.lock().await.push(bg_handle);

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
        &self.cell_manager
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
        }
        // Snapshot + purge after auxiliary drain so `last_snapshot_index` advances and
        // `.raftlog` retention follows the 3-watermark gate in production (metadbd's loop).
        for cell in self.global_manager.all_cells() {
            for (_, grp, _) in cell.local_shard_handles() {
                let retained = grp.log_store().retained_entry_count();
                if retained == 0 {
                    continue;
                }
                let (state_flushed, _, last_snap) = grp.watermarks().snapshot_values();
                // First snapshot (watermark still 0) or a large unpurged tail. Skip the
                // full-dataset snapshot on every 250ms tick while the log is still small.
                if last_snap > 0 && last_snap >= state_flushed && retained < 64 {
                    continue;
                }
                grp.flush_snapshot_and_purge().await?;
            }
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

    pub fn metrics_snapshot(&self) -> ClusterMetricsSnapshot {
        collect_cluster_metrics(&self.global_manager)
    }

    /// Gracefully stops gRPC/background loops, flushes all shard state to disk, and stops OpenRaft groups.
    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        let handles: Vec<_> = {
            let mut guard = self.task_handles.lock().await;
            std::mem::take(&mut *guard)
        };
        for h in handles {
            // Do not abandon the task: it may still hold the shard LSM lock.
            let _ = h.await;
        }
        self.flush_all().await?;
        self.host.shutdown_all().await
    }
}
