//! Unified MetaDB Server & Cluster Engine (`metadb-server`).
//!
//! Supports seamless three-stage evolution using a single unified codebase:
//! - **Stage 1 (Single-Node All-in-One)**: 1 BE node, 1-Voter Raft, embedded `CellLocalManager` and `MetaCache`.
//! - **Stage 2 (Single-Node Multi-Shard)**: Dynamic `group_key = PID` boundary shard splitting as metadata grows.
//! - **Stage 3 (Distributed Multi-Cell Cluster)**: Zero-downtime Raft `ConfChange` (`1-Voter -> 3-Voter`),
//!   Jumbo Shard dedicated-node isolation, and multi-Cell federation under `GlobalManager`.

use std::sync::Arc;

use metadb_cluster::{CellId, CellLocalManager, GlobalManager, MetaCache, ShardDescriptor};
use metadb_core::{
    FileId, HlcTimestamp, HybridLogicalClock, InodeAttr, InodeId, MetaEntry, NodeId, QuotaRecord,
    Result, SecondaryIndexRecord, UserId, ROOT_INODE_ID,
};
use metadb_proxy::{MetadataSqlCommand, SqlTemplateParser};
use metadb_storage::mvcc::DEFAULT_MVCC_RETENTION_MS;
use metadb_storage::LifecycleStorageEngine;
use metadb_tx::{
    AsyncAttrAggregator, AsyncAuxiliarySink, PushdownExecutor, TwoPhaseCommitCoordinator,
};

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
    /// Initializes a Stage 1 Single-Node All-in-One MetaDB instance with a root shard
    /// and pre-initialized root directory `(ROOT_INODE_ID = 1, "_attr_")`.
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
            .init_directory_attr(&root_storage, ROOT_INODE_ID, 0, 0o755, hlc);

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
            MetadataSqlCommand::ListDir { pid } => Ok(SqlQueryResult::Entries(self.listdir(pid)?)),
            MetadataSqlCommand::Rename {
                src_pid,
                src_name,
                dst_pid,
                dst_name,
            } => Ok(SqlQueryResult::Entry(
                self.rename(src_pid, &src_name, dst_pid, &dst_name)?,
            )),
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
