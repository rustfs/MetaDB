//! Deterministic `RaftStateMachine` backed by WAL-less `LifecycleStorageEngine`,
//! atomic SSTable snapshot build/install (`Resolution B6`), and `ShardApplyHandler`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use openraft::entry::RaftEntry;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{
    CommittedLeaderIdOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf,
};
use openraft::vote::RaftLeaderId;
use openraft::{EntryPayload, OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use tokio::sync::watch;
use tokio_stream::StreamExt;

use metadb_core::{
    append_crc32c_le, checksum_crc32c, verify_crc32c_le, HlcTimestamp, MetaDbError, Result,
    ShardEpoch, ShardId,
};
use metadb_storage::sstable::sync_dir;
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, PreFlushHook,
    ShardedBlockCache,
};

use crate::types::{
    AppliedFrontier, BasicKvCommand, MetaDbRaftRequest, MetaDbRaftResponse, MetaDbTypeConfig,
    ShardWatermarkTracker, SnapshotArchive, SnapshotFileEntry,
};

/// Pluggable application command handler executed deterministically inside `RaftStateMachine::apply`.
///
/// Implemented by `BasicKvApplyHandler` in Phase 3 tests and by `TxStateMachineHandler` in `metadb-tx` (Phase 4).
pub trait ShardApplyHandler: Send + Sync + 'static {
    fn current_epoch(&self) -> ShardEpoch;

    fn on_membership_changed(&self, new_epoch: ShardEpoch);

    fn encode_descriptor_bytes(&self) -> Vec<u8> {
        self.current_epoch().encode().to_vec()
    }

    fn apply_command(
        &self,
        raft_index: u64,
        commit_hlc: HlcTimestamp,
        request: &MetaDbRaftRequest,
        engine: &LifecycleStorageEngine,
    ) -> Result<Vec<u8>>;

    fn pre_flush_hook(&self) -> Option<&dyn PreFlushHook> {
        None
    }

    /// Invoked during snapshot creation when forcing a `0x07` outbox spill so 100% of pending
    /// auxiliary tasks `<= snapshot_index` reside inside the SSTable checkpoint (`Resolution B6`).
    fn prepare_for_snapshot(&self, _up_to_raft_index: u64) {}

    /// Invoked after an atomic `install_snapshot` swaps the shard's `LsmStorageBackend`.
    fn on_snapshot_installed(
        &self,
        checkpoint: &DurableShardCheckpoint,
        engine: &LifecycleStorageEngine,
    );
}

/// Built-in KV handler for standalone `metadb-raft` consensus & crash-recovery verification.
pub struct BasicKvApplyHandler {
    epoch: RwLock<ShardEpoch>,
    watermarks: Arc<ShardWatermarkTracker>,
}

impl BasicKvApplyHandler {
    pub fn new(initial_epoch: ShardEpoch, watermarks: Arc<ShardWatermarkTracker>) -> Self {
        Self {
            epoch: RwLock::new(initial_epoch),
            watermarks,
        }
    }
}

impl ShardApplyHandler for BasicKvApplyHandler {
    fn current_epoch(&self) -> ShardEpoch {
        *self.epoch.read().unwrap()
    }

    fn on_membership_changed(&self, new_epoch: ShardEpoch) {
        *self.epoch.write().unwrap() = new_epoch;
    }

    fn apply_command(
        &self,
        raft_index: u64,
        commit_hlc: HlcTimestamp,
        request: &MetaDbRaftRequest,
        engine: &LifecycleStorageEngine,
    ) -> Result<Vec<u8>> {
        let cmd: BasicKvCommand = bincode::deserialize(&request.payload)
            .map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        match cmd {
            BasicKvCommand::Put { key, value } => {
                engine.apply_committed_put(key, value, commit_hlc, raft_index);
            }
            BasicKvCommand::Delete { key } => {
                engine.apply_committed_delete(key, commit_hlc, raft_index);
            }
            BasicKvCommand::Noop => {}
        }
        // BasicKvApplyHandler has no remote outbox, so aux_outbox_durable_index tracks applied index.
        self.watermarks.advance_aux_outbox_durable(raft_index);
        Ok(Vec::new())
    }

    fn on_snapshot_installed(
        &self,
        checkpoint: &DurableShardCheckpoint,
        _engine: &LifecycleStorageEngine,
    ) {
        if checkpoint.descriptor_bytes.len() == 16 {
            if let Ok(ep) = ShardEpoch::decode(&checkpoint.descriptor_bytes) {
                *self.epoch.write().unwrap() = ep;
            }
        }
        self.watermarks
            .advance_aux_outbox_durable(checkpoint.flushed_raft_index);
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedSnapshotMeta {
    meta: SnapshotMetaOf<MetaDbTypeConfig>,
    checkpoint: DurableShardCheckpoint,
    file_specs: Vec<(String, u64, u32)>,
}

struct StateMachineShared {
    shard_id: ShardId,
    shard_dir: PathBuf,
    block_cache: Arc<ShardedBlockCache>,
    lsm_config: LsmEngineConfig,
    engine: RwLock<Arc<LifecycleStorageEngine>>,
    handler: Arc<dyn ShardApplyHandler>,
    watermarks: Arc<ShardWatermarkTracker>,
    last_applied_log_id: RwLock<Option<LogIdOf<MetaDbTypeConfig>>>,
    last_applied_hlc: RwLock<HlcTimestamp>,
    last_membership: RwLock<StoredMembershipOf<MetaDbTypeConfig>>,
    frontier_tx: watch::Sender<AppliedFrontier>,
}

impl StateMachineShared {
    fn current_engine(&self) -> Arc<LifecycleStorageEngine> {
        self.engine.read().unwrap().clone()
    }

    fn flush_to_lsm(&self) -> Result<(u64, DurableShardCheckpoint)> {
        let engine = self.current_engine();
        let last_log = *self.last_applied_log_id.read().unwrap();
        let last_hlc = *self.last_applied_hlc.read().unwrap();
        let membership = self.last_membership.read().unwrap().clone();
        let membership_bytes = bincode::serialize(&membership).unwrap_or_default();

        let (term, leader_node, index) = match last_log {
            Some(lid) => (
                lid.committed_leader_id().term,
                lid.committed_leader_id().node_id,
                lid.index,
            ),
            None => (0, 0, 0),
        };

        let cp = DurableShardCheckpoint {
            flushed_raft_term: term,
            flushed_raft_leader: leader_node,
            flushed_raft_index: index,
            flushed_max_hlc: last_hlc,
            contiguous_aux_acked_index: self.watermarks.snapshot_values().1,
            membership_bytes,
            descriptor_bytes: self.handler.encode_descriptor_bytes(),
        };

        let hook = self.handler.pre_flush_hook();
        let (state_flushed, aux_frontier) = engine.seal_and_flush_with_hook(cp, hook)?;
        self.watermarks.advance_state_flushed(state_flushed);
        if hook.is_some() {
            self.watermarks.advance_aux_outbox_durable(aux_frontier);
        }
        let updated_cp = engine.durable_checkpoint();
        Ok((state_flushed, updated_cp))
    }

    fn build_snapshot_sync(&self) -> io::Result<SnapshotOf<MetaDbTypeConfig, SnapshotArchive>> {
        let current_idx = self
            .last_applied_log_id
            .read()
            .unwrap()
            .map(|id| id.index)
            .unwrap_or(0);
        self.handler.prepare_for_snapshot(current_idx);

        let (_, _) = self
            .flush_to_lsm()
            .map_err(|e| io::Error::other(e.to_string()))?;

        let engine = self.current_engine();
        let snaps_root = self.shard_dir.join("raft/snapshots");
        fs::create_dir_all(&snaps_root)?;

        // Strict `snapshot_index == checkpoint.flushed_raft_index` alignment (`Resolution B6`).
        let ckpt_preview = engine.durable_checkpoint();
        let snap_index = ckpt_preview.flushed_raft_index;
        let tmp_dir = snaps_root.join(format!("snap_{:020}.tmp", snap_index));
        let final_dir = snaps_root.join(format!("snap_{:020}", snap_index));
        if tmp_dir.exists() {
            fs::remove_dir_all(&tmp_dir)?;
        }
        if final_dir.exists() {
            fs::remove_dir_all(&final_dir)?;
        }

        let ckpt_lsm_dir = tmp_dir.join("lsm");
        let checkpoint = engine
            .create_checkpoint(&ckpt_lsm_dir)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let last_log_id = if checkpoint.flushed_raft_index == 0 {
            None
        } else {
            Some(LogIdOf::<MetaDbTypeConfig>::new(
                CommittedLeaderIdOf::<MetaDbTypeConfig>::new(
                    checkpoint.flushed_raft_term,
                    checkpoint.flushed_raft_leader,
                ),
                checkpoint.flushed_raft_index,
            ))
        };

        let last_membership: StoredMembershipOf<MetaDbTypeConfig> =
            if checkpoint.membership_bytes.is_empty() {
                StoredMembership::default()
            } else {
                bincode::deserialize(&checkpoint.membership_bytes)
                    .unwrap_or_else(|_| self.last_membership.read().unwrap().clone())
            };

        let mut files: Vec<SnapshotFileEntry> = Vec::new();
        for d in fs::read_dir(&ckpt_lsm_dir)? {
            let d = d?;
            if d.file_type()?.is_file() {
                let fname = d.file_name().to_string_lossy().to_string();
                let mut buf = Vec::new();
                File::open(d.path())?.read_to_end(&mut buf)?;
                let expected_len = buf.len() as u64;
                let crc32c = checksum_crc32c(&buf);
                files.push(SnapshotFileEntry {
                    name: fname,
                    expected_len,
                    crc32c,
                    bytes: buf,
                });
            }
        }
        files.sort_by(|a, b| a.name.cmp(&b.name));
        let file_specs: Vec<(String, u64, u32)> = files
            .iter()
            .map(|f| (f.name.clone(), f.expected_len, f.crc32c))
            .collect();

        let meta: SnapshotMetaOf<MetaDbTypeConfig> = SnapshotMeta {
            last_log_id,
            last_membership,
        };

        let persisted_meta = PersistedSnapshotMeta {
            meta: meta.clone(),
            checkpoint: checkpoint.clone(),
            file_specs,
        };
        let mut meta_bytes = bincode::serialize(&persisted_meta)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        append_crc32c_le(&mut meta_bytes);
        {
            let mut mf = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(tmp_dir.join("SNAPSHOT_META"))?;
            mf.write_all(&meta_bytes)?;
            mf.sync_data()?;
        }

        fs::rename(&tmp_dir, &final_dir)?;
        sync_dir(&snaps_root).map_err(|e| io::Error::other(e.to_string()))?;
        // Remove older `snap_*` directories only AFTER `final_dir` is durably fsync'd in `snaps_root`.
        for d in fs::read_dir(&snaps_root)? {
            let d = d?;
            if d.path() != final_dir {
                let _ = fs::remove_dir_all(d.path());
            }
        }
        sync_dir(&snaps_root).map_err(|e| io::Error::other(e.to_string()))?;

        self.watermarks
            .advance_last_snapshot(checkpoint.flushed_raft_index);

        Ok(Snapshot {
            meta,
            snapshot: SnapshotArchive { checkpoint, files },
        })
    }

    fn load_current_snapshot_sync(
        &self,
    ) -> io::Result<Option<SnapshotOf<MetaDbTypeConfig, SnapshotArchive>>> {
        let snaps_root = self.shard_dir.join("raft/snapshots");
        if !snaps_root.exists() {
            return Ok(None);
        }
        let mut best: Option<(u64, PathBuf)> = None;
        for d in fs::read_dir(&snaps_root)? {
            let d = d?;
            let fname = d.file_name();
            let Some(name) = fname.to_str() else {
                continue;
            };
            if let Some(num) = name.strip_prefix("snap_") {
                if !num.ends_with(".tmp") {
                    if let Ok(idx) = num.parse::<u64>() {
                        if best.as_ref().map(|(b, _)| idx > *b).unwrap_or(true) {
                            best = Some((idx, d.path()));
                        }
                    }
                }
            }
        }
        let Some((_, snap_dir)) = best else {
            return Ok(None);
        };
        let mut raw_meta = Vec::new();
        File::open(snap_dir.join("SNAPSHOT_META"))?.read_to_end(&mut raw_meta)?;
        let body = verify_crc32c_le(&raw_meta, "SNAPSHOT_META")
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let pmeta: PersistedSnapshotMeta = bincode::deserialize(body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let lsm_dir = snap_dir.join("lsm");
        let mut files = Vec::with_capacity(pmeta.file_specs.len());
        for (fname, expected_len, expected_crc) in &pmeta.file_specs {
            let mut buf = Vec::new();
            File::open(lsm_dir.join(fname))?.read_to_end(&mut buf)?;
            if buf.len() as u64 != *expected_len || checksum_crc32c(&buf) != *expected_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("snapshot file {} failed length/CRC32C check", fname),
                ));
            }
            files.push(SnapshotFileEntry {
                name: fname.clone(),
                expected_len: *expected_len,
                crc32c: *expected_crc,
                bytes: buf,
            });
        }

        Ok(Some(Snapshot {
            meta: pmeta.meta,
            snapshot: SnapshotArchive {
                checkpoint: pmeta.checkpoint,
                files,
            },
        }))
    }
}

pub struct ShardSnapshotBuilder {
    shared: Arc<StateMachineShared>,
}

impl RaftSnapshotBuilder<MetaDbTypeConfig> for ShardSnapshotBuilder {
    type SnapshotData = SnapshotArchive;

    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<SnapshotOf<MetaDbTypeConfig, Self::SnapshotData>, io::Error> {
        self.shared.build_snapshot_sync()
    }
}

/// OpenRaft `RaftStateMachine` driving WAL-less `LifecycleStorageEngine`.
#[derive(Clone)]
pub struct ShardStateMachine {
    shared: Arc<StateMachineShared>,
}

impl ShardStateMachine {
    pub fn open(
        shard_id: ShardId,
        shard_dir: impl AsRef<Path>,
        block_cache: Arc<ShardedBlockCache>,
        lsm_config: LsmEngineConfig,
        handler: Arc<dyn ShardApplyHandler>,
        watermarks: Arc<ShardWatermarkTracker>,
    ) -> Result<(Self, watch::Receiver<AppliedFrontier>)> {
        let shard_dir = shard_dir.as_ref().to_path_buf();
        fs::create_dir_all(&shard_dir)?;

        // Recover interrupted `lsm_installing_*` or `lsm_old_*` swap if power failed mid-install.
        let lsm_dir = shard_dir.join("lsm");
        for d in fs::read_dir(&shard_dir)? {
            let d = d?;
            if let Some(name) = d.file_name().to_str() {
                if name.starts_with("lsm_installing_") {
                    let _ = fs::remove_dir_all(d.path());
                } else if name.starts_with("lsm_old_") {
                    if !lsm_dir.exists() {
                        fs::rename(d.path(), &lsm_dir)?;
                    } else {
                        let _ = fs::remove_dir_all(d.path());
                    }
                }
            }
        }

        let engine = Arc::new(LifecycleStorageEngine::open_lsm(
            shard_id,
            &lsm_dir,
            block_cache.clone(),
            lsm_config.clone(),
        )?);
        let cp = engine.durable_checkpoint();

        // Report `MANIFEST`'s `flushed_raft_log_id` (`F`) as `applied_state` on startup so OpenRaft
        // automatically replays committed `.raftlog` entries strictly above `F` (`Resolution B5`)!
        let initial_log_id = if cp.flushed_raft_index == 0 {
            None
        } else {
            Some(LogIdOf::<MetaDbTypeConfig>::new(
                CommittedLeaderIdOf::<MetaDbTypeConfig>::new(
                    cp.flushed_raft_term,
                    cp.flushed_raft_leader,
                ),
                cp.flushed_raft_index,
            ))
        };
        let initial_membership: StoredMembershipOf<MetaDbTypeConfig> =
            if cp.membership_bytes.is_empty() {
                StoredMembership::default()
            } else {
                bincode::deserialize(&cp.membership_bytes).unwrap_or_default()
            };

        watermarks.advance_state_flushed(cp.flushed_raft_index);
        watermarks.advance_aux_outbox_durable(cp.contiguous_aux_acked_index);
        handler.on_snapshot_installed(&cp, &engine);

        let initial_frontier = AppliedFrontier {
            raft_index: cp.flushed_raft_index,
            hlc: cp.flushed_max_hlc,
            epoch: handler.current_epoch(),
        };
        let (frontier_tx, frontier_rx) = watch::channel(initial_frontier);

        let shared = Arc::new(StateMachineShared {
            shard_id,
            shard_dir,
            block_cache,
            lsm_config,
            engine: RwLock::new(engine),
            handler,
            watermarks,
            last_applied_log_id: RwLock::new(initial_log_id),
            last_applied_hlc: RwLock::new(cp.flushed_max_hlc),
            last_membership: RwLock::new(initial_membership),
            frontier_tx,
        });

        Ok((Self { shared }, frontier_rx))
    }

    pub fn engine(&self) -> Arc<LifecycleStorageEngine> {
        self.shared.current_engine()
    }

    pub fn handler(&self) -> &Arc<dyn ShardApplyHandler> {
        &self.shared.handler
    }

    pub fn flush_to_lsm(&self) -> Result<(u64, DurableShardCheckpoint)> {
        self.shared.flush_to_lsm()
    }
}

impl RaftStateMachine<MetaDbTypeConfig> for ShardStateMachine {
    type SnapshotData = SnapshotArchive;
    type SnapshotBuilder = ShardSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (
            Option<LogIdOf<MetaDbTypeConfig>>,
            StoredMembershipOf<MetaDbTypeConfig>,
        ),
        io::Error,
    > {
        let log_id = *self.shared.last_applied_log_id.read().unwrap();
        let mem = self.shared.last_membership.read().unwrap().clone();
        Ok((log_id, mem))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> std::result::Result<(), io::Error>
    where
        Strm: tokio_stream::Stream<
                Item = std::result::Result<EntryResponder<MetaDbTypeConfig>, io::Error>,
            > + Unpin
            + OptionalSend,
    {
        let engine = self.shared.current_engine();

        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let log_id = entry.log_id();
            *self.shared.last_applied_log_id.write().unwrap() = Some(log_id);

            let resp = match entry.payload {
                EntryPayload::Blank => {
                    self.shared
                        .watermarks
                        .advance_aux_for_non_aux_entry(log_id.index);
                    let hlc = *self.shared.last_applied_hlc.read().unwrap();
                    let ep = self.shared.handler.current_epoch();
                    MetaDbRaftResponse {
                        applied_raft_index: log_id.index,
                        applied_hlc: hlc,
                        current_epoch: ep,
                        result: Ok(Vec::new()),
                    }
                }
                EntryPayload::Membership(mem) => {
                    self.shared
                        .watermarks
                        .advance_aux_for_non_aux_entry(log_id.index);
                    let was_uninitialized = self
                        .shared
                        .last_membership
                        .read()
                        .unwrap()
                        .membership()
                        .voter_ids()
                        .next()
                        .is_none();
                    *self.shared.last_membership.write().unwrap() =
                        StoredMembership::new(Some(log_id), mem);
                    let next_epoch = if was_uninitialized {
                        self.shared.handler.current_epoch()
                    } else {
                        let bumped = self.shared.handler.current_epoch().bump_conf_ver();
                        self.shared.handler.on_membership_changed(bumped);
                        bumped
                    };
                    let hlc = *self.shared.last_applied_hlc.read().unwrap();
                    MetaDbRaftResponse {
                        applied_raft_index: log_id.index,
                        applied_hlc: hlc,
                        current_epoch: next_epoch,
                        result: Ok(Vec::new()),
                    }
                }
                EntryPayload::Normal(req) => {
                    // Enforce strict `(hlc, raft_index)` monotonicity (`Resolution B2`).
                    let commit_hlc = {
                        let mut prev_hlc = self.shared.last_applied_hlc.write().unwrap();
                        let next_hlc = if req.hlc > *prev_hlc {
                            req.hlc
                        } else {
                            prev_hlc.next_strict()
                        };
                        *prev_hlc = next_hlc;
                        next_hlc
                    };

                    let cur_epoch = self.shared.handler.current_epoch();
                    let apply_res = if req.expected_epoch != cur_epoch {
                        Err(MetaDbError::EpochMismatch {
                            shard_id: self.shared.shard_id,
                            expected: req.expected_epoch,
                            actual: cur_epoch,
                        })
                    } else {
                        self.shared
                            .handler
                            .apply_command(log_id.index, commit_hlc, &req, &engine)
                    };

                    let updated_epoch = self.shared.handler.current_epoch();
                    MetaDbRaftResponse {
                        applied_raft_index: log_id.index,
                        applied_hlc: commit_hlc,
                        current_epoch: updated_epoch,
                        result: apply_res,
                    }
                }
            };

            let _ = self.shared.frontier_tx.send(AppliedFrontier {
                raft_index: log_id.index,
                hlc: resp.applied_hlc,
                epoch: resp.current_epoch,
            });

            if let Some(tx) = responder {
                tx.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        ShardSnapshotBuilder {
            shared: self.shared.clone(),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<MetaDbTypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> std::result::Result<(), io::Error> {
        let snap_idx = meta.last_log_id.map(|id| id.index).unwrap_or(0);
        if let Some(cur) = *self.shared.last_applied_log_id.read().unwrap() {
            if snap_idx <= cur.index {
                return Ok(());
            }
        }
        if snapshot.checkpoint.flushed_raft_index != snap_idx {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "snapshot index mismatch: meta={} != checkpoint.flushed_raft_index={}",
                    snap_idx, snapshot.checkpoint.flushed_raft_index
                ),
            ));
        }

        // Verify per-file `(expected_len, crc32c)` BEFORE staging or touching live `lsm/`.
        let mut file_specs = Vec::with_capacity(snapshot.files.len());
        for entry in &snapshot.files {
            if entry.name.is_empty() || entry.name.contains('/') || entry.name.contains("..") {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid snapshot relative file name: {:?}", entry.name),
                ));
            }
            if entry.bytes.len() as u64 != entry.expected_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "snapshot file {} length mismatch: expected {}, got {}",
                        entry.name,
                        entry.expected_len,
                        entry.bytes.len()
                    ),
                ));
            }
            let actual_crc = checksum_crc32c(&entry.bytes);
            if actual_crc != entry.crc32c {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "snapshot file {} CRC32C mismatch: expected 0x{:08x}, got 0x{:08x}",
                        entry.name, entry.crc32c, actual_crc
                    ),
                ));
            }
            file_specs.push((entry.name.clone(), entry.expected_len, entry.crc32c));
        }

        let staging_dir = self
            .shared
            .shard_dir
            .join(format!("lsm_installing_{:020}", snap_idx));
        if staging_dir.exists() {
            fs::remove_dir_all(&staging_dir)?;
        }
        fs::create_dir_all(&staging_dir)?;

        for entry in &snapshot.files {
            let fpath = staging_dir.join(&entry.name);
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&fpath)?;
            f.write_all(&entry.bytes)?;
            f.sync_data()?;
        }
        sync_dir(&staging_dir).map_err(|e| io::Error::other(e.to_string()))?;

        // Verify the staged LSM directory opens cleanly and passes all SSTable/MANIFEST CRC32Cs
        // BEFORE swapping!
        let verified_engine = Arc::new(
            LifecycleStorageEngine::open_lsm(
                self.shared.shard_id,
                &staging_dir,
                self.shared.block_cache.clone(),
                self.shared.lsm_config.clone(),
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?,
        );
        drop(verified_engine);

        let live_lsm_dir = self.shared.shard_dir.join("lsm");
        let old_lsm_dir = self
            .shared
            .shard_dir
            .join(format!("lsm_old_{:020}", snap_idx));
        if old_lsm_dir.exists() {
            fs::remove_dir_all(&old_lsm_dir)?;
        }
        if live_lsm_dir.exists() {
            fs::rename(&live_lsm_dir, &old_lsm_dir)?;
        }
        fs::rename(&staging_dir, &live_lsm_dir)?;
        sync_dir(&self.shared.shard_dir).map_err(|e| io::Error::other(e.to_string()))?;

        let reopened = Arc::new(
            LifecycleStorageEngine::open_lsm(
                self.shared.shard_id,
                &live_lsm_dir,
                self.shared.block_cache.clone(),
                self.shared.lsm_config.clone(),
            )
            .map_err(|e| io::Error::other(e.to_string()))?,
        );

        *self.shared.engine.write().unwrap() = reopened.clone();
        *self.shared.last_applied_log_id.write().unwrap() = meta.last_log_id;
        *self.shared.last_applied_hlc.write().unwrap() = snapshot.checkpoint.flushed_max_hlc;
        *self.shared.last_membership.write().unwrap() = meta.last_membership.clone();

        self.shared
            .handler
            .on_snapshot_installed(&snapshot.checkpoint, &reopened);
        self.shared.watermarks.advance_state_flushed(snap_idx);
        self.shared
            .watermarks
            .advance_aux_outbox_durable(snapshot.checkpoint.contiguous_aux_acked_index);
        self.shared.watermarks.advance_last_snapshot(snap_idx);

        // Persist in `raft/snapshots/snap_<S>` so `get_current_snapshot` returns it on restart!
        let snaps_root = self.shared.shard_dir.join("raft/snapshots");
        fs::create_dir_all(&snaps_root)?;
        let saved_snap_dir = snaps_root.join(format!("snap_{:020}", snap_idx));
        let saved_tmp = snaps_root.join(format!("snap_{:020}.tmp", snap_idx));
        if saved_tmp.exists() {
            fs::remove_dir_all(&saved_tmp)?;
        }
        if saved_snap_dir.exists() {
            fs::remove_dir_all(&saved_snap_dir)?;
        }
        let _ = reopened
            .create_checkpoint(&saved_tmp.join("lsm"))
            .map_err(|e| io::Error::other(e.to_string()))?;
        let pmeta = PersistedSnapshotMeta {
            meta: meta.clone(),
            checkpoint: snapshot.checkpoint.clone(),
            file_specs,
        };
        let mut meta_bytes = bincode::serialize(&pmeta)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        append_crc32c_le(&mut meta_bytes);
        {
            let mut mf = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(saved_tmp.join("SNAPSHOT_META"))?;
            mf.write_all(&meta_bytes)?;
            mf.sync_data()?;
        }
        fs::rename(&saved_tmp, &saved_snap_dir)?;
        sync_dir(&snaps_root).map_err(|e| io::Error::other(e.to_string()))?;
        for d in fs::read_dir(&snaps_root)? {
            let d = d?;
            if d.path() != saved_snap_dir {
                let _ = fs::remove_dir_all(d.path());
            }
        }
        sync_dir(&snaps_root).map_err(|e| io::Error::other(e.to_string()))?;

        if old_lsm_dir.exists() {
            let _ = fs::remove_dir_all(&old_lsm_dir);
        }

        let _ = self.shared.frontier_tx.send(AppliedFrontier {
            raft_index: snap_idx,
            hlc: snapshot.checkpoint.flushed_max_hlc,
            epoch: self.shared.handler.current_epoch(),
        });

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<SnapshotOf<MetaDbTypeConfig, Self::SnapshotData>>, io::Error>
    {
        self.shared.load_current_snapshot_sync()
    }
}
