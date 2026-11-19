//! Deterministic `RaftStateMachine` backed by WAL-less `LifecycleStorageEngine`,
//! atomic SSTable snapshot build/install (`Resolution B6`), and `ShardApplyHandler`.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use openraft::entry::RaftEntry;
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine};
use openraft::type_config::alias::{
    CommittedLeaderIdOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf,
};
use openraft::vote::RaftLeaderId;
use openraft::{EntryPayload, OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use tokio::sync::watch;
use tokio_stream::StreamExt;

use metadb_core::codec;
use metadb_core::{
    append_crc32c_le, checksum_crc32c, verify_crc32c_le, HlcTimestamp, MetaDbError, Result,
    ShardEpoch, ShardId,
};
use metadb_storage::sstable::{sst_file_name, sync_dir};
use metadb_storage::{
    DurableShardCheckpoint, FlushIdentity, LifecycleStorageEngine, LsmEngineConfig, PreFlushHook,
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

    /// Result of a proposal that already committed, if this request repeats its id.
    ///
    /// Checked before the epoch fence. A discarded log entry may already have
    /// committed under an older epoch; executing it again would apply a
    /// non-idempotent command twice.
    fn previously_committed_proposal(
        &self,
        _request: &MetaDbRaftRequest,
        _engine: &LifecycleStorageEngine,
    ) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

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
        let cmd: BasicKvCommand = codec::deserialize(&request.payload)
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

#[derive(
    Debug, Clone, serde::Serialize, serde::Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
struct PersistedSnapshotMeta {
    #[wincode(with = "metadb_core::codec::SerdeCompat<SnapshotMetaOf<MetaDbTypeConfig>>")]
    meta: SnapshotMetaOf<MetaDbTypeConfig>,
    checkpoint: DurableShardCheckpoint,
    file_specs: Vec<(String, u64, u32)>,
}

struct CommandFlush<'a> {
    engine: &'a LifecycleStorageEngine,
    open: bool,
}

impl CommandFlush<'_> {
    fn commit(mut self) -> Result<()> {
        self.open = false;
        self.engine.end_command(true)
    }
}

impl Drop for CommandFlush<'_> {
    fn drop(&mut self) {
        if self.open {
            let _ = self.engine.end_command(false);
        }
    }
}

fn attach_flush_hook(engine: &LifecycleStorageEngine, handler: &Arc<dyn ShardApplyHandler>) {
    engine.set_pre_flush_hook(Arc::new(HandlerFlushHook {
        handler: handler.clone(),
    }));
}

struct HandlerFlushHook {
    handler: Arc<dyn ShardApplyHandler>,
}

impl PreFlushHook for HandlerFlushHook {
    fn on_seal_memtable(
        &self,
        sealed_raft_index: u64,
        sealed_max_hlc: HlcTimestamp,
        view_at_m: &metadb_storage::ViewAtRaftIndex<'_>,
    ) -> Result<Vec<(Vec<u8>, HlcTimestamp, metadb_storage::MvccValue)>> {
        match self.handler.pre_flush_hook() {
            Some(hook) => hook.on_seal_memtable(sealed_raft_index, sealed_max_hlc, view_at_m),
            None => Ok(Vec::new()),
        }
    }

    fn on_flush_durable(&self, durable_raft_index: u64, durable_max_hlc: HlcTimestamp) -> u64 {
        match self.handler.pre_flush_hook() {
            Some(hook) => hook.on_flush_durable(durable_raft_index, durable_max_hlc),
            None => 0,
        }
    }
}

fn checkpoint_log_id(checkpoint: &DurableShardCheckpoint) -> Option<LogIdOf<MetaDbTypeConfig>> {
    if (
        checkpoint.flushed_raft_term,
        checkpoint.flushed_raft_leader,
        checkpoint.flushed_raft_index,
    ) == (0, 0, 0)
    {
        None
    } else {
        Some(LogIdOf::<MetaDbTypeConfig>::new(
            CommittedLeaderIdOf::<MetaDbTypeConfig>::new(
                checkpoint.flushed_raft_term,
                checkpoint.flushed_raft_leader,
            ),
            checkpoint.flushed_raft_index,
        ))
    }
}

struct StateMachineShared {
    // Serializes a complete command (including metadata publication) against
    // checkpoints, snapshot replacement, and read-cut acquisition.
    operation_lock: Mutex<()>,
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
    /// Set when a required snapshot cannot be rebuilt. Cluster status must not
    /// keep reporting this process as a live voter after RaftCore has failed.
    unrecoverable: Mutex<Option<String>>,
}

impl StateMachineShared {
    fn current_engine(&self) -> Arc<LifecycleStorageEngine> {
        self.engine.read().unwrap().clone()
    }

    fn flush_to_lsm(&self) -> Result<(u64, DurableShardCheckpoint)> {
        let _operation = self.operation_lock.lock().unwrap();
        self.flush_to_lsm_locked()
    }

    fn flush_to_lsm_locked(&self) -> Result<(u64, DurableShardCheckpoint)> {
        let engine = self.current_engine();
        let last_log = *self.last_applied_log_id.read().unwrap();
        let last_hlc = *self.last_applied_hlc.read().unwrap();
        let membership = self.last_membership.read().unwrap().clone();
        let membership_bytes = codec::serialize_serde(&membership).unwrap_or_default();

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

    fn refresh_flushed_checkpoint(
        &self,
        engine: &LifecycleStorageEngine,
        log_id: &LogIdOf<MetaDbTypeConfig>,
        applied_hlc: HlcTimestamp,
    ) -> Result<()> {
        let current = engine.durable_checkpoint();
        // Only the apply that flushed this index may stamp its term and descriptor.
        if current.flushed_raft_index != log_id.index {
            return Ok(());
        }
        let membership = self.last_membership.read().unwrap().clone();
        let membership_bytes = codec::serialize_serde(&membership).unwrap_or_default();
        let descriptor = self.handler.encode_descriptor_bytes();
        let corrected = DurableShardCheckpoint {
            flushed_raft_term: log_id.committed_leader_id().term,
            flushed_raft_leader: log_id.committed_leader_id().node_id,
            flushed_raft_index: current.flushed_raft_index,
            flushed_max_hlc: current.flushed_max_hlc.max(applied_hlc),
            contiguous_aux_acked_index: current.contiguous_aux_acked_index,
            membership_bytes,
            descriptor_bytes: descriptor,
        };
        if corrected != current {
            engine.persist_checkpoint(corrected.clone())?;
        }
        self.watermarks
            .advance_state_flushed(corrected.flushed_raft_index);
        if self.handler.pre_flush_hook().is_some() {
            self.watermarks
                .advance_aux_outbox_durable(engine.last_aux_frontier());
        }
        Ok(())
    }

    fn build_snapshot_sync(&self) -> io::Result<SnapshotOf<MetaDbTypeConfig, SnapshotArchive>> {
        let _operation = self.operation_lock.lock().unwrap();
        let current_idx = self
            .last_applied_log_id
            .read()
            .unwrap()
            .map(|id| id.index)
            .unwrap_or(0);
        self.handler.prepare_for_snapshot(current_idx);

        let (_, _) = self
            .flush_to_lsm_locked()
            .map_err(|e| io::Error::other(e.to_string()))?;

        let engine = self.current_engine();
        let snaps_root = self.shard_dir.join("raft/snapshots");
        fs::create_dir_all(&snaps_root)?;

        // Strict `snapshot_index == checkpoint.flushed_raft_index` alignment (`Resolution B6`).
        let ckpt_preview = engine.durable_checkpoint();
        let snap_index = ckpt_preview.flushed_raft_index;
        let tmp_dir = snaps_root.join(format!("snap_{:020}.tmp", snap_index));
        if tmp_dir.exists() {
            fs::remove_dir_all(&tmp_dir)?;
        }

        // Do not delete `snap_<index>` before this temporary directory is complete.
        // A reader that opens the live directory during that gap sees ENOENT and
        // OpenRaft turns that into a fatal `Read Snapshot` error.
        let ckpt_lsm_dir = tmp_dir.join("lsm");
        let checkpoint = engine
            .create_checkpoint(&ckpt_lsm_dir)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let last_log_id = checkpoint_log_id(&checkpoint);

        let last_membership: StoredMembershipOf<MetaDbTypeConfig> =
            if checkpoint.membership_bytes.is_empty() {
                StoredMembership::default()
            } else {
                codec::deserialize_serde(&checkpoint.membership_bytes)
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
        let mut meta_bytes = codec::serialize(&persisted_meta)
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

        // Persist SNAPSHOT_META's directory entry before publishing the snapshot.
        sync_dir(&tmp_dir).map_err(|e| io::Error::other(e.to_string()))?;
        publish_snapshot_dir(&snaps_root, snap_index, &tmp_dir)?;

        self.watermarks
            .advance_last_snapshot(checkpoint.flushed_raft_index);

        Ok(Snapshot {
            meta,
            snapshot: SnapshotArchive { checkpoint, files },
        })
    }

    fn clear_unrecoverable(&self) {
        *self.unrecoverable.lock().unwrap() = None;
    }

    fn fail_unrecoverable<T>(&self, reason: impl Into<String>) -> io::Result<T> {
        let reason = reason.into();
        *self.unrecoverable.lock().unwrap() = Some(reason.clone());
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot unrecoverable: {reason}"),
        ))
    }

    /// True when this process has published a snapshot that is no longer a complete directory.
    fn published_snapshot_missing(&self) -> bool {
        let index = self.watermarks.snapshot_values().2;
        if index == 0 {
            return false;
        }
        let dir = self
            .shard_dir
            .join(format!("raft/snapshots/snap_{index:020}"));
        !snapshot_dir_is_complete(&dir)
    }

    fn load_current_snapshot_sync(
        &self,
    ) -> io::Result<Option<SnapshotOf<MetaDbTypeConfig, SnapshotArchive>>> {
        let snaps_root = self.shard_dir.join("raft/snapshots");
        let candidates = snapshot_candidates(&snaps_root);
        // A missing file used to return `NotFound`. OpenRaft maps that to
        // `StorageError { subject: Snapshot(None), verb: Read }` and stops RaftCore,
        // while the process kept answering cluster status from cached membership.
        // Skip a broken directory, including a `.bak` left by an in-progress replace.
        for (_, snap_dir) in &candidates {
            match load_snapshot_dir(snap_dir) {
                Ok(snapshot) => {
                    self.clear_unrecoverable();
                    return Ok(Some(snapshot));
                }
                Err(error) => {
                    tracing::warn!(
                        path = %snap_dir.display(),
                        error = %error,
                        "skipping unreadable raft snapshot directory"
                    );
                }
            }
        }

        let applied = self
            .last_applied_log_id
            .read()
            .unwrap()
            .map(|id| id.index)
            .unwrap_or(0);
        let flushed = self
            .current_engine()
            .durable_checkpoint()
            .flushed_raft_index;
        let raft_dir = self.shard_dir.join("raft");
        let purged = crate::log_store::read_persisted_purged_index(&raft_dir).unwrap_or(0);
        let snapshot_required =
            purged > 0 || self.watermarks.snapshot_values().2 > 0 || !candidates.is_empty();
        // The segmented raft log is the WAL until it is purged. Before that,
        // a missing snapshot file is not fatal: restart replays the log.
        if !snapshot_required {
            self.clear_unrecoverable();
            return Ok(None);
        }

        // After purge, the LSM is the only remaining copy. Rewrite the snapshot
        // from it before returning, so a live process does not keep serving
        // with the snapshot file gone.
        match self.build_snapshot_sync() {
            Ok(snapshot) => {
                let covered = snapshot
                    .meta
                    .last_log_id
                    .as_ref()
                    .map(|id| id.index)
                    .unwrap_or(0);
                if purged > covered {
                    return self.fail_unrecoverable(format!(
                        "rebuilt snapshot index {covered} does not cover purged raft log {purged}. The segmented raft log is the WAL only until it is purged. A missing snapshot is rebuilt from the LSM. If that LSM is also missing, committed data is not reconstructed"
                    ));
                }
                self.clear_unrecoverable();
                Ok(Some(snapshot))
            }
            Err(error) => self.fail_unrecoverable(format!(
                "cannot rebuild a missing snapshot from the LSM ({error}). purged log index is {purged}, flushed LSM index is {flushed}, applied index is {applied}. The segmented raft log is the WAL only until it is purged. If the log and the LSM are both gone, committed data is not reconstructed"
            )),
        }
    }
}

/// Highest snapshot index whose directory is complete. Log purge must not pass this.
pub(crate) fn highest_durable_snapshot_index(raft_dir: &Path) -> u64 {
    newest_complete_snapshot(raft_dir)
        .map(|snap| snap.index)
        .unwrap_or(0)
}

/// Newest complete snapshot directory, preferring `snap_<index>` over `snap_<index>.bak`.
pub(crate) fn newest_complete_snapshot(raft_dir: &Path) -> Option<CompleteSnapshot> {
    snapshot_candidates(&raft_dir.join("snapshots"))
        .into_iter()
        .find(|(_, path)| snapshot_dir_is_complete(path))
        .map(|(index, path)| CompleteSnapshot { index, path })
}

/// Newest complete snapshot whose index is still covered by a backup hold.
pub(crate) fn newest_snapshot_at_or_below(raft_dir: &Path, index: u64) -> Option<CompleteSnapshot> {
    snapshot_candidates(&raft_dir.join("snapshots"))
        .into_iter()
        .filter(|(candidate, path)| *candidate <= index && snapshot_dir_is_complete(path))
        .max_by_key(|(candidate, _)| *candidate)
        .map(|(index, path)| CompleteSnapshot { index, path })
}

#[derive(Debug, Clone)]
pub(crate) struct CompleteSnapshot {
    pub index: u64,
    pub path: PathBuf,
}

fn snapshot_candidates(snaps_root: &Path) -> Vec<(u64, PathBuf)> {
    let Ok(entries) = fs::read_dir(snaps_root) else {
        return Vec::new();
    };
    let mut found = Vec::new();
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(rest) = name.strip_prefix("snap_") else {
            continue;
        };
        let (index_text, rank) = if let Some(index_text) = rest.strip_suffix(".bak") {
            (index_text, 0u8)
        } else if rest.ends_with(".tmp") || rest.contains('.') {
            continue;
        } else {
            (rest, 1u8)
        };
        let Ok(index) = index_text.parse::<u64>() else {
            continue;
        };
        found.push((index, rank, entry.path()));
    }
    found.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
    found
        .into_iter()
        .map(|(index, _, path)| (index, path))
        .collect()
}

pub(crate) fn snapshot_dir_is_complete(snap_dir: &Path) -> bool {
    load_snapshot_meta(snap_dir)
        .map(|meta| snapshot_files_present(snap_dir, &meta))
        .unwrap_or(false)
}

fn snapshot_files_present(snap_dir: &Path, meta: &PersistedSnapshotMeta) -> bool {
    let lsm_dir = snap_dir.join("lsm");
    meta.file_specs.iter().all(|(name, expected_len, _)| {
        fs::metadata(lsm_dir.join(name))
            .map(|info| info.is_file() && info.len() == *expected_len)
            .unwrap_or(false)
    })
}

fn load_snapshot_meta(snap_dir: &Path) -> io::Result<PersistedSnapshotMeta> {
    let mut raw_meta = Vec::new();
    File::open(snap_dir.join("SNAPSHOT_META"))?.read_to_end(&mut raw_meta)?;
    let body = verify_crc32c_le(&raw_meta, "SNAPSHOT_META")
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    codec::deserialize(body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

fn load_snapshot_dir(snap_dir: &Path) -> io::Result<SnapshotOf<MetaDbTypeConfig, SnapshotArchive>> {
    let pmeta = load_snapshot_meta(snap_dir)?;
    let lsm_dir = snap_dir.join("lsm");
    let mut files = Vec::with_capacity(pmeta.file_specs.len());
    for (fname, expected_len, expected_crc) in &pmeta.file_specs {
        let mut buf = Vec::new();
        File::open(lsm_dir.join(fname))?.read_to_end(&mut buf)?;
        if buf.len() as u64 != *expected_len || checksum_crc32c(&buf) != *expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot file {fname} failed length/CRC32C check"),
            ));
        }
        files.push(SnapshotFileEntry {
            name: fname.clone(),
            expected_len: *expected_len,
            crc32c: *expected_crc,
            bytes: buf,
        });
    }
    Ok(Snapshot {
        meta: pmeta.meta,
        snapshot: SnapshotArchive {
            checkpoint: pmeta.checkpoint,
            files,
        },
    })
}

/// Publishes `tmp_dir` as `snap_<index>` without removing the previous complete
/// snapshot until the new directory is renamed into place and the parent is synced.
fn publish_snapshot_dir(snaps_root: &Path, index: u64, tmp_dir: &Path) -> io::Result<()> {
    let final_dir = snaps_root.join(format!("snap_{index:020}"));
    let bak_dir = snaps_root.join(format!("snap_{index:020}.bak"));
    // A backup hold keeps the snapshot bytes an in-progress copy may be reading.
    // Replacing or deleting that directory would make the backup unrestorable.
    if snapshot_hold_protects(snaps_root, index) && (final_dir.exists() || bak_dir.exists()) {
        let _ = fs::remove_dir_all(tmp_dir);
        return Ok(());
    }
    if bak_dir.exists() && !snapshot_hold_protects(snaps_root, index) {
        fs::remove_dir_all(&bak_dir)?;
    }
    if final_dir.exists() {
        fs::rename(&final_dir, &bak_dir)?;
    }
    fs::rename(tmp_dir, &final_dir)?;
    sync_dir(snaps_root).map_err(|e| io::Error::other(e.to_string()))?;
    if bak_dir.exists() && !snapshot_hold_protects(snaps_root, index) {
        let _ = fs::remove_dir_all(&bak_dir);
    }
    if let Ok(entries) = fs::read_dir(snaps_root) {
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            let Some(rest) = name.strip_prefix("snap_") else {
                continue;
            };
            let protected_name = if let Some(index_text) = rest.strip_suffix(".bak") {
                index_text
            } else if rest.ends_with(".tmp") || rest.contains('.') {
                continue;
            } else {
                rest
            };
            if protected_name
                .parse::<u64>()
                .ok()
                .is_some_and(|older| older < index && !snapshot_hold_protects(snaps_root, older))
            {
                let _ = fs::remove_dir_all(entry.path());
            }
        }
    }
    sync_dir(snaps_root).map_err(|e| io::Error::other(e.to_string()))?;
    Ok(())
}

fn snapshot_hold_protects(snaps_root: &Path, index: u64) -> bool {
    crate::backup::snapshot_index_protected(snaps_root, index)
}

/// Writes an empty complete snapshot directory at `index`.
///
/// Recovery tests use this to cover a log prefix without building an LSM
/// checkpoint. A directory without `SNAPSHOT_META` does not cover a purge.
pub fn write_complete_snapshot_marker(raft_dir: &Path, index: u64) -> io::Result<()> {
    let snaps_root = raft_dir.join("snapshots");
    fs::create_dir_all(&snaps_root)?;
    let snap_dir = snaps_root.join(format!("snap_{index:020}"));
    if snap_dir.exists() {
        if snapshot_hold_protects(&snaps_root, index) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "backup hold protects this snapshot directory",
            ));
        }
        fs::remove_dir_all(&snap_dir)?;
    }
    fs::create_dir_all(&snap_dir)?;
    let persisted = PersistedSnapshotMeta {
        meta: SnapshotMeta {
            last_log_id: Some(LogIdOf::<MetaDbTypeConfig>::new(
                CommittedLeaderIdOf::<MetaDbTypeConfig>::new(1, 1),
                index,
            )),
            last_membership: StoredMembership::default(),
        },
        checkpoint: DurableShardCheckpoint {
            flushed_raft_index: index,
            ..DurableShardCheckpoint::default()
        },
        file_specs: Vec::new(),
    };
    let mut bytes = codec::serialize(&persisted)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    append_crc32c_le(&mut bytes);
    fs::write(snap_dir.join("SNAPSHOT_META"), bytes)?;
    Ok(())
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
        attach_flush_hook(&engine, &handler);
        let cp = engine.durable_checkpoint();

        // Report `MANIFEST`'s `flushed_raft_log_id` (`F`) as `applied_state` on startup so OpenRaft
        // automatically replays committed `.raftlog` entries strictly above `F` (`Resolution B5`)!
        let initial_log_id = checkpoint_log_id(&cp);
        let initial_membership: StoredMembershipOf<MetaDbTypeConfig> =
            if cp.membership_bytes.is_empty() {
                StoredMembership::default()
            } else {
                codec::deserialize_serde(&cp.membership_bytes).unwrap_or_default()
            };

        watermarks.advance_state_flushed(cp.flushed_raft_index);
        watermarks.advance_aux_outbox_durable(cp.contiguous_aux_acked_index);
        // The MANIFEST flushed index is the local durability floor. Without this,
        // `last_snapshot_index` stays 0 across process restarts and the 3-watermark
        // purge gate never moves in production until an explicit OpenRaft snapshot.
        watermarks.advance_last_snapshot(cp.flushed_raft_index);
        handler.on_snapshot_installed(&cp, &engine);

        let initial_frontier = AppliedFrontier {
            raft_index: cp.flushed_raft_index,
            hlc: cp.flushed_max_hlc,
            epoch: handler.current_epoch(),
        };
        let (frontier_tx, frontier_rx) = watch::channel(initial_frontier);

        let shared = Arc::new(StateMachineShared {
            operation_lock: Mutex::new(()),
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
            unrecoverable: Mutex::new(None),
        });

        Ok((Self { shared }, frontier_rx))
    }

    pub fn engine(&self) -> Arc<LifecycleStorageEngine> {
        self.shared.current_engine()
    }

    /// Reason this process must not be advertised as a live voter.
    pub fn storage_unrecoverable(&self) -> Option<String> {
        self.shared.unrecoverable.lock().unwrap().clone()
    }

    /// The snapshot index already published in memory has no complete directory.
    pub fn published_snapshot_missing(&self) -> bool {
        self.shared.published_snapshot_missing()
    }

    pub fn handler(&self) -> &Arc<dyn ShardApplyHandler> {
        &self.shared.handler
    }

    pub fn flush_to_lsm(&self) -> Result<(u64, DurableShardCheckpoint)> {
        self.shared.flush_to_lsm()
    }

    /// Pin a read cut only between complete state-machine commands and validate
    /// the epoch under the same lock as split/membership application.
    pub fn acquire_snapshot(
        &self,
        expected_epoch: ShardEpoch,
        read_index: u64,
        barrier_hlc: Option<HlcTimestamp>,
    ) -> Result<metadb_storage::SnapshotGuard> {
        let _operation = self.shared.operation_lock.lock().unwrap();
        let actual = self.shared.handler.current_epoch();
        if actual != expected_epoch {
            return Err(MetaDbError::EpochMismatch {
                shard_id: self.shared.shard_id,
                expected: expected_epoch,
                actual,
            });
        }
        let engine = self.shared.current_engine();
        match barrier_hlc {
            Some(hlc) => engine.acquire_fixed_snapshot(hlc),
            None => engine.acquire_linearizable_snapshot(read_index),
        }
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
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let _operation = self.shared.operation_lock.lock().unwrap();
            let engine = self.shared.current_engine();
            let log_id = entry.log_id();

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
                    let apply_res = match self
                        .shared
                        .handler
                        .previously_committed_proposal(&req, &engine)
                    {
                        Ok(Some(prior)) => {
                            self.shared
                                .watermarks
                                .advance_aux_for_non_aux_entry(log_id.index);
                            Ok(prior)
                        }
                        Err(error) => Err(error),
                        Ok(None) if req.expected_epoch != cur_epoch => {
                            Err(MetaDbError::EpochMismatch {
                                shard_id: self.shared.shard_id,
                                expected: req.expected_epoch,
                                actual: cur_epoch,
                            })
                        }
                        Ok(None) => {
                            let membership = self.shared.last_membership.read().unwrap().clone();
                            engine.set_flush_identity(FlushIdentity {
                                term: log_id.committed_leader_id().term,
                                leader: log_id.committed_leader_id().node_id,
                                membership: codec::serialize_serde(&membership).unwrap_or_default(),
                                descriptor: self.shared.handler.encode_descriptor_bytes(),
                                aux_index: self.shared.watermarks.snapshot_values().1,
                            });
                            // Seal only after every key and side effect of this command is in the memtable.
                            engine.begin_command();
                            let command = CommandFlush {
                                engine: &engine,
                                open: true,
                            };
                            match self.shared.handler.apply_command(
                                log_id.index,
                                commit_hlc,
                                &req,
                                &engine,
                            ) {
                                Ok(bytes) => command.commit().map(|()| bytes),
                                Err(error) => {
                                    drop(command);
                                    Err(error)
                                }
                            }
                        }
                    };
                    let apply_res = match apply_res {
                        Ok(bytes) => self
                            .shared
                            .refresh_flushed_checkpoint(&engine, &log_id, commit_hlc)
                            .map(|()| bytes),
                        Err(error) => Err(error),
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

            // Publish the applied index only after the handler has finished writing.
            // A concurrent checkpoint must not observe index N before N's mutation is in the memtable.
            *self.shared.last_applied_log_id.write().unwrap() = Some(log_id);

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
        let _operation = self.shared.operation_lock.lock().unwrap();
        let snap_idx = meta.last_log_id.map(|id| id.index).unwrap_or(0);
        if snapshot.checkpoint.flushed_raft_index != snap_idx {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "snapshot index mismatch: meta={} != checkpoint.flushed_raft_index={}",
                    snap_idx, snapshot.checkpoint.flushed_raft_index
                ),
            ));
        }
        if meta.last_log_id != checkpoint_log_id(&snapshot.checkpoint) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot log identity differs from checkpoint",
            ));
        }
        let membership: StoredMembershipOf<MetaDbTypeConfig> =
            if snapshot.checkpoint.membership_bytes.is_empty() {
                StoredMembership::default()
            } else {
                codec::deserialize_serde(&snapshot.checkpoint.membership_bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
            };
        if membership != meta.last_membership {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot membership differs from checkpoint",
            ));
        }

        // Verify per-file `(expected_len, crc32c)` BEFORE staging or touching live `lsm/`.
        let mut file_specs = Vec::with_capacity(snapshot.files.len());
        let mut supplied_names = BTreeSet::new();
        for entry in &snapshot.files {
            if entry.name.is_empty()
                || entry.name == "."
                || entry.name.contains('/')
                || entry.name.contains('\\')
                || entry.name.contains("..")
                || !supplied_names.insert(entry.name.clone())
            {
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
        if !supplied_names.contains("CURRENT") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot is missing CURRENT",
            ));
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
            LifecycleStorageEngine::open_lsm_existing(
                self.shared.shard_id,
                &staging_dir,
                self.shared.block_cache.clone(),
                self.shared.lsm_config.clone(),
            )
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?,
        );
        if verified_engine.durable_checkpoint() != snapshot.checkpoint {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged MANIFEST checkpoint differs from advertised checkpoint",
            ));
        }
        let mut required_names = BTreeSet::from([
            "CURRENT".to_string(),
            fs::read_to_string(staging_dir.join("CURRENT"))?
                .trim()
                .to_string(),
        ]);
        for reader in verified_engine
            .backend()
            .current_version()
            .levels
            .iter()
            .flatten()
        {
            required_names.insert(sst_file_name(reader.meta().file_id));
            reader
                .verify_all_data_blocks()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        }
        if supplied_names != required_names {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot file set differs from the MANIFEST requirements",
            ));
        }
        drop(verified_engine);

        if let Some(current) = *self.shared.last_applied_log_id.read().unwrap() {
            if snap_idx == current.index && meta.last_log_id != Some(current) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot conflicts with the applied log identity",
                ));
            }
            if snap_idx <= current.index {
                fs::remove_dir_all(&staging_dir)?;
                return Ok(());
            }
        }

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
        attach_flush_hook(&reopened, &self.shared.handler);

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
        let saved_tmp = snaps_root.join(format!("snap_{:020}.tmp", snap_idx));
        if saved_tmp.exists() {
            fs::remove_dir_all(&saved_tmp)?;
        }
        let _ = reopened
            .create_checkpoint(&saved_tmp.join("lsm"))
            .map_err(|e| io::Error::other(e.to_string()))?;
        let pmeta = PersistedSnapshotMeta {
            meta: meta.clone(),
            checkpoint: snapshot.checkpoint.clone(),
            file_specs,
        };
        let mut meta_bytes = codec::serialize(&pmeta)
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
        sync_dir(&saved_tmp).map_err(|e| io::Error::other(e.to_string()))?;
        publish_snapshot_dir(&snaps_root, snap_idx, &saved_tmp)?;

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
