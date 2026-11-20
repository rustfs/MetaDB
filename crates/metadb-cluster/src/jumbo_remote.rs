//! Loopback placement of one Jumbo shard directory onto another listener.
//!
//! The durability class matches the local dedicated-disk move: bytes are
//! checked, `MOVE_STAGED` is fsynced, then `MOVE_LIVE` is renamed into place.
//! The destination does not answer a placed-file read until that live cut's
//! directory fsync succeeds. A failure before `MOVE_LIVE` is visible removes
//! the destination tree and leaves the source directory in place.
//!
//! This listener is not started by `metadbd`. It binds `127.0.0.1` only. It
//! does not open Raft on the copied directory, does not call `transfer_leader`,
//! does not replicate `GLOBAL_CELL_DIRECTORY`, and does not forward requests.
//! A spare-voter replacement is recorded only after the live cut. Placement
//! rows in `CELL_CONTROL_PLANE` are not deleted here.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use metadb_core::codec;
use metadb_core::{
    append_crc32c_le, checksum_crc32c, verify_crc32c_le, MetaDbError, NodeId, Result, ShardId,
};
use metadb_storage::sstable::sync_dir;

use crate::identity::node_id_in_ha_range;
use crate::jumbo_files::{
    live_dirsync_is_durable, load_live_dir, refuse_dedicated_root, remove_tree, seal_live_dirsync,
    shard_move_dir, testing_fail_live_dirsync_at, write_move_phase, MovePhaseError, MovedFile,
    JUMBO_LIVE_DURABLE_FILE, JUMBO_MOVE_FILE, MOVE_LIVE, MOVE_STAGED,
};
use crate::jumbo_placement::SpareDrainPlan;

pub const JUMBO_SPARE_DRAIN_FILE: &str = "JUMBO_SPARE_DRAIN";
pub const JUMBO_REMOTE_HANDOFF_FILE: &str = "JUMBO_REMOTE_HANDOFF";

const FILE_VERSION: u32 = 1;
const WIRE_VERSION: u32 = 1;
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_FILES: usize = 4_096;
const MAX_FRAME: usize = (MAX_FILE_BYTES as usize) + 4_096;

const KIND_HELLO: u32 = 1;
const KIND_HELLO_OK: u32 = 2;
const KIND_FILE: u32 = 3;
const KIND_FILE_OK: u32 = 4;
const KIND_STAGE: u32 = 5;
const KIND_STAGE_OK: u32 = 6;
const KIND_LIVE: u32 = 7;
const KIND_LIVE_OK: u32 = 8;
const KIND_ABORT: u32 = 9;
const KIND_ERR: u32 = 10;
const KIND_LIVE_DIRSYNC: u32 = 11;

const HELLO_ACCEPT: u32 = 1;
const HELLO_ALREADY_LIVE: u32 = 2;
/// `MOVE_LIVE` is visible. The destination directory fsync has not finished.
const HELLO_LIVE_NOT_DURABLE: u32 = 3;

/// One spare replacement committed only after a remote `MOVE_LIVE` cut.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct SpareDrainRecord {
    pub jumbo_shard_id: u64,
    pub drain_shard_id: u64,
    pub removed: u64,
    pub added: u64,
    pub voters_after: Vec<u64>,
    /// True only when this process observed the planned voter set on a local Raft group.
    pub raft_membership_changed: bool,
    /// This path does not call `transfer_leader`. A stored record keeps false.
    pub transfer_leader_called: bool,
}

/// The source process has handed the shard directory to a destination that
/// already has `MOVE_LIVE`. Restart must not open a fresh empty group.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct RemoteHandoffRecord {
    pub shard_id: u64,
    pub dest_node: u64,
    /// This increment does not open Raft on the destination. Stored false.
    pub destination_raft_opened: bool,
    /// This increment does not call `transfer_leader`. Stored false.
    pub transfer_leader_called: bool,
}

/// Result of a remote placement that finished the live cut and the spare record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteMoveOutcome {
    pub drain_shard_id: ShardId,
    pub removed: NodeId,
    pub added: NodeId,
    pub voters_after: BTreeSet<NodeId>,
    pub raft_membership_changed: bool,
    pub destination_raft_opened: bool,
    pub transfer_leader_called: bool,
}

#[derive(Debug)]
pub struct PushFailure {
    pub live_visible: bool,
    pub message: String,
}

impl std::fmt::Display for PushFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

pub enum PushResult {
    /// Directory fsync of `MOVE_LIVE` finished. The source may be removed.
    LiveCut,
}

/// Disk endpoint that accepts one Jumbo shard tree over a loopback TCP session.
pub struct RemoteJumboDisk {
    node_id: NodeId,
    dedicated_root: PathBuf,
    listener: TcpListener,
    local_addr: SocketAddr,
    serving: Mutex<BTreeSet<ShardId>>,
    fail_before_live: AtomicBool,
    /// The next `MOVE_LIVE` directory fsync on this disk fails after the rename.
    fail_visible_dirsync: AtomicBool,
}

impl RemoteJumboDisk {
    pub async fn bind(node_id: NodeId, data_dir: &Path, dedicated_root: &Path) -> Result<Self> {
        node_id_in_ha_range(node_id)?;
        refuse_dedicated_root(data_dir, dedicated_root)?;
        let serving = scan_serving(dedicated_root)?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| MetaDbError::StorageIoError(error.to_string()))?;
        let local_addr = listener
            .local_addr()
            .map_err(|error| MetaDbError::StorageIoError(error.to_string()))?;
        refuse_non_loopback(local_addr)?;
        Ok(Self {
            node_id,
            dedicated_root: dedicated_root.to_path_buf(),
            listener,
            local_addr,
            serving: Mutex::new(serving),
            fail_before_live: AtomicBool::new(false),
            fail_visible_dirsync: AtomicBool::new(false),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn dedicated_root(&self) -> &Path {
        &self.dedicated_root
    }

    /// The next live cut fails before `MOVE_LIVE` is renamed into place.
    pub fn testing_fail_before_next_live_cut(&self) {
        self.fail_before_live.store(true, Ordering::Relaxed);
    }

    /// The next live rename is left in place, then reported as a directory
    /// fsync failure. The destination does not start serving.
    pub fn testing_report_visible_dirsync_failure(&self) {
        self.fail_visible_dirsync.store(true, Ordering::Relaxed);
    }

    pub fn is_serving(&self, shard_id: ShardId) -> bool {
        self.serving.lock().unwrap().contains(&shard_id)
    }

    /// Reads a placed file only after the live cut has been published.
    pub fn read_placed_file(&self, shard_id: ShardId, relative: &str) -> Result<Vec<u8>> {
        if !self.is_serving(shard_id) {
            return Err(MetaDbError::ShardRoutingError(format!(
                "destination is not serving shard {shard_id}; the live cut is not complete and transfer_leader was not called"
            )));
        }
        if load_live_dir(&self.dedicated_root, shard_id)?.is_none() {
            return Err(MetaDbError::StorageIoError(format!(
                "shard {shard_id} is marked serving without MOVE_LIVE"
            )));
        }
        let path = placed_path(&shard_move_dir(&self.dedicated_root, shard_id), relative)?;
        refuse_symlink(&path)?;
        let mut bytes = Vec::new();
        File::open(&path)?.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    pub async fn serve(self: Arc<Self>) {
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    if let Err(error) = self.handle(stream).await {
                        tracing::warn!(%error, "jumbo remote session ended");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "jumbo remote listener stopped");
                    break;
                }
            }
        }
    }

    async fn handle(&self, mut stream: TcpStream) -> Result<()> {
        let _ = stream.set_nodelay(true);
        let mut session = SessionGuard {
            root: &self.dedicated_root,
            shard_id: None,
            keep_tree: false,
        };
        let mut received: Vec<MovedFile> = Vec::new();
        loop {
            let frame = match read_frame(&mut stream).await {
                Ok(frame) => frame,
                Err(error) => {
                    if session.shard_id.is_some() && !session.keep_tree {
                        session.cleanup();
                    }
                    return Err(error);
                }
            };
            match frame.kind {
                KIND_HELLO => {
                    let (shard_id, _source, dedicated) = decode_hello(&frame.payload)?;
                    if dedicated != self.node_id {
                        let message = format!(
                            "destination node {} refused dedicated node {dedicated}; nothing was published",
                            self.node_id
                        );
                        write_err(&mut stream, &message).await?;
                        return Err(MetaDbError::ShardRoutingError(message));
                    }
                    let dest = shard_move_dir(&self.dedicated_root, shard_id);
                    if load_live_dir(&self.dedicated_root, shard_id)?.is_some() {
                        session.shard_id = Some(shard_id);
                        session.keep_tree = true;
                        if live_dirsync_is_durable(&dest)? {
                            self.mark_serving(shard_id);
                            write_frame(
                                &mut stream,
                                KIND_HELLO_OK,
                                &HELLO_ALREADY_LIVE.to_le_bytes(),
                            )
                            .await?;
                            return Ok(());
                        }
                        // The rename is visible. The directory fsync is not.
                        // Do not serve and do not let the source delete its copy.
                        write_frame(
                            &mut stream,
                            KIND_HELLO_OK,
                            &HELLO_LIVE_NOT_DURABLE.to_le_bytes(),
                        )
                        .await?;
                        continue;
                    }
                    if dest.exists() {
                        remove_tree(&dest)?;
                    }
                    if let Some(parent) = dest.parent() {
                        fs::create_dir_all(parent)?;
                        refuse_symlink(parent)?;
                    }
                    fs::create_dir_all(&dest)?;
                    refuse_symlink(&dest)?;
                    session.shard_id = Some(shard_id);
                    session.keep_tree = false;
                    received.clear();
                    write_frame(&mut stream, KIND_HELLO_OK, &HELLO_ACCEPT.to_le_bytes()).await?;
                }
                KIND_FILE => {
                    let shard_id = session.shard_id.ok_or_else(|| {
                        MetaDbError::StorageIoError(
                            "jumbo file arrived before HELLO; nothing was published".to_string(),
                        )
                    })?;
                    if session.keep_tree {
                        return Err(MetaDbError::StorageIoError(
                            "refusing to write into a destination that already has MOVE_LIVE"
                                .to_string(),
                        ));
                    }
                    if received.len() >= MAX_FILES {
                        session.cleanup();
                        let message =
                            "jumbo remote move has too many files; MOVE_LIVE was not published";
                        write_err(&mut stream, message).await?;
                        return Err(MetaDbError::StorageIoError(message.to_string()));
                    }
                    let file = write_received_file(
                        &shard_move_dir(&self.dedicated_root, shard_id),
                        &frame.payload,
                    )?;
                    received.push(file);
                    write_frame(&mut stream, KIND_FILE_OK, &[]).await?;
                }
                KIND_STAGE => {
                    let shard_id = session.shard_id.ok_or_else(|| {
                        MetaDbError::StorageIoError(
                            "jumbo stage arrived before HELLO; nothing was published".to_string(),
                        )
                    })?;
                    let manifest = decode_manifest(&frame.payload)?;
                    if manifest != received {
                        session.cleanup();
                        let message =
                            "jumbo stage manifest does not match the received files; MOVE_LIVE was not published";
                        write_err(&mut stream, message).await?;
                        return Err(MetaDbError::StorageIoError(message.to_string()));
                    }
                    let dest = shard_move_dir(&self.dedicated_root, shard_id);
                    verify_received(&dest, &manifest)?;
                    sync_tree(&dest)?;
                    if let Err(error) = write_move_phase(&dest, shard_id, MOVE_STAGED, &manifest) {
                        session.cleanup();
                        let message = format!(
                            "MOVE_STAGED was not published; the source copy was not deleted by this listener: {error}"
                        );
                        write_err(&mut stream, &message).await?;
                        return Err(MetaDbError::StorageIoError(message));
                    }
                    write_frame(&mut stream, KIND_STAGE_OK, &[]).await?;
                }
                KIND_LIVE => {
                    let shard_id = session.shard_id.ok_or_else(|| {
                        MetaDbError::StorageIoError(
                            "jumbo live cut arrived before HELLO; nothing was published"
                                .to_string(),
                        )
                    })?;
                    let dest = shard_move_dir(&self.dedicated_root, shard_id);
                    if load_live_dir(&self.dedicated_root, shard_id)?.is_some() {
                        if self.fail_visible_dirsync.swap(false, Ordering::Relaxed) {
                            testing_fail_live_dirsync_at(&dest, 2);
                        }
                        match seal_live_dirsync(&dest, shard_id) {
                            Ok(()) => {
                                session.keep_tree = true;
                                self.mark_serving(shard_id);
                                write_frame(&mut stream, KIND_LIVE_OK, &[]).await?;
                                return Ok(());
                            }
                            Err(MovePhaseError::LiveVisibleDirSyncFailed(error)) => {
                                session.keep_tree = true;
                                let message = format!(
                                    "MOVE_LIVE is visible on {} but the directory fsync failed; the destination copy was not deleted and it is not serving: {error}",
                                    dest.display()
                                );
                                write_frame(&mut stream, KIND_LIVE_DIRSYNC, message.as_bytes())
                                    .await?;
                                return Err(MetaDbError::StorageIoError(message));
                            }
                            Err(MovePhaseError::NotPublished(error)) => {
                                let message = format!(
                                    "failed before the live cut; MOVE_LIVE was not published: {error}"
                                );
                                write_err(&mut stream, &message).await?;
                                return Err(MetaDbError::StorageIoError(message));
                            }
                        }
                    }
                    if self.fail_before_live.swap(false, Ordering::Relaxed) {
                        session.cleanup();
                        let message = "failed before the live cut; MOVE_LIVE was not published";
                        write_err(&mut stream, message).await?;
                        return Err(MetaDbError::StorageIoError(message.to_string()));
                    }
                    if self.fail_visible_dirsync.swap(false, Ordering::Relaxed) {
                        testing_fail_live_dirsync_at(&dest, 2);
                    }
                    match write_move_phase(&dest, shard_id, MOVE_LIVE, &received) {
                        Ok(()) => {
                            session.keep_tree = true;
                            self.mark_serving(shard_id);
                            write_frame(&mut stream, KIND_LIVE_OK, &[]).await?;
                            return Ok(());
                        }
                        Err(MovePhaseError::LiveVisibleDirSyncFailed(error)) => {
                            session.keep_tree = true;
                            let message = format!(
                                "MOVE_LIVE is visible on {} but the directory fsync failed; the destination copy was not deleted and it is not serving: {error}",
                                dest.display()
                            );
                            write_frame(&mut stream, KIND_LIVE_DIRSYNC, message.as_bytes()).await?;
                            return Err(MetaDbError::StorageIoError(message));
                        }
                        Err(MovePhaseError::NotPublished(error)) => {
                            session.cleanup();
                            let message = format!(
                                "failed before the live cut; MOVE_LIVE was not published: {error}"
                            );
                            write_err(&mut stream, &message).await?;
                            return Err(MetaDbError::StorageIoError(message));
                        }
                    }
                }
                KIND_ABORT => {
                    if !session.keep_tree {
                        session.cleanup();
                    }
                    return Ok(());
                }
                other => {
                    return Err(MetaDbError::StorageIoError(format!(
                        "unknown jumbo remote frame {other}"
                    )));
                }
            }
        }
    }

    fn mark_serving(&self, shard_id: ShardId) {
        self.serving.lock().unwrap().insert(shard_id);
    }
}

struct SessionGuard<'a> {
    root: &'a Path,
    shard_id: Option<ShardId>,
    keep_tree: bool,
}

impl SessionGuard<'_> {
    fn cleanup(&mut self) {
        let Some(shard_id) = self.shard_id else {
            return;
        };
        if self.keep_tree {
            return;
        }
        if load_live_dir(self.root, shard_id).ok().flatten().is_some() {
            self.keep_tree = true;
            return;
        }
        let _ = remove_tree(&shard_move_dir(self.root, shard_id));
        self.shard_id = None;
    }
}

impl Drop for SessionGuard<'_> {
    fn drop(&mut self) {
        self.cleanup();
    }
}

pub fn refuse_non_loopback(addr: SocketAddr) -> Result<()> {
    if addr.ip().is_loopback() {
        Ok(())
    } else {
        Err(MetaDbError::ShardRoutingError(format!(
            "jumbo remote move to {addr} was refused; this increment listens on 127.0.0.1 only and does not call transfer_leader"
        )))
    }
}

pub async fn push_jumbo_shard(
    source_dir: &Path,
    shard_id: ShardId,
    source_node: NodeId,
    dest_node: NodeId,
    dest_addr: SocketAddr,
) -> std::result::Result<PushResult, PushFailure> {
    if let Err(error) = refuse_non_loopback(dest_addr) {
        return Err(before_live(error.to_string()));
    }
    let files = match list_shard_files(source_dir) {
        Ok(files) => files,
        Err(error) => return Err(before_live(error.to_string())),
    };
    if files.is_empty() {
        return Err(before_live(format!(
            "jumbo shard {shard_id} directory {} has no files; MOVE_LIVE was not published",
            source_dir.display()
        )));
    }
    let mut stream = match TcpStream::connect(dest_addr).await {
        Ok(stream) => stream,
        Err(error) => {
            return Err(before_live(format!(
                "jumbo remote destination {dest_addr} did not accept the session: {error}"
            )))
        }
    };
    let _ = stream.set_nodelay(true);
    let hello = encode_hello(shard_id, source_node, dest_node);
    if let Err(error) = write_frame(&mut stream, KIND_HELLO, &hello).await {
        return Err(before_live(error.to_string()));
    }
    let hello_ok = match read_frame(&mut stream).await {
        Ok(frame) => frame,
        Err(error) => return Err(before_live(error.to_string())),
    };
    if hello_ok.kind == KIND_ERR {
        return Err(before_live(message_of(&hello_ok.payload)));
    }
    if hello_ok.kind != KIND_HELLO_OK || hello_ok.payload.len() != 4 {
        return Err(before_live(
            "jumbo remote destination returned an unexpected hello".to_string(),
        ));
    }
    let status = u32::from_le_bytes(hello_ok.payload.as_slice().try_into().unwrap());
    if status == HELLO_ALREADY_LIVE {
        return Ok(PushResult::LiveCut);
    }
    if status == HELLO_LIVE_NOT_DURABLE {
        return seal_from_open_session(&mut stream).await;
    }
    if status != HELLO_ACCEPT {
        return Err(before_live(format!(
            "jumbo remote destination refused the hello with status {status}"
        )));
    }
    let mut manifest = Vec::with_capacity(files.len());
    for (relative, path) in &files {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) => {
                abort_best_effort(&mut stream).await;
                return Err(before_live(format!(
                    "failed to read {} before the live cut: {error}",
                    path.display()
                )));
            }
        };
        if bytes.len() as u64 > MAX_FILE_BYTES {
            abort_best_effort(&mut stream).await;
            return Err(before_live(format!(
                "jumbo file {} is larger than {MAX_FILE_BYTES} bytes; MOVE_LIVE was not published",
                relative
            )));
        }
        let crc = checksum_crc32c(&bytes);
        let payload = encode_file(relative, crc, &bytes);
        if let Err(error) = write_frame(&mut stream, KIND_FILE, &payload).await {
            abort_best_effort(&mut stream).await;
            return Err(before_live(error.to_string()));
        }
        match expect_ok(&mut stream, KIND_FILE_OK).await {
            Ok(()) => {}
            Err(error) => {
                abort_best_effort(&mut stream).await;
                return Err(error);
            }
        }
        manifest.push(MovedFile {
            relative_path: relative.clone(),
            len: bytes.len() as u64,
            crc32c: crc,
        });
    }
    let stage = encode_manifest(&manifest);
    if let Err(error) = write_frame(&mut stream, KIND_STAGE, &stage).await {
        abort_best_effort(&mut stream).await;
        return Err(before_live(error.to_string()));
    }
    if let Err(error) = expect_ok(&mut stream, KIND_STAGE_OK).await {
        abort_best_effort(&mut stream).await;
        return Err(error);
    }
    if let Err(error) = write_frame(&mut stream, KIND_LIVE, &[]).await {
        abort_best_effort(&mut stream).await;
        return Err(before_live(error.to_string()));
    }
    let live = match read_frame(&mut stream).await {
        Ok(frame) => frame,
        Err(error) => return Err(before_live(error.to_string())),
    };
    match live.kind {
        KIND_LIVE_OK => Ok(PushResult::LiveCut),
        KIND_LIVE_DIRSYNC => Err(PushFailure {
            live_visible: true,
            message: message_of(&live.payload),
        }),
        KIND_ERR => Err(before_live(message_of(&live.payload))),
        other => Err(before_live(format!(
            "jumbo remote live cut returned frame {other}; MOVE_LIVE was not confirmed"
        ))),
    }
}

/// What the destination has published. `Durable` is the only state that may
/// delete the source. A visible rename without a directory fsync is not enough.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationCut {
    Absent,
    VisibleNotDurable,
    Durable,
}

/// HELLO only. Does not delete a visible `MOVE_LIVE` tree.
pub async fn destination_live_cut(
    shard_id: ShardId,
    source_node: NodeId,
    dest_node: NodeId,
    dest_addr: SocketAddr,
) -> Result<DestinationCut> {
    refuse_non_loopback(dest_addr)?;
    let mut stream = TcpStream::connect(dest_addr)
        .await
        .map_err(|error| MetaDbError::StorageIoError(error.to_string()))?;
    write_frame(
        &mut stream,
        KIND_HELLO,
        &encode_hello(shard_id, source_node, dest_node),
    )
    .await?;
    let frame = read_frame(&mut stream).await?;
    if frame.kind == KIND_ERR {
        return Err(MetaDbError::StorageIoError(message_of(&frame.payload)));
    }
    if frame.kind != KIND_HELLO_OK || frame.payload.len() != 4 {
        return Err(MetaDbError::StorageIoError(
            "jumbo remote destination returned an unexpected hello".to_string(),
        ));
    }
    let status = u32::from_le_bytes(frame.payload.as_slice().try_into().unwrap());
    let cut = match status {
        HELLO_ALREADY_LIVE => DestinationCut::Durable,
        HELLO_LIVE_NOT_DURABLE => DestinationCut::VisibleNotDurable,
        HELLO_ACCEPT => DestinationCut::Absent,
        other => {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo remote hello status {other} is not a live cut"
            )))
        }
    };
    if cut == DestinationCut::Absent {
        abort_best_effort(&mut stream).await;
    }
    Ok(cut)
}

async fn seal_from_open_session(
    stream: &mut TcpStream,
) -> std::result::Result<PushResult, PushFailure> {
    if let Err(error) = write_frame(stream, KIND_LIVE, &[]).await {
        return Err(PushFailure {
            live_visible: true,
            message: error.to_string(),
        });
    }
    let live = match read_frame(stream).await {
        Ok(frame) => frame,
        Err(error) => {
            return Err(PushFailure {
                live_visible: true,
                message: error.to_string(),
            })
        }
    };
    match live.kind {
        KIND_LIVE_OK => Ok(PushResult::LiveCut),
        KIND_LIVE_DIRSYNC => Err(PushFailure {
            live_visible: true,
            message: message_of(&live.payload),
        }),
        KIND_ERR => Err(PushFailure {
            live_visible: true,
            message: message_of(&live.payload),
        }),
        other => Err(PushFailure {
            live_visible: true,
            message: format!(
                "jumbo remote live cut returned frame {other}; the directory fsync was not confirmed"
            ),
        }),
    }
}

pub fn load_spare_drains(dir: &Path) -> Result<Vec<SpareDrainRecord>> {
    load_spare_file(dir)
}

pub fn append_spare_drain(dir: &Path, record: SpareDrainRecord) -> Result<Vec<SpareDrainRecord>> {
    if record.transfer_leader_called {
        return Err(MetaDbError::ConsensusError(
            "refusing to record a spare drain that claims transfer_leader was called".to_string(),
        ));
    }
    let mut records = load_spare_file(dir)?;
    if let Some(existing) = records.iter().find(|stored| same_drain(stored, &record)) {
        if existing.voters_after != record.voters_after
            || existing.raft_membership_changed != record.raft_membership_changed
        {
            return Err(MetaDbError::StorageIoError(
                "spare drain record conflicts with the one already stored; the older record was kept"
                    .to_string(),
            ));
        }
        return Ok(records);
    }
    records.push(record);
    write_spare_file(dir, &records)?;
    Ok(records)
}

pub fn load_handoffs(dir: &Path) -> Result<Vec<RemoteHandoffRecord>> {
    load_handoff_file(dir)
}

pub fn append_handoff(dir: &Path, record: RemoteHandoffRecord) -> Result<()> {
    if record.destination_raft_opened || record.transfer_leader_called {
        return Err(MetaDbError::ConsensusError(
            "refusing to record a handoff that claims Raft was opened on the destination or transfer_leader was called"
                .to_string(),
        ));
    }
    let mut records = load_handoff_file(dir)?;
    if let Some(existing) = records
        .iter()
        .find(|stored| stored.shard_id == record.shard_id)
    {
        if existing.dest_node != record.dest_node {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo shard {} is already handed off to node {}; the record was not replaced",
                record.shard_id, existing.dest_node
            )));
        }
        return Ok(());
    }
    records.push(record);
    write_handoff_file(dir, &records)
}

pub fn handoff_blocks_local_open(dir: &Path, shard_id: ShardId) -> Result<bool> {
    Ok(load_handoffs(dir)?
        .iter()
        .any(|record| record.shard_id == shard_id))
}

pub fn outcome_from_drain(record: &SpareDrainRecord) -> RemoteMoveOutcome {
    RemoteMoveOutcome {
        drain_shard_id: record.drain_shard_id,
        removed: record.removed,
        added: record.added,
        voters_after: record.voters_after.iter().copied().collect(),
        raft_membership_changed: record.raft_membership_changed,
        destination_raft_opened: false,
        transfer_leader_called: false,
    }
}

pub fn drain_record_for(
    jumbo_shard_id: ShardId,
    plan: &SpareDrainPlan,
    raft_membership_changed: bool,
) -> SpareDrainRecord {
    let mut voters_after: Vec<NodeId> = plan.voters_after.iter().copied().collect();
    voters_after.sort_unstable();
    SpareDrainRecord {
        jumbo_shard_id,
        drain_shard_id: plan.shard_id,
        removed: plan.remove,
        added: plan.add,
        voters_after,
        raft_membership_changed,
        transfer_leader_called: false,
    }
}

fn same_drain(left: &SpareDrainRecord, right: &SpareDrainRecord) -> bool {
    left.jumbo_shard_id == right.jumbo_shard_id
        && left.drain_shard_id == right.drain_shard_id
        && left.removed == right.removed
        && left.added == right.added
}

fn before_live(message: String) -> PushFailure {
    PushFailure {
        live_visible: false,
        message,
    }
}

async fn expect_ok(stream: &mut TcpStream, kind: u32) -> std::result::Result<(), PushFailure> {
    let frame = read_frame(stream)
        .await
        .map_err(|error| before_live(error.to_string()))?;
    if frame.kind == KIND_ERR {
        return Err(before_live(message_of(&frame.payload)));
    }
    if frame.kind != kind {
        return Err(before_live(format!(
            "expected jumbo remote frame {kind}, received {}",
            frame.kind
        )));
    }
    Ok(())
}

async fn abort_best_effort(stream: &mut TcpStream) {
    let _ = write_frame(stream, KIND_ABORT, &[]).await;
}

async fn write_err(stream: &mut TcpStream, message: &str) -> Result<()> {
    write_frame(stream, KIND_ERR, message.as_bytes()).await
}

struct Frame {
    kind: u32,
    payload: Vec<u8>,
}

async fn write_frame(stream: &mut TcpStream, kind: u32, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_FRAME {
        return Err(MetaDbError::StorageIoError(
            "jumbo remote frame is too large; MOVE_LIVE was not published".to_string(),
        ));
    }
    let mut raw = Vec::with_capacity(20 + payload.len());
    raw.extend_from_slice(b"MDJM");
    raw.extend_from_slice(&WIRE_VERSION.to_le_bytes());
    raw.extend_from_slice(&kind.to_le_bytes());
    raw.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    raw.extend_from_slice(payload);
    let crc = checksum_crc32c(&raw);
    raw.extend_from_slice(&crc.to_le_bytes());
    stream
        .write_all(&raw)
        .await
        .map_err(|error| MetaDbError::StorageIoError(error.to_string()))?;
    Ok(())
}

async fn read_frame(stream: &mut TcpStream) -> Result<Frame> {
    let mut header = [0u8; 16];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|error| MetaDbError::StorageIoError(error.to_string()))?;
    if &header[0..4] != b"MDJM" {
        return Err(MetaDbError::StorageIoError(
            "jumbo remote frame magic is not MDJM".to_string(),
        ));
    }
    let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
    if version != WIRE_VERSION {
        return Err(MetaDbError::StorageIoError(format!(
            "jumbo remote frame version {version} is not supported"
        )));
    }
    let kind = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
    if len > MAX_FRAME {
        return Err(MetaDbError::StorageIoError(
            "jumbo remote frame is too large".to_string(),
        ));
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream
            .read_exact(&mut payload)
            .await
            .map_err(|error| MetaDbError::StorageIoError(error.to_string()))?;
    }
    let mut crc_buf = [0u8; 4];
    stream
        .read_exact(&mut crc_buf)
        .await
        .map_err(|error| MetaDbError::StorageIoError(error.to_string()))?;
    let mut covered = header.to_vec();
    covered.extend_from_slice(&payload);
    let actual = checksum_crc32c(&covered);
    let expected = u32::from_le_bytes(crc_buf);
    if actual != expected {
        return Err(MetaDbError::ChecksumMismatch {
            context: "jumbo remote frame".to_string(),
            expected,
            actual,
        });
    }
    Ok(Frame { kind, payload })
}

fn encode_hello(shard_id: ShardId, source_node: NodeId, dest_node: NodeId) -> Vec<u8> {
    let mut raw = Vec::with_capacity(24);
    raw.extend_from_slice(&shard_id.to_le_bytes());
    raw.extend_from_slice(&source_node.to_le_bytes());
    raw.extend_from_slice(&dest_node.to_le_bytes());
    raw
}

fn decode_hello(payload: &[u8]) -> Result<(ShardId, NodeId, NodeId)> {
    if payload.len() != 24 {
        return Err(MetaDbError::StorageIoError(
            "jumbo remote hello has the wrong length".to_string(),
        ));
    }
    let shard_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let source = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    let dest = u64::from_le_bytes(payload[16..24].try_into().unwrap());
    Ok((shard_id, source, dest))
}

fn encode_file(relative: &str, crc: u32, bytes: &[u8]) -> Vec<u8> {
    let path = relative.as_bytes();
    let mut raw = Vec::with_capacity(2 + path.len() + 4 + 8 + bytes.len());
    raw.extend_from_slice(&(path.len() as u16).to_le_bytes());
    raw.extend_from_slice(path);
    raw.extend_from_slice(&crc.to_le_bytes());
    raw.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    raw.extend_from_slice(bytes);
    raw
}

fn write_received_file(dest_root: &Path, payload: &[u8]) -> Result<MovedFile> {
    if payload.len() < 2 {
        return Err(MetaDbError::StorageIoError(
            "jumbo remote file frame is truncated".to_string(),
        ));
    }
    let path_len = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    if payload.len() < 2 + path_len + 4 + 8 {
        return Err(MetaDbError::StorageIoError(
            "jumbo remote file frame is truncated".to_string(),
        ));
    }
    let relative = std::str::from_utf8(&payload[2..2 + path_len]).map_err(|_| {
        MetaDbError::StorageIoError("jumbo remote file path is not utf-8".to_string())
    })?;
    let crc_at = 2 + path_len;
    let claimed_crc = u32::from_le_bytes(payload[crc_at..crc_at + 4].try_into().unwrap());
    let len_at = crc_at + 4;
    let claimed_len = u64::from_le_bytes(payload[len_at..len_at + 8].try_into().unwrap());
    let data = &payload[len_at + 8..];
    if data.len() as u64 != claimed_len || claimed_len > MAX_FILE_BYTES {
        return Err(MetaDbError::StorageIoError(format!(
            "jumbo file {relative} length does not match the frame; MOVE_LIVE was not published"
        )));
    }
    let actual = checksum_crc32c(data);
    if actual != claimed_crc {
        return Err(MetaDbError::StorageIoError(format!(
            "jumbo file {relative} failed the frame checksum; MOVE_LIVE was not published"
        )));
    }
    let path = placed_path(dest_root, relative)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        refuse_symlink(parent)?;
    }
    refuse_symlink_allow_missing(&path)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?;
    file.write_all(data)?;
    file.sync_data()?;
    drop(file);
    let reread = fs::read(&path)?;
    if reread.len() as u64 != claimed_len || checksum_crc32c(&reread) != claimed_crc {
        return Err(MetaDbError::StorageIoError(format!(
            "jumbo file {} changed while it was written; MOVE_LIVE was not published",
            path.display()
        )));
    }
    Ok(MovedFile {
        relative_path: relative.to_string(),
        len: claimed_len,
        crc32c: claimed_crc,
    })
}

fn encode_manifest(files: &[MovedFile]) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&(files.len() as u32).to_le_bytes());
    for file in files {
        let path = file.relative_path.as_bytes();
        raw.extend_from_slice(&(path.len() as u16).to_le_bytes());
        raw.extend_from_slice(path);
        raw.extend_from_slice(&file.len.to_le_bytes());
        raw.extend_from_slice(&file.crc32c.to_le_bytes());
    }
    raw
}

fn decode_manifest(payload: &[u8]) -> Result<Vec<MovedFile>> {
    if payload.len() < 4 {
        return Err(MetaDbError::StorageIoError(
            "jumbo stage manifest is truncated".to_string(),
        ));
    }
    let count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    if count > MAX_FILES {
        return Err(MetaDbError::StorageIoError(
            "jumbo stage manifest has too many files".to_string(),
        ));
    }
    let mut offset = 4;
    let mut files = Vec::with_capacity(count);
    for _ in 0..count {
        if payload.len() < offset + 2 {
            return Err(MetaDbError::StorageIoError(
                "jumbo stage manifest is truncated".to_string(),
            ));
        }
        let path_len = u16::from_le_bytes(payload[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if payload.len() < offset + path_len + 8 + 4 {
            return Err(MetaDbError::StorageIoError(
                "jumbo stage manifest is truncated".to_string(),
            ));
        }
        let relative = std::str::from_utf8(&payload[offset..offset + path_len]).map_err(|_| {
            MetaDbError::StorageIoError("jumbo stage path is not utf-8".to_string())
        })?;
        offset += path_len;
        let len = u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap());
        offset += 8;
        let crc32c = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let _ = placed_path(Path::new("."), relative)?;
        files.push(MovedFile {
            relative_path: relative.to_string(),
            len,
            crc32c,
        });
    }
    if offset != payload.len() {
        return Err(MetaDbError::StorageIoError(
            "jumbo stage manifest has trailing bytes".to_string(),
        ));
    }
    Ok(files)
}

fn verify_received(dest: &Path, files: &[MovedFile]) -> Result<()> {
    for file in files {
        let path = placed_path(dest, &file.relative_path)?;
        let bytes = fs::read(&path)?;
        if bytes.len() as u64 != file.len || checksum_crc32c(&bytes) != file.crc32c {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo file {} failed verification before MOVE_STAGED",
                path.display()
            )));
        }
    }
    Ok(())
}

fn list_shard_files(source: &Path) -> Result<Vec<(String, PathBuf)>> {
    refuse_symlink(source)?;
    if !source.is_dir() {
        return Err(MetaDbError::StorageIoError(format!(
            "jumbo source {} is not a directory",
            source.display()
        )));
    }
    let mut files = Vec::new();
    collect_files(source, "", &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    if files.len() > MAX_FILES {
        return Err(MetaDbError::StorageIoError(
            "jumbo source has too many files; MOVE_LIVE was not published".to_string(),
        ));
    }
    Ok(files)
}

fn collect_files(source: &Path, prefix: &str, files: &mut Vec<(String, PathBuf)>) -> Result<()> {
    refuse_symlink(source)?;
    let mut names: Vec<_> = fs::read_dir(source)?
        .map(|entry| entry.map(|item| item.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    for name in names {
        let name = name.to_str().ok_or_else(|| {
            MetaDbError::StorageIoError(format!(
                "jumbo shard file name in {} is not utf-8",
                source.display()
            ))
        })?;
        let move_tmp = format!("{JUMBO_MOVE_FILE}.tmp");
        let durable_tmp = format!("{JUMBO_LIVE_DURABLE_FILE}.tmp");
        if name == JUMBO_MOVE_FILE
            || name == move_tmp
            || name == JUMBO_LIVE_DURABLE_FILE
            || name == durable_tmp
        {
            continue;
        }
        if name.is_empty()
            || name == "."
            || name == ".."
            || name.contains('/')
            || name.contains('\\')
        {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo shard file name {name} is not a single path component"
            )));
        }
        let from = source.join(name);
        let relative = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };
        let meta = fs::symlink_metadata(&from)?;
        if meta.file_type().is_symlink() {
            return Err(MetaDbError::StorageIoError(format!(
                "refusing to follow symlink {}",
                from.display()
            )));
        }
        if meta.is_dir() {
            collect_files(&from, &relative, files)?;
        } else if meta.is_file() {
            files.push((relative, from));
        } else {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo shard entry {} is not a regular file or directory",
                from.display()
            )));
        }
    }
    Ok(())
}

fn placed_path(root: &Path, relative: &str) -> Result<PathBuf> {
    if relative.is_empty()
        || relative.len() > 512
        || relative.contains('\\')
        || relative.starts_with('/')
    {
        return Err(MetaDbError::StorageIoError(format!(
            "jumbo remote path {relative} is not a relative file"
        )));
    }
    let mut path = root.to_path_buf();
    for part in relative.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo remote path {relative} is not a relative file"
            )));
        }
        path.push(part);
    }
    Ok(path)
}

fn message_of(payload: &[u8]) -> String {
    String::from_utf8_lossy(payload).into_owned()
}

fn scan_serving(root: &Path) -> Result<BTreeSet<ShardId>> {
    let mut serving = BTreeSet::new();
    let shards = root.join("shards");
    if !shards.exists() {
        return Ok(serving);
    }
    refuse_symlink(&shards)?;
    for entry in fs::read_dir(&shards)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(text) = name.to_str() else {
            continue;
        };
        let Ok(shard_id) = text.parse::<ShardId>() else {
            continue;
        };
        let dest = shard_move_dir(root, shard_id);
        if load_live_dir(root, shard_id)?.is_some() && live_dirsync_is_durable(&dest)? {
            serving.insert(shard_id);
        }
    }
    Ok(serving)
}

fn sync_tree(dir: &Path) -> Result<()> {
    refuse_symlink(dir)?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        refuse_symlink(&path)?;
        if fs::symlink_metadata(&path)?.is_dir() {
            sync_tree(&path)?;
        }
    }
    sync_dir(dir)
}

fn refuse_symlink(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(MetaDbError::StorageIoError(format!(
            "refusing to follow symlink {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn refuse_symlink_allow_missing(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(MetaDbError::StorageIoError(format!(
            "refusing to follow symlink {}",
            path.display()
        ))),
        Ok(_) | Err(_) => Ok(()),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]
struct SpareDrainFile {
    version: u32,
    records: Vec<SpareDrainRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]
struct HandoffFile {
    version: u32,
    records: Vec<RemoteHandoffRecord>,
}

fn load_spare_file(dir: &Path) -> Result<Vec<SpareDrainRecord>> {
    let path = dir.join(JUMBO_SPARE_DRAIN_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let body = read_crc_file(&path, JUMBO_SPARE_DRAIN_FILE)?;
    let file: SpareDrainFile = codec::deserialize(&body)
        .map_err(|error| MetaDbError::CodecError(format!("{JUMBO_SPARE_DRAIN_FILE}: {error}")))?;
    if file.version != FILE_VERSION {
        return Err(MetaDbError::CodecError(format!(
            "{JUMBO_SPARE_DRAIN_FILE} version {} is not supported",
            file.version
        )));
    }
    Ok(file.records)
}

fn load_handoff_file(dir: &Path) -> Result<Vec<RemoteHandoffRecord>> {
    let path = dir.join(JUMBO_REMOTE_HANDOFF_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let body = read_crc_file(&path, JUMBO_REMOTE_HANDOFF_FILE)?;
    let file: HandoffFile = codec::deserialize(&body).map_err(|error| {
        MetaDbError::CodecError(format!("{JUMBO_REMOTE_HANDOFF_FILE}: {error}"))
    })?;
    if file.version != FILE_VERSION {
        return Err(MetaDbError::CodecError(format!(
            "{JUMBO_REMOTE_HANDOFF_FILE} version {} is not supported",
            file.version
        )));
    }
    Ok(file.records)
}

fn read_crc_file(path: &Path, name: &'static str) -> Result<Vec<u8>> {
    refuse_symlink(path)?;
    let mut raw = Vec::new();
    File::open(path)?.read_to_end(&mut raw)?;
    let body = verify_crc32c_le(&raw, name)?;
    Ok(body.to_vec())
}

fn write_spare_file(dir: &Path, records: &[SpareDrainRecord]) -> Result<()> {
    let file = SpareDrainFile {
        version: FILE_VERSION,
        records: records.to_vec(),
    };
    write_crc_file(dir, JUMBO_SPARE_DRAIN_FILE, &file)
}

fn write_handoff_file(dir: &Path, records: &[RemoteHandoffRecord]) -> Result<()> {
    let file = HandoffFile {
        version: FILE_VERSION,
        records: records.to_vec(),
    };
    write_crc_file(dir, JUMBO_REMOTE_HANDOFF_FILE, &file)
}

fn write_crc_file<T>(dir: &Path, name: &str, value: &T) -> Result<()>
where
    T: wincode::SchemaWrite<wincode::config::Configuration<true, { usize::MAX }>, Src = T>,
{
    let mut raw = codec::serialize(value)
        .map_err(|error| MetaDbError::CodecError(format!("{name}: {error}")))?;
    append_crc32c_le(&mut raw);
    let tmp = dir.join(format!("{name}.tmp"));
    let dst = dir.join(name);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(&raw)?;
        file.sync_data()?;
    }
    fs::rename(&tmp, &dst)?;
    sync_dir(dir)?;
    Ok(())
}
