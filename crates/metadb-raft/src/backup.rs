//! Operator backup hold, backup artifact, and restore.
//!
//! The durable image is a complete Raft snapshot or an equivalent flushed LSM
//! checkpoint, plus the segmented log that still has to be replayed. There is
//! no separate Raft WAL. A hold keeps log purge and snapshot-directory deletion
//! from removing that image while the copy runs. The hold is a file under the
//! server data directory so a crash does not depend on a live process, and it
//! expires on the wall clock so a missed release cannot pin purge forever.
//!
//! Copy and restore anchor at the operator-supplied data directory, artifact
//! directory, or staging directory this process created. Each component below
//! that anchor is one relative name, checked before it is opened, and opened
//! without following a symlink (`cap-primitives` on Unix and Windows). A
//! symlink, a non-UTF-8 name, or any other non-regular entry fails the command.
//! Ancestors of the operator path are still resolved. A regular file can still
//! be replaced by a concurrent local writer.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use cap_fs_ext::OpenOptionsFollowExt;
use cap_primitives::fs::{
    open as cap_open, open_dir_nofollow, read_base_dir, remove_dir as cap_remove_dir,
    remove_file as cap_remove_file, rename as cap_rename, stat, FollowSymlinks,
    OpenOptions as CapOptions,
};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metadb_core::codec;
use metadb_core::{
    append_crc32c_le, checksum_crc32c, verify_crc32c_le, Crc32cHasher, MetaDbError, Result, ShardId,
};
use metadb_storage::sstable::{sst_file_name, sync_dir};
use metadb_storage::{LifecycleStorageEngine, ManifestLog};

use crate::log_store::{read_persisted_purged_index, RaftLogInspection, SegmentedRaftLogStore};
use crate::state_machine::highest_durable_snapshot_index;

const STREAM_COPY_CHUNK_BYTES: usize = 64 * 1024;

/// Validation callback checking shard coverage against cell control-plane state frames.
pub type CheckBackupTreeFn = dyn Fn(&[ShardId], &[(String, Vec<u8>)]) -> Result<()>;

fn write_serialized(path: &Path, payload: Vec<u8>) -> Result<()> {
    let mut bytes = payload;
    append_crc32c_le(&mut bytes);
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(backup_error("backup record path is not a utf-8 file name"));
    };
    let tmp_name = format!("{name}.tmp");
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    refuse_existing_symlink(&parent)?;
    fs::create_dir_all(&parent)?;
    refuse_existing_symlink(&parent.join(&tmp_name))?;
    refuse_existing_symlink(path)?;
    let dir = open_plain_dir(&parent)?;
    write_child_truncating(&dir, &tmp_name, &bytes)?;
    cap_rename(&dir, Path::new(&tmp_name), &dir, Path::new(name))
        .map_err(|error| map_child_error(name, error))?;
    dir.sync_all()?;
    Ok(())
}

fn refuse_existing_symlink(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(backup_error(format!(
            "refusing to follow symlink {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn read_serialized(path: &Path) -> Result<Vec<u8>> {
    let bytes = fs::read(path)?;
    Ok(verify_crc32c_le(&bytes, "backup record")?.to_vec())
}

macro_rules! write_record {
    ($path:expr, $value:expr) => {{
        let payload =
            codec::serialize($value).map_err(|error| MetaDbError::CodecError(error.to_string()))?;
        write_serialized($path, payload)
    }};
}

macro_rules! read_record {
    ($path:expr) => {{
        let payload = read_serialized($path)?;
        codec::deserialize(&payload).map_err(|error| MetaDbError::CodecError(error.to_string()))
    }};
}

pub const BACKUP_HOLD_FILE: &str = "BACKUP_HOLD";
pub const BACKUP_MANIFEST_FILE: &str = "BACKUP_MANIFEST";
pub const BACKUP_TREE_DIR: &str = "tree";
pub const BACKUP_JOB_FILE: &str = "BACKUP_JOB";
pub const BACKUP_JOB_RUNNING: &str = "BACKUP_JOB.running";
pub const BACKUP_JOB_RESULT: &str = "BACKUP_JOB_RESULT";
pub const SHARD_SET_LOCK: &str = "SHARD_SET.lock";
pub const BACKUP_HOLD_VERSION: u32 = 1;
pub const BACKUP_MANIFEST_VERSION: u32 = 1;
pub const HOLD_PHASE_PENDING: u8 = 1;
pub const HOLD_PHASE_ACTIVE: u8 = 2;
pub const HOLD_PHASE_RELEASED: u8 = 3;
pub const IMAGE_FLUSHED_LSM: u8 = 1;
pub const IMAGE_SNAPSHOT: u8 = 2;
/// Longest hold a caller may install. Expiry is wall-clock time, not a process timer.
pub const MAX_BACKUP_HOLD: Duration = Duration::from_secs(24 * 60 * 60);
const MIN_BACKUP_HOLD: Duration = Duration::from_secs(1);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct ShardHoldFloor {
    pub shard_id: ShardId,
    /// Log purge must not advance past this index while the hold is active.
    pub max_purge_index: u64,
    /// Snapshot directories at or below this index must not be deleted.
    pub retain_snapshots_through: u64,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct BackupHoldRecord {
    pub version: u32,
    pub hold_id: u64,
    pub created_unix_ms: u64,
    pub expires_unix_ms: u64,
    pub phase: u8,
    pub floors: Vec<ShardHoldFloor>,
}

impl BackupHoldRecord {
    pub fn is_expired_at(&self, now_unix_ms: u64) -> bool {
        now_unix_ms >= self.expires_unix_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginHold {
    Pending,
    AlreadyActive(BackupHoldRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupSummary {
    pub hold_id: u64,
    pub shard_count: usize,
    pub file_count: usize,
    pub destination: PathBuf,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
struct BackupFileRecord {
    path: String,
    len: u64,
    crc32c: u32,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
struct ShardImageRecord {
    shard_id: ShardId,
    image_kind: u8,
    image_index: u64,
    last_purged_index: u64,
    committed_index: u64,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
struct BackupManifest {
    version: u32,
    created_unix_ms: u64,
    hold_id: u64,
    files: Vec<BackupFileRecord>,
    shards: Vec<ShardImageRecord>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct BackupJobRecord {
    pub version: u32,
    pub output_dir: String,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct BackupJobResultRecord {
    pub version: u32,
    pub output_dir: String,
    pub ok: bool,
    pub message: String,
}

/// Live shard inputs for an online backup. The log copy runs under the log lock.
pub struct OnlineShard<'a> {
    pub shard_id: ShardId,
    pub raft_dir: &'a Path,
    pub log_store: &'a SegmentedRaftLogStore,
    pub engine: &'a LifecycleStorageEngine,
}

enum HoldPhase {
    Inactive,
    Pending,
    Active(BackupHoldRecord),
}

struct HoldMemory {
    phase: HoldPhase,
}

struct HoldRuntime {
    inner: Mutex<HoldMemory>,
}

fn runtimes() -> &'static Mutex<BTreeMap<PathBuf, std::sync::Arc<HoldRuntime>>> {
    static RUNTIMES: OnceLock<Mutex<BTreeMap<PathBuf, std::sync::Arc<HoldRuntime>>>> =
        OnceLock::new();
    RUNTIMES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn hold_error(message: impl AsRef<str>) -> MetaDbError {
    MetaDbError::StorageIoError(format!("backup hold: {}", message.as_ref()))
}

fn backup_error(message: impl AsRef<str>) -> MetaDbError {
    MetaDbError::StorageIoError(format!("backup rejected: {}", message.as_ref()))
}

fn is_not_found(error: &MetaDbError) -> bool {
    let text = error.to_string();
    text.contains("os error 2") || text.contains("No such file")
}

fn map_child_error(name: &str, error: std::io::Error) -> MetaDbError {
    let raw = error.raw_os_error();
    let text = error.to_string().to_ascii_lowercase();
    // ELOOP is 40 on Linux and 62 on macOS/BSD. Windows stopped-on-symlink is 681.
    // ENOTDIR (20) is what a directory open reports for a symlink on Linux.
    if matches!(raw, Some(20 | 40 | 62 | 681))
        || text.contains("symbolic link")
        || text.contains("reparse")
    {
        backup_error(format!("refusing to follow symlink {name}"))
    } else {
        error.into()
    }
}

fn reject_reparse(meta: &std::fs::Metadata, name: &str) -> Result<()> {
    if meta.file_type().is_symlink() {
        return Err(backup_error(format!("refusing to follow symlink {name}")));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(backup_error(format!(
                "refusing to follow reparse point {name}"
            )));
        }
    }
    Ok(())
}

fn nofollow_read() -> CapOptions {
    let mut options = CapOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options
}

/// `open_dir_nofollow` on Linux returns an `O_PATH` descriptor. Opening
/// `/proc/self/fd/<n>` names that same inode; it does not walk the original path.
fn materialize_dir(dir: File) -> Result<File> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let path = PathBuf::from(format!("/proc/self/fd/{}", dir.as_raw_fd()));
        File::open(path).map_err(Into::into)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(dir)
    }
}

fn component_name(name: &OsStr) -> Result<String> {
    let Some(text) = name.to_str() else {
        return Err(backup_error(
            "backup directory entry is not utf-8; refusing to copy it",
        ));
    };
    if text.is_empty()
        || text == "."
        || text == ".."
        || text.contains('/')
        || text.contains('\\')
        || text.contains('\0')
        || text.contains(':')
    {
        return Err(backup_error(format!(
            "backup directory entry {text:?} is not a single path component"
        )));
    }
    Ok(text.to_string())
}

/// Opens `path` without following a symlink in its final component.
///
/// This is the anchor. Components above `path` are the operator-supplied path
/// and are still resolved. Components below the returned handle must use `open_under`.
fn open_plain_dir(path: &Path) -> Result<File> {
    if path == Path::new(".") || path.as_os_str().is_empty() {
        return open_plain_dir(&std::env::current_dir()?);
    }
    let Some(raw_name) = path.file_name() else {
        return Err(backup_error(format!("{} has no file name", path.display())));
    };
    let name = component_name(raw_name)?;
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    let parent = File::open(parent)?;
    let dir = open_dir_nofollow(&parent, Path::new(&name))
        .map_err(|error| map_child_error(&name, error))?;
    let meta = dir.metadata()?;
    reject_reparse(&meta, &name)?;
    if !meta.is_dir() {
        return Err(backup_error(format!(
            "{} is not a plain directory; refusing to follow it",
            path.display()
        )));
    }
    materialize_dir(dir)
}

fn open_child(dir: &File, name: &str, directory: bool) -> Result<File> {
    // Reject absolute paths and `..` before any open. `Path::join` would drop
    // the directory handle when `name` is absolute, and `../` would leave it.
    let name = component_name(OsStr::new(name))?;
    let child = Path::new(&name);
    if directory {
        let opened =
            open_dir_nofollow(dir, child).map_err(|error| map_child_error(&name, error))?;
        reject_reparse(&opened.metadata()?, &name)?;
        materialize_dir(opened)
    } else {
        let opened = cap_open(dir, child, &nofollow_read())
            .map_err(|error| map_child_error(&name, error))?;
        reject_reparse(&opened.metadata()?, &name)?;
        Ok(opened)
    }
}

fn open_under(root: &File, relative: &str) -> Result<File> {
    let parts = relative_parts(Path::new(relative))?;
    descend_dirs(root, parts.iter().map(String::as_str))
}

fn ensure_under(root: &File, relative: &str) -> Result<File> {
    let parts = relative_parts(Path::new(relative))?;
    let mut dir = root.try_clone()?;
    for name in &parts {
        dir = create_child_dir(&dir, name)?;
    }
    Ok(dir)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntryKind {
    File,
    Dir,
    Other,
}

pub(crate) struct ListedEntry {
    pub(crate) name: String,
    pub(crate) kind: EntryKind,
}

/// Lists one directory fd. A symlink fails the listing instead of being followed.
pub(crate) fn list_plain_dir(dir: &File) -> Result<Vec<ListedEntry>> {
    let mut out = Vec::new();
    for entry in read_base_dir(dir)? {
        let entry = entry?;
        let name = component_name(&entry.file_name())?;
        let file_type = entry
            .file_type()
            .map_err(|error| backup_error(format!("cannot read the type of {name}: {error}")))?;
        if file_type.is_symlink() {
            return Err(backup_error(format!("refusing to follow symlink {name}")));
        }
        let kind = if file_type.is_dir() {
            EntryKind::Dir
        } else if file_type.is_file() {
            EntryKind::File
        } else {
            EntryKind::Other
        };
        out.push(ListedEntry { name, kind });
    }
    out.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(out)
}

pub(crate) fn read_listed_file(dir: &File, name: &str) -> Result<Vec<u8>> {
    let mut file = open_child(dir, name, false)?;
    let meta = file.metadata()?;
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Err(backup_error(format!(
            "{name} is not a regular file; refusing to copy it"
        )));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn stream_hash_child_file(dir: &File, name: &str) -> Result<(u64, u32)> {
    let mut file = open_child(dir, name, false)?;
    let meta = file.metadata()?;
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Err(backup_error(format!(
            "{name} is not a regular file; refusing to copy it"
        )));
    }
    let mut buf = vec![0u8; STREAM_COPY_CHUNK_BYTES];
    let mut hasher = Crc32cHasher::new();
    let mut total_len = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total_len = total_len.saturating_add(n as u64);
    }
    Ok((total_len, hasher.finalize()))
}

fn stream_copy_child_file(
    src_dir: &File,
    src_name: &str,
    dst_dir: &File,
    dst_name: &str,
) -> Result<(u64, u32)> {
    let mut src_file = open_child(src_dir, src_name, false)?;
    let meta = src_file.metadata()?;
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Err(backup_error(format!(
            "{src_name} is not a regular file; refusing to copy it"
        )));
    }
    let dst_name = component_name(OsStr::new(dst_name))?;
    let mut opts = CapOptions::new();
    opts.write(true)
        .create(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
    let mut dst_file = cap_open(dst_dir, Path::new(&dst_name), &opts)
        .map_err(|error| map_child_error(&dst_name, error))?;

    let mut buf = vec![0u8; STREAM_COPY_CHUNK_BYTES];
    let mut hasher = Crc32cHasher::new();
    let mut total_len = 0u64;
    loop {
        let n = src_file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        dst_file.write_all(&buf[..n])?;
        total_len = total_len.saturating_add(n as u64);
    }
    dst_file.sync_data()?;
    dst_dir.sync_all()?;
    Ok((total_len, hasher.finalize()))
}

fn create_child_dir(dir: &File, name: &str) -> Result<File> {
    let name = component_name(OsStr::new(name))?;
    let child = Path::new(&name);
    match stat(dir, child, FollowSymlinks::No) {
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => {
            return Err(backup_error(format!(
                "refusing to follow symlink or non-directory {name}"
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            cap_primitives::fs::create_dir(dir, child, &cap_primitives::fs::DirOptions::new())
                .map_err(|error| map_child_error(&name, error))?;
        }
        Err(error) => return Err(map_child_error(&name, error)),
    }
    open_child(dir, &name, true)
}

pub(crate) fn write_child_file(dir: &File, name: &str, bytes: &[u8]) -> Result<()> {
    write_child_bytes(dir, name, bytes, true)
}

fn write_child_truncating(dir: &File, name: &str, bytes: &[u8]) -> Result<()> {
    write_child_bytes(dir, name, bytes, false)
}

fn write_child_bytes(dir: &File, name: &str, bytes: &[u8], exclusive: bool) -> Result<()> {
    let name = component_name(OsStr::new(name))?;
    let mut opts = CapOptions::new();
    opts.write(true).create(true).follow(FollowSymlinks::No);
    if exclusive {
        opts.create_new(true);
    } else {
        opts.truncate(true);
    }
    let mut file =
        cap_open(dir, Path::new(&name), &opts).map_err(|error| map_child_error(&name, error))?;
    file.write_all(bytes)?;
    file.sync_data()?;
    if exclusive {
        dir.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
fn ensure_dest_dir(dest: &Path) -> Result<File> {
    match fs::symlink_metadata(dest) {
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => Err(backup_error(format!(
            "refusing to follow symlink {}",
            dest.display()
        ))),
        Ok(_) => open_plain_dir(dest),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = dest.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
            fs::create_dir(dest).map_err(|error| {
                map_child_error(
                    dest.file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("dest"),
                    error,
                )
            })?;
            open_plain_dir(dest)
        }
        Err(error) => Err(error.into()),
    }
}

fn copy_plain_tree(src: &File, dst: &File) -> Result<()> {
    for entry in list_plain_dir(src)? {
        match entry.kind {
            EntryKind::Dir => {
                let child_src = open_child(src, &entry.name, true)?;
                let child_dst = create_child_dir(dst, &entry.name)?;
                copy_plain_tree(&child_src, &child_dst)?;
            }
            EntryKind::File => {
                stream_copy_child_file(src, &entry.name, dst, &entry.name)?;
            }
            EntryKind::Other => {
                return Err(backup_error(format!(
                    "refusing to copy special file {}",
                    entry.name
                )));
            }
        }
    }
    Ok(())
}

fn walk_plain_dir(dir: &File, prefix: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in list_plain_dir(dir)? {
        let rel = prefix.join(&entry.name);
        match entry.kind {
            EntryKind::Dir => {
                let child = open_child(dir, &entry.name, true)?;
                walk_plain_dir(&child, &rel, out)?;
            }
            EntryKind::File => out.push(rel),
            EntryKind::Other => {
                return Err(backup_error(format!(
                    "refusing to copy special file {}",
                    entry.name
                )));
            }
        }
    }
    Ok(())
}

fn relative_parts(relative: &Path) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(name) => parts.push(component_name(name)?),
            _ => {
                return Err(backup_error(format!(
                    "path {} is not a relative backup path",
                    relative.display()
                )));
            }
        }
    }
    if parts.is_empty() {
        return Err(backup_error("backup path is empty"));
    }
    Ok(parts)
}

fn descend_dirs<'a>(root: &File, dirs: impl IntoIterator<Item = &'a str>) -> Result<File> {
    let mut dir = root.try_clone()?;
    for name in dirs {
        dir = open_child(&dir, name, true)?;
    }
    Ok(dir)
}

fn stream_hash_relative(root: &File, relative: &Path) -> Result<(u64, u32)> {
    let parts = relative_parts(relative)?;
    let (file_name, dirs) = parts.split_last().expect("relative path has a file name");
    let dir = descend_dirs(root, dirs.iter().map(String::as_str))?;
    stream_hash_child_file(&dir, file_name)
}

fn stream_copy_relative(src_root: &File, dst_root: &File, relative: &Path) -> Result<(u64, u32)> {
    let parts = relative_parts(relative)?;
    let (file_name, dirs) = parts.split_last().expect("relative path has a file name");
    let src_dir = descend_dirs(src_root, dirs.iter().map(String::as_str))?;
    let mut dst_dir = dst_root.try_clone()?;
    for name in dirs {
        dst_dir = create_child_dir(&dst_dir, name)?;
    }
    stream_copy_child_file(&src_dir, file_name, &dst_dir, file_name)
}

fn relative_string(path: &Path) -> Result<String> {
    let parts = relative_parts(path)?;
    Ok(parts.join("/"))
}

fn restore_error(message: impl AsRef<str>) -> MetaDbError {
    MetaDbError::StorageIoError(format!("backup restore rejected: {}", message.as_ref()))
}

fn now_unix_ms() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .map_err(|error| {
            hold_error(format!(
                "system clock is before the unix epoch ({error}); refusing to expire a hold"
            ))
        })
}

fn next_hold_id(now_unix_ms: u64) -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    now_unix_ms ^ n.rotate_left(17)
}

pub fn canonical_data_dir(data_dir: &Path) -> Result<PathBuf> {
    fs::create_dir_all(data_dir)?;
    fs::canonicalize(data_dir).map_err(MetaDbError::from)
}

fn runtime_for(data_dir: &Path) -> Result<std::sync::Arc<HoldRuntime>> {
    let key = canonical_data_dir(data_dir)?;
    let mut map = runtimes().lock().unwrap();
    let runtime = map.entry(key).or_insert_with(|| {
        std::sync::Arc::new(HoldRuntime {
            inner: Mutex::new(HoldMemory {
                phase: HoldPhase::Inactive,
            }),
        })
    });
    Ok(runtime.clone())
}

/// Loads a durable hold, if any, so purge observes it before shards serve traffic.
pub fn register_runtime(data_dir: &Path) -> Result<()> {
    let runtime = runtime_for(data_dir)?;
    let mut guard = runtime.inner.lock().unwrap();
    refresh_locked(&mut guard, data_dir)?;
    Ok(())
}

pub fn unregister_runtime(data_dir: &Path) {
    let Ok(key) = canonical_data_dir(data_dir) else {
        return;
    };
    runtimes().lock().unwrap().remove(&key);
}

fn server_data_dir(start: &Path) -> Option<PathBuf> {
    let mut current = start;
    loop {
        if current.join("be").join("shards").is_dir()
            || (current.join("shards").is_dir()
                && current.file_name().is_none_or(|name| name != "be"))
        {
            return fs::canonicalize(current).ok();
        }
        current = current.parent()?;
    }
}

fn shard_id_near(start: &Path) -> Option<ShardId> {
    let mut current = Some(start);
    while let Some(path) = current {
        if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            if name.len() == 20 && name.bytes().all(|byte| byte.is_ascii_digit()) {
                return name.parse().ok();
            }
        }
        current = path.parent();
    }
    None
}

fn refresh_locked(memory: &mut HoldMemory, data_dir: &Path) -> Result<()> {
    let Some(record) = read_hold_file(data_dir)? else {
        memory.phase = HoldPhase::Inactive;
        return Ok(());
    };
    if record.version != BACKUP_HOLD_VERSION {
        return Err(hold_error(format!(
            "unsupported hold version {}",
            record.version
        )));
    }
    let now = now_unix_ms().unwrap_or(0);
    if record.phase == HOLD_PHASE_RELEASED || (now > 0 && record.is_expired_at(now)) {
        let _ = remove_hold_file(data_dir);
        memory.phase = HoldPhase::Inactive;
        return Ok(());
    }
    memory.phase = match record.phase {
        HOLD_PHASE_PENDING => HoldPhase::Pending,
        HOLD_PHASE_ACTIVE => HoldPhase::Active(record),
        other => {
            return Err(hold_error(format!("unknown hold phase {other}")));
        }
    };
    Ok(())
}

fn effective_phase(data_dir: &Path) -> Result<HoldPhase> {
    if let Ok(key) = canonical_data_dir(data_dir) {
        if let Some(runtime) = runtimes().lock().unwrap().get(&key).cloned() {
            let mut guard = runtime.inner.lock().unwrap();
            refresh_locked(&mut guard, data_dir)?;
            return Ok(match &guard.phase {
                HoldPhase::Inactive => HoldPhase::Inactive,
                HoldPhase::Pending => HoldPhase::Pending,
                HoldPhase::Active(record) => HoldPhase::Active(record.clone()),
            });
        }
    }
    let Some(record) = read_hold_file(data_dir)? else {
        return Ok(HoldPhase::Inactive);
    };
    let now = now_unix_ms().unwrap_or(0);
    if record.phase == HOLD_PHASE_RELEASED || (now > 0 && record.is_expired_at(now)) {
        let _ = remove_hold_file(data_dir);
        return Ok(HoldPhase::Inactive);
    }
    match record.phase {
        HOLD_PHASE_PENDING => Ok(HoldPhase::Pending),
        HOLD_PHASE_ACTIVE => Ok(HoldPhase::Active(record)),
        other => Err(hold_error(format!("unknown hold phase {other}"))),
    }
}

/// Extra purge clamp. `Some(0)` blocks a log that has not been purged.
pub(crate) fn hold_purge_cap(raft_dir: &Path) -> Option<u64> {
    let data_dir = server_data_dir(raft_dir)?;
    let shard_id = shard_id_near(raft_dir)?;
    match effective_phase(&data_dir).ok()? {
        HoldPhase::Inactive => None,
        HoldPhase::Pending => Some(0),
        HoldPhase::Active(record) => Some(
            record
                .floors
                .iter()
                .find(|floor| floor.shard_id == shard_id)
                .map(|floor| floor.max_purge_index)
                .unwrap_or(0),
        ),
    }
}

/// Snapshot directories a hold still needs. Pending holds protect every index.
pub(crate) fn snapshot_index_protected(snaps_root: &Path, index: u64) -> bool {
    let Some(data_dir) = server_data_dir(snaps_root) else {
        return false;
    };
    let Some(shard_id) = shard_id_near(snaps_root) else {
        return false;
    };
    match effective_phase(&data_dir) {
        Ok(HoldPhase::Pending) => true,
        Ok(HoldPhase::Active(record)) => match record
            .floors
            .iter()
            .find(|floor| floor.shard_id == shard_id)
        {
            Some(floor) => index <= floor.retain_snapshots_through,
            None => true,
        },
        _ => false,
    }
}

pub fn read_hold(data_dir: &Path) -> Result<Option<BackupHoldRecord>> {
    match effective_phase(data_dir)? {
        HoldPhase::Inactive => Ok(None),
        HoldPhase::Pending => read_hold_file(data_dir),
        HoldPhase::Active(record) => Ok(Some(record)),
    }
}

/// Installs a pending hold. A second call while the same hold is active does not move its floor.
pub fn begin_hold(data_dir: &Path, ttl: Duration) -> Result<BeginHold> {
    if ttl < MIN_BACKUP_HOLD || ttl > MAX_BACKUP_HOLD {
        return Err(hold_error(format!(
            "ttl must be from {}s through {}s",
            MIN_BACKUP_HOLD.as_secs(),
            MAX_BACKUP_HOLD.as_secs()
        )));
    }
    let runtime = runtime_for(data_dir)?;
    let mut guard = runtime.inner.lock().unwrap();
    refresh_locked(&mut guard, data_dir)?;
    match &guard.phase {
        HoldPhase::Active(record) => return Ok(BeginHold::AlreadyActive(record.clone())),
        HoldPhase::Pending => return Ok(BeginHold::Pending),
        HoldPhase::Inactive => {}
    }
    let now = now_unix_ms()?;
    let record = BackupHoldRecord {
        version: BACKUP_HOLD_VERSION,
        hold_id: next_hold_id(now),
        created_unix_ms: now,
        expires_unix_ms: now.saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
        phase: HOLD_PHASE_PENDING,
        floors: Vec::new(),
    };
    write_hold_file(data_dir, &record)?;
    guard.phase = HoldPhase::Pending;
    Ok(BeginHold::Pending)
}

/// Latches purge and snapshot floors. Purge stays frozen from [`begin_hold`] until this returns.
pub fn finish_hold(data_dir: &Path, mut floors: Vec<ShardHoldFloor>) -> Result<BackupHoldRecord> {
    floors.sort_by_key(|floor| floor.shard_id);
    floors.dedup_by(|left, right| left.shard_id == right.shard_id);
    let runtime = runtime_for(data_dir)?;
    let mut guard = runtime.inner.lock().unwrap();
    refresh_locked(&mut guard, data_dir)?;
    match &guard.phase {
        HoldPhase::Active(record) => return Ok(record.clone()),
        HoldPhase::Inactive => {
            return Err(hold_error(
                "no pending hold is installed; begin a hold before latching floors",
            ));
        }
        HoldPhase::Pending => {}
    }
    let Some(pending) = read_hold_file(data_dir)? else {
        guard.phase = HoldPhase::Inactive;
        return Err(hold_error(
            "pending hold file disappeared before floors were latched",
        ));
    };
    let now = now_unix_ms()?;
    if pending.is_expired_at(now) {
        let _ = remove_hold_file(data_dir);
        guard.phase = HoldPhase::Inactive;
        return Err(hold_error(
            "hold expired before floors were latched; purge is allowed again",
        ));
    }
    let active = BackupHoldRecord {
        phase: HOLD_PHASE_ACTIVE,
        floors,
        ..pending
    };
    write_hold_file(data_dir, &active)?;
    guard.phase = HoldPhase::Active(active.clone());
    Ok(active)
}

pub fn release_hold(data_dir: &Path) -> Result<()> {
    if let Ok(runtime) = runtime_for(data_dir) {
        let mut guard = runtime.inner.lock().unwrap();
        remove_hold_file(data_dir)?;
        guard.phase = HoldPhase::Inactive;
        return Ok(());
    }
    remove_hold_file(data_dir)
}

/// Test hook. Rewrites expiry into the past and drops the hold so purge can resume.
pub fn testing_force_expire(data_dir: &Path) -> Result<()> {
    let Some(mut record) = read_hold_file(data_dir)? else {
        return Err(hold_error("no hold file to expire"));
    };
    record.expires_unix_ms = 0;
    write_hold_file(data_dir, &record)?;
    if let Ok(runtime) = runtime_for(data_dir) {
        let mut guard = runtime.inner.lock().unwrap();
        refresh_locked(&mut guard, data_dir)?;
    }
    Ok(())
}

pub fn floor_for_open_log(
    shard_id: ShardId,
    raft_dir: &Path,
    max_purge_index: u64,
) -> ShardHoldFloor {
    ShardHoldFloor {
        shard_id,
        max_purge_index,
        retain_snapshots_through: highest_durable_snapshot_index(raft_dir),
    }
}

pub fn disk_floor(shard_id: ShardId, raft_dir: &Path) -> Result<ShardHoldFloor> {
    let max_purge_index = if raft_dir.join("HARD_STATE").is_file() {
        read_persisted_purged_index(raft_dir)?
    } else {
        0
    };
    Ok(floor_for_open_log(shard_id, raft_dir, max_purge_index))
}

pub fn list_shard_ids(data_dir: &Path) -> Result<Vec<ShardId>> {
    let root = open_plain_dir(data_dir)?;
    shard_ids_in_anchor(&root)
}

fn shard_ids_in_anchor(root: &File) -> Result<Vec<ShardId>> {
    let dir = match open_under(root, "be/shards") {
        Ok(dir) => dir,
        Err(error) if is_not_found(&error) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut ids = Vec::new();
    for entry in list_plain_dir(&dir)? {
        if entry.name.len() == 20 && entry.name.bytes().all(|byte| byte.is_ascii_digit()) {
            if entry.kind != EntryKind::Dir {
                return Err(backup_error(format!(
                    "refusing to follow shard path {}",
                    entry.name
                )));
            }
            ids.push(entry.name.parse::<u64>().map_err(|error| {
                backup_error(format!(
                    "shard directory {} is not a shard id: {error}",
                    entry.name
                ))
            })?);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// `CELL_MANAGER_STATE` frames under `root`, read through that directory handle.
///
/// A symlink cell directory or a symlink state file fails the read. The bytes are
/// what the publish-time shard-reference check receives. It does not open a path.
fn read_cell_states(root: &File) -> Result<Vec<(String, Vec<u8>)>> {
    let mut states = Vec::new();
    for entry in list_plain_dir(root)? {
        if !entry.name.starts_with("cell_") {
            continue;
        }
        if entry.kind != EntryKind::Dir {
            return Err(backup_error(format!(
                "refusing to follow symlink {}",
                entry.name
            )));
        }
        let cell = open_child(root, &entry.name, true)?;
        let listed = list_plain_dir(&cell)?;
        let Some(state) = listed.iter().find(|item| item.name == "CELL_MANAGER_STATE") else {
            continue;
        };
        if state.kind != EntryKind::File {
            return Err(backup_error(format!(
                "refusing to follow symlink {}/CELL_MANAGER_STATE",
                entry.name
            )));
        }
        let bytes = read_listed_file(&cell, "CELL_MANAGER_STATE")?;
        states.push((entry.name, bytes));
    }
    Ok(states)
}

fn check_anchored_tree(root: &File, check_tree: &CheckBackupTreeFn) -> Result<()> {
    let shard_ids = shard_ids_in_anchor(root)?;
    let cells = read_cell_states(root)?;
    check_tree(&shard_ids, &cells)
}

/// Exclusive lock covering shard-directory creation and registry publication.
///
/// Backup holds it from the final shard-set check through the artifact rename so a
/// new shard cannot appear in that window and still be reported as a successful copy.
pub struct ShardSetLock {
    _file: File,
}

pub fn lock_be_shard_set(be_dir: &Path) -> Result<ShardSetLock> {
    fs::create_dir_all(be_dir)?;
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(be_dir.join(SHARD_SET_LOCK))?;
    file.lock()
        .map_err(|error| backup_error(format!("shard set lock: {error}")))?;
    Ok(ShardSetLock { _file: file })
}

/// `be` directory when `shard_dir` is `<data>/be/shards/<id>`.
pub fn be_dir_for_shard(shard_dir: &Path) -> Option<PathBuf> {
    let shards = shard_dir.parent()?;
    if shards.file_name()? != "shards" {
        return None;
    }
    let be = shards.parent()?;
    if be.file_name()? != "be" {
        return None;
    }
    Some(be.to_path_buf())
}

pub fn lsm_directory_is_locked(data_dir: &Path) -> bool {
    let Ok(root) = open_plain_dir(data_dir) else {
        return false;
    };
    let Ok(shards) = open_under(&root, "be/shards") else {
        return false;
    };
    let Ok(entries) = list_plain_dir(&shards) else {
        return true;
    };
    for entry in entries {
        if entry.kind != EntryKind::Dir {
            continue;
        }
        let Ok(shard) = open_child(&shards, &entry.name, true) else {
            return true;
        };
        let Ok(lsm) = open_child(&shard, "lsm", true) else {
            continue;
        };
        let mut options = CapOptions::new();
        options.read(true).write(true).follow(FollowSymlinks::No);
        let Ok(file) = cap_open(&lsm, Path::new("LOCK"), &options) else {
            continue;
        };
        if file.try_lock().is_err() {
            return true;
        }
    }
    false
}

pub fn create_online_backup(
    data_dir: &Path,
    destination: &Path,
    shards: &[OnlineShard<'_>],
    check_tree: &CheckBackupTreeFn,
) -> Result<BackupSummary> {
    let hold = require_active_hold(data_dir)?;
    reject_destination(data_dir, destination)?;
    let mut staging = Staging::create(destination)?;
    let source = open_plain_dir(data_dir)?;
    let staging_dir = open_plain_dir(&staging.path)?;
    let tree_dir = ensure_under(&staging_dir, BACKUP_TREE_DIR)?;
    let mut files = Vec::new();
    let mut images = Vec::new();
    let mut covered = BTreeSet::new();
    for shard in shards {
        let image = backup_online_shard(&hold, shard, data_dir, &source, &tree_dir, &mut files)?;
        covered.insert(image.shard_id);
        images.push(image);
    }
    for shard_id in list_shard_ids(data_dir)? {
        if covered.contains(&shard_id) {
            continue;
        }
        let image = backup_stopped_shard(&hold, shard_id, &source, &tree_dir, &mut files)?;
        images.push(image);
    }
    copy_node_metadata(&source, &tree_dir, &mut files)?;
    let staging_path = staging.path.clone();
    finish_manifest(
        data_dir,
        &hold,
        images,
        files,
        &staging_path,
        destination,
        &mut staging,
        check_tree,
    )
}

pub fn create_offline_backup(
    data_dir: &Path,
    destination: &Path,
    check_tree: &CheckBackupTreeFn,
) -> Result<BackupSummary> {
    if lsm_directory_is_locked(data_dir) {
        return Err(backup_error(
            "the data directory is open by a running process; run the command again so that process copies the log under its lock",
        ));
    }
    let hold = require_active_hold(data_dir)?;
    reject_destination(data_dir, destination)?;
    let mut staging = Staging::create(destination)?;
    let source = open_plain_dir(data_dir)?;
    let staging_dir = open_plain_dir(&staging.path)?;
    let tree_dir = ensure_under(&staging_dir, BACKUP_TREE_DIR)?;
    let mut files = Vec::new();
    let mut images = Vec::new();
    let shard_ids = list_shard_ids(data_dir)?;
    if shard_ids.is_empty() {
        return Err(backup_error(
            "no shard directories under be/shards; refusing an empty backup",
        ));
    }
    for shard_id in shard_ids {
        let image = backup_stopped_shard(&hold, shard_id, &source, &tree_dir, &mut files)?;
        images.push(image);
    }
    copy_node_metadata(&source, &tree_dir, &mut files)?;
    let staging_path = staging.path.clone();
    finish_manifest(
        data_dir,
        &hold,
        images,
        files,
        &staging_path,
        destination,
        &mut staging,
        check_tree,
    )
}

pub fn restore_backup(
    artifact: &Path,
    destination: &Path,
    check_tree: &CheckBackupTreeFn,
) -> Result<BackupSummary> {
    inspect_restore_destination(destination)?;
    let manifest = read_manifest(artifact)?;
    if manifest.version != BACKUP_MANIFEST_VERSION {
        return Err(restore_error(format!(
            "unsupported manifest version {}",
            manifest.version
        )));
    }
    if manifest.shards.is_empty() || manifest.files.is_empty() {
        return Err(restore_error(
            "manifest lists no shards or files; refusing an empty restore",
        ));
    }
    let artifact_dir = open_plain_dir(artifact)
        .map_err(|error| restore_error(format!("backup artifact is missing or unsafe: {error}")))?;
    let tree = open_child(&artifact_dir, BACKUP_TREE_DIR, true).map_err(|error| {
        restore_error(format!(
            "backup tree/ directory is missing or unsafe: {error}"
        ))
    })?;
    let mut discovered = Vec::new();
    walk_plain_dir(&tree, Path::new(""), &mut discovered)?;
    discovered.sort();
    let mut seen = BTreeSet::new();
    let mut verified_files = Vec::with_capacity(manifest.files.len());
    for file in &manifest.files {
        if !seen.insert(file.path.clone()) {
            return Err(restore_error(format!(
                "manifest lists {} more than once",
                file.path
            )));
        }
        let relative = safe_relative(&file.path).map_err(restore_error)?;
        let (actual_len, actual_crc) = stream_hash_relative(&tree, &relative)
            .map_err(|error| restore_error(format!("missing {}: {error}", file.path)))?;
        if actual_len != file.len || actual_crc != file.crc32c {
            return Err(restore_error(format!(
                "{} failed length or CRC32C check",
                file.path
            )));
        }
        verified_files.push((file.path.clone(), relative, file.len, file.crc32c));
    }
    let discovered_set = discovered
        .iter()
        .map(|path| relative_string(path))
        .collect::<Result<BTreeSet<_>>>()?;
    if discovered_set != seen {
        return Err(restore_error(
            "backup tree contains files that are not listed in the manifest",
        ));
    }
    let mut staging = Staging::create(destination)?;
    let staging_dir = open_plain_dir(&staging.path)?;
    for (display_path, relative, expected_len, expected_crc) in &verified_files {
        let (copied_len, copied_crc) = stream_copy_relative(&tree, &staging_dir, relative)
            .map_err(|error| restore_error(format!("copying {display_path}: {error}")))?;
        if copied_len != *expected_len || copied_crc != *expected_crc {
            return Err(restore_error(format!(
                "{display_path} changed during streaming restore"
            )));
        }
    }
    // Open MANIFEST on the writable staging copy. The source artifact stays read-only.
    validate_manifest_tree(&staging_dir, &manifest)?;
    check_anchored_tree(&staging_dir, check_tree)
        .map_err(|error| restore_error(error.to_string()))?;
    sync_tree(&staging.path)?;
    match inspect_restore_destination(destination)? {
        RestoreDestination::Missing => {
            let parent = destination_parent(destination);
            sync_directory_and_ancestors(&parent)?;
            match fs::symlink_metadata(destination) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    return Err(restore_error(format!(
                        "refusing to follow symlink {}",
                        destination.display()
                    )));
                }
                Ok(_) => {
                    return Err(restore_error(format!(
                        "destination {} appeared during restore",
                        destination.display()
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            fs::rename(&staging.path, destination)?;
            staging.disarm();
            sync_dir(&parent)?;
        }
        RestoreDestination::EmptyDir(dir) => {
            for (display_path, relative, expected_len, expected_crc) in &verified_files {
                let (copied_len, copied_crc) = stream_copy_relative(&staging_dir, &dir, relative)
                    .map_err(|error| {
                    restore_error(format!("copying {display_path}: {error}"))
                })?;
                if copied_len != *expected_len || copied_crc != *expected_crc {
                    return Err(restore_error(format!(
                        "{display_path} changed during streaming restore"
                    )));
                }
            }
            sync_plain_dir(&dir)?;
            staging.disarm();
            remove_plain_dir(&staging.path)?;
        }
    }
    Ok(BackupSummary {
        hold_id: manifest.hold_id,
        shard_count: manifest.shards.len(),
        file_count: manifest.files.len(),
        destination: destination.to_path_buf(),
    })
}

pub fn write_backup_job(data_dir: &Path, output_dir: &Path) -> Result<()> {
    let _ = fs::remove_file(data_dir.join(BACKUP_JOB_RESULT));
    let record = BackupJobRecord {
        version: 1,
        output_dir: output_dir.to_string_lossy().into_owned(),
    };
    write_record!(&data_dir.join(BACKUP_JOB_FILE), &record)
}

pub fn claim_backup_job(data_dir: &Path) -> Result<Option<BackupJobRecord>> {
    let source = data_dir.join(BACKUP_JOB_FILE);
    let running = data_dir.join(BACKUP_JOB_RUNNING);
    if running.is_file() {
        let record: BackupJobRecord = read_record!(&running)?;
        Staging::cleanup_stale(Path::new(&record.output_dir))?;
        return Ok(Some(record));
    }
    if !source.is_file() {
        return Ok(None);
    }
    fs::rename(&source, &running)?;
    match read_record!(&running) {
        Ok(record) => Ok(Some(record)),
        Err(error) => {
            let _ = fs::remove_file(&running);
            Err(error)
        }
    }
}

pub fn complete_backup_job(
    data_dir: &Path,
    output_dir: &str,
    result: std::result::Result<BackupSummary, String>,
) -> Result<()> {
    let (ok, message) = match result {
        Ok(summary) => (
            true,
            format!(
                "backup wrote {} files={} shards={} hold_id={}",
                summary.destination.display(),
                summary.file_count,
                summary.shard_count,
                summary.hold_id
            ),
        ),
        Err(error) => (false, error),
    };
    let record = BackupJobResultRecord {
        version: 1,
        output_dir: output_dir.to_string(),
        ok,
        message,
    };
    write_record!(&data_dir.join(BACKUP_JOB_RESULT), &record)?;
    let _ = fs::remove_file(data_dir.join(BACKUP_JOB_RUNNING));
    Ok(())
}

pub fn read_backup_job_result(data_dir: &Path) -> Result<Option<BackupJobResultRecord>> {
    let path = data_dir.join(BACKUP_JOB_RESULT);
    if !path.is_file() {
        return Ok(None);
    }
    read_record!(&path).map(Some)
}

fn backup_online_shard(
    hold: &BackupHoldRecord,
    shard: &OnlineShard<'_>,
    data_dir: &Path,
    source: &File,
    tree_dir: &File,
    files: &mut Vec<BackupFileRecord>,
) -> Result<ShardImageRecord> {
    let floor = floor_for_shard(hold, shard.shard_id)?;
    let purged_now = shard.log_store.last_purged_index();
    if purged_now > floor.max_purge_index {
        return Err(backup_error(format!(
            "shard {} purged past the hold floor {}",
            shard.shard_id, floor.max_purge_index
        )));
    }
    let relative_raft = format!("be/shards/{:020}/raft", shard.shard_id);
    let live_raft = open_under(source, &relative_raft)?;
    // Freeze the LSM or snapshot image before copying the log. Commits that land
    // during the checkpoint are still in the log copy, and the hold blocks purge.
    let live_index = shard.engine.durable_checkpoint().flushed_raft_index;
    let (kind, image_index) = if live_index >= purged_now {
        // Hard-linked checkpoints must stay on the source filesystem.
        let lsm_rel = format!("be/shards/{:020}/lsm", shard.shard_id);
        let lsm_dest = ensure_under(tree_dir, &lsm_rel)?;
        let checkpoint = with_checkpoint_scratch(data_dir, shard.shard_id, |temp| {
            let checkpoint = shard.engine.create_checkpoint(temp).map_err(|error| {
                backup_error(format!(
                    "shard {} checkpoint failed: {error}",
                    shard.shard_id
                ))
            })?;
            let scratch = open_plain_dir(temp)?;
            copy_plain_tree(&scratch, &lsm_dest)?;
            Ok(checkpoint)
        })?;
        record_tree_fd(&lsm_dest, &lsm_rel, files)?;
        (IMAGE_FLUSHED_LSM, checkpoint.flushed_raft_index)
    } else {
        (IMAGE_SNAPSHOT, 0)
    };
    copy_protected_snapshot(&live_raft, floor, &relative_raft, tree_dir, files)?;
    let dest_raft = ensure_under(tree_dir, &relative_raft)?;
    let inspection = shard
        .log_store
        .copy_durable_log(&live_raft, &dest_raft)
        .map_err(|error| {
            backup_error(format!("shard {} log copy failed: {error}", shard.shard_id))
        })?;
    record_log_fd(&dest_raft, &relative_raft, files)?;
    if inspection.last_purged_index > floor.max_purge_index {
        return Err(backup_error(format!(
            "copied log for shard {} is purged past the hold",
            shard.shard_id
        )));
    }
    if kind == IMAGE_FLUSHED_LSM {
        return finish_image(shard.shard_id, kind, image_index, &inspection);
    }
    image_from_snapshot(
        shard.shard_id,
        &live_raft,
        floor,
        &inspection,
        tree_dir,
        files,
    )
}

fn backup_stopped_shard(
    hold: &BackupHoldRecord,
    shard_id: ShardId,
    source: &File,
    tree_dir: &File,
    files: &mut Vec<BackupFileRecord>,
) -> Result<ShardImageRecord> {
    let relative_raft = format!("be/shards/{shard_id:020}/raft");
    let raft = open_under(source, &relative_raft)?;
    let floor = floor_for_shard(hold, shard_id)?;
    let purged = match read_listed_file(&raft, "HARD_STATE") {
        Ok(bytes) => crate::log_store::purged_index_from_hard_state_bytes(&bytes)
            .map_err(|error| backup_error(error.to_string()))?,
        Err(error) if is_not_found(&error) => 0,
        Err(error) => return Err(error),
    };
    if purged > floor.max_purge_index {
        return Err(backup_error(format!(
            "shard {shard_id} purged past the hold floor {}",
            floor.max_purge_index
        )));
    }
    let dest_raft = ensure_under(tree_dir, &relative_raft)?;
    copy_raft_fd(&raft, &dest_raft)?;
    record_log_fd(&dest_raft, &relative_raft, files)?;
    let inspection = crate::log_store::SegmentedRaftLogStore::inspect_copied_dir(&dest_raft)
        .map_err(|error| {
            backup_error(format!(
                "shard {shard_id} log is not a complete backup image: {error}"
            ))
        })?;
    copy_protected_snapshot(&raft, floor, &relative_raft, tree_dir, files)?;
    let lsm_rel = format!("be/shards/{shard_id:020}/lsm");
    let live_index = match open_under(source, &lsm_rel) {
        Ok(lsm) => read_lsm_index(&lsm)?,
        Err(error) if is_not_found(&error) => None,
        Err(error) => return Err(error),
    };
    if live_index.is_some_and(|index| index >= inspection.last_purged_index) {
        let lsm = open_under(source, &lsm_rel)?;
        let lsm_dest = ensure_under(tree_dir, &lsm_rel)?;
        copy_lsm_dir(&lsm, &lsm_dest)?;
        record_tree_fd(&lsm_dest, &lsm_rel, files)?;
        let index = read_lsm_index(&lsm_dest)?.unwrap_or(0);
        return finish_image(shard_id, IMAGE_FLUSHED_LSM, index, &inspection);
    }
    image_from_snapshot(shard_id, &raft, floor, &inspection, tree_dir, files)
}

fn image_from_snapshot(
    shard_id: ShardId,
    raft: &File,
    floor: &ShardHoldFloor,
    inspection: &RaftLogInspection,
    tree_dir: &File,
    files: &mut Vec<BackupFileRecord>,
) -> Result<ShardImageRecord> {
    let Some(index) = newest_anchored_snapshot(raft, floor.retain_snapshots_through)? else {
        return Err(backup_error(format!(
            "shard {shard_id} has no flushed LSM or complete snapshot covering purged index {}",
            inspection.last_purged_index
        )));
    };
    if index < inspection.last_purged_index {
        return Err(backup_error(format!(
            "shard {shard_id} snapshot {index} does not cover purged index {}",
            inspection.last_purged_index
        )));
    }
    let lsm_rel = format!("be/shards/{shard_id:020}/lsm");
    if stream_hash_relative(tree_dir, Path::new(&format!("{lsm_rel}/CURRENT"))).is_err() {
        let lsm = open_under(raft, &format!("snapshots/snap_{index:020}/lsm"))?;
        let lsm_dest = ensure_under(tree_dir, &lsm_rel)?;
        copy_lsm_dir(&lsm, &lsm_dest)?;
        record_tree_fd(&lsm_dest, &lsm_rel, files)?;
    }
    finish_image(shard_id, IMAGE_SNAPSHOT, index, inspection)
}

fn newest_anchored_snapshot(raft: &File, max_index: u64) -> Result<Option<u64>> {
    let snaps = match open_child(raft, "snapshots", true) {
        Ok(dir) => dir,
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut best: Option<(u64, u8)> = None;
    for entry in list_plain_dir(&snaps)? {
        if entry.kind != EntryKind::Dir {
            continue;
        }
        let Some(rest) = entry.name.strip_prefix("snap_") else {
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
        if index > max_index {
            continue;
        }
        let snap = open_child(&snaps, &entry.name, true)?;
        if read_listed_file(&snap, "SNAPSHOT_META").is_err() {
            continue;
        }
        match best {
            Some((best_index, best_rank))
                if best_index > index || (best_index == index && best_rank >= rank) => {}
            _ => best = Some((index, rank)),
        }
    }
    Ok(best.map(|(index, _)| index))
}

fn finish_image(
    shard_id: ShardId,
    image_kind: u8,
    image_index: u64,
    inspection: &RaftLogInspection,
) -> Result<ShardImageRecord> {
    if image_index < inspection.last_purged_index {
        return Err(backup_error(format!(
            "shard {shard_id} image index {image_index} is behind purged index {}",
            inspection.last_purged_index
        )));
    }
    if let Some(committed) = inspection.committed_index {
        if committed > image_index {
            let from = image_index.saturating_add(1);
            if !inspection.covers_inclusive(from, committed) {
                return Err(backup_error(format!(
                    "shard {shard_id} log does not contain committed indexes {from}..={committed}"
                )));
            }
        }
    }
    Ok(ShardImageRecord {
        shard_id,
        image_kind,
        image_index,
        last_purged_index: inspection.last_purged_index,
        committed_index: inspection.committed_index.unwrap_or(0),
    })
}

fn copy_protected_snapshot(
    raft: &File,
    floor: &ShardHoldFloor,
    relative_raft: &str,
    tree_dir: &File,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    let Some(index) = newest_anchored_snapshot(raft, floor.retain_snapshots_through)? else {
        return Ok(());
    };
    let rel = format!("{relative_raft}/snapshots/snap_{index:020}");
    let src = open_under(raft, &format!("snapshots/snap_{index:020}"))?;
    let dest = ensure_under(tree_dir, &rel)?;
    copy_plain_tree(&src, &dest)?;
    record_tree_fd(&dest, &rel, files)
}

fn copy_node_metadata(
    source: &File,
    tree_dir: &File,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    let allocator = open_under(source, "global_allocator")?;
    let allocator_dest = ensure_under(tree_dir, "global_allocator")?;
    copy_child_frame(
        &allocator,
        "ID_ALLOCATOR_STATE",
        &allocator_dest,
        "global_allocator/ID_ALLOCATOR_STATE",
        files,
    )?;
    let registry = open_under(source, "be")?;
    let registry_dest = ensure_under(tree_dir, "be")?;
    copy_child_frame(
        &registry,
        "NODE_SHARD_REGISTRY",
        &registry_dest,
        "be/NODE_SHARD_REGISTRY",
        files,
    )?;
    let mut cells = 0usize;
    for entry in list_plain_dir(source)? {
        if !entry.name.starts_with("cell_") {
            continue;
        }
        if entry.kind != EntryKind::Dir {
            return Err(backup_error(format!(
                "refusing to follow symlink {}",
                entry.name
            )));
        }
        let cell = open_child(source, &entry.name, true)?;
        let listed = list_plain_dir(&cell)?;
        if !listed.iter().any(|item| item.name == "CELL_MANAGER_STATE") {
            continue;
        }
        let cell_dest = ensure_under(tree_dir, &entry.name)?;
        copy_child_frame(
            &cell,
            "CELL_MANAGER_STATE",
            &cell_dest,
            &format!("{}/CELL_MANAGER_STATE", entry.name),
            files,
        )?;
        if listed.iter().any(|item| item.name == "CELL_OWNER") {
            copy_child_frame(
                &cell,
                "CELL_OWNER",
                &cell_dest,
                &format!("{}/CELL_OWNER", entry.name),
                files,
            )?;
        }
        if !listed
            .iter()
            .any(|item| item.name == "CLUSTER_IDENTITY" && item.kind == EntryKind::File)
        {
            return Err(backup_error(format!(
                "{}/CLUSTER_IDENTITY is missing; refusing a backup that cannot reopen this node's instance id",
                entry.name
            )));
        }
        copy_child_frame(
            &cell,
            "CLUSTER_IDENTITY",
            &cell_dest,
            &format!("{}/CLUSTER_IDENTITY", entry.name),
            files,
        )?;
        cells += 1;
    }
    if cells == 0 {
        return Err(backup_error(
            "CELL_MANAGER_STATE is missing; refusing a backup that cannot reopen the node",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn finish_manifest(
    data_dir: &Path,
    hold: &BackupHoldRecord,
    mut shards: Vec<ShardImageRecord>,
    mut files: Vec<BackupFileRecord>,
    staging: &Path,
    destination: &Path,
    staging_guard: &mut Staging,
    check_tree: &CheckBackupTreeFn,
) -> Result<BackupSummary> {
    let current = require_active_hold(data_dir)?;
    if current.hold_id != hold.hold_id {
        return Err(backup_error(
            "backup hold changed while the copy was running",
        ));
    }
    let copied: BTreeSet<ShardId> = shards.iter().map(|shard| shard.shard_id).collect();
    let staging_root = open_plain_dir(staging)?;
    let tree = open_under(&staging_root, BACKUP_TREE_DIR)?;
    ensure_registry_matches(&tree, &copied)?;
    for shard_id in list_shard_ids(data_dir)? {
        if !copied.contains(&shard_id) {
            return Err(backup_error(format!(
                "shard {shard_id} appeared during the backup and is not in the artifact"
            )));
        }
    }
    let source = open_plain_dir(data_dir)?;
    for floor in &current.floors {
        if copied.contains(&floor.shard_id) {
            continue;
        }
        match open_under(&source, &format!("be/shards/{:020}", floor.shard_id)) {
            Ok(_) => {
                return Err(backup_error(format!(
                    "hold covers shard {} but that shard was not copied",
                    floor.shard_id
                )));
            }
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(error),
        }
    }
    check_anchored_tree(&tree, check_tree)?;
    shards.sort_by_key(|shard| shard.shard_id);
    files.sort_by(|left, right| left.path.cmp(&right.path));
    let manifest = BackupManifest {
        version: BACKUP_MANIFEST_VERSION,
        created_unix_ms: now_unix_ms()?,
        hold_id: hold.hold_id,
        files,
        shards,
    };
    write_record!(&staging.join(BACKUP_MANIFEST_FILE), &manifest)?;
    sync_tree(staging)?;
    if destination.exists() {
        return Err(backup_error(format!(
            "destination {} already exists",
            destination.display()
        )));
    }
    let parent = destination_parent(destination);
    sync_directory_and_ancestors(&parent)?;
    publish_staging_under_shard_lock(data_dir, &copied, staging, destination, staging_guard)?;
    sync_dir(&parent)?;
    Ok(BackupSummary {
        hold_id: manifest.hold_id,
        shard_count: manifest.shards.len(),
        file_count: manifest.files.len(),
        destination: destination.to_path_buf(),
    })
}

#[cfg(test)]
static BEFORE_LOCKED_RENAME: Mutex<Option<fn(&Path)>> = Mutex::new(None);

fn publish_staging_under_shard_lock(
    data_dir: &Path,
    copied: &BTreeSet<ShardId>,
    staging: &Path,
    destination: &Path,
    staging_guard: &mut Staging,
) -> Result<()> {
    let _shard_set = lock_be_shard_set(&data_dir.join("be"))?;
    reject_uncovered_live_shards(data_dir, copied)?;
    #[cfg(test)]
    {
        if let Some(hook) = BEFORE_LOCKED_RENAME.lock().unwrap().take() {
            hook(data_dir);
        }
    }
    reject_uncovered_live_shards(data_dir, copied)?;
    fs::rename(staging, destination)?;
    staging_guard.disarm();
    Ok(())
}

fn reject_uncovered_live_shards(data_dir: &Path, copied: &BTreeSet<ShardId>) -> Result<()> {
    for shard_id in list_shard_ids(data_dir)? {
        if !copied.contains(&shard_id) {
            return Err(backup_error(format!(
                "shard {shard_id} appeared during the backup and is not in the artifact"
            )));
        }
    }
    let root = open_plain_dir(data_dir)?;
    match open_under(&root, "be") {
        Ok(be) => match read_listed_file(&be, "NODE_SHARD_REGISTRY") {
            Ok(bytes) => {
                for shard_id in crate::group_host::shard_ids_in_registry_bytes(&bytes)? {
                    if !copied.contains(&shard_id) {
                        return Err(backup_error(format!(
                            "NODE_SHARD_REGISTRY references shard {shard_id} which was not copied"
                        )));
                    }
                }
            }
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(error),
        },
        Err(error) if is_not_found(&error) => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

fn checkpoint_scratch_name(shard_id: ShardId) -> String {
    format!(".backup-ckpt-{shard_id:020}")
}

fn checkpoint_scratch_lock_name(shard_id: ShardId) -> String {
    format!(".backup-ckpt-{shard_id:020}.lock")
}

struct CheckpointScratchGuard {
    _file: File,
}

fn lock_checkpoint_scratch(data_dir: &Path, shard_id: ShardId) -> Result<CheckpointScratchGuard> {
    let dir = open_plain_dir(data_dir)?;
    let name = checkpoint_scratch_lock_name(shard_id);
    let mut options = CapOptions::new();
    options
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .follow(FollowSymlinks::No);
    let file = cap_open(&dir, Path::new(&name), &options)
        .map_err(|error| map_child_error(&name, error))?;
    file.lock().map_err(|error| {
        backup_error(format!(
            "checkpoint scratch lock for shard {shard_id}: {error}"
        ))
    })?;
    Ok(CheckpointScratchGuard { _file: file })
}

/// Clears crash residue, then runs `body` while this process still owns the scratch.
/// Another backup of the same shard waits instead of deleting the directory mid-copy.
fn with_checkpoint_scratch<T>(
    data_dir: &Path,
    shard_id: ShardId,
    body: impl FnOnce(&Path) -> Result<T>,
) -> Result<T> {
    let _guard = lock_checkpoint_scratch(data_dir, shard_id)?;
    clear_checkpoint_scratch(data_dir, shard_id)?;
    let path = data_dir.join(checkpoint_scratch_name(shard_id));
    let result = body(&path);
    if path.exists() {
        if let Err(error) = clear_checkpoint_scratch(data_dir, shard_id) {
            if result.is_ok() {
                return Err(error);
            }
        }
    }
    result
}

/// Removes one crash scratch directory. A symlink or a non-directory is left in place.
fn clear_checkpoint_scratch(data_dir: &Path, shard_id: ShardId) -> Result<()> {
    let path = data_dir.join(checkpoint_scratch_name(shard_id));
    remove_plain_dir(&path).map_err(|error| {
        if error.to_string().contains("refusing to delete") {
            backup_error(format!(
                "checkpoint scratch {} is not a plain directory; refusing to delete it",
                path.display()
            ))
        } else {
            error
        }
    })
}

fn ensure_registry_matches(root: &File, copied: &BTreeSet<ShardId>) -> Result<()> {
    let be = open_under(root, "be")?;
    let bytes = read_listed_file(&be, "NODE_SHARD_REGISTRY")?;
    let referenced = crate::group_host::shard_ids_in_registry_bytes(&bytes)?;
    for shard_id in referenced {
        if !copied.contains(&shard_id) {
            return Err(backup_error(format!(
                "NODE_SHARD_REGISTRY references shard {shard_id} which was not copied"
            )));
        }
    }
    Ok(())
}

fn destination_parent(destination: &Path) -> PathBuf {
    match destination.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Creates any missing ancestors of `dir`, then syncs from `dir` up through the
/// first ancestor that already existed so a later rename cannot depend on a
/// directory entry that was never persisted.
fn sync_directory_and_ancestors(dir: &Path) -> Result<()> {
    let mut created = Vec::new();
    let mut cursor = dir.to_path_buf();
    while !cursor.exists() {
        created.push(cursor.clone());
        if !cursor.pop() {
            break;
        }
    }
    let preexisting = cursor;
    if !dir.exists() {
        fs::create_dir_all(dir)?;
    }
    for path in &created {
        sync_dir(path)?;
    }
    if preexisting.as_os_str().is_empty() {
        sync_dir(Path::new("."))?;
    } else {
        sync_dir(&preexisting)?;
    }
    Ok(())
}

/// Resolves `path` against the current directory without requiring the final component to exist.
pub fn absolute_output_path(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(backup_error("backup path is empty"));
    }
    let Some(name) = path.file_name() else {
        return Err(backup_error("backup path has no file name"));
    };
    if name == "." || name == ".." {
        return Err(backup_error("backup path has no file name"));
    }
    let parent = fs::canonicalize(destination_parent(path))?;
    Ok(parent.join(name))
}

fn sync_tree(dir: &Path) -> Result<()> {
    let file = open_plain_dir(dir)?;
    sync_plain_dir(&file)
}

fn validate_manifest_tree(tree: &File, manifest: &BackupManifest) -> Result<()> {
    let listed: BTreeSet<&str> = manifest
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect();
    if !listed.contains("global_allocator/ID_ALLOCATOR_STATE") {
        return Err(restore_error("ID_ALLOCATOR_STATE is missing"));
    }
    if !listed.contains("be/NODE_SHARD_REGISTRY") {
        return Err(restore_error("NODE_SHARD_REGISTRY is missing"));
    }
    let copied: BTreeSet<ShardId> = manifest.shards.iter().map(|shard| shard.shard_id).collect();
    ensure_registry_matches(tree, &copied).map_err(|error| restore_error(error.to_string()))?;
    if !manifest
        .files
        .iter()
        .any(|file| file.path.starts_with("cell_") && file.path.ends_with("/CELL_MANAGER_STATE"))
    {
        return Err(restore_error("CELL_MANAGER_STATE is missing"));
    }
    for file in &manifest.files {
        if !(file.path.starts_with("cell_") && file.path.ends_with("/CELL_MANAGER_STATE")) {
            continue;
        }
        let identity = file.path.replace("CELL_MANAGER_STATE", "CLUSTER_IDENTITY");
        if !listed.contains(identity.as_str()) {
            return Err(restore_error(format!(
                "{identity} is missing; refusing a restore that would mint a new node instance id"
            )));
        }
    }
    for shard in &manifest.shards {
        let prefix = format!("be/shards/{:020}", shard.shard_id);
        let raft = open_under(tree, &format!("{prefix}/raft")).map_err(|error| {
            restore_error(format!(
                "shard {} log is incomplete: {error}",
                shard.shard_id
            ))
        })?;
        let inspection = crate::log_store::SegmentedRaftLogStore::inspect_copied_dir(&raft)
            .map_err(|error| {
                restore_error(format!(
                    "shard {} log is incomplete: {error}",
                    shard.shard_id
                ))
            })?;
        if inspection.last_purged_index > shard.image_index {
            return Err(restore_error(format!(
                "shard {} purged index {} is ahead of image {}",
                shard.shard_id, inspection.last_purged_index, shard.image_index
            )));
        }
        if shard.committed_index > shard.image_index {
            let from = shard.image_index.saturating_add(1);
            if !inspection.covers_inclusive(from, shard.committed_index) {
                return Err(restore_error(format!(
                    "shard {} is missing committed log indexes {from}..={}",
                    shard.shard_id, shard.committed_index
                )));
            }
        }
        let lsm = open_under(tree, &format!("{prefix}/lsm")).map_err(|error| {
            restore_error(format!(
                "shard {} LSM image is incomplete: {error}",
                shard.shard_id
            ))
        })?;
        let recovered = read_lsm_recovered(&lsm).map_err(|error| {
            restore_error(format!(
                "shard {} LSM image is incomplete: {error}",
                shard.shard_id
            ))
        })?;
        if recovered.checkpoint.flushed_raft_index != shard.image_index {
            return Err(restore_error(format!(
                "shard {} LSM checkpoint {} does not match manifest image {}",
                shard.shard_id, recovered.checkpoint.flushed_raft_index, shard.image_index
            )));
        }
        for meta in recovered.live_files.values() {
            let name = sst_file_name(meta.file_id);
            let file = open_child(&lsm, &name, false).map_err(|error| {
                restore_error(format!(
                    "shard {} is missing SST {name}: {error}",
                    shard.shard_id
                ))
            })?;
            let info = file.metadata()?;
            if !info.is_file() {
                return Err(restore_error(format!(
                    "shard {} is missing SST {name}",
                    shard.shard_id
                )));
            }
        }
        if shard.image_kind == IMAGE_SNAPSHOT {
            let snap = open_under(&raft, &format!("snapshots/snap_{:020}", shard.image_index))
                .map_err(|error| {
                    restore_error(format!(
                        "shard {} snapshot {} is incomplete: {error}",
                        shard.shard_id, shard.image_index
                    ))
                })?;
            if read_listed_file(&snap, "SNAPSHOT_META").is_err() {
                return Err(restore_error(format!(
                    "shard {} snapshot {} is incomplete",
                    shard.shard_id, shard.image_index
                )));
            }
        } else if shard.image_kind != IMAGE_FLUSHED_LSM {
            return Err(restore_error(format!(
                "shard {} has unknown image kind {}",
                shard.shard_id, shard.image_kind
            )));
        }
        if !listed
            .iter()
            .any(|path| path.starts_with(&format!("{prefix}/")))
        {
            return Err(restore_error(format!(
                "shard {} has no listed files",
                shard.shard_id
            )));
        }
    }
    Ok(())
}

fn require_active_hold(data_dir: &Path) -> Result<BackupHoldRecord> {
    match effective_phase(data_dir)? {
        HoldPhase::Active(record) => {
            if record.floors.is_empty() {
                return Err(backup_error(
                    "hold is active but has no shard floors; latch the hold before copying",
                ));
            }
            Ok(record)
        }
        HoldPhase::Pending => Err(backup_error(
            "hold is still pending; wait until floors are latched before copying",
        )),
        HoldPhase::Inactive => Err(backup_error(
            "no active backup hold; install a hold before copying so purge cannot remove the image",
        )),
    }
}

fn floor_for_shard(hold: &BackupHoldRecord, shard_id: ShardId) -> Result<&ShardHoldFloor> {
    hold.floors
        .iter()
        .find(|floor| floor.shard_id == shard_id)
        .ok_or_else(|| {
            backup_error(format!(
                "shard {shard_id} is not covered by hold {}; release the hold and acquire it again",
                hold.hold_id
            ))
        })
}

fn reject_destination(data_dir: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        return Err(backup_error(format!(
            "destination {} already exists",
            destination.display()
        )));
    }
    let data = canonical_data_dir(data_dir)?;
    let parent = destination_parent(destination);
    fs::create_dir_all(&parent)?;
    let parent = fs::canonicalize(&parent)?;
    if parent.starts_with(&data) {
        return Err(backup_error(
            "backup destination must be outside the live data directory",
        ));
    }
    Ok(())
}

fn copy_raft_fd(src: &File, dst: &File) -> Result<()> {
    let mut copied_hard_state = false;
    for entry in list_plain_dir(src)? {
        let copy_name = entry.name == "HARD_STATE"
            || entry.name == "INITIALIZED"
            || (entry.name.starts_with("segment_") && entry.name.ends_with(".raftlog"));
        if !copy_name {
            continue;
        }
        if entry.kind != EntryKind::File {
            return Err(backup_error(format!(
                "refusing to copy special file {}",
                entry.name
            )));
        }
        stream_copy_child_file(src, &entry.name, dst, &entry.name)?;
        if entry.name == "HARD_STATE" {
            copied_hard_state = true;
        }
    }
    if !copied_hard_state {
        return Err(backup_error(
            "HARD_STATE is missing from the anchored raft directory",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn copy_lsm_files(source: &Path, dest: &Path) -> Result<()> {
    let src = open_plain_dir(source).map_err(|error| {
        backup_error(format!(
            "LSM directory {} is missing: {error}",
            source.display()
        ))
    })?;
    let dst = ensure_dest_dir(dest)?;
    copy_lsm_dir(&src, &dst)
}

fn copy_lsm_dir(src: &File, dst: &File) -> Result<()> {
    let mut saw_current = false;
    for entry in list_plain_dir(src)? {
        if entry.name == "LOCK" || entry.name.ends_with(".tmp") {
            if entry.kind != EntryKind::File {
                return Err(backup_error(format!(
                    "refusing to copy special file {}",
                    entry.name
                )));
            }
            continue;
        }
        if entry.kind != EntryKind::File {
            return Err(backup_error(format!(
                "refusing to copy special file {}",
                entry.name
            )));
        }
        stream_copy_child_file(src, &entry.name, dst, &entry.name)?;
        if entry.name == "CURRENT" {
            saw_current = true;
        }
    }
    if !saw_current {
        return Err(backup_error(
            "CURRENT is missing from the anchored LSM image",
        ));
    }
    Ok(())
}

fn current_manifest_name(current: &[u8]) -> Result<String> {
    let text = std::str::from_utf8(current)
        .map_err(|error| backup_error(format!("CURRENT is not utf-8: {error}")))?;
    // Reject before any child open. `Path::join` would drop the directory fd
    // for an absolute name and would leave it for `../`.
    component_name(OsStr::new(text.trim())).map_err(|_| {
        backup_error(format!(
            "CURRENT names {:?}, which is not a single file in this directory",
            text.trim()
        ))
    })
}

fn read_lsm_recovered(lsm: &File) -> Result<metadb_storage::manifest::RecoveredManifestState> {
    let current = read_listed_file(lsm, "CURRENT")?;
    let name = current_manifest_name(&current)?;
    let raw = read_listed_file(lsm, &name)?;
    ManifestLog::recovered_from_manifest_bytes(&name, &raw)
}

fn read_lsm_index(lsm: &File) -> Result<Option<u64>> {
    match read_listed_file(lsm, "CURRENT") {
        Ok(current) => {
            let name = current_manifest_name(&current)?;
            let raw = read_listed_file(lsm, &name)?;
            Ok(Some(
                ManifestLog::checkpoint_from_manifest_bytes(&name, &raw)?.flushed_raft_index,
            ))
        }
        Err(error) if is_not_found(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
fn copy_dir_bytes(source: &Path, dest: &Path) -> Result<()> {
    let src = open_plain_dir(source)?;
    let dst = ensure_dest_dir(dest)?;
    copy_plain_tree(&src, &dst)?;
    if let Some(parent) = dest.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn copy_child_frame(
    dir: &File,
    name: &str,
    dest: &File,
    relative: &str,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    let mut last_error = String::from("missing");
    for _ in 0..8 {
        match read_listed_file(dir, name) {
            Ok(bytes) => match verify_crc32c_le(&bytes, "backup source") {
                Ok(_) => {
                    write_child_file(dest, name, &bytes)?;
                    files.push(BackupFileRecord {
                        path: relative.replace('\\', "/"),
                        len: bytes.len() as u64,
                        crc32c: checksum_crc32c(&bytes),
                    });
                    return Ok(());
                }
                Err(error) => last_error = error.to_string(),
            },
            Err(error) if error.to_string().contains("refusing to follow") => return Err(error),
            Err(error) => last_error = error.to_string(),
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(backup_error(format!(
        "{relative} stayed unreadable while copying ({last_error})"
    )))
}

fn record_log_fd(
    dir: &File,
    relative_prefix: &str,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    for entry in list_plain_dir(dir)? {
        let copy_name = entry.name == "HARD_STATE"
            || entry.name == "INITIALIZED"
            || (entry.name.starts_with("segment_") && entry.name.ends_with(".raftlog"));
        if !copy_name {
            continue;
        }
        if entry.kind != EntryKind::File {
            return Err(backup_error(format!(
                "refusing to copy special file {}",
                entry.name
            )));
        }
        let (len, crc32c) = stream_hash_child_file(dir, &entry.name)?;
        files.push(BackupFileRecord {
            path: format!("{relative_prefix}/{}", entry.name),
            len,
            crc32c,
        });
    }
    Ok(())
}

fn record_tree_fd(
    dir: &File,
    relative_prefix: &str,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    let mut discovered = Vec::new();
    walk_plain_dir(dir, Path::new(""), &mut discovered)?;
    discovered.sort();
    for path in discovered {
        let suffix = relative_string(&path)?;
        let relative = format!("{}/{}", relative_prefix.trim_end_matches('/'), suffix);
        let (len, crc32c) = stream_hash_relative(dir, &path)?;
        files.push(BackupFileRecord {
            path: relative,
            len,
            crc32c,
        });
    }
    Ok(())
}

fn safe_relative(path: &str) -> std::result::Result<PathBuf, String> {
    if path.is_empty() || path.starts_with('/') || path.contains('\\') {
        return Err(format!("path {path} is not a relative backup path"));
    }
    let mut out = PathBuf::new();
    for part in path.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(format!("path {path} is not a relative backup path"));
        }
        out.push(part);
    }
    Ok(out)
}

enum RestoreDestination {
    Missing,
    EmptyDir(File),
}

fn inspect_restore_destination(destination: &Path) -> Result<RestoreDestination> {
    match fs::symlink_metadata(destination) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(RestoreDestination::Missing)
        }
        Err(error) => Err(error.into()),
        Ok(meta) if meta.file_type().is_symlink() => Err(restore_error(format!(
            "refusing to follow symlink {}",
            destination.display()
        ))),
        Ok(meta) if !meta.is_dir() => Err(restore_error(format!(
            "destination {} is not a directory",
            destination.display()
        ))),
        Ok(_) => {
            let dir = open_plain_dir(destination)?;
            if !list_plain_dir(&dir)?.is_empty() {
                return Err(restore_error(format!(
                    "destination {} is not empty",
                    destination.display()
                )));
            }
            Ok(RestoreDestination::EmptyDir(dir))
        }
    }
}

fn sync_plain_dir(dir: &File) -> Result<()> {
    for entry in list_plain_dir(dir)? {
        if entry.kind == EntryKind::Dir {
            let child = open_child(dir, &entry.name, true)?;
            sync_plain_dir(&child)?;
        } else if entry.kind != EntryKind::File {
            return Err(backup_error(format!(
                "refusing to copy special file {}",
                entry.name
            )));
        }
    }
    dir.sync_all()?;
    Ok(())
}

pub(crate) fn same_opened_file(opened: &std::fs::Metadata, named: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        opened.dev() == named.dev() && opened.ino() == named.ino()
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        opened.file_index().is_some()
            && opened.volume_serial_number() == named.volume_serial_number()
            && opened.file_index() == named.file_index()
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (opened, named);
        false
    }
}

fn remove_plain_dir(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => {
            return Err(backup_error(format!(
                "refusing to delete {}",
                path.display()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    let dir = open_plain_dir(path)?;
    remove_dir_fd(&dir)?;
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => {
            return Err(backup_error(format!(
                "refusing to delete {}",
                path.display()
            )));
        }
        Ok(meta) => {
            let opened = dir.metadata()?;
            if !same_opened_file(&opened, &meta) {
                return Err(backup_error(format!(
                    "refusing to delete {}",
                    path.display()
                )));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    // Removal is by name. A different empty directory that replaces this
    // path after the identity check can still be removed. The children
    // deleted above belong to the directory that was opened.
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(backup_error(format!(
            "refusing to delete {}",
            path.display()
        )));
    };
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::env::current_dir()?,
    };
    let parent = File::open(parent)?;
    cap_remove_dir(&parent, Path::new(name)).map_err(|error| map_child_error(name, error))?;
    Ok(())
}

fn remove_dir_fd(dir: &File) -> Result<()> {
    for entry in list_plain_dir(dir)? {
        match entry.kind {
            EntryKind::Dir => {
                let nested = open_child(dir, &entry.name, true)?;
                remove_dir_fd(&nested)?;
                cap_remove_dir(dir, Path::new(&entry.name))
                    .map_err(|error| map_child_error(&entry.name, error))?;
            }
            EntryKind::File => {
                cap_remove_file(dir, Path::new(&entry.name))
                    .map_err(|error| map_child_error(&entry.name, error))?;
            }
            EntryKind::Other => {
                return Err(backup_error(format!(
                    "refusing to delete special file {}",
                    entry.name
                )));
            }
        }
    }
    Ok(())
}

fn read_manifest(artifact: &Path) -> Result<BackupManifest> {
    let dir = open_plain_dir(artifact)
        .map_err(|error| restore_error(format!("backup artifact is missing or unsafe: {error}")))?;
    let bytes = match read_listed_file(&dir, BACKUP_MANIFEST_FILE) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Err(restore_error(format!(
                "BACKUP_MANIFEST is missing or unsafe: {error}"
            )));
        }
    };
    let payload = verify_crc32c_le(&bytes, "backup record")
        .map_err(|error| restore_error(error.to_string()))?;
    codec::deserialize(payload).map_err(|error| restore_error(error.to_string()))
}

fn hold_path(data_dir: &Path) -> PathBuf {
    data_dir.join(BACKUP_HOLD_FILE)
}

fn read_hold_file(data_dir: &Path) -> Result<Option<BackupHoldRecord>> {
    let path = hold_path(data_dir);
    if !path.is_file() {
        return Ok(None);
    }
    read_record!(&path).map(Some)
}

fn write_hold_file(data_dir: &Path, record: &BackupHoldRecord) -> Result<()> {
    write_record!(&hold_path(data_dir), record)
}

fn remove_hold_file(data_dir: &Path) -> Result<()> {
    let path = hold_path(data_dir);
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

struct Staging {
    path: PathBuf,
    armed: bool,
}

impl Staging {
    fn partial_path(destination: &Path) -> PathBuf {
        let parent = destination_parent(destination);
        let name = destination
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "backup".to_string());
        parent.join(format!(".{name}.partial"))
    }

    fn cleanup_stale(destination: &Path) -> Result<()> {
        let path = Self::partial_path(destination);
        remove_plain_dir(&path)
    }

    fn create(destination: &Path) -> Result<Self> {
        let parent = destination_parent(destination);
        fs::create_dir_all(&parent)?;
        let path = Self::partial_path(destination);
        match fs::create_dir(&path) {
            Ok(()) => Ok(Self { path, armed: true }),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(backup_error(format!(
                    "staging directory {} already exists; refusing to delete it",
                    path.display()
                )))
            }
            Err(error) => Err(error.into()),
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        if self.armed {
            let _ = remove_plain_dir(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard_raft(data_dir: &Path, shard_id: u64) -> PathBuf {
        data_dir
            .join("be")
            .join("shards")
            .join(format!("{shard_id:020}"))
            .join("raft")
    }

    #[test]
    fn pending_hold_freezes_purge_and_active_hold_clamps_to_the_floor() {
        let dir = tempfile::tempdir().unwrap();
        let raft = shard_raft(dir.path(), 1);
        fs::create_dir_all(&raft).unwrap();
        assert!(begin_hold(dir.path(), Duration::from_secs(60)).unwrap() == BeginHold::Pending);
        assert_eq!(hold_purge_cap(&raft), Some(0));
        assert!(snapshot_index_protected(&raft.join("snapshots"), 9));

        let record = finish_hold(
            dir.path(),
            vec![ShardHoldFloor {
                shard_id: 1,
                max_purge_index: 4,
                retain_snapshots_through: 4,
            }],
        )
        .unwrap();
        assert_eq!(record.phase, HOLD_PHASE_ACTIVE);
        assert_eq!(hold_purge_cap(&raft), Some(4));
        assert!(snapshot_index_protected(&raft.join("snapshots"), 4));
        assert!(!snapshot_index_protected(&raft.join("snapshots"), 5));
        assert!(matches!(
            begin_hold(dir.path(), Duration::from_secs(60)).unwrap(),
            BeginHold::AlreadyActive(_)
        ));

        testing_force_expire(dir.path()).unwrap();
        assert_eq!(hold_purge_cap(&raft), None);
        assert!(!snapshot_index_protected(&raft.join("snapshots"), 4));
        release_hold(dir.path()).unwrap();
    }

    #[test]
    fn staging_refuses_to_delete_an_existing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("archive");
        let staging = dir.path().join(".archive.partial");
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("KEEP"), b"live").unwrap();
        let error = match Staging::create(&destination) {
            Err(error) => error,
            Ok(_) => panic!("staging creation deleted or reused an existing directory"),
        };
        assert!(error.to_string().contains("refusing to delete"), "{error}");
        assert_eq!(fs::read(staging.join("KEEP")).unwrap(), b"live");
    }

    #[test]
    fn relative_destination_parent_is_the_current_directory() {
        assert_eq!(
            destination_parent(Path::new("restored")),
            PathBuf::from(".")
        );
        assert_eq!(
            destination_parent(Path::new("/var/lib/archive")),
            PathBuf::from("/var/lib")
        );
        let anchored = absolute_output_path(Path::new("metadb-backup-out")).unwrap();
        assert_eq!(
            anchored,
            std::env::current_dir().unwrap().join("metadb-backup-out")
        );
        assert!(anchored.is_absolute());
    }

    #[test]
    fn new_ancestors_are_created_and_the_leaf_exists() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("level-one").join("level-two");
        sync_directory_and_ancestors(&parent).unwrap();
        assert!(parent.is_dir());
        assert!(dir.path().join("level-one").is_dir());
    }

    #[test]
    fn registry_that_names_an_uncopied_shard_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("be")).unwrap();
        let mut registry = BTreeMap::new();
        registry.insert(
            99u64,
            crate::group_host::RegistryShardState::Active {
                peers: BTreeMap::new(),
            },
        );
        let mut bytes = metadb_core::codec::serialize(&registry).unwrap();
        metadb_core::append_crc32c_le(&mut bytes);
        fs::write(dir.path().join("be").join("NODE_SHARD_REGISTRY"), bytes).unwrap();
        let copied = BTreeSet::from([1u64]);
        let root = open_plain_dir(dir.path()).unwrap();
        let error = ensure_registry_matches(&root, &copied).unwrap_err();
        assert!(error.to_string().contains("shard 99"), "{error}");
    }

    #[test]
    fn offline_backup_without_a_hold_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("be").join("shards")).unwrap();
        let destination = dir.path().join("out");
        let error = create_offline_backup(dir.path(), &destination, &|_, _| Ok(())).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("install a hold before copying so purge cannot remove the image"),
            "{error}"
        );
        assert!(!destination.exists());
    }

    #[test]
    fn shard_directory_cannot_appear_while_the_publish_lock_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let be = dir.path().join("be");
        fs::create_dir_all(be.join("shards")).unwrap();
        let guard = lock_be_shard_set(&be).unwrap();
        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let created = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let be_thread = be.clone();
        let started_thread = started.clone();
        let created_thread = created.clone();
        let handle = thread::spawn(move || {
            started_thread.store(true, std::sync::atomic::Ordering::Release);
            let shard = be_thread.join("shards").join(format!("{:020}", 99u64));
            if let Some(parent) = be_dir_for_shard(&shard) {
                let _held = lock_be_shard_set(&parent).unwrap();
                fs::create_dir_all(&shard).unwrap();
            }
            created_thread.store(true, std::sync::atomic::Ordering::Release);
        });
        while !started.load(std::sync::atomic::Ordering::Acquire) {
            thread::yield_now();
        }
        thread::sleep(Duration::from_millis(50));
        assert!(!created.load(std::sync::atomic::Ordering::Acquire));
        assert!(!be.join("shards").join(format!("{:020}", 99u64)).exists());
        drop(guard);
        handle.join().unwrap();
        assert!(created.load(std::sync::atomic::Ordering::Acquire));
        assert!(be.join("shards").join(format!("{:020}", 99u64)).is_dir());
    }

    #[test]
    fn a_shard_created_before_rename_rejects_publication() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(
            dir.path()
                .join("be")
                .join("shards")
                .join(format!("{:020}", 1u64)),
        )
        .unwrap();
        let staging_parent = dir.path().join("stage-parent");
        fs::create_dir_all(&staging_parent).unwrap();
        let destination = staging_parent.join("artifact");
        let mut staging = Staging::create(&destination).unwrap();
        fs::write(staging.path.join("marker"), b"ok").unwrap();
        *BEFORE_LOCKED_RENAME.lock().unwrap() = Some(|data_dir| {
            fs::create_dir_all(
                data_dir
                    .join("be")
                    .join("shards")
                    .join(format!("{:020}", 99u64)),
            )
            .unwrap();
        });
        let copied = BTreeSet::from([1u64]);
        let staging_path = staging.path.clone();
        let error = publish_staging_under_shard_lock(
            dir.path(),
            &copied,
            &staging_path,
            &destination,
            &mut staging,
        )
        .unwrap_err();
        assert!(error.to_string().contains("shard 99"), "{error}");
        assert!(!destination.exists());
        assert!(dir
            .path()
            .join("be")
            .join("shards")
            .join(format!("{:020}", 99u64))
            .is_dir());
    }

    #[test]
    fn checkpoint_scratch_is_not_cleared_while_another_backup_holds_it() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().join(checkpoint_scratch_name(1));
        let release = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cleared = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dir_holder = dir.path().to_path_buf();
        let release_holder = release.clone();
        let started_holder = started.clone();
        let holder = thread::spawn(move || {
            let guard = lock_checkpoint_scratch(&dir_holder, 1).unwrap();
            fs::create_dir_all(&scratch).unwrap();
            fs::write(scratch.join("KEEP"), b"live").unwrap();
            started_holder.store(true, std::sync::atomic::Ordering::Release);
            while !release_holder.load(std::sync::atomic::Ordering::Acquire) {
                thread::yield_now();
            }
            drop(guard);
        });
        while !started.load(std::sync::atomic::Ordering::Acquire) {
            thread::yield_now();
        }
        let dir_worker = dir.path().to_path_buf();
        let cleared_worker = cleared.clone();
        let worker = thread::spawn(move || {
            with_checkpoint_scratch(&dir_worker, 1, |_path| {
                cleared_worker.store(true, std::sync::atomic::Ordering::Release);
                Ok(())
            })
            .unwrap();
        });
        thread::sleep(Duration::from_millis(100));
        assert!(
            dir.path()
                .join(checkpoint_scratch_name(1))
                .join("KEEP")
                .is_file(),
            "the waiting backup removed a scratch that was still in use"
        );
        assert!(!cleared.load(std::sync::atomic::Ordering::Acquire));
        release.store(true, std::sync::atomic::Ordering::Release);
        holder.join().unwrap();
        worker.join().unwrap();
        assert!(cleared.load(std::sync::atomic::Ordering::Acquire));
        assert!(!dir.path().join(checkpoint_scratch_name(1)).exists());
    }

    #[cfg(unix)]
    #[test]
    fn crash_checkpoint_scratch_is_removed_without_touching_other_names() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().join(checkpoint_scratch_name(1));
        fs::create_dir_all(scratch.join("nested")).unwrap();
        fs::write(scratch.join("nested").join("marker"), b"old").unwrap();
        let decoy = dir.path().join(".backup-ckpt-0001");
        fs::create_dir_all(&decoy).unwrap();
        fs::write(decoy.join("keep"), b"keep").unwrap();
        let keep_dir = dir.path().join("keep-me");
        fs::create_dir_all(&keep_dir).unwrap();
        clear_checkpoint_scratch(dir.path(), 1).unwrap();
        assert!(!scratch.exists());
        assert_eq!(fs::read(decoy.join("keep")).unwrap(), b"keep");
        assert!(keep_dir.is_dir());

        let target = dir.path().join("real-data");
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("KEEP"), b"live").unwrap();
        std::os::unix::fs::symlink(&target, dir.path().join(checkpoint_scratch_name(2))).unwrap();
        let error = clear_checkpoint_scratch(dir.path(), 2).unwrap_err();
        assert!(error.to_string().contains("refusing to delete"), "{error}");
        assert_eq!(fs::read(target.join("KEEP")).unwrap(), b"live");
    }

    #[test]
    fn completing_a_job_leaves_a_newer_request_in_place() {
        let dir = tempfile::tempdir().unwrap();
        write_backup_job(dir.path(), Path::new("/tmp/a")).unwrap();
        let job = claim_backup_job(dir.path()).unwrap().unwrap();
        write_backup_job(dir.path(), Path::new("/tmp/b")).unwrap();
        complete_backup_job(dir.path(), &job.output_dir, Err("stopped".into())).unwrap();
        assert!(dir.path().join(BACKUP_JOB_FILE).is_file());
        assert!(!dir.path().join(BACKUP_JOB_RUNNING).is_file());
        let next = claim_backup_job(dir.path()).unwrap().unwrap();
        assert_eq!(next.output_dir, "/tmp/b");
    }

    #[cfg(unix)]
    fn external_sentinel(dir: &Path) -> PathBuf {
        let external = dir.join("external");
        fs::create_dir_all(external.join("nested")).unwrap();
        fs::write(external.join("SENTINEL"), b"keep-me").unwrap();
        external
    }

    #[cfg(unix)]
    fn assert_sentinel_untouched(external: &Path) {
        assert_eq!(fs::read(external.join("SENTINEL")).unwrap(), b"keep-me");
        assert!(external.join("nested").is_dir());
        let mut names = fs::read_dir(external)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec![
                std::ffi::OsString::from("SENTINEL"),
                std::ffi::OsString::from("nested")
            ]
        );
    }

    #[cfg(unix)]
    fn assert_no_sentinel_bytes(root: &Path) {
        if !root.exists() {
            return;
        }
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let meta = fs::symlink_metadata(&dir).unwrap();
            assert!(
                !meta.file_type().is_symlink(),
                "copied tree contains symlink {}",
                dir.display()
            );
            if meta.is_dir() {
                for entry in fs::read_dir(&dir).unwrap() {
                    stack.push(entry.unwrap().path());
                }
            } else if meta.is_file() {
                assert_ne!(fs::read(&dir).unwrap(), b"keep-me");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn copy_dir_bytes_rejects_a_file_symlink_and_keeps_the_external_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let external = external_sentinel(dir.path());
        let source = dir.path().join("source");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("inside"), b"local").unwrap();
        std::os::unix::fs::symlink(external.join("SENTINEL"), source.join("leak")).unwrap();
        let dest = dir.path().join("dest");
        let error = copy_dir_bytes(&source, &dest).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "{error}"
        );
        assert_sentinel_untouched(&external);
        assert_no_sentinel_bytes(&dest);
    }

    #[cfg(unix)]
    #[test]
    fn copy_dir_bytes_rejects_a_directory_symlink_and_keeps_the_external_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let external = external_sentinel(dir.path());
        let source = dir.path().join("source");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("inside"), b"local").unwrap();
        std::os::unix::fs::symlink(&external, source.join("escape")).unwrap();
        let dest = dir.path().join("dest");
        let error = copy_dir_bytes(&source, &dest).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "{error}"
        );
        assert_sentinel_untouched(&external);
        assert!(!dest.join("escape").exists());
        assert_no_sentinel_bytes(&dest);
    }

    #[cfg(unix)]
    #[test]
    fn copy_lsm_files_rejects_a_file_symlink_and_keeps_the_external_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let external = external_sentinel(dir.path());
        let source = dir.path().join("lsm");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("CURRENT"), b"MANIFEST-000001\n").unwrap();
        std::os::unix::fs::symlink(external.join("SENTINEL"), source.join("000001.sst")).unwrap();
        let dest = dir.path().join("dest");
        let error = copy_lsm_files(&source, &dest).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "{error}"
        );
        assert_sentinel_untouched(&external);
        assert_no_sentinel_bytes(&dest);
    }

    #[cfg(unix)]
    fn write_min_manifest(artifact: &Path, body: &[u8]) -> Result<()> {
        let tree = artifact.join(BACKUP_TREE_DIR);
        fs::create_dir_all(&tree).unwrap();
        fs::write(tree.join("note"), body).unwrap();
        let manifest = BackupManifest {
            version: BACKUP_MANIFEST_VERSION,
            created_unix_ms: 1,
            hold_id: 1,
            files: vec![BackupFileRecord {
                path: "note".to_string(),
                len: body.len() as u64,
                crc32c: checksum_crc32c(body),
            }],
            shards: vec![ShardImageRecord {
                shard_id: 1,
                image_kind: IMAGE_FLUSHED_LSM,
                image_index: 0,
                last_purged_index: 0,
                committed_index: 0,
            }],
        };
        write_record!(&artifact.join(BACKUP_MANIFEST_FILE), &manifest)?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn restore_rejects_a_symlink_destination_and_keeps_the_external_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let external = external_sentinel(dir.path());
        let dest = dir.path().join("restored");
        std::os::unix::fs::symlink(&external, &dest).unwrap();
        let artifact = dir.path().join("artifact");
        write_min_manifest(&artifact, b"hello").unwrap();
        let error = restore_backup(&artifact, &dest, &|_, _| Ok(())).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "{error}"
        );
        assert_sentinel_untouched(&external);
        assert!(fs::symlink_metadata(&dest)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[cfg(unix)]
    #[test]
    fn restore_rejects_a_file_symlink_in_the_artifact_and_keeps_the_external_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let external = external_sentinel(dir.path());
        let artifact = dir.path().join("artifact");
        write_min_manifest(&artifact, b"hello").unwrap();
        std::os::unix::fs::symlink(
            external.join("SENTINEL"),
            artifact.join(BACKUP_TREE_DIR).join("leak"),
        )
        .unwrap();
        let dest = dir.path().join("restored");
        let error = restore_backup(&artifact, &dest, &|_, _| Ok(())).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "{error}"
        );
        assert_sentinel_untouched(&external);
        assert!(!dest.exists());
        assert_no_sentinel_bytes(&dest);
    }

    #[cfg(unix)]
    #[test]
    fn restore_rejects_a_directory_symlink_in_the_artifact_and_keeps_the_external_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let external = external_sentinel(dir.path());
        let artifact = dir.path().join("artifact");
        write_min_manifest(&artifact, b"hello").unwrap();
        std::os::unix::fs::symlink(&external, artifact.join(BACKUP_TREE_DIR).join("escape"))
            .unwrap();
        let dest = dir.path().join("restored");
        let error = restore_backup(&artifact, &dest, &|_, _| Ok(())).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "{error}"
        );
        assert_sentinel_untouched(&external);
        assert!(!dest.exists());
        assert!(!external.join("note").exists());
    }

    #[cfg(unix)]
    #[test]
    fn live_log_symlink_is_not_copied_and_the_sentinel_stays() {
        let dir = tempfile::tempdir().unwrap();
        let external = external_sentinel(dir.path());
        let raft = dir.path().join("raft");
        let store = crate::log_store::SegmentedRaftLogStore::open(
            &raft,
            std::sync::Arc::new(crate::types::ShardWatermarkTracker::default()),
        )
        .unwrap();
        fs::remove_file(raft.join("HARD_STATE")).unwrap();
        std::os::unix::fs::symlink(external.join("SENTINEL"), raft.join("HARD_STATE")).unwrap();
        let source = open_plain_dir(&raft).unwrap();
        let dest = dir.path().join("copied");
        fs::create_dir(&dest).unwrap();
        let dest_dir = open_plain_dir(&dest).unwrap();
        let error = store.copy_durable_log(&source, &dest_dir).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "{error}"
        );
        assert_sentinel_untouched(&external);
        assert_no_sentinel_bytes(&dest);
    }

    #[cfg(unix)]
    #[test]
    fn intermediate_shard_directory_symlink_is_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let external = external_sentinel(dir.path());
        let real_shard = dir.path().join("real-shard");
        fs::create_dir_all(real_shard.join("lsm")).unwrap();
        fs::write(real_shard.join("lsm").join("CURRENT"), b"MANIFEST-000001\n").unwrap();
        fs::copy(
            external.join("SENTINEL"),
            real_shard.join("lsm").join("SENTINEL"),
        )
        .unwrap();
        let data = dir.path().join("data");
        let shards = data.join("be").join("shards");
        fs::create_dir_all(&shards).unwrap();
        std::os::unix::fs::symlink(&real_shard, shards.join(format!("{:020}", 1u64))).unwrap();
        let root = open_plain_dir(&data).unwrap();
        let error = open_under(&root, &format!("be/shards/{:020}/lsm", 1u64)).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "{error}"
        );
        assert_eq!(fs::read(external.join("SENTINEL")).unwrap(), b"keep-me");
        assert_eq!(
            fs::read(real_shard.join("lsm").join("SENTINEL")).unwrap(),
            b"keep-me"
        );
        let listed = list_shard_ids(&data).unwrap_err();
        assert!(
            listed.to_string().contains("refusing to follow symlink"),
            "{listed}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn manifest_tmp_symlink_is_not_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let sentinel = dir.path().join("SENTINEL");
        fs::write(&sentinel, b"keep-me").unwrap();
        let record = dir.path().join("BACKUP_MANIFEST");
        std::os::unix::fs::symlink(&sentinel, dir.path().join("BACKUP_MANIFEST.tmp")).unwrap();
        let error = write_serialized(&record, b"payload".to_vec()).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "{error}"
        );
        assert_eq!(fs::read(&sentinel).unwrap(), b"keep-me");
        assert!(fs::symlink_metadata(dir.path().join("BACKUP_MANIFEST.tmp"))
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[cfg(unix)]
    #[test]
    fn current_escape_is_rejected_before_the_external_file_is_opened() {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("outside.fifo");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let opened = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fifo_for_writer = fifo.clone();
        let opened_for_writer = opened.clone();
        let writer = thread::spawn(move || {
            if OpenOptions::new()
                .write(true)
                .open(&fifo_for_writer)
                .is_ok()
            {
                opened_for_writer.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });
        thread::sleep(Duration::from_millis(50));
        let absolute = fifo.to_str().expect("fifo path is utf-8");
        for label in ["absolute", "dotdot"] {
            let lsm_path = dir.path().join(label);
            fs::create_dir(&lsm_path).unwrap();
            let pointer = if label == "absolute" {
                absolute.to_string()
            } else {
                "../outside.fifo".to_string()
            };
            fs::write(lsm_path.join("CURRENT"), &pointer).unwrap();
            let lsm = open_plain_dir(&lsm_path).unwrap();
            let error = read_lsm_index(&lsm).unwrap_err();
            assert!(
                error.to_string().contains("not a single file"),
                "{label}: {error}"
            );
            let error = ManifestLog::read_checkpoint_readonly(&lsm_path).unwrap_err();
            assert!(
                error.to_string().contains("invalid CURRENT"),
                "{label}: {error}"
            );
        }
        assert!(
            !opened.load(std::sync::atomic::Ordering::SeqCst),
            "external CURRENT target was opened"
        );
        let _ = File::open(&fifo);
        let _ = writer.join();
    }

    #[cfg(unix)]
    #[test]
    fn cell_state_symlink_is_rejected_before_the_external_file_is_opened() {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("outside.fifo");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let opened = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fifo_for_writer = fifo.clone();
        let opened_for_writer = opened.clone();
        let writer = thread::spawn(move || {
            if OpenOptions::new()
                .write(true)
                .open(&fifo_for_writer)
                .is_ok()
            {
                opened_for_writer.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });
        thread::sleep(Duration::from_millis(50));

        let linked_cell = dir.path().join("linked-cell");
        fs::create_dir(&linked_cell).unwrap();
        std::os::unix::fs::symlink(&fifo, linked_cell.join("CELL_MANAGER_STATE")).unwrap();
        let link_root = dir.path().join("link-root");
        fs::create_dir(&link_root).unwrap();
        std::os::unix::fs::symlink(&linked_cell, link_root.join("cell_1")).unwrap();
        let error =
            check_anchored_tree(&open_plain_dir(&link_root).unwrap(), &|_, _| Ok(())).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "cell directory symlink: {error}"
        );

        let file_root = dir.path().join("file-root");
        let cell = file_root.join("cell_1");
        fs::create_dir_all(&cell).unwrap();
        std::os::unix::fs::symlink(&fifo, cell.join("CELL_MANAGER_STATE")).unwrap();
        let error =
            check_anchored_tree(&open_plain_dir(&file_root).unwrap(), &|_, _| Ok(())).unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "state file symlink: {error}"
        );

        let link = dir.path().join("reparse");
        std::os::unix::fs::symlink(&fifo, &link).unwrap();
        let meta = fs::symlink_metadata(&link).unwrap();
        let error = reject_reparse(&meta, "reparse").unwrap_err();
        assert!(
            error.to_string().contains("refusing to follow symlink"),
            "reparse: {error}"
        );

        assert!(
            !opened.load(std::sync::atomic::Ordering::SeqCst),
            "external CELL_MANAGER_STATE target was opened"
        );
        let _ = File::open(&fifo);
        let _ = writer.join();
    }
}
