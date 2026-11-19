//! Operator backup hold, backup artifact, and restore.
//!
//! The durable image is a complete Raft snapshot or an equivalent flushed LSM
//! checkpoint, plus the segmented log that still has to be replayed. There is
//! no separate Raft WAL. A hold keeps log purge and snapshot-directory deletion
//! from removing that image while the copy runs. The hold is a file under the
//! server data directory so a crash does not depend on a live process, and it
//! expires on the wall clock so a missed release cannot pin purge forever.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metadb_core::codec;
use metadb_core::{
    append_crc32c_le, checksum_crc32c, verify_crc32c_le, MetaDbError, Result, ShardId,
};
use metadb_storage::sstable::{sst_file_name, sync_dir};
use metadb_storage::{LifecycleStorageEngine, ManifestLog};

use crate::log_store::{
    inspect_raft_dir, read_persisted_purged_index, RaftLogInspection, SegmentedRaftLogStore,
};
use crate::state_machine::{
    highest_durable_snapshot_index, newest_snapshot_at_or_below, snapshot_dir_is_complete,
};

fn write_serialized(path: &Path, payload: Vec<u8>) -> Result<()> {
    let mut bytes = payload;
    append_crc32c_le(&mut bytes);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_data()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
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
        if current.join("be").join("shards").is_dir() {
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
    let shards = data_dir.join("be").join("shards");
    if !shards.is_dir() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for entry in fs::read_dir(&shards)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if name.len() == 20 && name.bytes().all(|byte| byte.is_ascii_digit()) {
            ids.push(name.parse::<u64>().map_err(|error| {
                backup_error(format!("shard directory {name} is not a shard id: {error}"))
            })?);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

pub fn lsm_directory_is_locked(data_dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(data_dir.join("be").join("shards")) else {
        return false;
    };
    for entry in entries.flatten() {
        let lock_path = entry.path().join("lsm").join("LOCK");
        if !lock_path.is_file() {
            continue;
        }
        let Ok(file) = OpenOptions::new().read(true).write(true).open(&lock_path) else {
            return true;
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
    check_tree: &dyn Fn(&Path) -> Result<()>,
) -> Result<BackupSummary> {
    let hold = require_active_hold(data_dir)?;
    reject_destination(data_dir, destination)?;
    let mut staging = Staging::create(destination)?;
    let tree = staging.path.join(BACKUP_TREE_DIR);
    fs::create_dir_all(&tree)?;
    let mut files = Vec::new();
    let mut images = Vec::new();
    let mut covered = BTreeSet::new();
    for shard in shards {
        let image = backup_online_shard(&hold, shard, data_dir, &tree, &mut files)?;
        covered.insert(image.shard_id);
        images.push(image);
    }
    for shard_id in list_shard_ids(data_dir)? {
        if covered.contains(&shard_id) {
            continue;
        }
        let shard_dir = shard_dir(data_dir, shard_id);
        let image = backup_stopped_shard(&hold, shard_id, &shard_dir, &tree, &mut files)?;
        images.push(image);
    }
    copy_node_metadata(data_dir, &tree, &mut files)?;
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
    check_tree: &dyn Fn(&Path) -> Result<()>,
) -> Result<BackupSummary> {
    if lsm_directory_is_locked(data_dir) {
        return Err(backup_error(
            "the data directory is open by a running process; run the command again so that process copies the log under its lock",
        ));
    }
    let hold = require_active_hold(data_dir)?;
    reject_destination(data_dir, destination)?;
    let mut staging = Staging::create(destination)?;
    let tree = staging.path.join(BACKUP_TREE_DIR);
    fs::create_dir_all(&tree)?;
    let mut files = Vec::new();
    let mut images = Vec::new();
    let shard_ids = list_shard_ids(data_dir)?;
    if shard_ids.is_empty() {
        return Err(backup_error(
            "no shard directories under be/shards; refusing an empty backup",
        ));
    }
    for shard_id in shard_ids {
        let image = backup_stopped_shard(
            &hold,
            shard_id,
            &shard_dir(data_dir, shard_id),
            &tree,
            &mut files,
        )?;
        images.push(image);
    }
    copy_node_metadata(data_dir, &tree, &mut files)?;
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
    check_tree: &dyn Fn(&Path) -> Result<()>,
) -> Result<BackupSummary> {
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
    let tree = artifact.join(BACKUP_TREE_DIR);
    if !tree.is_dir() {
        return Err(restore_error("backup tree/ directory is missing"));
    }
    let mut seen = BTreeSet::new();
    for file in &manifest.files {
        if !seen.insert(file.path.clone()) {
            return Err(restore_error(format!(
                "manifest lists {} more than once",
                file.path
            )));
        }
        let relative = safe_relative(&file.path).map_err(restore_error)?;
        let source = tree.join(&relative);
        let bytes = fs::read(&source)
            .map_err(|error| restore_error(format!("missing {}: {error}", file.path)))?;
        if bytes.len() as u64 != file.len || checksum_crc32c(&bytes) != file.crc32c {
            return Err(restore_error(format!(
                "{} failed length or CRC32C check",
                file.path
            )));
        }
    }
    let discovered = walk_files(&tree)?;
    if discovered.len() != manifest.files.len() {
        return Err(restore_error(
            "backup tree contains files that are not listed in the manifest",
        ));
    }
    if destination.exists() {
        let mut entries = fs::read_dir(destination)?;
        if entries.next().is_some() {
            return Err(restore_error(format!(
                "destination {} is not empty",
                destination.display()
            )));
        }
    }
    let mut staging = Staging::create(destination)?;
    for file in &manifest.files {
        let relative = safe_relative(&file.path).map_err(restore_error)?;
        let bytes = fs::read(tree.join(&relative))?;
        write_new_bytes(&staging.path.join(&relative), &bytes)?;
    }
    // Open MANIFEST on the writable staging copy. The source artifact stays read-only.
    validate_manifest_tree(&staging.path, &manifest)?;
    check_tree(&staging.path).map_err(|error| restore_error(error.to_string()))?;
    sync_tree(&staging.path)?;
    if destination.exists() {
        for file in &manifest.files {
            let relative = safe_relative(&file.path).map_err(restore_error)?;
            let bytes = fs::read(staging.path.join(&relative))?;
            write_new_bytes(&destination.join(&relative), &bytes)?;
        }
        sync_tree(destination)?;
        staging.disarm();
        fs::remove_dir_all(&staging.path)?;
    } else {
        let parent = destination_parent(destination);
        sync_directory_and_ancestors(&parent)?;
        fs::rename(&staging.path, destination)?;
        staging.disarm();
        sync_dir(&parent)?;
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
        return read_record!(&running).map(Some);
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
    tree: &Path,
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
    // Freeze the LSM or snapshot image before copying the log. Commits that land
    // during the checkpoint are still in the log copy, and the hold blocks purge.
    let live_index = shard.engine.durable_checkpoint().flushed_raft_index;
    let (kind, image_index) = if live_index >= purged_now {
        // Hard-linked checkpoints must stay on the source filesystem.
        let temp = data_dir.join(format!(".backup-ckpt-{:020}", shard.shard_id));
        if temp.exists() {
            return Err(backup_error(format!(
                "checkpoint scratch {} already exists; refusing to delete it",
                temp.display()
            )));
        }
        let checkpoint = shard.engine.create_checkpoint(&temp).map_err(|error| {
            let _ = fs::remove_dir_all(&temp);
            backup_error(format!(
                "shard {} checkpoint failed: {error}",
                shard.shard_id
            ))
        })?;
        let lsm_rel = format!("be/shards/{:020}/lsm", shard.shard_id);
        let lsm_dest = tree.join(safe_relative(&lsm_rel).map_err(backup_error)?);
        copy_dir_bytes(&temp, &lsm_dest)?;
        let _ = fs::remove_dir_all(&temp);
        record_tree_files(&lsm_dest, &lsm_rel, files)?;
        (IMAGE_FLUSHED_LSM, checkpoint.flushed_raft_index)
    } else {
        (IMAGE_SNAPSHOT, 0)
    };
    copy_protected_snapshot(shard.raft_dir, floor, &relative_raft, tree, files)?;
    let dest_raft = tree.join(safe_relative(&relative_raft).map_err(backup_error)?);
    let inspection = shard
        .log_store
        .copy_durable_log(&dest_raft)
        .map_err(|error| {
            backup_error(format!("shard {} log copy failed: {error}", shard.shard_id))
        })?;
    record_log_files(&dest_raft, &relative_raft, files)?;
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
        shard.raft_dir,
        floor,
        &inspection,
        tree,
        files,
    )
}

fn backup_stopped_shard(
    hold: &BackupHoldRecord,
    shard_id: ShardId,
    shard_directory: &Path,
    tree: &Path,
    files: &mut Vec<BackupFileRecord>,
) -> Result<ShardImageRecord> {
    let raft_dir = shard_directory.join("raft");
    let floor = floor_for_shard(hold, shard_id)?;
    let purged = if raft_dir.join("HARD_STATE").is_file() {
        read_persisted_purged_index(&raft_dir)?
    } else {
        0
    };
    if purged > floor.max_purge_index {
        return Err(backup_error(format!(
            "shard {shard_id} purged past the hold floor {}",
            floor.max_purge_index
        )));
    }
    let relative_raft = format!("be/shards/{shard_id:020}/raft");
    let dest_raft = tree.join(safe_relative(&relative_raft).map_err(backup_error)?);
    copy_raft_files_from_disk(&raft_dir, &dest_raft)?;
    record_tree_files(&dest_raft, &relative_raft, files)?;
    let inspection = inspect_raft_dir(&dest_raft).map_err(|error| {
        backup_error(format!(
            "shard {shard_id} log is not a complete backup image: {error}"
        ))
    })?;
    copy_protected_snapshot(&raft_dir, floor, &relative_raft, tree, files)?;
    let lsm_dir = shard_directory.join("lsm");
    let live_index = if lsm_dir.join("CURRENT").is_file() {
        let (manifest, recovered) = ManifestLog::open_existing(&lsm_dir)?;
        drop(manifest);
        recovered.checkpoint.flushed_raft_index
    } else {
        0
    };
    if live_index >= inspection.last_purged_index && lsm_dir.join("CURRENT").is_file() {
        let lsm_rel = format!("be/shards/{shard_id:020}/lsm");
        let lsm_dest = tree.join(safe_relative(&lsm_rel).map_err(backup_error)?);
        copy_lsm_files(&lsm_dir, &lsm_dest)?;
        record_tree_files(&lsm_dest, &lsm_rel, files)?;
        let (manifest, recovered) = ManifestLog::open_existing(&lsm_dest)?;
        drop(manifest);
        return finish_image(
            shard_id,
            IMAGE_FLUSHED_LSM,
            recovered.checkpoint.flushed_raft_index,
            &inspection,
        );
    }
    image_from_snapshot(shard_id, &raft_dir, floor, &inspection, tree, files)
}

fn image_from_snapshot(
    shard_id: ShardId,
    raft_dir: &Path,
    floor: &ShardHoldFloor,
    inspection: &RaftLogInspection,
    tree: &Path,
    files: &mut Vec<BackupFileRecord>,
) -> Result<ShardImageRecord> {
    let Some(snapshot) = newest_snapshot_at_or_below(raft_dir, floor.retain_snapshots_through)
    else {
        return Err(backup_error(format!(
            "shard {shard_id} has no flushed LSM or complete snapshot covering purged index {}",
            inspection.last_purged_index
        )));
    };
    if snapshot.index < inspection.last_purged_index {
        return Err(backup_error(format!(
            "shard {shard_id} snapshot {} does not cover purged index {}",
            snapshot.index, inspection.last_purged_index
        )));
    }
    let lsm_rel = format!("be/shards/{shard_id:020}/lsm");
    let lsm_dest = tree.join(safe_relative(&lsm_rel).map_err(backup_error)?);
    if !lsm_dest.join("CURRENT").is_file() {
        copy_lsm_files(&snapshot.path.join("lsm"), &lsm_dest)?;
        record_tree_files(&lsm_dest, &lsm_rel, files)?;
    }
    finish_image(shard_id, IMAGE_SNAPSHOT, snapshot.index, inspection)
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
    raft_dir: &Path,
    floor: &ShardHoldFloor,
    relative_raft: &str,
    tree: &Path,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    let Some(snapshot) = newest_snapshot_at_or_below(raft_dir, floor.retain_snapshots_through)
    else {
        return Ok(());
    };
    let rel = format!("{relative_raft}/snapshots/snap_{:020}", snapshot.index);
    let dest = tree.join(safe_relative(&rel).map_err(backup_error)?);
    copy_dir_bytes(&snapshot.path, &dest)?;
    record_tree_files(&dest, &rel, files)
}

fn copy_node_metadata(
    data_dir: &Path,
    tree: &Path,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    let allocator = data_dir.join("global_allocator").join("ID_ALLOCATOR_STATE");
    copy_verified_frame(
        &allocator,
        &tree.join("global_allocator").join("ID_ALLOCATOR_STATE"),
        "global_allocator/ID_ALLOCATOR_STATE",
        files,
    )?;
    let registry = data_dir.join("be").join("NODE_SHARD_REGISTRY");
    copy_verified_frame(
        &registry,
        &tree.join("be").join("NODE_SHARD_REGISTRY"),
        "be/NODE_SHARD_REGISTRY",
        files,
    )?;
    let mut cells = 0usize;
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !name.starts_with("cell_") {
            continue;
        }
        let state = entry.path().join("CELL_MANAGER_STATE");
        if !state.is_file() {
            continue;
        }
        copy_verified_frame(
            &state,
            &tree.join(&name).join("CELL_MANAGER_STATE"),
            &format!("{name}/CELL_MANAGER_STATE"),
            files,
        )?;
        let owner = entry.path().join("CELL_OWNER");
        if owner.is_file() {
            copy_verified_frame(
                &owner,
                &tree.join(&name).join("CELL_OWNER"),
                &format!("{name}/CELL_OWNER"),
                files,
            )?;
        }
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
    check_tree: &dyn Fn(&Path) -> Result<()>,
) -> Result<BackupSummary> {
    let current = require_active_hold(data_dir)?;
    if current.hold_id != hold.hold_id {
        return Err(backup_error(
            "backup hold changed while the copy was running",
        ));
    }
    let copied: BTreeSet<ShardId> = shards.iter().map(|shard| shard.shard_id).collect();
    let tree = staging.join(BACKUP_TREE_DIR);
    ensure_registry_matches(&tree, &copied)?;
    for shard_id in list_shard_ids(data_dir)? {
        if !copied.contains(&shard_id) {
            return Err(backup_error(format!(
                "shard {shard_id} appeared during the backup and is not in the artifact"
            )));
        }
    }
    for floor in &current.floors {
        if !copied.contains(&floor.shard_id) && shard_dir(data_dir, floor.shard_id).is_dir() {
            return Err(backup_error(format!(
                "hold covers shard {} but that shard was not copied",
                floor.shard_id
            )));
        }
    }
    check_tree(&tree)?;
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
    fs::rename(staging, destination)?;
    staging_guard.disarm();
    sync_dir(&parent)?;
    Ok(BackupSummary {
        hold_id: manifest.hold_id,
        shard_count: manifest.shards.len(),
        file_count: manifest.files.len(),
        destination: destination.to_path_buf(),
    })
}

fn ensure_registry_matches(root: &Path, copied: &BTreeSet<ShardId>) -> Result<()> {
    let registry = root.join("be").join("NODE_SHARD_REGISTRY");
    let referenced = crate::group_host::shard_ids_in_registry_file(&registry)?;
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
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            sync_tree(&path)?;
        }
    }
    sync_dir(dir)?;
    Ok(())
}

fn validate_manifest_tree(tree: &Path, manifest: &BackupManifest) -> Result<()> {
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
    for shard in &manifest.shards {
        let prefix = format!("be/shards/{:020}", shard.shard_id);
        let raft = tree
            .join("be")
            .join("shards")
            .join(format!("{:020}", shard.shard_id))
            .join("raft");
        let inspection = inspect_raft_dir(&raft).map_err(|error| {
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
        let lsm = tree
            .join("be")
            .join("shards")
            .join(format!("{:020}", shard.shard_id))
            .join("lsm");
        let (opened, recovered) = ManifestLog::open_existing(&lsm).map_err(|error| {
            restore_error(format!(
                "shard {} LSM image is incomplete: {error}",
                shard.shard_id
            ))
        })?;
        drop(opened);
        if recovered.checkpoint.flushed_raft_index != shard.image_index {
            return Err(restore_error(format!(
                "shard {} LSM checkpoint {} does not match manifest image {}",
                shard.shard_id, recovered.checkpoint.flushed_raft_index, shard.image_index
            )));
        }
        for meta in recovered.live_files.values() {
            if !lsm.join(sst_file_name(meta.file_id)).is_file() {
                return Err(restore_error(format!(
                    "shard {} is missing SST {}",
                    shard.shard_id,
                    sst_file_name(meta.file_id)
                )));
            }
        }
        if shard.image_kind == IMAGE_SNAPSHOT {
            let snap = raft
                .join("snapshots")
                .join(format!("snap_{:020}", shard.image_index));
            if !snapshot_dir_is_complete(&snap) {
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

fn shard_dir(data_dir: &Path, shard_id: ShardId) -> PathBuf {
    data_dir
        .join("be")
        .join("shards")
        .join(format!("{shard_id:020}"))
}

fn copy_raft_files_from_disk(raft_dir: &Path, dest: &Path) -> Result<()> {
    fs::create_dir_all(dest)?;
    for name in ["HARD_STATE", "INITIALIZED"] {
        let source = raft_dir.join(name);
        if source.is_file() {
            let bytes = fs::read(&source)?;
            write_new_bytes(&dest.join(name), &bytes)?;
        }
    }
    if !dest.join("HARD_STATE").is_file() {
        return Err(backup_error(format!(
            "HARD_STATE is missing from {}",
            raft_dir.display()
        )));
    }
    for entry in fs::read_dir(raft_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(text) = name.to_str() else {
            continue;
        };
        if text.starts_with("segment_") && text.ends_with(".raftlog") {
            let bytes = fs::read(entry.path())?;
            write_new_bytes(&dest.join(text), &bytes)?;
        }
    }
    Ok(())
}

fn copy_lsm_files(source: &Path, dest: &Path) -> Result<()> {
    if !source.is_dir() {
        return Err(backup_error(format!(
            "LSM directory {} is missing",
            source.display()
        )));
    }
    fs::create_dir_all(dest)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(text) = name.to_str() else {
            continue;
        };
        if text == "LOCK" || text.ends_with(".tmp") {
            continue;
        }
        let path = entry.path();
        if path.is_file() {
            let bytes = fs::read(&path)?;
            write_new_bytes(&dest.join(text), &bytes)?;
        }
    }
    if !dest.join("CURRENT").is_file() {
        return Err(backup_error(format!(
            "CURRENT is missing from LSM image {}",
            source.display()
        )));
    }
    Ok(())
}

fn copy_dir_bytes(source: &Path, dest: &Path) -> Result<()> {
    fs::create_dir_all(dest)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let name = entry.file_name();
        let path = entry.path();
        let target = dest.join(&name);
        if path.is_dir() {
            copy_dir_bytes(&path, &target)?;
        } else if path.is_file() {
            let bytes = fs::read(&path)?;
            write_new_bytes(&target, &bytes)?;
        }
    }
    if let Some(parent) = dest.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn copy_verified_frame(
    source: &Path,
    dest: &Path,
    relative: &str,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    let mut last_error = String::from("missing");
    for _ in 0..8 {
        match fs::read(source) {
            Ok(bytes) => match verify_crc32c_le(&bytes, "backup source") {
                Ok(_) => {
                    write_new_bytes(dest, &bytes)?;
                    files.push(BackupFileRecord {
                        path: relative.replace('\\', "/"),
                        len: bytes.len() as u64,
                        crc32c: checksum_crc32c(&bytes),
                    });
                    return Ok(());
                }
                Err(error) => last_error = error.to_string(),
            },
            Err(error) => last_error = error.to_string(),
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err(backup_error(format!(
        "{relative} stayed unreadable while copying ({last_error})"
    )))
}

fn record_log_files(
    raft_dir: &Path,
    relative_prefix: &str,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    for name in ["HARD_STATE", "INITIALIZED"] {
        let path = raft_dir.join(name);
        if path.is_file() {
            record_one_file(&path, &format!("{relative_prefix}/{name}"), files)?;
        }
    }
    for entry in fs::read_dir(raft_dir)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if name.starts_with("segment_") && name.ends_with(".raftlog") {
            record_one_file(&entry.path(), &format!("{relative_prefix}/{name}"), files)?;
        }
    }
    Ok(())
}

fn record_one_file(path: &Path, relative: &str, files: &mut Vec<BackupFileRecord>) -> Result<()> {
    let bytes = fs::read(path)?;
    files.push(BackupFileRecord {
        path: relative.to_string(),
        len: bytes.len() as u64,
        crc32c: checksum_crc32c(&bytes),
    });
    Ok(())
}

fn record_tree_files(
    root: &Path,
    relative_prefix: &str,
    files: &mut Vec<BackupFileRecord>,
) -> Result<()> {
    let discovered = walk_files(root)?;
    for path in discovered {
        let suffix = path.strip_prefix(root).map_err(|error| {
            backup_error(format!(
                "backup path {} escaped the tree: {error}",
                path.display()
            ))
        })?;
        let mut relative = relative_prefix.trim_end_matches('/').to_string();
        for component in suffix.components() {
            relative.push('/');
            relative.push_str(&component.as_os_str().to_string_lossy());
        }
        let bytes = fs::read(&path)?;
        files.push(BackupFileRecord {
            path: relative,
            len: bytes.len() as u64,
            crc32c: checksum_crc32c(&bytes),
        });
    }
    Ok(())
}

fn walk_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    walk_files_inner(root, &mut out)?;
    out.sort();
    Ok(out)
}

fn walk_files_inner(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_files_inner(&path, out)?;
        } else if path.is_file() {
            out.push(path);
        }
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

fn write_new_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_data()?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn read_manifest(artifact: &Path) -> Result<BackupManifest> {
    let path = artifact.join(BACKUP_MANIFEST_FILE);
    if !path.is_file() {
        return Err(restore_error("BACKUP_MANIFEST is missing"));
    }
    read_record!(&path).map_err(|error| restore_error(error.to_string()))
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
    fn create(destination: &Path) -> Result<Self> {
        let parent = destination_parent(destination);
        fs::create_dir_all(&parent)?;
        let name = destination
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "backup".to_string());
        let path = parent.join(format!(".{name}.partial"));
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
            let _ = fs::remove_dir_all(&self.path);
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
        let error = ensure_registry_matches(dir.path(), &copied).unwrap_err();
        assert!(error.to_string().contains("shard 99"), "{error}");
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
}
