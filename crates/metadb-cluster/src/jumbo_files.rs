//! Move one local Jumbo shard directory onto a dedicated disk root.
//!
//! This module does not open a socket. A loopback placement onto another
//! listener's disk is [`crate::jumbo_remote`]. The dedicated node for this
//! function must be this process. Peer replicas are left where they are.
//! Automatic leadership transfer is not involved.
//!
//! A symlink anywhere in the source tree, the destination, or the dedicated
//! root is refused. Linux is the supported platform.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use serde::{Deserialize, Serialize};

use metadb_core::codec;
use metadb_core::{append_crc32c_le, verify_crc32c_le, MetaDbError, Result, ShardId};
use metadb_storage::sstable::sync_dir;

pub const JUMBO_MOVE_FILE: &str = "JUMBO_FILE_MOVE";
/// Written only after the `MOVE_LIVE` directory fsync succeeds.
pub const JUMBO_LIVE_DURABLE_FILE: &str = "JUMBO_LIVE_DURABLE";
const MOVE_VERSION: u32 = 1;
pub const MOVE_STAGED: u32 = 1;
pub const MOVE_LIVE: u32 = 2;

static FAIL_LIVE_DIRSYNC: AtomicU32 = AtomicU32::new(0);
static FAIL_LIVE_DIRSYNC_AT: std::sync::Mutex<Vec<(std::path::PathBuf, u32)>> =
    std::sync::Mutex::new(Vec::new());

/// The next `n` directory fsyncs after a successful `MOVE_LIVE` rename fail.
/// The renamed file stays visible.
pub fn testing_fail_next_live_dirsync(n: u32) {
    FAIL_LIVE_DIRSYNC.store(n, Ordering::Relaxed);
}

/// The next `n` `MOVE_LIVE` directory fsyncs for this destination fail.
/// The renamed `MOVE_LIVE` file stays visible. Other directories are not affected.
pub fn testing_fail_live_dirsync_at(dest: &Path, n: u32) {
    let mut pending = FAIL_LIVE_DIRSYNC_AT.lock().unwrap();
    if let Some((_, left)) = pending.iter_mut().find(|(path, _)| path == dest) {
        *left = n;
    } else {
        pending.push((dest.to_path_buf(), n));
    }
}

/// Why [`write_move_phase`] failed.
#[derive(Debug)]
pub enum MovePhaseError {
    /// The phase name is not visible. A staged copy was not published.
    NotPublished(String),
    /// `MOVE_LIVE` was renamed into place. The following directory fsync failed.
    LiveVisibleDirSyncFailed(String),
}

impl std::fmt::Display for MovePhaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPublished(message) | Self::LiveVisibleDirSyncFailed(message) => {
                formatter.write_str(message)
            }
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct MovedFile {
    pub relative_path: String,
    pub len: u64,
    pub crc32c: u32,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
struct MoveRecord {
    version: u32,
    shard_id: u64,
    phase: u32,
    files: Vec<MovedFile>,
}

pub fn shard_move_dir(dedicated_root: &Path, shard_id: ShardId) -> PathBuf {
    dedicated_root
        .join("shards")
        .join(format!("{shard_id:020}"))
}

/// The dedicated root must be a real directory, not a symlink, and not the
/// shared `be` tree or a directory inside it.
pub fn refuse_dedicated_root(data_dir: &Path, dedicated_root: &Path) -> Result<()> {
    if dedicated_root.as_os_str().is_empty() {
        return Err(MetaDbError::ShardRoutingError(
            "jumbo dedicated root is empty; shard files were not moved".to_string(),
        ));
    }
    refuse_symlink(dedicated_root)?;
    if !dedicated_root.exists() {
        return Err(MetaDbError::ShardRoutingError(format!(
            "jumbo dedicated root {} does not exist; shard files were not moved",
            dedicated_root.display()
        )));
    }
    let root_meta = fs::symlink_metadata(dedicated_root)?;
    if !root_meta.is_dir() {
        return Err(MetaDbError::ShardRoutingError(format!(
            "jumbo dedicated root {} is not a directory; shard files were not moved",
            dedicated_root.display()
        )));
    }
    let root = fs::canonicalize(dedicated_root)?;
    let data = fs::canonicalize(data_dir)?;
    if root == data || root.starts_with(&data) || data.starts_with(&root) {
        return Err(MetaDbError::ShardRoutingError(format!(
            "jumbo dedicated root {} is the shared data directory {}; shard files were not moved",
            root.display(),
            data.display()
        )));
    }
    Ok(())
}

pub fn load_live_dir(dedicated_root: &Path, shard_id: ShardId) -> Result<Option<PathBuf>> {
    let dest = shard_move_dir(dedicated_root, shard_id);
    let Some(record) = read_record(&dest)? else {
        return Ok(None);
    };
    if record.shard_id != shard_id || record.phase != MOVE_LIVE {
        return Ok(None);
    }
    // Checksums were required before the group reopened. After that, Raft and
    // the LSM append. A later open only rejects a missing file or a symlink.
    ensure_live_files_present(&dest, &record.files)?;
    Ok(Some(dest))
}

/// `MOVE_LIVE` is visible and the destination directory fsync finished.
///
/// A visible rename without this marker is not a completed live cut.
pub fn live_dirsync_is_durable(dest: &Path) -> Result<bool> {
    let path = dest.join(JUMBO_LIVE_DURABLE_FILE);
    if !path.exists() {
        return Ok(false);
    }
    refuse_symlink(&path)?;
    let mut raw = Vec::new();
    File::open(&path)?.read_to_end(&mut raw)?;
    let body = verify_crc32c_le(&raw, JUMBO_LIVE_DURABLE_FILE)?;
    if body.len() != 8 {
        return Err(MetaDbError::CodecError(format!(
            "{JUMBO_LIVE_DURABLE_FILE} is the wrong length"
        )));
    }
    let recorded = ShardId::from_le_bytes(body.try_into().unwrap());
    let Some(record) = read_record(dest)? else {
        return Ok(false);
    };
    Ok(record.phase == MOVE_LIVE && record.shard_id == recorded)
}

fn ensure_live_files_present(dest: &Path, files: &[MovedFile]) -> Result<()> {
    for file in files {
        if file
            .relative_path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo move record has an invalid path {}",
                file.relative_path
            )));
        }
        let path = file
            .relative_path
            .split('/')
            .fold(dest.to_path_buf(), |acc, part| acc.join(part));
        refuse_symlink(&path)?;
        if !fs::metadata(&path)?.is_file() {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo file {} from the verified copy is missing",
                path.display()
            )));
        }
    }
    Ok(())
}

pub fn copy_shard_tree(source: &Path, dest: &Path) -> Result<Vec<MovedFile>> {
    refuse_symlink(source)?;
    if dest.exists() {
        return Err(MetaDbError::StorageIoError(format!(
            "jumbo destination {} already exists; the source was left in place",
            dest.display()
        )));
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
        refuse_symlink(parent)?;
    }
    let mut files = Vec::new();
    copy_dir(source, dest, "", &mut files)?;
    sync_tree(dest)?;
    // Reading the copied Raft segment can observe one last appended frame.
    // Hash until two passes agree, then require the following read to match.
    pre_read(dest, &files)?;
    settle_copied_files(dest, &mut files)?;
    verify_tree(dest, &files)?;
    Ok(files)
}

fn pre_read(dest: &Path, files: &[MovedFile]) -> Result<()> {
    for file in files {
        let path = file
            .relative_path
            .split('/')
            .fold(dest.to_path_buf(), |acc, part| acc.join(part));
        let _ = hash_file(&path)?;
    }
    Ok(())
}

fn settle_copied_files(dest: &Path, files: &mut [MovedFile]) -> Result<()> {
    std::thread::sleep(std::time::Duration::from_millis(200));
    for _ in 0..25 {
        std::thread::sleep(std::time::Duration::from_millis(20));
        let mut changed = false;
        for file in files.iter_mut() {
            let path = file
                .relative_path
                .split('/')
                .fold(dest.to_path_buf(), |acc, part| acc.join(part));
            let (len, crc) = hash_file(&path)?;
            if len != file.len || crc != file.crc32c {
                file.len = len;
                file.crc32c = crc;
                changed = true;
            }
        }
        if !changed {
            return Ok(());
        }
    }
    Err(MetaDbError::StorageIoError(
        "jumbo shard files kept changing after the copy; the source was left in place".to_string(),
    ))
}

pub fn write_move_phase(
    dest: &Path,
    shard_id: ShardId,
    phase: u32,
    files: &[MovedFile],
) -> std::result::Result<(), MovePhaseError> {
    let record = MoveRecord {
        version: MOVE_VERSION,
        shard_id,
        phase,
        files: files.to_vec(),
    };
    let mut raw = codec::serialize(&record)
        .map_err(|error| MovePhaseError::NotPublished(format!("{JUMBO_MOVE_FILE}: {error}")))?;
    append_crc32c_le(&mut raw);
    let path = dest.join(JUMBO_MOVE_FILE);
    refuse_symlink_allow_missing(&path)
        .map_err(|error| MovePhaseError::NotPublished(error.to_string()))?;
    let tmp = dest.join(format!("{JUMBO_MOVE_FILE}.tmp"));
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|error| MovePhaseError::NotPublished(error.to_string()))?;
        file.write_all(&raw)
            .map_err(|error| MovePhaseError::NotPublished(error.to_string()))?;
        file.sync_data()
            .map_err(|error| MovePhaseError::NotPublished(error.to_string()))?;
    }
    fs::rename(&tmp, &path).map_err(|error| {
        MovePhaseError::NotPublished(format!(
            "{JUMBO_MOVE_FILE} rename failed and the phase is not visible: {error}"
        ))
    })?;
    // The new name is visible. A directory fsync failure must not hide that,
    // and it must not mark the live cut durable.
    if let Err(error) = sync_published_dir(dest, phase) {
        if phase == MOVE_LIVE {
            return Err(MovePhaseError::LiveVisibleDirSyncFailed(error));
        }
        return Err(MovePhaseError::NotPublished(error));
    }
    if phase == MOVE_LIVE {
        if let Err(error) = mark_live_dirsync_durable(dest, shard_id) {
            return Err(MovePhaseError::LiveVisibleDirSyncFailed(format!(
                "{JUMBO_MOVE_FILE} is visible but the durable directory marker was not written: {error}"
            )));
        }
    }
    Ok(())
}

/// Retries the directory fsync for a `MOVE_LIVE` file that is already visible.
///
/// Success writes [`JUMBO_LIVE_DURABLE_FILE`]. Failure leaves the rename in place
/// and does not create that marker.
pub fn seal_live_dirsync(
    dest: &Path,
    shard_id: ShardId,
) -> std::result::Result<(), MovePhaseError> {
    let visible =
        read_record(dest).map_err(|error| MovePhaseError::NotPublished(error.to_string()))?;
    let Some(record) = visible else {
        return Err(MovePhaseError::NotPublished(format!(
            "{JUMBO_MOVE_FILE} is not visible; the live cut was not sealed"
        )));
    };
    if record.phase != MOVE_LIVE || record.shard_id != shard_id {
        return Err(MovePhaseError::NotPublished(format!(
            "{JUMBO_MOVE_FILE} is not a live record for shard {shard_id}"
        )));
    }
    if let Err(error) = sync_published_dir(dest, MOVE_LIVE) {
        return Err(MovePhaseError::LiveVisibleDirSyncFailed(error));
    }
    mark_live_dirsync_durable(dest, shard_id).map_err(|error| {
        MovePhaseError::LiveVisibleDirSyncFailed(format!(
            "{JUMBO_MOVE_FILE} is visible but the durable directory marker was not written: {error}"
        ))
    })
}

fn mark_live_dirsync_durable(dest: &Path, shard_id: ShardId) -> Result<()> {
    let mut raw = shard_id.to_le_bytes().to_vec();
    append_crc32c_le(&mut raw);
    let path = dest.join(JUMBO_LIVE_DURABLE_FILE);
    refuse_symlink_allow_missing(&path)?;
    let tmp = dest.join(format!("{JUMBO_LIVE_DURABLE_FILE}.tmp"));
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(&raw)?;
        file.sync_data()?;
    }
    fs::rename(&tmp, &path)?;
    sync_dir(dest)?;
    Ok(())
}

fn consume_path_dirsync_failure(dest: &Path) -> bool {
    let mut pending = FAIL_LIVE_DIRSYNC_AT.lock().unwrap();
    let Some(index) = pending.iter().position(|(path, _)| path == dest) else {
        return false;
    };
    let left = pending[index].1;
    if left <= 1 {
        pending.remove(index);
    } else {
        pending[index].1 = left - 1;
    }
    true
}

fn sync_published_dir(dest: &Path, phase: u32) -> std::result::Result<(), String> {
    let attempts = if phase == MOVE_LIVE { 2 } else { 1 };
    let mut last = String::new();
    for _ in 0..attempts {
        if phase == MOVE_LIVE && FAIL_LIVE_DIRSYNC.load(Ordering::Relaxed) > 0 {
            FAIL_LIVE_DIRSYNC.fetch_sub(1, Ordering::Relaxed);
            last = format!("{JUMBO_MOVE_FILE} is visible but the directory fsync failed");
            continue;
        }
        if phase == MOVE_LIVE && consume_path_dirsync_failure(dest) {
            last = format!("{JUMBO_MOVE_FILE} is visible but the directory fsync failed");
            continue;
        }
        match sync_dir(dest) {
            Ok(()) => return Ok(()),
            Err(error) => last = error.to_string(),
        }
    }
    Err(last)
}

pub fn remove_tree(path: &Path) -> Result<()> {
    if !path.exists() && fs::symlink_metadata(path).is_err() {
        return Ok(());
    }
    remove_nofollow(path)
}

fn copy_dir(source: &Path, dest: &Path, prefix: &str, files: &mut Vec<MovedFile>) -> Result<()> {
    refuse_symlink(source)?;
    fs::create_dir_all(dest)?;
    refuse_symlink(dest)?;
    let mut names: Vec<_> = fs::read_dir(source)?
        .map(|entry| entry.map(|item| item.file_name()))
        .collect::<std::io::Result<Vec<_>>>()?;
    names.sort();
    for name in names {
        let name = name.to_str().ok_or_else(|| {
            MetaDbError::StorageIoError(format!(
                "jumbo shard file name in {} is not utf-8; the source was left in place",
                source.display()
            ))
        })?;
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
        let to = dest.join(name);
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
            copy_dir(&from, &to, &relative, files)?;
        } else if meta.is_file() {
            let (len, crc) = copy_file(&from, &to)?;
            files.push(MovedFile {
                relative_path: relative,
                len,
                crc32c: crc,
            });
        } else {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo shard entry {} is not a regular file or directory; the source was left in place",
                from.display()
            )));
        }
    }
    Ok(())
}

fn copy_file(from: &Path, to: &Path) -> Result<(u64, u32)> {
    refuse_symlink(from)?;
    let mut input = File::open(from)?;
    let mut output = OpenOptions::new().create_new(true).write(true).open(to)?;
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    let mut buf = [0u8; 64 * 1024];
    let mut len = 0u64;
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        output.write_all(&buf[..n])?;
        digest.update(&buf[..n]);
        len += n as u64;
    }
    output.sync_data()?;
    drop(output);
    let (reread_len, reread_crc) = hash_file(to)?;
    let crc = digest.finalize() as u32;
    if reread_len != len || reread_crc != crc {
        return Err(MetaDbError::StorageIoError(format!(
            "jumbo file {} changed during copy (wrote {len} crc {crc:08x}, reread {reread_len} crc {reread_crc:08x})",
            to.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let src_ino = fs::metadata(from)?.ino();
        let dst_ino = fs::metadata(to)?.ino();
        if src_ino == dst_ino {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo copy {} and {} are the same inode; the source was left in place",
                from.display(),
                to.display()
            )));
        }
    }
    Ok((len, digest.finalize() as u32))
}

fn hash_file(path: &Path) -> Result<(u64, u32)> {
    refuse_symlink(path)?;
    let mut input = File::open(path)?;
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    let mut buf = [0u8; 64 * 1024];
    let mut len = 0u64;
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        digest.update(&buf[..n]);
        len += n as u64;
    }
    Ok((len, digest.finalize() as u32))
}

fn verify_tree(dest: &Path, files: &[MovedFile]) -> Result<()> {
    for file in files {
        if file
            .relative_path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo move record has an invalid path {}",
                file.relative_path
            )));
        }
        let path = file
            .relative_path
            .split('/')
            .fold(dest.to_path_buf(), |acc, part| acc.join(part));
        let (len, crc) = hash_file(&path)?;
        if len != file.len || crc != file.crc32c {
            return Err(MetaDbError::StorageIoError(format!(
                "jumbo file {} failed verification (copied {} bytes crc {:08x}, reread {len} bytes crc {crc:08x}); the dedicated copy was not published",
                path.display(),
                file.len,
                file.crc32c
            )));
        }
    }
    Ok(())
}

fn sync_tree(dir: &Path) -> Result<()> {
    refuse_symlink(dir)?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        refuse_symlink(&path)?;
        let meta = fs::symlink_metadata(&path)?;
        if meta.is_dir() {
            sync_tree(&path)?;
        }
    }
    sync_dir(dir)
}

/// Phase byte of a visible `JUMBO_FILE_MOVE`, if the file names this shard.
///
/// `MOVE_STAGED` and `MOVE_LIVE` both block leadership transfer. A missing file
/// is `Ok(None)`. A record for a different shard is an error so the caller
/// fails closed instead of treating the directory as idle.
pub fn visible_move_phase(dest: &Path, shard_id: ShardId) -> Result<Option<u32>> {
    let Some(record) = read_record(dest)? else {
        return Ok(None);
    };
    if record.shard_id != shard_id {
        return Err(MetaDbError::CodecError(format!(
            "{JUMBO_MOVE_FILE} names shard {} and not {shard_id}",
            record.shard_id
        )));
    }
    Ok(Some(record.phase))
}

fn read_record(dest: &Path) -> Result<Option<MoveRecord>> {
    let path = dest.join(JUMBO_MOVE_FILE);
    if !path.exists() {
        return Ok(None);
    }
    refuse_symlink(&path)?;
    let mut raw = Vec::new();
    File::open(&path)?.read_to_end(&mut raw)?;
    let body = verify_crc32c_le(&raw, JUMBO_MOVE_FILE)?;
    let record: MoveRecord =
        wincode::config::deserialize_exact(body, wincode::config::DefaultConfig::default())
            .map_err(|error| MetaDbError::CodecError(format!("{JUMBO_MOVE_FILE}: {error}")))?;
    if record.version != MOVE_VERSION {
        return Err(MetaDbError::CodecError(format!(
            "{JUMBO_MOVE_FILE} version {} is not supported",
            record.version
        )));
    }
    Ok(Some(record))
}

fn remove_nofollow(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Err(MetaDbError::StorageIoError(format!(
            "refusing to follow symlink {}",
            path.display()
        )));
    }
    if meta.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            remove_nofollow(&entry.path())?;
        }
        fs::remove_dir(path)?;
    } else if meta.is_file() {
        fs::remove_file(path)?;
    } else {
        return Err(MetaDbError::StorageIoError(format!(
            "refusing to remove non-regular entry {}",
            path.display()
        )));
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use metadb_core::checksum_crc32c;

    #[test]
    fn digest_matches_the_codec_crc32c() {
        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
        digest.update(b"123456789");
        assert_eq!(digest.finalize() as u32, checksum_crc32c(b"123456789"));
    }

    #[test]
    fn copy_verifies_bytes_and_refuses_a_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let dest = dir.path().join("dest");
        fs::create_dir_all(source.join("raft")).unwrap();
        fs::write(source.join("raft").join("log"), b"shard-bytes").unwrap();
        let files = copy_shard_tree(&source, &dest).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(
            fs::read(dest.join("raft").join("log")).unwrap(),
            b"shard-bytes"
        );
        write_move_phase(&dest, 7, MOVE_LIVE, &files).unwrap();
        let root = dir.path().join("disk");
        fs::create_dir_all(root.join("shards")).unwrap();
        fs::rename(&dest, shard_move_dir(&root, 7)).unwrap();
        assert!(load_live_dir(&root, 7).unwrap().is_some());

        let linked = dir.path().join("linked");
        std::os::unix::fs::symlink(&source, &linked).unwrap();
        let err = copy_shard_tree(&linked, &dir.path().join("nope")).unwrap_err();
        assert!(err.to_string().contains("symlink"), "{err}");
    }

    #[test]
    fn dedicated_root_must_be_outside_the_data_dir() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("be");
        let disk = dir.path().join("disk");
        fs::create_dir_all(&data).unwrap();
        fs::create_dir_all(&disk).unwrap();
        assert!(refuse_dedicated_root(&data, &disk).is_ok());
        assert!(refuse_dedicated_root(&data, &data).is_err());
        let inside = data.join("other");
        fs::create_dir_all(&inside).unwrap();
        assert!(refuse_dedicated_root(&data, &inside).is_err());
    }
}
