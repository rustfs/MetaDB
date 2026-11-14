//! Atomic `MANIFEST` edit log, `CURRENT` pointer rotation, and strict crash recovery.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use metadb_core::{checksum_crc32c, HlcTimestamp, MetaDbError, Result};

use crate::sstable::{sst_file_name, sync_dir, SstFileMeta};

const EDIT_FORMAT_VERSION: u8 = 1;

/// Durable Raft + shard state checkpoint persisted atomically inside `MANIFEST` (`Resolution B5`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableShardCheckpoint {
    pub flushed_raft_term: u64,
    pub flushed_raft_leader: u64,
    pub flushed_raft_index: u64,
    pub flushed_max_hlc: HlcTimestamp,
    pub contiguous_aux_acked_index: u64,
    pub membership_bytes: Vec<u8>,
    pub descriptor_bytes: Vec<u8>,
}

/// Atomic state transition appended to `MANIFEST-<seq:06>`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionEdit {
    pub next_file_id: Option<u64>,
    pub next_flush_generation: Option<u64>,
    pub checkpoint: Option<DurableShardCheckpoint>,
    pub deleted_files: Vec<(u8, u64)>,
    pub added_files: Vec<SstFileMeta>,
}

impl VersionEdit {
    /// Encodes `VersionEdit` using the exact length-framed binary grammar (`storage_lsm_mvcc.md` §7.3).
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        buf.push(EDIT_FORMAT_VERSION);

        match self.next_file_id {
            Some(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            None => buf.push(0),
        }

        match self.next_flush_generation {
            Some(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            None => buf.push(0),
        }

        match &self.checkpoint {
            Some(cp) => {
                buf.push(1);
                buf.extend_from_slice(&cp.flushed_raft_term.to_le_bytes());
                buf.extend_from_slice(&cp.flushed_raft_leader.to_le_bytes());
                buf.extend_from_slice(&cp.flushed_raft_index.to_le_bytes());
                buf.extend_from_slice(&cp.flushed_max_hlc.0.to_le_bytes());
                buf.extend_from_slice(&cp.contiguous_aux_acked_index.to_le_bytes());
                buf.extend_from_slice(&(cp.membership_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&cp.membership_bytes);
                buf.extend_from_slice(&(cp.descriptor_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&cp.descriptor_bytes);
            }
            None => buf.push(0),
        }

        buf.extend_from_slice(&(self.deleted_files.len() as u32).to_le_bytes());
        for &(level, file_id) in &self.deleted_files {
            buf.push(level);
            buf.extend_from_slice(&file_id.to_le_bytes());
        }

        buf.extend_from_slice(&(self.added_files.len() as u32).to_le_bytes());
        for meta in &self.added_files {
            buf.push(meta.level);
            buf.extend_from_slice(&meta.file_id.to_le_bytes());
            buf.extend_from_slice(&meta.flush_generation.to_le_bytes());
            buf.extend_from_slice(&meta.file_size.to_le_bytes());
            buf.extend_from_slice(&(meta.min_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&meta.min_key);
            buf.extend_from_slice(&(meta.max_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&meta.max_key);
            buf.extend_from_slice(&meta.num_entries.to_le_bytes());
            buf.extend_from_slice(&meta.num_tombstones.to_le_bytes());
            buf.extend_from_slice(&meta.max_raft_index.to_le_bytes());
            buf.extend_from_slice(&meta.max_hlc.0.to_le_bytes());
        }
        buf
    }

    pub fn decode_payload(raw: &[u8]) -> Result<Self> {
        let mut pos = 0usize;
        let read_u8 = |p: &mut usize| -> Result<u8> {
            if *p >= raw.len() {
                return Err(MetaDbError::ManifestCorrupted(
                    "unexpected EOF in VersionEdit".to_string(),
                ));
            }
            let v = raw[*p];
            *p += 1;
            Ok(v)
        };
        let read_u32 = |p: &mut usize| -> Result<u32> {
            if *p + 4 > raw.len() {
                return Err(MetaDbError::ManifestCorrupted(
                    "unexpected EOF reading u32 in VersionEdit".to_string(),
                ));
            }
            let v = u32::from_le_bytes(raw[*p..*p + 4].try_into().unwrap());
            *p += 4;
            Ok(v)
        };
        let read_u64 = |p: &mut usize| -> Result<u64> {
            if *p + 8 > raw.len() {
                return Err(MetaDbError::ManifestCorrupted(
                    "unexpected EOF reading u64 in VersionEdit".to_string(),
                ));
            }
            let v = u64::from_le_bytes(raw[*p..*p + 8].try_into().unwrap());
            *p += 8;
            Ok(v)
        };
        let read_vec = |p: &mut usize| -> Result<Vec<u8>> {
            let len = read_u32(p)? as usize;
            if *p + len > raw.len() {
                return Err(MetaDbError::ManifestCorrupted(
                    "unexpected EOF reading bytes in VersionEdit".to_string(),
                ));
            }
            let v = raw[*p..*p + len].to_vec();
            *p += len;
            Ok(v)
        };

        let ver = read_u8(&mut pos)?;
        if ver != EDIT_FORMAT_VERSION {
            return Err(MetaDbError::ManifestCorrupted(format!(
                "unsupported VersionEdit version {}",
                ver
            )));
        }

        let next_file_id = match read_u8(&mut pos)? {
            0 => None,
            1 => Some(read_u64(&mut pos)?),
            other => {
                return Err(MetaDbError::ManifestCorrupted(format!(
                    "invalid has_next_file_id tag {}",
                    other
                )))
            }
        };

        let next_flush_generation = match read_u8(&mut pos)? {
            0 => None,
            1 => Some(read_u64(&mut pos)?),
            other => {
                return Err(MetaDbError::ManifestCorrupted(format!(
                    "invalid has_next_flush_gen tag {}",
                    other
                )))
            }
        };

        let checkpoint = match read_u8(&mut pos)? {
            0 => None,
            1 => {
                let flushed_raft_term = read_u64(&mut pos)?;
                let flushed_raft_leader = read_u64(&mut pos)?;
                let flushed_raft_index = read_u64(&mut pos)?;
                let flushed_max_hlc = HlcTimestamp(read_u64(&mut pos)?);
                let contiguous_aux_acked_index = read_u64(&mut pos)?;
                let membership_bytes = read_vec(&mut pos)?;
                let descriptor_bytes = read_vec(&mut pos)?;
                Some(DurableShardCheckpoint {
                    flushed_raft_term,
                    flushed_raft_leader,
                    flushed_raft_index,
                    flushed_max_hlc,
                    contiguous_aux_acked_index,
                    membership_bytes,
                    descriptor_bytes,
                })
            }
            other => {
                return Err(MetaDbError::ManifestCorrupted(format!(
                    "invalid has_checkpoint tag {}",
                    other
                )))
            }
        };

        let deleted_count = read_u32(&mut pos)? as usize;
        let mut deleted_files = Vec::with_capacity(deleted_count);
        for _ in 0..deleted_count {
            let level = read_u8(&mut pos)?;
            let file_id = read_u64(&mut pos)?;
            deleted_files.push((level, file_id));
        }

        let added_count = read_u32(&mut pos)? as usize;
        let mut added_files = Vec::with_capacity(added_count);
        for _ in 0..added_count {
            let level = read_u8(&mut pos)?;
            let file_id = read_u64(&mut pos)?;
            let flush_generation = read_u64(&mut pos)?;
            let file_size = read_u64(&mut pos)?;
            let min_key = read_vec(&mut pos)?;
            let max_key = read_vec(&mut pos)?;
            let num_entries = read_u64(&mut pos)?;
            let num_tombstones = read_u64(&mut pos)?;
            let max_raft_index = read_u64(&mut pos)?;
            let max_hlc = HlcTimestamp(read_u64(&mut pos)?);
            added_files.push(SstFileMeta {
                level,
                file_id,
                flush_generation,
                file_size,
                min_key,
                max_key,
                num_entries,
                num_tombstones,
                max_raft_index,
                max_hlc,
            });
        }

        if pos != raw.len() {
            return Err(MetaDbError::ManifestCorrupted(
                "trailing bytes inside VersionEdit record".to_string(),
            ));
        }

        Ok(Self {
            next_file_id,
            next_flush_generation,
            checkpoint,
            deleted_files,
            added_files,
        })
    }
}

/// Reconstructed state after replaying `MANIFEST`.
#[derive(Debug, Clone)]
pub struct RecoveredManifestState {
    pub next_file_id: u64,
    pub next_flush_generation: u64,
    pub checkpoint: DurableShardCheckpoint,
    pub live_files: BTreeMap<(u8, u64), SstFileMeta>,
}

/// Append-only `MANIFEST-<seq:06>` log with `CURRENT` atomic pointer rotation.
pub struct ManifestLog {
    lsm_dir: PathBuf,
    manifest_seq: u64,
    manifest_path: PathBuf,
    file: File,
}

impl ManifestLog {
    /// Opens an existing `lsm_dir` (replaying `CURRENT` + cleaning `.sst.tmp` / orphan `.sst`s)
    /// or initializes a fresh `MANIFEST-000001`.
    pub fn open_or_create(lsm_dir: &Path) -> Result<(Self, RecoveredManifestState)> {
        fs::create_dir_all(lsm_dir)?;
        let current_path = lsm_dir.join("CURRENT");

        if !current_path.exists() {
            let seq = 1u64;
            let manifest_name = format!("MANIFEST-{:06}", seq);
            let manifest_path = lsm_dir.join(&manifest_name);
            let mut f = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&manifest_path)?;
            let init_edit = VersionEdit {
                next_file_id: Some(1),
                next_flush_generation: Some(1),
                checkpoint: Some(DurableShardCheckpoint::default()),
                deleted_files: vec![],
                added_files: vec![],
            };
            Self::write_record(&mut f, &init_edit)?;
            Self::write_current_atomic(lsm_dir, &manifest_name)?;
            Self::cleanup_orphan_files(lsm_dir, &BTreeMap::new(), &manifest_name)?;
            return Ok((
                Self {
                    lsm_dir: lsm_dir.to_path_buf(),
                    manifest_seq: seq,
                    manifest_path,
                    file: f,
                },
                RecoveredManifestState {
                    next_file_id: 1,
                    next_flush_generation: 1,
                    checkpoint: DurableShardCheckpoint::default(),
                    live_files: BTreeMap::new(),
                },
            ));
        }

        let manifest_name = fs::read_to_string(&current_path)?.trim().to_string();
        let seq = manifest_name
            .strip_prefix("MANIFEST-")
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| {
                MetaDbError::ManifestCorrupted(format!(
                    "invalid CURRENT content '{}'",
                    manifest_name
                ))
            })?;
        let manifest_path = lsm_dir.join(&manifest_name);

        let mut raw = Vec::new();
        File::open(&manifest_path)?.read_to_end(&mut raw)?;

        let mut next_file_id = 1u64;
        let mut next_flush_generation = 1u64;
        let mut checkpoint = DurableShardCheckpoint::default();
        let mut live_files: BTreeMap<(u8, u64), SstFileMeta> = BTreeMap::new();

        let mut offset = 0usize;
        let mut truncate_to: Option<usize> = None;

        while offset < raw.len() {
            // Only an incomplete/torn final record at EOF may be truncated (Resolution B3).
            if offset + 8 > raw.len() {
                truncate_to = Some(offset);
                break;
            }
            let payload_len =
                u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(raw[offset + 4..offset + 8].try_into().unwrap());
            let record_end = offset.saturating_add(8).saturating_add(payload_len);

            if record_end > raw.len() {
                // Torn final record at EOF!
                truncate_to = Some(offset);
                break;
            }

            let payload = &raw[offset + 8..record_end];
            let actual_crc = checksum_crc32c(payload);
            if actual_crc != expected_crc {
                // A bad CRC on the final record of this MANIFEST is treated as a torn
                // tail write. A bad CRC with more bytes after it is real corruption.
                if record_end == raw.len() {
                    truncate_to = Some(offset);
                    break;
                }
                return Err(MetaDbError::ManifestCorrupted(format!(
                    "MANIFEST CRC32C mismatch at offset {} (expected 0x{:08x}, got 0x{:08x})",
                    offset, expected_crc, actual_crc
                )));
            }

            let edit = VersionEdit::decode_payload(payload)?;
            if let Some(id) = edit.next_file_id {
                next_file_id = next_file_id.max(id);
            }
            if let Some(gen) = edit.next_flush_generation {
                next_flush_generation = next_flush_generation.max(gen);
            }
            if let Some(cp) = edit.checkpoint {
                checkpoint = cp;
            }
            for del in edit.deleted_files {
                live_files.remove(&del);
            }
            for add in edit.added_files {
                next_file_id = next_file_id.max(add.file_id + 1);
                next_flush_generation = next_flush_generation.max(add.flush_generation + 1);
                live_files.insert((add.level, add.file_id), add);
            }

            offset = record_end;
        }

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&manifest_path)?;
        if let Some(valid_len) = truncate_to {
            let trunc_file = OpenOptions::new().write(true).open(&manifest_path)?;
            trunc_file.set_len(valid_len as u64)?;
            trunc_file.sync_data()?;
        }

        Self::cleanup_orphan_files(lsm_dir, &live_files, &manifest_name)?;

        Ok((
            Self {
                lsm_dir: lsm_dir.to_path_buf(),
                manifest_seq: seq,
                manifest_path,
                file,
            },
            RecoveredManifestState {
                next_file_id,
                next_flush_generation,
                checkpoint,
                live_files,
            },
        ))
    }

    /// Appends a CRC32C-framed `VersionEdit` and calls `fdatasync` (`sync_data`).
    pub fn append(&mut self, edit: &VersionEdit) -> Result<()> {
        Self::write_record(&mut self.file, edit)
    }

    /// Rotates to `MANIFEST-<seq+1>` writing a compact baseline `VersionEdit` and atomically updating `CURRENT`.
    pub fn rotate(&mut self, snapshot_edit: &VersionEdit) -> Result<()> {
        let new_seq = self.manifest_seq + 1;
        let new_name = format!("MANIFEST-{:06}", new_seq);
        let new_path = self.lsm_dir.join(&new_name);
        let mut new_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&new_path)?;
        Self::write_record(&mut new_file, snapshot_edit)?;
        Self::write_current_atomic(&self.lsm_dir, &new_name)?;

        let old_path = self.manifest_path.clone();
        self.manifest_seq = new_seq;
        self.manifest_path = new_path;
        self.file = new_file;
        let _ = fs::remove_file(old_path);
        Ok(())
    }

    /// Writes a standalone `MANIFEST-000001` + `CURRENT` into `dest_dir` for hard-linked Raft checkpoints.
    pub fn write_standalone_checkpoint(dest_dir: &Path, snapshot_edit: &VersionEdit) -> Result<()> {
        let manifest_name = "MANIFEST-000001";
        let manifest_path = dest_dir.join(manifest_name);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&manifest_path)?;
        Self::write_record(&mut f, snapshot_edit)?;
        Self::write_current_atomic(dest_dir, manifest_name)?;
        Ok(())
    }

    fn write_record(file: &mut File, edit: &VersionEdit) -> Result<()> {
        let payload = edit.encode_payload();
        let crc = checksum_crc32c(&payload);
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);
        file.write_all(&frame)?;
        file.sync_data()?;
        Ok(())
    }

    fn write_current_atomic(lsm_dir: &Path, manifest_name: &str) -> Result<()> {
        let tmp = lsm_dir.join("CURRENT.tmp");
        let final_current = lsm_dir.join("CURRENT");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(format!("{}\n", manifest_name).as_bytes())?;
            f.sync_data()?;
        }
        fs::rename(&tmp, &final_current)?;
        sync_dir(lsm_dir)?;
        Ok(())
    }

    fn cleanup_orphan_files(
        lsm_dir: &Path,
        live_files: &BTreeMap<(u8, u64), SstFileMeta>,
        active_manifest: &str,
    ) -> Result<()> {
        let live_names: std::collections::HashSet<String> = live_files
            .values()
            .map(|m| sst_file_name(m.file_id))
            .collect();

        for entry in fs::read_dir(lsm_dir)? {
            let entry = entry?;
            let fname = entry.file_name();
            let Some(name) = fname.to_str() else {
                continue;
            };
            let is_orphan = name.ends_with(".tmp")
                || (name.ends_with(".sst") && !live_names.contains(name))
                || (name.starts_with("MANIFEST-") && name != active_manifest);
            if is_orphan {
                let _ = fs::remove_file(entry.path());
            }
        }
        sync_dir(lsm_dir)?;
        Ok(())
    }
}
