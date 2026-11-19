//! Segmented CRC32C `RaftLogStorage` with durable `HARD_STATE` (`vote` + `last_committed`)
//! and 3-watermark clamped prefix purging (`consensus_and_transactions.md` §5.1–§5.2).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::type_config::alias::{EntryOf, LogIdOf, VoteOf};

use metadb_core::codec;
use metadb_core::{append_crc32c_le, checksum_crc32c, verify_crc32c_le};
use metadb_storage::sstable::sync_dir;

use crate::state_machine::highest_durable_snapshot_index;
use crate::types::{MetaDbTypeConfig, ShardWatermarkTracker};

fn purge_limit(raft_dir: &Path, watermark_safe: u64, requested: u64) -> u64 {
    let covered = highest_durable_snapshot_index(raft_dir);
    let mut limit = watermark_safe.min(requested).min(covered);
    // A backup hold clamps the purge even after a newer snapshot is published.
    // `Some(0)` freezes a log that has not been purged yet.
    if let Some(held) = crate::backup::hold_purge_cap(raft_dir) {
        limit = limit.min(held);
    }
    limit
}

fn io_backup(message: impl AsRef<str>) -> io::Error {
    io::Error::other(message.as_ref())
}

pub(crate) fn purged_index_from_hard_state_bytes(raw: &[u8]) -> io::Result<u64> {
    LogStoreInner::purged_index_from_hard_state_bytes(raw)
}

/// Read-only check used by backup restore. Does not truncate a torn tail.
pub fn inspect_raft_dir(raft_dir: &Path) -> io::Result<RaftLogInspection> {
    LogStoreInner::inspect_dir(raft_dir)
}

/// Last purged index stored in `HARD_STATE`, or 0 when the log has not been purged.
pub fn read_persisted_purged_index(raft_dir: &Path) -> io::Result<u64> {
    let state = LogStoreInner::load_hard_state_readonly(raft_dir)?;
    Ok(state.last_purged_log_id.map(|id| id.index).unwrap_or(0))
}

/// Read-only summary of a raft directory. A torn tail is an error: backup restore
/// must not silently drop frames the way crash recovery can.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftLogInspection {
    pub last_purged_index: u64,
    pub committed_index: Option<u64>,
    pub first_retained: Option<u64>,
    pub last_retained: Option<u64>,
    pub retained_count: u64,
}

impl RaftLogInspection {
    /// True when every index in `from..=to` is present as a contiguous retained span.
    pub fn covers_inclusive(&self, from: u64, to: u64) -> bool {
        if from > to {
            return true;
        }
        let (Some(first), Some(last)) = (self.first_retained, self.last_retained) else {
            return false;
        };
        if self.retained_count != last.saturating_sub(first).saturating_add(1) {
            return false;
        }
        from >= first && to <= last
    }
}

pub const RAFT_SEGMENT_MAGIC: &[u8; 8] = b"MRAFTLO1";
pub const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

#[derive(
    Debug,
    Clone,
    Default,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
struct PersistedHardState {
    #[wincode(with = "metadb_core::codec::SerdeCompat<Option<VoteOf<MetaDbTypeConfig>>>")]
    vote: Option<VoteOf<MetaDbTypeConfig>>,
    #[wincode(with = "metadb_core::codec::SerdeCompat<Option<LogIdOf<MetaDbTypeConfig>>>")]
    committed: Option<LogIdOf<MetaDbTypeConfig>>,
    #[wincode(with = "metadb_core::codec::SerdeCompat<Option<LogIdOf<MetaDbTypeConfig>>>")]
    last_purged_log_id: Option<LogIdOf<MetaDbTypeConfig>>,
}

struct SegmentFile {
    first_index: u64,
    last_index: u64,
    path: PathBuf,
    byte_len: u64,
    /// Maps each log index stored in this segment to its frame end byte offset (`>= 8`),
    /// enabling crash-safe in-place `set_len` suffix truncation without rewriting committed prefixes.
    entry_end_offsets: BTreeMap<u64, u64>,
}

struct LogStoreInner {
    raft_dir: PathBuf,
    hard_state: PersistedHardState,
    entries: BTreeMap<u64, EntryOf<MetaDbTypeConfig>>,
    segments: Vec<SegmentFile>,
    active_file: Option<File>,
    max_segment_bytes: u64,
    /// Test hook: the next `n` append fsyncs fail after the bytes are written.
    fail_syncs: u32,
}

impl LogStoreInner {
    fn open_or_recover(raft_dir: &Path, max_segment_bytes: u64) -> io::Result<Self> {
        fs::create_dir_all(raft_dir)?;
        let hard_state = Self::load_hard_state(raft_dir)?;

        let mut seg_paths: Vec<(u64, PathBuf)> = Vec::new();
        for d in fs::read_dir(raft_dir)? {
            let d = d?;
            let fname = d.file_name();
            let Some(name) = fname.to_str() else {
                continue;
            };
            if let Some(mid) = name
                .strip_prefix("segment_")
                .and_then(|s| s.strip_suffix(".raftlog"))
            {
                if let Ok(first_idx) = mid.parse::<u64>() {
                    seg_paths.push((first_idx, d.path()));
                }
            }
        }
        seg_paths.sort_by_key(|(idx, _)| *idx);

        let purged_idx_opt = hard_state.last_purged_log_id.map(|id| id.index);

        let mut entries: BTreeMap<u64, EntryOf<MetaDbTypeConfig>> = BTreeMap::new();
        let mut segments: Vec<SegmentFile> = Vec::new();
        let total_segs = seg_paths.len();
        let mut previous_index = None;
        let mut tail_repairs = Vec::new();

        for (seg_pos, (first_index, path)) in seg_paths.into_iter().enumerate() {
            let is_last_segment = seg_pos + 1 == total_segs;
            let mut raw = Vec::new();
            File::open(&path)?.read_to_end(&mut raw)?;

            if raw.len() < 8 {
                if is_last_segment {
                    tail_repairs.push((path, None));
                    continue;
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("corrupted short raft segment {:?}", path),
                    ));
                }
            }
            if &raw[0..8] != RAFT_SEGMENT_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid raft segment magic in {:?}", path),
                ));
            }

            let mut offset = 8usize;
            let mut last_idx_in_seg = first_index;
            let mut truncate_tail: Option<usize> = None;
            let mut entry_end_offsets: BTreeMap<u64, u64> = BTreeMap::new();

            while offset < raw.len() {
                if offset + 8 > raw.len() {
                    if is_last_segment {
                        truncate_tail = Some(offset);
                        break;
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("truncated non-final raft segment {:?}", path),
                        ));
                    }
                }
                let frame_len =
                    u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
                let expected_crc =
                    u32::from_le_bytes(raw[offset + 4..offset + 8].try_into().unwrap());
                let frame_end = offset.saturating_add(8).saturating_add(frame_len);

                if frame_end > raw.len() {
                    if is_last_segment {
                        truncate_tail = Some(offset);
                        break;
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("torn frame in non-final raft segment {:?}", path),
                        ));
                    }
                }
                if frame_len < 8 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid raft frame_len {} < 8 in {:?}", frame_len, path),
                    ));
                }

                let frame_body = &raw[offset + 8..frame_end];
                let actual_crc = checksum_crc32c(frame_body);
                if actual_crc != expected_crc {
                    // Truncate only a bad last frame of the last segment (torn tail).
                    // Any earlier complete frame, or a non-final segment, refuses to open.
                    let is_last_frame = frame_end == raw.len();
                    if is_last_segment && is_last_frame {
                        truncate_tail = Some(offset);
                        break;
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "CRC32C mismatch in complete raft frame at offset {} of {:?}",
                            offset, path
                        ),
                    ));
                }

                let log_index = u64::from_le_bytes(frame_body[0..8].try_into().unwrap());
                let entry: EntryOf<MetaDbTypeConfig> =
                    codec::deserialize_serde(&frame_body[8..])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                if entry.log_id().index != log_index
                    || previous_index.is_some_and(|prev| log_index <= prev)
                    || (entry_end_offsets.is_empty() && log_index != first_index)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "inconsistent or duplicate raft frame index {log_index} in {path:?}"
                        ),
                    ));
                }
                previous_index = Some(log_index);

                last_idx_in_seg = log_index;
                entry_end_offsets.insert(log_index, frame_end as u64);
                let keep_entry = match purged_idx_opt {
                    None => true,
                    Some(purged_idx) => log_index > purged_idx,
                };
                if keep_entry {
                    entries.insert(log_index, entry);
                }
                offset = frame_end;
            }

            let final_len = if let Some(valid_len) = truncate_tail {
                tail_repairs.push((path.clone(), Some(valid_len as u64)));
                valid_len as u64
            } else {
                raw.len() as u64
            };

            segments.push(SegmentFile {
                first_index,
                last_index: last_idx_in_seg,
                path,
                byte_len: final_len,
                entry_end_offsets,
            });
        }

        Self::ensure_committed_recovered(&hard_state, &entries)?;
        // Validate the entire history before changing any recovery evidence.
        for (path, valid_len) in tail_repairs {
            if let Some(valid_len) = valid_len {
                let wf = OpenOptions::new().write(true).open(path)?;
                wf.set_len(valid_len)?;
                wf.sync_data()?;
            } else {
                fs::remove_file(path)?;
            }
        }
        for d in fs::read_dir(raft_dir)? {
            let d = d?;
            if d.file_name()
                .to_str()
                .is_some_and(|name| name.ends_with(".tmp"))
            {
                fs::remove_file(d.path())?;
            }
        }
        Self::persist_initialization_marker(raft_dir)?;
        let active_file = if let Some(last_seg) = segments.last() {
            Some(OpenOptions::new().append(true).open(&last_seg.path)?)
        } else {
            None
        };

        Ok(Self {
            raft_dir: raft_dir.to_path_buf(),
            hard_state,
            entries,
            segments,
            active_file,
            max_segment_bytes,
            fail_syncs: 0,
        })
    }

    fn ensure_committed_recovered(
        hard_state: &PersistedHardState,
        entries: &BTreeMap<u64, EntryOf<MetaDbTypeConfig>>,
    ) -> io::Result<()> {
        let Some(committed) = hard_state.committed else {
            return Ok(());
        };
        if hard_state
            .last_purged_log_id
            .is_some_and(|p| p.index >= committed.index)
        {
            return Ok(());
        }
        let first = hard_state.last_purged_log_id.map_or(0, |p| p.index + 1);
        let mut expected = first;
        for (&index, entry) in entries.range(first..=committed.index) {
            if index != expected || entry.log_id().index != index {
                break;
            }
            if index == committed.index {
                if entry.log_id() == committed {
                    return Ok(());
                }
                break;
            }
            expected = index + 1;
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "committed raft prefix {first}..={} is missing or corrupt at index {expected}",
                committed.index,
            ),
        ))
    }

    fn load_hard_state(raft_dir: &Path) -> io::Result<PersistedHardState> {
        let path = raft_dir.join("HARD_STATE");
        if !path.try_exists()? {
            if fs::read_dir(raft_dir)?.next().is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HARD_STATE is missing from an initialized or nonempty raft directory",
                ));
            }
            let initial = PersistedHardState::default();
            Self::persist_hard_state_value(raft_dir, &initial)?;
            Self::persist_initialization_marker(raft_dir)?;
            return Ok(initial);
        }
        let mut raw = Vec::new();
        File::open(&path)?.read_to_end(&mut raw)?;
        let body = verify_crc32c_le(&raw, "Raft HARD_STATE")
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        codec::deserialize(body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    fn persist_hard_state(&self) -> io::Result<()> {
        Self::persist_hard_state_value(&self.raft_dir, &self.hard_state)
    }

    fn persist_initialization_marker(raft_dir: &Path) -> io::Result<()> {
        let path = raft_dir.join("INITIALIZED");
        if !path.try_exists()? {
            let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
            file.write_all(b"MRAFTIN1")?;
            file.sync_data()?;
            sync_dir(raft_dir).map_err(|e| io::Error::other(e.to_string()))?;
        }
        Ok(())
    }

    fn persist_hard_state_value(
        raft_dir: &Path,
        hard_state: &PersistedHardState,
    ) -> io::Result<()> {
        let mut raw = codec::serialize(hard_state)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        append_crc32c_le(&mut raw);
        let tmp = raft_dir.join("HARD_STATE.tmp");
        let dst = raft_dir.join("HARD_STATE");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(&raw)?;
            f.sync_data()?;
        }
        fs::rename(&tmp, &dst)?;
        sync_dir(raft_dir).map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }

    fn rotate_new_segment(&mut self, first_index: u64) -> io::Result<()> {
        let name = format!("segment_{:020}.raftlog", first_index);
        let path = self.raft_dir.join(name);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        f.write_all(RAFT_SEGMENT_MAGIC)?;
        f.sync_data()?;
        drop(f);
        sync_dir(&self.raft_dir).map_err(|e| io::Error::other(e.to_string()))?;

        let append_f = OpenOptions::new().append(true).open(&path)?;
        self.segments.push(SegmentFile {
            first_index,
            last_index: first_index,
            path,
            byte_len: 8,
            entry_end_offsets: BTreeMap::new(),
        });
        self.active_file = Some(append_f);
        Ok(())
    }

    fn append_entries_sync(&mut self, batch: Vec<EntryOf<MetaDbTypeConfig>>) -> io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let prior_segments = self.segments.len();
        let (tail_byte_len, tail_last_index) = self
            .segments
            .last()
            .map(|s| (s.byte_len, s.last_index))
            .unwrap_or((0, 0));
        let new_indexes: Vec<u64> = batch.iter().map(|e| e.log_id().index).collect();
        if let Err(err) = self.append_entries_mutating(batch) {
            self.rollback_append(prior_segments, tail_byte_len, tail_last_index, &new_indexes);
            return Err(err);
        }
        Ok(())
    }

    fn append_entries_mutating(&mut self, batch: Vec<EntryOf<MetaDbTypeConfig>>) -> io::Result<()> {
        for entry in batch {
            let log_idx = entry.log_id().index;
            let need_new_seg = match self.segments.last() {
                None => true,
                Some(seg) => seg.byte_len >= self.max_segment_bytes,
            };
            if need_new_seg {
                // One append batch can cross segment boundaries. Closing the old file
                // does not make its frames durable; sync it before replacing the handle.
                if let Some(active) = self.active_file.as_mut() {
                    active.sync_data()?;
                }
                self.rotate_new_segment(log_idx)?;
            }

            let serialized = codec::serialize_serde(&entry)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            let frame_len = (8 + serialized.len()) as u32;
            let mut body = Vec::with_capacity(8 + serialized.len());
            body.extend_from_slice(&log_idx.to_le_bytes());
            body.extend_from_slice(&serialized);
            let crc = checksum_crc32c(&body);

            let mut frame = Vec::with_capacity(8 + body.len());
            frame.extend_from_slice(&frame_len.to_le_bytes());
            frame.extend_from_slice(&crc.to_le_bytes());
            frame.extend_from_slice(&body);

            let af = self.active_file.as_mut().unwrap();
            af.write_all(&frame)?;
            if let Some(seg) = self.segments.last_mut() {
                seg.last_index = log_idx;
                seg.byte_len += frame.len() as u64;
                seg.entry_end_offsets.insert(log_idx, seg.byte_len);
            }
            self.entries.insert(log_idx, entry);
        }

        if self.fail_syncs > 0 {
            self.fail_syncs -= 1;
            return Err(io::Error::other("injected raft append sync failure"));
        }
        if let Some(af) = self.active_file.as_mut() {
            af.sync_data()?;
        }
        Ok(())
    }

    fn rollback_append(
        &mut self,
        prior_segments: usize,
        tail_byte_len: u64,
        tail_last_index: u64,
        new_indexes: &[u64],
    ) {
        for idx in new_indexes {
            self.entries.remove(idx);
        }
        self.active_file = None;
        while self.segments.len() > prior_segments {
            if let Some(seg) = self.segments.pop() {
                let _ = fs::remove_file(&seg.path);
            }
        }
        if prior_segments == 0 {
            return;
        }
        if let Some(seg) = self.segments.last_mut() {
            seg.byte_len = tail_byte_len;
            seg.last_index = tail_last_index;
            seg.entry_end_offsets.retain(|_, end| *end <= tail_byte_len);
        }
        let Some(path) = self.segments.last().map(|s| s.path.clone()) else {
            return;
        };
        if let Ok(f) = OpenOptions::new().write(true).open(&path) {
            let _ = f.set_len(tail_byte_len);
            let _ = f.sync_data();
        }
        self.active_file = OpenOptions::new().append(true).open(&path).ok();
    }

    /// Crash-safe suffix-only truncation: never unlinks or rewrites committed prefix segments!
    /// Truncates the target segment containing `keep_upto` in-place via `set_len(valid_end_offset)` + `sync_data()`,
    /// and unlinks only trailing segment files whose `first_index > keep_upto`.
    fn truncate_suffix_after(
        &mut self,
        last_log_id: Option<LogIdOf<MetaDbTypeConfig>>,
    ) -> io::Result<()> {
        self.active_file = None;

        let Some(lid) = last_log_id else {
            self.entries.clear();
            let mut removed_any = false;
            for seg in self.segments.drain(..) {
                match fs::remove_file(seg.path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
                removed_any = true;
            }
            if removed_any {
                sync_dir(&self.raft_dir).map_err(|e| io::Error::other(e.to_string()))?;
            }
            return Ok(());
        };

        let keep_upto = lid.index;
        let _ = self.entries.split_off(&(keep_upto.saturating_add(1)));

        let mut removed_trailing = false;
        while let Some(last_seg) = self.segments.last() {
            if last_seg.first_index > keep_upto {
                let seg = self.segments.pop().unwrap();
                match fs::remove_file(&seg.path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
                removed_trailing = true;
            } else {
                break;
            }
        }
        if removed_trailing {
            sync_dir(&self.raft_dir).map_err(|e| io::Error::other(e.to_string()))?;
        }

        if let Some(tail_seg) = self.segments.last_mut() {
            if tail_seg.last_index > keep_upto {
                let valid_end_offset = tail_seg
                    .entry_end_offsets
                    .range(..=keep_upto)
                    .next_back()
                    .map(|(_, &end_off)| end_off)
                    .unwrap_or(8);
                let wf = OpenOptions::new().write(true).open(&tail_seg.path)?;
                wf.set_len(valid_end_offset)?;
                wf.sync_data()?;
                drop(wf);

                let _ = tail_seg
                    .entry_end_offsets
                    .split_off(&(keep_upto.saturating_add(1)));
                tail_seg.byte_len = valid_end_offset;
                tail_seg.last_index = tail_seg
                    .entry_end_offsets
                    .last_key_value()
                    .map(|(&idx, _)| idx)
                    .unwrap_or(tail_seg.first_index);
            }
            self.active_file = Some(OpenOptions::new().append(true).open(&tail_seg.path)?);
        }

        Ok(())
    }

    fn inspect_dir(raft_dir: &Path) -> io::Result<RaftLogInspection> {
        if !raft_dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("raft directory {} is missing", raft_dir.display()),
            ));
        }
        let hard_state = Self::load_hard_state_readonly(raft_dir)?;
        let mut seg_bytes: Vec<(u64, String, Vec<u8>)> = Vec::new();
        for entry in fs::read_dir(raft_dir)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if let Some(mid) = name
                .strip_prefix("segment_")
                .and_then(|rest| rest.strip_suffix(".raftlog"))
            {
                let first_idx = mid.parse::<u64>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid raft segment name {name}"),
                    )
                })?;
                let mut raw = Vec::new();
                File::open(entry.path())?.read_to_end(&mut raw)?;
                seg_bytes.push((first_idx, name, raw));
            }
        }
        Self::inspect_loaded(hard_state, seg_bytes)
    }

    fn inspect_fd(dir: &File) -> io::Result<RaftLogInspection> {
        let listing =
            crate::backup::list_plain_dir(dir).map_err(|error| io_backup(error.to_string()))?;
        let hard_bytes = crate::backup::read_listed_file(dir, "HARD_STATE")
            .map_err(|error| io_backup(error.to_string()))?;
        let hard_state = Self::hard_state_from_bytes(&hard_bytes)?;
        let mut seg_bytes = Vec::new();
        for entry in listing {
            let Some(mid) = entry
                .name
                .strip_prefix("segment_")
                .and_then(|rest| rest.strip_suffix(".raftlog"))
            else {
                continue;
            };
            if entry.kind != crate::backup::EntryKind::File {
                return Err(io_backup(format!(
                    "refusing to copy special file {}",
                    entry.name
                )));
            }
            let first_idx = mid.parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid raft segment name {}", entry.name),
                )
            })?;
            let raw = crate::backup::read_listed_file(dir, &entry.name)
                .map_err(|error| io_backup(error.to_string()))?;
            seg_bytes.push((first_idx, entry.name, raw));
        }
        Self::inspect_loaded(hard_state, seg_bytes)
    }

    pub(crate) fn inspect_copied_dir(dir: &File) -> io::Result<RaftLogInspection> {
        Self::inspect_fd(dir)
    }

    pub(crate) fn purged_index_from_hard_state_bytes(raw: &[u8]) -> io::Result<u64> {
        let state = Self::hard_state_from_bytes(raw)?;
        Ok(state.last_purged_log_id.map(|id| id.index).unwrap_or(0))
    }

    fn hard_state_from_bytes(raw: &[u8]) -> io::Result<PersistedHardState> {
        let body = verify_crc32c_le(raw, "Raft HARD_STATE")
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        codec::deserialize(body)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
    }

    fn inspect_loaded(
        hard_state: PersistedHardState,
        mut seg_bytes: Vec<(u64, String, Vec<u8>)>,
    ) -> io::Result<RaftLogInspection> {
        let purged_idx = hard_state.last_purged_log_id.map(|id| id.index);
        seg_bytes.sort_by_key(|(idx, _, _)| *idx);
        let mut entries: BTreeMap<u64, EntryOf<MetaDbTypeConfig>> = BTreeMap::new();
        let mut previous_index = None;
        for (first_index, name, raw) in seg_bytes {
            if raw.len() < 8 || &raw[0..8] != RAFT_SEGMENT_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("raft segment {} is incomplete or has a bad magic", name),
                ));
            }
            let mut offset = 8usize;
            let mut entry_end_offsets: BTreeMap<u64, u64> = BTreeMap::new();
            while offset < raw.len() {
                if offset + 8 > raw.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "torn raft frame in {} (backup images must not need tail repair)",
                            name
                        ),
                    ));
                }
                let frame_len =
                    u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
                let expected_crc =
                    u32::from_le_bytes(raw[offset + 4..offset + 8].try_into().unwrap());
                let frame_end = offset.saturating_add(8).saturating_add(frame_len);
                if frame_len < 8 || frame_end > raw.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("torn or short raft frame in {name}"),
                    ));
                }
                let frame_body = &raw[offset + 8..frame_end];
                let actual_crc = checksum_crc32c(frame_body);
                if actual_crc != expected_crc {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("CRC32C mismatch in raft segment {name}"),
                    ));
                }
                let log_index = u64::from_le_bytes(frame_body[0..8].try_into().unwrap());
                let entry: EntryOf<MetaDbTypeConfig> =
                    codec::deserialize_serde(&frame_body[8..])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                if entry.log_id().index != log_index
                    || previous_index.is_some_and(|prev| log_index <= prev)
                    || (entry_end_offsets.is_empty() && log_index != first_index)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("inconsistent raft frame index {log_index} in {}", name),
                    ));
                }
                previous_index = Some(log_index);
                entry_end_offsets.insert(log_index, frame_end as u64);
                let keep = purged_idx.is_none_or(|purged| log_index > purged);
                if keep {
                    entries.insert(log_index, entry);
                }
                offset = frame_end;
            }
        }
        Self::ensure_committed_recovered(&hard_state, &entries)?;
        let first_retained = entries.keys().next().copied();
        let last_retained = entries.keys().next_back().copied();
        Ok(RaftLogInspection {
            last_purged_index: purged_idx.unwrap_or(0),
            committed_index: hard_state.committed.map(|id| id.index),
            first_retained,
            last_retained,
            retained_count: entries.len() as u64,
        })
    }

    fn load_hard_state_readonly(raft_dir: &Path) -> io::Result<PersistedHardState> {
        let path = raft_dir.join("HARD_STATE");
        if !path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("HARD_STATE is missing from {}", raft_dir.display()),
            ));
        }
        let mut raw = Vec::new();
        File::open(&path)?.read_to_end(&mut raw)?;
        let body = verify_crc32c_le(&raw, "Raft HARD_STATE")
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        codec::deserialize(body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }
}

/// Cloneable segmented CRC32C Raft log store implementing both `RaftLogStorage` and `RaftLogReader`.
#[derive(Clone)]
pub struct SegmentedRaftLogStore {
    inner: Arc<Mutex<LogStoreInner>>,
    watermarks: Arc<ShardWatermarkTracker>,
}

impl SegmentedRaftLogStore {
    pub fn open(
        raft_dir: impl AsRef<Path>,
        watermarks: Arc<ShardWatermarkTracker>,
    ) -> io::Result<Self> {
        Self::open_with_segment_size(raft_dir, DEFAULT_MAX_SEGMENT_BYTES, watermarks)
    }

    pub fn open_with_segment_size(
        raft_dir: impl AsRef<Path>,
        max_segment_bytes: u64,
        watermarks: Arc<ShardWatermarkTracker>,
    ) -> io::Result<Self> {
        let inner = LogStoreInner::open_or_recover(raft_dir.as_ref(), max_segment_bytes)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            watermarks,
        })
    }

    pub fn retained_entry_count(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    /// True when startup must replay a committed or purged log before serving.
    ///
    /// A fresh store has neither, and OpenRaft's recovery wait cannot succeed
    /// until `initialize` creates the first committed entry.
    pub fn has_committed_entries(&self) -> bool {
        let guard = self.inner.lock().unwrap();
        guard.hard_state.committed.is_some()
            || guard.hard_state.last_purged_log_id.is_some()
            || !guard.entries.is_empty()
    }

    /// Commit or purge index that startup replay must apply before the shard can serve.
    ///
    /// `None` when the store has neither. A non-empty uncommitted tail does not
    /// count: those entries are not yet a committed prefix the state machine must replay.
    pub fn committed_or_purged_index(&self) -> Option<u64> {
        let guard = self.inner.lock().unwrap();
        let committed = guard.hard_state.committed.as_ref().map(|id| id.index);
        let purged = guard
            .hard_state
            .last_purged_log_id
            .as_ref()
            .map(|id| id.index);
        match (committed, purged) {
            (None, None) => None,
            (Some(index), None) | (None, Some(index)) => Some(index),
            (Some(committed), Some(purged)) => Some(committed.max(purged)),
        }
    }

    /// Test hook. The next `n` append fsyncs fail and roll the in-memory log back.
    pub fn testing_fail_next_syncs(&self, n: u32) {
        self.inner.lock().unwrap().fail_syncs = n;
    }

    pub fn segment_file_count(&self) -> usize {
        self.inner.lock().unwrap().segments.len()
    }

    /// Index already removed from the log. A backup hold must not purge past this.
    pub fn last_purged_index(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .hard_state
            .last_purged_log_id
            .map(|id| id.index)
            .unwrap_or(0)
    }

    /// Copies `HARD_STATE`, `INITIALIZED`, and segment files while appends are blocked.
    ///
    /// `source` must be the live raft directory opened from the data-directory anchor
    /// without following `be`, `shards`, the shard directory, or `raft`. The active
    /// segment is synced before those bytes are read. The live directory is not modified.
    /// File names are read from `source`; segment paths stored in memory are not opened.
    pub(crate) fn inspect_copied_dir(dir: &File) -> io::Result<RaftLogInspection> {
        LogStoreInner::inspect_copied_dir(dir)
    }

    pub fn copy_durable_log(&self, source: &File, dest: &File) -> io::Result<RaftLogInspection> {
        self.copy_durable_log_anchored(source, dest)
    }

    fn copy_durable_log_anchored(
        &self,
        source: &File,
        dest: &File,
    ) -> io::Result<RaftLogInspection> {
        let guard = self.inner.lock().unwrap();
        if let Some(active) = guard.active_file.as_ref() {
            active.sync_data()?;
        }
        let live = fs::symlink_metadata(&guard.raft_dir)?;
        if live.file_type().is_symlink() || !live.is_dir() {
            return Err(io_backup(
                "refusing to follow symlink for the live raft directory",
            ));
        }
        let opened = source.metadata()?;
        if !crate::backup::same_opened_file(&opened, &live) {
            return Err(io_backup(
                "live raft copy was not anchored at the log's raft directory",
            ));
        }
        let listing =
            crate::backup::list_plain_dir(source).map_err(|error| io_backup(error.to_string()))?;
        let mut copied_hard_state = false;
        for entry in &listing {
            let copy_name = entry.name == "HARD_STATE"
                || entry.name == "INITIALIZED"
                || (entry.name.starts_with("segment_") && entry.name.ends_with(".raftlog"));
            if !copy_name {
                continue;
            }
            if entry.kind != crate::backup::EntryKind::File {
                return Err(io_backup(format!(
                    "refusing to copy special file {}",
                    entry.name
                )));
            }
            let bytes = crate::backup::read_listed_file(source, &entry.name)
                .map_err(|error| io_backup(error.to_string()))?;
            crate::backup::write_child_file(dest, &entry.name, &bytes)
                .map_err(|error| io_backup(error.to_string()))?;
            if entry.name == "HARD_STATE" {
                copied_hard_state = true;
            }
        }
        if !copied_hard_state {
            return Err(io_backup(
                "HARD_STATE is missing from the live raft directory",
            ));
        }
        for segment in &guard.segments {
            let Some(name) = segment.path.file_name().and_then(|name| name.to_str()) else {
                return Err(io_backup("raft segment path has no utf-8 file name"));
            };
            if !listing.iter().any(|entry| entry.name == name) {
                return Err(io_backup(format!(
                    "refusing to follow raft segment {name}; it is not a regular file in the anchored directory"
                )));
            }
        }
        drop(guard);
        LogStoreInner::inspect_fd(dest)
    }

    /// Triggers opportunistic physical segment cleanup up to `watermarks.safe_purge_index()`
    /// if OpenRaft has already requested a purge or if called after advancing watermarks.
    pub fn try_purge_to_safe_watermark(&self) -> io::Result<usize> {
        let mut guard = self.inner.lock().unwrap();
        let safe_idx = purge_limit(
            &guard.raft_dir,
            self.watermarks.safe_purge_index(),
            u64::MAX,
        );
        if safe_idx == 0 {
            return Ok(0);
        }
        let target_log_id = guard
            .entries
            .range(..=safe_idx)
            .next_back()
            .map(|(_, e)| e.log_id());
        let Some(clamped_log_id) = target_log_id else {
            return Ok(0);
        };

        let mut purged_entries = 0usize;
        while let Some((&idx, _)) = guard.entries.first_key_value() {
            if idx <= clamped_log_id.index {
                guard.entries.pop_first();
                purged_entries += 1;
            } else {
                break;
            }
        }

        guard.hard_state.last_purged_log_id = Some(clamped_log_id);
        guard.persist_hard_state()?;

        // Unlink closed segments (never the active tail segment if it still holds unpurged entries)
        // whose `last_index <= clamped_log_id.index`.
        let seg_len = guard.segments.len();
        let mut keep = Vec::with_capacity(seg_len);
        let all_segs: Vec<SegmentFile> = std::mem::take(&mut guard.segments);
        let all_empty = guard.entries.is_empty();
        if all_empty {
            guard.active_file = None;
        }
        for (pos, seg) in all_segs.into_iter().enumerate() {
            let is_active_tail = pos + 1 == seg_len && !all_empty;
            if !is_active_tail && seg.last_index <= clamped_log_id.index {
                let _ = fs::remove_file(&seg.path);
            } else {
                keep.push(seg);
            }
        }
        guard.segments = keep;
        Ok(purged_entries)
    }
}

impl RaftLogReader<MetaDbTypeConfig> for SegmentedRaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<MetaDbTypeConfig>>, io::Error> {
        let guard = self.inner.lock().unwrap();
        let start = match range.start_bound() {
            Bound::Included(&x) => Bound::Included(x),
            Bound::Excluded(&x) => Bound::Excluded(x),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(&x) => Bound::Included(x),
            Bound::Excluded(&x) => Bound::Excluded(x),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(guard
            .entries
            .range((start, end))
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<MetaDbTypeConfig>>, io::Error> {
        Ok(self.inner.lock().unwrap().hard_state.vote)
    }
}

impl RaftLogStorage<MetaDbTypeConfig> for SegmentedRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<MetaDbTypeConfig>, io::Error> {
        let guard = self.inner.lock().unwrap();
        let last_purged_log_id = guard.hard_state.last_purged_log_id;
        let last_log_id = guard
            .entries
            .last_key_value()
            .map(|(_, e)| e.log_id())
            .or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<MetaDbTypeConfig>) -> Result<(), io::Error> {
        let mut guard = self.inner.lock().unwrap();
        guard.hard_state.vote = Some(*vote);
        guard.persist_hard_state()
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<MetaDbTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut guard = self.inner.lock().unwrap();
        if guard.hard_state.committed != committed {
            guard.hard_state.committed = committed;
            guard.persist_hard_state()?;
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<MetaDbTypeConfig>>, io::Error> {
        Ok(self.inner.lock().unwrap().hard_state.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<MetaDbTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<MetaDbTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let batch: Vec<EntryOf<MetaDbTypeConfig>> = entries.into_iter().collect();
        let res = {
            let mut guard = self.inner.lock().unwrap();
            guard.append_entries_sync(batch)
        };
        callback.io_completed(
            res.as_ref()
                .copied()
                .map_err(|e| io::Error::new(e.kind(), e.to_string())),
        );
        res
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<MetaDbTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut guard = self.inner.lock().unwrap();
        if let Some(comm) = guard.hard_state.committed {
            let keep_ok = match last_log_id {
                Some(lid) => lid.index >= comm.index,
                None => false,
            };
            if !keep_ok {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "refusing to truncate committed raft log index {:?} below {}",
                        last_log_id.map(|id| id.index),
                        comm.index
                    ),
                ));
            }
        }
        guard.truncate_suffix_after(last_log_id)
    }

    async fn purge(&mut self, log_id: LogIdOf<MetaDbTypeConfig>) -> Result<(), io::Error> {
        // Clamp OpenRaft's requested `log_id` against the 3-watermark durability gate
        // and against a snapshot directory that is still readable. Purging the log
        // past a missing snapshot leaves restart with nothing to read.
        // Re-read the backup hold after the lock is taken so a hold that became
        // visible after the first check still blocks this deletion.
        let safe_idx = {
            let guard = self.inner.lock().unwrap();
            purge_limit(
                &guard.raft_dir,
                self.watermarks.safe_purge_index(),
                log_id.index,
            )
        };
        if safe_idx == 0 {
            return Ok(());
        }
        let mut guard = self.inner.lock().unwrap();
        let safe_idx = purge_limit(
            &guard.raft_dir,
            self.watermarks.safe_purge_index(),
            safe_idx,
        );
        if safe_idx == 0 {
            return Ok(());
        }
        let clamped = if safe_idx == log_id.index {
            Some(log_id)
        } else {
            guard
                .entries
                .range(..=safe_idx)
                .next_back()
                .map(|(_, e)| e.log_id())
        };
        let Some(clamped_id) = clamped else {
            return Ok(());
        };
        if let Some(prev) = guard.hard_state.last_purged_log_id {
            if clamped_id.index <= prev.index {
                return Ok(());
            }
        }
        while let Some((&idx, _)) = guard.entries.first_key_value() {
            if idx <= clamped_id.index {
                guard.entries.pop_first();
            } else {
                break;
            }
        }
        guard.hard_state.last_purged_log_id = Some(clamped_id);
        guard.persist_hard_state()?;

        let seg_len = guard.segments.len();
        let all_empty = guard.entries.is_empty();
        if all_empty {
            guard.active_file = None;
        }
        let old_segs: Vec<SegmentFile> = std::mem::take(&mut guard.segments);
        let mut keep = Vec::new();
        for (pos, seg) in old_segs.into_iter().enumerate() {
            let is_active_tail = pos + 1 == seg_len && !all_empty;
            if !is_active_tail && seg.last_index <= clamped_id.index {
                let _ = fs::remove_file(&seg.path);
            } else {
                keep.push(seg);
            }
        }
        guard.segments = keep;
        Ok(())
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    use openraft::type_config::alias::CommittedLeaderIdOf;
    use openraft::vote::RaftLeaderId;
    use openraft::EntryPayload;

    fn committed_log() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let mut inner = LogStoreInner::open_or_recover(dir.path(), 1024).unwrap();
        let log_id =
            LogIdOf::<MetaDbTypeConfig>::new(CommittedLeaderIdOf::<MetaDbTypeConfig>::new(1, 1), 1);
        inner
            .append_entries_sync(
                (0..=1)
                    .map(|index| EntryOf::<MetaDbTypeConfig> {
                        log_id: LogIdOf::<MetaDbTypeConfig>::new(
                            CommittedLeaderIdOf::<MetaDbTypeConfig>::new(1, 1),
                            index,
                        ),
                        payload: EntryPayload::Blank,
                    })
                    .collect(),
            )
            .unwrap();
        inner.hard_state.committed = Some(log_id);
        inner.persist_hard_state().unwrap();
        let segment = inner.segments[0].path.clone();
        drop(inner);
        (dir, segment)
    }

    #[test]
    fn truncation_never_acknowledges_failed_segment_unlink() {
        let dir = tempfile::tempdir().unwrap();
        let mut inner = LogStoreInner::open_or_recover(dir.path(), 1024).unwrap();
        inner
            .append_entries_sync(vec![EntryOf::<MetaDbTypeConfig> {
                log_id: LogIdOf::<MetaDbTypeConfig>::new(
                    CommittedLeaderIdOf::<MetaDbTypeConfig>::new(1, 1),
                    1,
                ),
                payload: EntryPayload::Blank,
            }])
            .unwrap();
        let segment = inner.segments[0].path.clone();
        fs::rename(&segment, segment.with_extension("saved")).unwrap();
        fs::create_dir(&segment).unwrap();
        assert!(
            inner.truncate_suffix_after(None).is_err(),
            "failed unlink was reported as durable truncation"
        );
    }

    #[test]
    fn committed_final_frame_crc_corruption_is_fatal() {
        let (dir, segment) = committed_log();
        let mut raw = fs::read(&segment).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 1;
        fs::write(&segment, &raw).unwrap();
        let result = LogStoreInner::open_or_recover(dir.path(), 1024);
        assert!(
            matches!(&result, Err(e) if e.kind() == io::ErrorKind::InvalidData),
            "committed final frame must not be silently removed; remaining entries: {:?}",
            result.as_ref().ok().map(|s| s.entries.len())
        );
        assert_eq!(
            fs::read(segment).unwrap(),
            raw,
            "corruption detection must preserve evidence"
        );
    }

    #[test]
    fn committed_final_frame_truncation_is_fatal() {
        for removed in 1..=40 {
            let (dir, segment) = committed_log();
            let mut raw = fs::read(&segment).unwrap();
            if removed >= raw.len() {
                break;
            }
            raw.truncate(raw.len() - removed);
            fs::write(&segment, &raw).unwrap();
            let result = LogStoreInner::open_or_recover(dir.path(), 1024);
            assert!(
                matches!(&result, Err(e) if e.kind() == io::ErrorKind::InvalidData),
                "truncating {removed} bytes discarded committed index 1; remaining entries: {:?}",
                result.as_ref().ok().map(|s| s.entries.len())
            );
        }
    }

    #[test]
    fn fresh_store_has_nothing_to_replay() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            SegmentedRaftLogStore::open(dir.path(), Arc::new(ShardWatermarkTracker::default()))
                .unwrap();
        assert!(!store.has_committed_entries());
    }

    #[test]
    fn purge_retains_the_log_when_no_complete_snapshot_exists() {
        let dir = tempfile::tempdir().unwrap();
        let watermarks = Arc::new(ShardWatermarkTracker::new(10, 10, 10));
        let store = SegmentedRaftLogStore::open(dir.path(), watermarks).unwrap();
        store
            .inner
            .lock()
            .unwrap()
            .append_entries_sync(vec![EntryOf::<MetaDbTypeConfig> {
                log_id: LogIdOf::<MetaDbTypeConfig>::new(
                    CommittedLeaderIdOf::<MetaDbTypeConfig>::new(1, 1),
                    1,
                ),
                payload: EntryPayload::Blank,
            }])
            .unwrap();
        assert_eq!(store.try_purge_to_safe_watermark().unwrap(), 0);
        assert_eq!(store.retained_entry_count(), 1);
    }
}
