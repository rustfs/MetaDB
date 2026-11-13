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

use metadb_core::{append_crc32c_le, checksum_crc32c, verify_crc32c_le};
use metadb_storage::sstable::sync_dir;

use crate::types::{MetaDbTypeConfig, ShardWatermarkTracker};

pub const RAFT_SEGMENT_MAGIC: &[u8; 8] = b"MRAFTLO1";
pub const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PersistedHardState {
    vote: Option<VoteOf<MetaDbTypeConfig>>,
    committed: Option<LogIdOf<MetaDbTypeConfig>>,
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
}

impl LogStoreInner {
    fn open_or_recover(raft_dir: &Path, max_segment_bytes: u64) -> io::Result<Self> {
        fs::create_dir_all(raft_dir)?;
        let hard_state = Self::load_hard_state(raft_dir)?;

        // Clean up any leftover `.tmp` files from interrupted atomic renames.
        for d in fs::read_dir(raft_dir)? {
            let d = d?;
            if let Some(name) = d.file_name().to_str() {
                if name.ends_with(".tmp") {
                    let _ = fs::remove_file(d.path());
                }
            }
        }

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

        for (seg_pos, (first_index, path)) in seg_paths.into_iter().enumerate() {
            let is_last_segment = seg_pos + 1 == total_segs;
            let mut raw = Vec::new();
            File::open(&path)?.read_to_end(&mut raw)?;

            if raw.len() < 8 {
                if is_last_segment {
                    let _ = fs::remove_file(&path);
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
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "CRC32C mismatch in complete raft frame at offset {} of {:?}",
                            offset, path
                        ),
                    ));
                }

                let log_index = u64::from_le_bytes(frame_body[0..8].try_into().unwrap());
                let entry: EntryOf<MetaDbTypeConfig> = bincode::deserialize(&frame_body[8..])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

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
                let wf = OpenOptions::new().write(true).open(&path)?;
                wf.set_len(valid_len as u64)?;
                wf.sync_data()?;
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
        })
    }

    fn load_hard_state(raft_dir: &Path) -> io::Result<PersistedHardState> {
        let path = raft_dir.join("HARD_STATE");
        if !path.exists() {
            return Ok(PersistedHardState::default());
        }
        let mut raw = Vec::new();
        File::open(&path)?.read_to_end(&mut raw)?;
        let body = verify_crc32c_le(&raw, "Raft HARD_STATE")
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        bincode::deserialize(body)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    fn persist_hard_state(&self) -> io::Result<()> {
        let mut raw = bincode::serialize(&self.hard_state)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        append_crc32c_le(&mut raw);
        let tmp = self.raft_dir.join("HARD_STATE.tmp");
        let dst = self.raft_dir.join("HARD_STATE");
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
        sync_dir(&self.raft_dir).map_err(|e| io::Error::other(e.to_string()))?;
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

        for entry in batch {
            let log_idx = entry.log_id().index;
            let need_new_seg = match self.segments.last() {
                None => true,
                Some(seg) => seg.byte_len >= self.max_segment_bytes,
            };
            if need_new_seg {
                self.rotate_new_segment(log_idx)?;
            }

            let serialized = bincode::serialize(&entry)
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

        if let Some(af) = self.active_file.as_mut() {
            af.sync_data()?;
        }
        Ok(())
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
                let _ = fs::remove_file(seg.path);
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
                let _ = fs::remove_file(&seg.path);
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

    pub fn segment_file_count(&self) -> usize {
        self.inner.lock().unwrap().segments.len()
    }

    /// Triggers opportunistic physical segment cleanup up to `watermarks.safe_purge_index()`
    /// if OpenRaft has already requested a purge or if called after advancing watermarks.
    pub fn try_purge_to_safe_watermark(&self) -> io::Result<usize> {
        let mut guard = self.inner.lock().unwrap();
        let safe_idx = self.watermarks.safe_purge_index();
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
        // Clamp OpenRaft's requested `log_id` against the 3-watermark durability gate!
        let safe_idx = self.watermarks.safe_purge_index().min(log_id.index);
        if safe_idx == 0 {
            return Ok(());
        }
        let mut guard = self.inner.lock().unwrap();
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
