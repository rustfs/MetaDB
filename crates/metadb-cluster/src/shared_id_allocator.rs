//! One durable id-block authority for every cell that calls it.
//!
//! Grants are written before they are returned. A reopen continues after the
//! last persisted block, so a crash cannot hand that block out again. Two
//! `cell_id` values share this cursor; their blocks do not overlap.
//!
//! This file is not `GLOBAL_CELL_DIRECTORY` and it is not `ID_ALLOCATOR_STATE`.
//! A cell that still mints from its own node-namespaced allocator is not using
//! this authority.

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use metadb_core::codec;
use metadb_core::{append_crc32c_le, make_file_inode_id, verify_crc32c_le, MetaDbError, Result};
use metadb_storage::sstable::sync_dir;

use crate::id_allocator::AllocIdKind;

const SHARED_ALLOCATOR_FILE: &str = "SHARED_ID_ALLOCATOR";
const SHARED_ALLOCATOR_MAGIC: &[u8; 8] = b"MSID0001";
const SHARED_ALLOCATOR_HEADER_LEN: usize = 41;
const MAX_CACHED_GRANT_UUIDS: usize = 4_096;

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]
struct SharedGrant {
    cell_id: u64,
    kind: AllocIdKind,
    start: u64,
    count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]
struct SharedAllocatorState {
    next_file_inode_seq: u64,
    next_dir_inode_id: u64,
    next_file_id: u64,
    next_shard_id: u64,
    recent_grants: BTreeMap<u128, SharedGrant>,
    grant_fifo: VecDeque<u128>,
}

impl Default for SharedAllocatorState {
    fn default() -> Self {
        Self {
            next_file_inode_seq: 1,
            next_dir_inode_id: 2,
            next_file_id: 1,
            next_shard_id: 4,
            recent_grants: BTreeMap::new(),
            grant_fifo: VecDeque::new(),
        }
    }
}

/// Durable shared block issuer. One process holds the file lock.
pub struct SharedIdAllocator {
    state_dir: PathBuf,
    _lock_file: File,
    state: Mutex<SharedAllocatorState>,
    fail_persists: AtomicU32,
}

impl SharedIdAllocator {
    pub fn open(state_dir: impl AsRef<Path>) -> Result<Self> {
        let state_dir = state_dir.as_ref().to_path_buf();
        fs::create_dir_all(&state_dir)?;
        let lock_path = state_dir.join("LOCK");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file.try_lock().map_err(|error| {
            MetaDbError::StorageIoError(format!(
                "failed to acquire exclusive shared id allocator lock on {}: {error}",
                lock_path.display()
            ))
        })?;
        let path = state_dir.join(SHARED_ALLOCATOR_FILE);
        let state = if path.exists() {
            let mut raw = Vec::new();
            File::open(&path)?.read_to_end(&mut raw)?;
            let body = verify_crc32c_le(&raw, SHARED_ALLOCATOR_FILE)?;
            decode_state(body)?
        } else {
            let init = SharedAllocatorState::default();
            Self::write_state_file(&state_dir, &init)?;
            init
        };
        Ok(Self {
            state_dir,
            _lock_file: lock_file,
            state: Mutex::new(state),
            fail_persists: AtomicU32::new(0),
        })
    }

    /// The next `n` persists fail before the file or the in-memory cursor moves.
    pub fn testing_fail_next_persists(&self, n: u32) {
        self.fail_persists.store(n, Ordering::Relaxed);
    }

    /// Grants `count` ids to `cell_id`.
    ///
    /// `request_uuid == 0` is not remembered. Any other uuid returns the same
    /// block when the cell, kind, and count match, and refuses the call when
    /// they do not. The block is durable before this returns `Ok`.
    pub fn allocate(
        &self,
        cell_id: u64,
        request_uuid: u128,
        kind: AllocIdKind,
        count: u64,
    ) -> Result<(u64, u64)> {
        let count = count.max(1);
        let mut guard = self.state.lock().unwrap();
        if request_uuid != 0 {
            if let Some(cached) = guard.recent_grants.get(&request_uuid) {
                if cached.cell_id != cell_id || cached.kind != kind || cached.count != count {
                    return Err(MetaDbError::StorageIoError(format!(
                        "request uuid {request_uuid:#x} already granted cell {} {:?} x{}, refused for cell {cell_id} {kind:?} x{count}",
                        cached.cell_id, cached.kind, cached.count
                    )));
                }
                return Ok((cached.start, cached.count));
            }
        }

        let mut next = guard.clone();
        let start =
            match kind {
                AllocIdKind::FileInode => {
                    let seq = next.next_file_inode_seq;
                    next.next_file_inode_seq = next
                        .next_file_inode_seq
                        .checked_add(count)
                        .filter(|end| *end <= metadb_core::FILE_INODE_TAG_BIT)
                        .ok_or_else(|| {
                            MetaDbError::StorageIoError("file inode bitspace exhausted".to_string())
                        })?;
                    make_file_inode_id(seq)
                }
                AllocIdKind::DirInode => {
                    let seq = next.next_dir_inode_id;
                    let advanced = next.next_dir_inode_id.checked_add(count).ok_or_else(|| {
                        MetaDbError::StorageIoError("dir inode overflow".to_string())
                    })?;
                    if (advanced & metadb_core::FILE_INODE_TAG_BIT) != 0 {
                        return Err(MetaDbError::StorageIoError(
                            "directory PID bitspace exhausted".to_string(),
                        ));
                    }
                    next.next_dir_inode_id = advanced;
                    seq
                }
                AllocIdKind::FileId => {
                    let seq = next.next_file_id;
                    next.next_file_id = next.next_file_id.checked_add(count).ok_or_else(|| {
                        MetaDbError::StorageIoError("file_id overflow".to_string())
                    })?;
                    seq
                }
                AllocIdKind::ShardId => {
                    let seq = next.next_shard_id;
                    next.next_shard_id =
                        next.next_shard_id.checked_add(count).ok_or_else(|| {
                            MetaDbError::StorageIoError("shard_id overflow".to_string())
                        })?;
                    seq
                }
            };
        if request_uuid != 0 {
            if next.grant_fifo.len() >= MAX_CACHED_GRANT_UUIDS {
                if let Some(old_uuid) = next.grant_fifo.pop_front() {
                    next.recent_grants.remove(&old_uuid);
                }
            }
            next.grant_fifo.push_back(request_uuid);
            next.recent_grants.insert(
                request_uuid,
                SharedGrant {
                    cell_id,
                    kind,
                    start,
                    count,
                },
            );
        }
        self.persist_sync(&next)?;
        *guard = next;
        Ok((start, count))
    }

    fn persist_sync(&self, state: &SharedAllocatorState) -> Result<()> {
        if self.fail_persists.load(Ordering::Relaxed) > 0 {
            self.fail_persists.fetch_sub(1, Ordering::Relaxed);
            return Err(MetaDbError::StorageIoError(
                "injected shared allocator persist failure".to_string(),
            ));
        }
        Self::write_state_file(&self.state_dir, state)
    }

    fn write_state_file(dir: &Path, state: &SharedAllocatorState) -> Result<()> {
        let payload =
            codec::serialize(state).map_err(|error| MetaDbError::CodecError(error.to_string()))?;
        let mut raw = Vec::with_capacity(SHARED_ALLOCATOR_HEADER_LEN + payload.len());
        raw.extend_from_slice(SHARED_ALLOCATOR_MAGIC);
        raw.extend(std::iter::repeat_n(0u8, 32));
        raw.push(0xff);
        raw.extend_from_slice(&payload);
        append_crc32c_le(&mut raw);
        let tmp = dir.join(format!("{SHARED_ALLOCATOR_FILE}.tmp"));
        let dst = dir.join(SHARED_ALLOCATOR_FILE);
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
}

fn header_ok(body: &[u8]) -> bool {
    body.len() >= SHARED_ALLOCATOR_HEADER_LEN
        && body.starts_with(SHARED_ALLOCATOR_MAGIC)
        && body[8..40].iter().all(|byte| *byte == 0)
        && body[40] == 0xff
}

fn decode_state(body: &[u8]) -> Result<SharedAllocatorState> {
    if !header_ok(body) {
        return Err(MetaDbError::CodecError(
            "invalid shared id allocator header".to_string(),
        ));
    }
    codec::deserialize(&body[SHARED_ALLOCATOR_HEADER_LEN..])
        .map_err(|error| MetaDbError::CodecError(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blocks_disjoint(ranges: &[(u64, u64)]) -> bool {
        let mut ordered = ranges.to_vec();
        ordered.sort_by_key(|(start, _)| *start);
        for pair in ordered.windows(2) {
            let (start, count) = pair[0];
            if start + count > pair[1].0 {
                return false;
            }
        }
        true
    }

    #[test]
    fn two_cells_get_disjoint_blocks_and_restart_does_not_reissue() {
        let dir = tempfile::tempdir().unwrap();
        let first = SharedIdAllocator::open(dir.path()).unwrap();
        let cell_a = first.allocate(1, 0xA, AllocIdKind::FileId, 4).unwrap();
        let cell_b = first.allocate(2, 0xB, AllocIdKind::FileId, 4).unwrap();
        let replay = first.allocate(1, 0xA, AllocIdKind::FileId, 4).unwrap();
        assert_eq!(replay, cell_a);
        assert!(blocks_disjoint(&[cell_a, cell_b]));
        let refused = first.allocate(2, 0xA, AllocIdKind::FileId, 4);
        assert!(
            refused.is_err(),
            "a uuid granted to cell 1 is not reissued to cell 2"
        );
        drop(first);

        let reopened = SharedIdAllocator::open(dir.path()).unwrap();
        let again = reopened.allocate(1, 0xA, AllocIdKind::FileId, 4).unwrap();
        assert_eq!(again, cell_a, "restart returns the persisted grant");
        let next = reopened.allocate(2, 0xC, AllocIdKind::FileId, 4).unwrap();
        assert!(blocks_disjoint(&[cell_a, cell_b, next]));
        assert!(next.0 >= cell_b.0 + cell_b.1);
    }

    #[test]
    fn failed_persist_does_not_consume_a_block() {
        let dir = tempfile::tempdir().unwrap();
        let allocator = SharedIdAllocator::open(dir.path()).unwrap();
        allocator.testing_fail_next_persists(1);
        let failed = allocator.allocate(1, 1, AllocIdKind::FileId, 8);
        assert!(failed.is_err());
        let granted = allocator.allocate(2, 2, AllocIdKind::FileId, 8).unwrap();
        assert_eq!(granted.0, 1, "the failed call must not advance the cursor");
        drop(allocator);
        let reopened = SharedIdAllocator::open(dir.path()).unwrap();
        let next = reopened.allocate(1, 3, AllocIdKind::FileId, 8).unwrap();
        assert_eq!(next.0, granted.0 + granted.1);
    }

    #[test]
    fn a_second_open_waits_until_the_lock_is_released() {
        let dir = tempfile::tempdir().unwrap();
        let held = SharedIdAllocator::open(dir.path()).unwrap();
        let blocked = SharedIdAllocator::open(dir.path());
        assert!(blocked.is_err());
        drop(held);
        assert!(SharedIdAllocator::open(dir.path()).is_ok());
    }
}
