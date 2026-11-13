//! Durable write-before-grant `IdRangeAllocator` with `request_uuid` idempotency
//! and 64-bit File/Directory bitspace separation (`Resolution B18`).

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use metadb_core::{
    append_crc32c_le, make_file_inode_id, verify_crc32c_le, FileId, HlcTimestamp, InodeId,
    MetaDbError, Result, ShardId,
};
use metadb_storage::sstable::sync_dir;

pub const GLOBAL_ID_BLOCK_SIZE: u64 = 65_536;
pub const CELL_FILE_ID_BLOCK_SIZE: u64 = 4_096;
pub const CELL_DIR_AFFINITY_BLOCK_SIZE: u64 = 256;
pub const GLOBAL_SHARD_ID_BLOCK_SIZE: u64 = 1_024;
const MAX_CACHED_GRANT_UUIDS: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AllocIdKind {
    FileInode = 1,
    DirInode = 2,
    FileId = 3,
    ShardId = 4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedAllocatorState {
    next_file_inode_seq: u64,
    next_dir_inode_id: InodeId,
    next_file_id: FileId,
    next_shard_id: ShardId,
    rename_fence_token: u64,
    active_rename_lease: Option<(u64, HlcTimestamp)>,
    recent_grants: BTreeMap<u128, (u64, u64)>,
    grant_fifo: VecDeque<u128>,
}

impl Default for PersistedAllocatorState {
    fn default() -> Self {
        Self {
            next_file_inode_seq: 1,
            // 0 = invalid, 1 = ROOT_INODE_ID -> start dynamic directory IDs at 2!
            next_dir_inode_id: 2,
            next_file_id: 1,
            // Shards 1, 2, 3 reserved for initial bootstrap table shards -> start dynamic shards at 4!
            next_shard_id: 4,
            rename_fence_token: 0,
            active_rename_lease: None,
            recent_grants: BTreeMap::new(),
            grant_fifo: VecDeque::new(),
        }
    }
}

/// Crash-safe persistent ID & lease allocator enforcing **Write-Before-Grant** (`Resolution B18`).
pub struct DurableIdAllocator {
    state_dir: PathBuf,
    state: Mutex<PersistedAllocatorState>,
}

impl DurableIdAllocator {
    pub fn open(state_dir: impl AsRef<Path>) -> Result<Self> {
        let state_dir = state_dir.as_ref().to_path_buf();
        fs::create_dir_all(&state_dir)?;
        let path = state_dir.join("ID_ALLOCATOR_STATE");
        let state = if path.exists() {
            let mut raw = Vec::new();
            File::open(&path)?.read_to_end(&mut raw)?;
            let body = verify_crc32c_le(&raw, "ID_ALLOCATOR_STATE")?;
            bincode::deserialize(body).map_err(|e| MetaDbError::CodecError(e.to_string()))?
        } else {
            let init = PersistedAllocatorState::default();
            Self::persist_sync(&state_dir, &init)?;
            init
        };
        Ok(Self {
            state_dir,
            state: Mutex::new(state),
        })
    }

    /// Durably allocates `(start_id, count)` with `request_uuid` idempotency (`fdatasync` BEFORE return).
    /// Note: For `AllocIdKind::FileInode`, callers combine returned sequential `seq` via `make_file_inode_id(seq)`
    /// (setting Bit 63 = `1`), whereas `DirInode` keeps Bit 63 = `0`!
    pub fn allocate_block(
        &self,
        request_uuid: u128,
        kind: AllocIdKind,
        count: u64,
    ) -> Result<(u64, u64)> {
        let count = count.max(1);
        let mut guard = self.state.lock().unwrap();
        if request_uuid != 0 {
            if let Some(&cached) = guard.recent_grants.get(&request_uuid) {
                return Ok(cached);
            }
        }

        let start = match kind {
            AllocIdKind::FileInode => {
                let s = guard.next_file_inode_seq;
                guard.next_file_inode_seq = guard
                    .next_file_inode_seq
                    .checked_add(count)
                    .ok_or_else(|| {
                        MetaDbError::StorageIoError("file inode seq overflow".to_string())
                    })?;
                make_file_inode_id(s)
            }
            AllocIdKind::DirInode => {
                let s = guard.next_dir_inode_id;
                let next = guard
                    .next_dir_inode_id
                    .checked_add(count)
                    .ok_or_else(|| MetaDbError::StorageIoError("dir inode overflow".to_string()))?;
                if (next & metadb_core::FILE_INODE_TAG_BIT) != 0 {
                    return Err(MetaDbError::StorageIoError(
                        "directory PID bitspace exhausted".to_string(),
                    ));
                }
                guard.next_dir_inode_id = next;
                s
            }
            AllocIdKind::FileId => {
                let s = guard.next_file_id;
                guard.next_file_id = guard
                    .next_file_id
                    .checked_add(count)
                    .ok_or_else(|| MetaDbError::StorageIoError("file_id overflow".to_string()))?;
                s
            }
            AllocIdKind::ShardId => {
                let s = guard.next_shard_id;
                guard.next_shard_id = guard
                    .next_shard_id
                    .checked_add(count)
                    .ok_or_else(|| MetaDbError::StorageIoError("shard_id overflow".to_string()))?;
                s
            }
        };

        let grant = (start, count);
        if request_uuid != 0 {
            if guard.grant_fifo.len() >= MAX_CACHED_GRANT_UUIDS {
                if let Some(old_uuid) = guard.grant_fifo.pop_front() {
                    guard.recent_grants.remove(&old_uuid);
                }
            }
            guard.grant_fifo.push_back(request_uuid);
            guard.recent_grants.insert(request_uuid, grant);
        }

        // Persist incremented high-water mark BEFORE returning grant!
        Self::persist_sync(&self.state_dir, &guard)?;
        Ok(grant)
    }

    /// Acquires a monotonic fenced `DirRenameLease` (`Resolution B13`), rejecting concurrent leases
    /// until the active lease is released or expired.
    pub fn acquire_dir_rename_lease(
        &self,
        now_hlc: HlcTimestamp,
        ttl_ms: u64,
    ) -> Result<(u64, HlcTimestamp)> {
        let mut guard = self.state.lock().unwrap();
        if let Some((_, active_exp)) = guard.active_rename_lease {
            if now_hlc <= active_exp {
                return Err(MetaDbError::TransactionAborted(
                    "another cross-directory directory rename lease is currently active"
                        .to_string(),
                ));
            }
        }
        guard.rename_fence_token = guard.rename_fence_token.saturating_add(1);
        let token = guard.rename_fence_token;
        let expire_hlc = HlcTimestamp::new(now_hlc.physical_ms() + ttl_ms.max(100), 0);
        guard.active_rename_lease = Some((token, expire_hlc));
        Self::persist_sync(&self.state_dir, &guard)?;
        Ok((token, expire_hlc))
    }

    pub fn release_dir_rename_lease(&self, token: u64) -> Result<()> {
        let mut guard = self.state.lock().unwrap();
        if let Some((active_tok, _)) = guard.active_rename_lease {
            if active_tok == token {
                guard.active_rename_lease = None;
                Self::persist_sync(&self.state_dir, &guard)?;
            }
        }
        Ok(())
    }

    fn persist_sync(dir: &Path, st: &PersistedAllocatorState) -> Result<()> {
        let mut raw = bincode::serialize(st).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        append_crc32c_le(&mut raw);
        let tmp = dir.join("ID_ALLOCATOR_STATE.tmp");
        let dst = dir.join("ID_ALLOCATOR_STATE");
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
        sync_dir(dir)?;
        Ok(())
    }
}

#[derive(Default)]
struct LocalBlockCursor {
    next: u64,
    end: u64,
}

/// Fast local consumer buffer leasing blocks from `DurableIdAllocator` with zero per-file disk I/O.
pub struct LocalIdBuffer {
    allocator: std::sync::Arc<DurableIdAllocator>,
    uuid_seed: u128,
    seq: std::sync::atomic::AtomicU64,
    file_inodes: Mutex<LocalBlockCursor>,
    dir_inodes: Mutex<LocalBlockCursor>,
    file_ids: Mutex<LocalBlockCursor>,
}

impl LocalIdBuffer {
    pub fn new(allocator: std::sync::Arc<DurableIdAllocator>, node_id: u64) -> Self {
        Self {
            allocator,
            uuid_seed: (node_id as u128) << 64,
            seq: std::sync::atomic::AtomicU64::new(1),
            file_inodes: Mutex::new(LocalBlockCursor::default()),
            dir_inodes: Mutex::new(LocalBlockCursor::default()),
            file_ids: Mutex::new(LocalBlockCursor::default()),
        }
    }

    fn next_uuid(&self) -> u128 {
        self.uuid_seed | (self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u128)
    }

    pub fn next_file_inode_id(&self) -> Result<InodeId> {
        let mut g = self.file_inodes.lock().unwrap();
        if g.next >= g.end {
            let (start, count) = self.allocator.allocate_block(
                self.next_uuid(),
                AllocIdKind::FileInode,
                CELL_FILE_ID_BLOCK_SIZE,
            )?;
            g.next = start;
            g.end = start + count;
        }
        let id = g.next;
        g.next += 1;
        Ok(id)
    }

    pub fn next_dir_inode_id(&self) -> Result<InodeId> {
        let mut g = self.dir_inodes.lock().unwrap();
        if g.next >= g.end {
            let (start, count) = self.allocator.allocate_block(
                self.next_uuid(),
                AllocIdKind::DirInode,
                CELL_DIR_AFFINITY_BLOCK_SIZE,
            )?;
            g.next = start;
            g.end = start + count;
        }
        let id = g.next;
        g.next += 1;
        Ok(id)
    }

    pub fn next_file_id(&self) -> Result<FileId> {
        let mut g = self.file_ids.lock().unwrap();
        if g.next >= g.end {
            let (start, count) = self.allocator.allocate_block(
                self.next_uuid(),
                AllocIdKind::FileId,
                CELL_FILE_ID_BLOCK_SIZE,
            )?;
            g.next = start;
            g.end = start + count;
        }
        let id = g.next;
        g.next += 1;
        Ok(id)
    }
}
