//! Durable write-before-grant `IdRangeAllocator` with `request_uuid` idempotency
//! and 64-bit File/Directory bitspace separation (`Resolution B18`).

use std::collections::{BTreeMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use metadb_core::codec;
use metadb_core::{
    append_crc32c_le, make_file_inode_id, verify_crc32c_le, FileId, HlcTimestamp, InodeId,
    LeaseRenewReject, MetaDbError, Result, ShardId,
};
use metadb_storage::sstable::sync_dir;

pub const GLOBAL_ID_BLOCK_SIZE: u64 = 65_536;
pub const CELL_FILE_ID_BLOCK_SIZE: u64 = 4_096;
pub const CELL_DIR_AFFINITY_BLOCK_SIZE: u64 = 256;
pub const GLOBAL_SHARD_ID_BLOCK_SIZE: u64 = 1_024;
const MAX_CACHED_GRANT_UUIDS: usize = 4_096;
const ALLOCATOR_FORMAT_MAGIC_V2: &[u8; 8] = b"MIDA0002";
const ALLOCATOR_FORMAT_MAGIC: &[u8; 8] = b"MIDA0003";
const ALLOCATOR_HEADER_LEN: usize = 41;

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]
struct ActiveRenameLease {
    token: u64,
    expire_hlc: HlcTimestamp,
    /// Renewal cannot move `expire_hlc` past this instant.
    deadline_hlc: HlcTimestamp,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub enum AllocIdKind {
    FileInode = 1,
    DirInode = 2,
    FileId = 3,
    ShardId = 4,
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]
struct PersistedAllocatorState {
    next_file_inode_seq: u64,
    next_dir_inode_id: InodeId,
    next_file_id: FileId,
    next_shard_id: ShardId,
    rename_fence_token: u64,
    active_rename_lease: Option<ActiveRenameLease>,
    recent_grants: BTreeMap<u128, CachedIdGrant>,
    grant_fifo: VecDeque<u128>,
    /// Next low 64 bits of a `LocalIdBuffer` request UUID. Persisted so a reopened
    /// buffer cannot replay an earlier UUID and receive a different kind of grant.
    next_client_seq: u64,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
struct CachedIdGrant {
    kind: AllocIdKind,
    start: u64,
    count: u64,
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
            next_client_seq: 1,
        }
    }
}

#[derive(Deserialize, wincode::SchemaRead)]
struct PersistedAllocatorStateV2 {
    next_file_inode_seq: u64,
    next_dir_inode_id: InodeId,
    next_file_id: FileId,
    next_shard_id: ShardId,
    rename_fence_token: u64,
    active_rename_lease: Option<(u64, HlcTimestamp)>,
    recent_grants: BTreeMap<u128, CachedIdGrant>,
    grant_fifo: VecDeque<u128>,
    next_client_seq: u64,
}

#[derive(Deserialize, wincode::SchemaRead)]
struct LegacyAllocatorState {
    next_file_inode_seq: u64,
    next_dir_inode_id: InodeId,
    next_file_id: FileId,
    next_shard_id: ShardId,
    rename_fence_token: u64,
    active_rename_lease: Option<(u64, HlcTimestamp)>,
    recent_grants: BTreeMap<u128, (u64, u64)>,
    grant_fifo: VecDeque<u128>,
}

fn allocator_header_ok(body: &[u8]) -> bool {
    body.len() >= ALLOCATOR_HEADER_LEN
        && body[8..40].iter().all(|byte| *byte == 0)
        && body[40] == 0xff
}

fn lease_from_v2(active: Option<(u64, HlcTimestamp)>) -> Option<ActiveRenameLease> {
    active.map(|(token, expire_hlc)| ActiveRenameLease {
        token,
        expire_hlc,
        // A lease granted before renewal existed cannot be pushed past the
        // expiry that was already published.
        deadline_hlc: expire_hlc,
    })
}

fn decode_allocator_state(body: &[u8]) -> Result<PersistedAllocatorState> {
    if body.starts_with(ALLOCATOR_FORMAT_MAGIC) {
        if !allocator_header_ok(body) {
            return Err(MetaDbError::CodecError(
                "invalid allocator version 3 header".to_string(),
            ));
        }
        return codec::deserialize(&body[ALLOCATOR_HEADER_LEN..])
            .map_err(|e| MetaDbError::CodecError(e.to_string()));
    }
    if body.starts_with(ALLOCATOR_FORMAT_MAGIC_V2) {
        if !allocator_header_ok(body) {
            return Err(MetaDbError::CodecError(
                "invalid allocator version 2 header".to_string(),
            ));
        }
        let old: PersistedAllocatorStateV2 = codec::deserialize(&body[ALLOCATOR_HEADER_LEN..])
            .map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        return Ok(PersistedAllocatorState {
            next_file_inode_seq: old.next_file_inode_seq,
            next_dir_inode_id: old.next_dir_inode_id,
            next_file_id: old.next_file_id,
            next_shard_id: old.next_shard_id,
            rename_fence_token: old.rename_fence_token,
            active_rename_lease: lease_from_v2(old.active_rename_lease),
            recent_grants: old.recent_grants,
            grant_fifo: old.grant_fifo,
            next_client_seq: old.next_client_seq,
        });
    }
    if let Ok(current) = codec::deserialize::<PersistedAllocatorState>(body) {
        return Ok(current);
    }
    let old: LegacyAllocatorState = codec::deserialize(body)
        .map_err(|e| MetaDbError::CodecError(format!("ID_ALLOCATOR_STATE: {e}")))?;
    // Legacy tuples do not record the ID kind. Keep the UUID so a replay is
    // rejected, and keep the counters so new grants do not reuse those IDs.
    let recent_grants = old
        .recent_grants
        .into_iter()
        .map(|(uuid, (start, _))| {
            (
                uuid,
                CachedIdGrant {
                    kind: AllocIdKind::FileId,
                    start,
                    count: 0,
                },
            )
        })
        .collect();
    Ok(PersistedAllocatorState {
        next_file_inode_seq: old.next_file_inode_seq,
        next_dir_inode_id: old.next_dir_inode_id,
        next_file_id: old.next_file_id,
        next_shard_id: old.next_shard_id,
        rename_fence_token: old.rename_fence_token,
        active_rename_lease: lease_from_v2(old.active_rename_lease),
        recent_grants,
        grant_fifo: old.grant_fifo,
        next_client_seq: 1,
    })
}

fn namespaced_initial_state(node_id: u64) -> PersistedAllocatorState {
    let mut state = PersistedAllocatorState::default();
    if node_id > 1 {
        let high = node_id << 48;
        state.next_file_inode_seq = high + 1;
        state.next_dir_inode_id = high + 2;
        state.next_file_id = high + 1;
        state.next_shard_id = (node_id << 32) + 4;
    }
    state
}

fn allocator_is_pristine(state: &PersistedAllocatorState) -> bool {
    let fresh = PersistedAllocatorState::default();
    state.next_file_inode_seq == fresh.next_file_inode_seq
        && state.next_dir_inode_id == fresh.next_dir_inode_id
        && state.next_file_id == fresh.next_file_id
        && state.next_shard_id == fresh.next_shard_id
        && state.rename_fence_token == 0
        && state.active_rename_lease.is_none()
        && state.recent_grants.is_empty()
        && state.grant_fifo.is_empty()
        && state.next_client_seq == fresh.next_client_seq
}

/// Crash-safe persistent ID & lease allocator enforcing **Write-Before-Grant** (`Resolution B18`).
pub struct DurableIdAllocator {
    state_dir: PathBuf,
    _lock_file: File,
    state: Mutex<PersistedAllocatorState>,
    /// Test hook: the next `n` persists fail before any byte is written.
    fail_persists: AtomicU32,
}

impl DurableIdAllocator {
    pub fn open(state_dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_for_node(state_dir, 1)
    }

    /// Opens the allocator for `node_id`.
    ///
    /// A fresh directory for `node_id > 1` starts in that node's namespace so its
    /// inode, file, and shard ids cannot collide with node 1 or any other node id
    /// in `1..=32767`. An allocator that has already granted ids is reopened as stored.
    pub fn open_for_node(state_dir: impl AsRef<Path>, node_id: u64) -> Result<Self> {
        crate::identity::node_id_in_ha_range(node_id)?;
        let state_dir = state_dir.as_ref().to_path_buf();
        fs::create_dir_all(&state_dir)?;
        let had_existing_files = fs::read_dir(&state_dir)?.next().transpose()?.is_some();
        let lock_path = state_dir.join("LOCK");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file.try_lock().map_err(|e| {
            MetaDbError::StorageIoError(format!(
                "failed to acquire exclusive allocator lock on {}: {e}",
                lock_path.display()
            ))
        })?;
        let path = state_dir.join("ID_ALLOCATOR_STATE");
        let state = if path.exists() {
            let mut raw = Vec::new();
            File::open(&path)?.read_to_end(&mut raw)?;
            let body = verify_crc32c_le(&raw, "ID_ALLOCATOR_STATE")?;
            let mut loaded = decode_allocator_state(body)?;
            if node_id > 1 && allocator_is_pristine(&loaded) {
                loaded = namespaced_initial_state(node_id);
                Self::write_state_file(&state_dir, &loaded)?;
            }
            loaded
        } else if had_existing_files {
            return Err(MetaDbError::StorageIoError(
                "ID_ALLOCATOR_STATE is missing from an existing allocator directory".to_string(),
            ));
        } else {
            let init = namespaced_initial_state(node_id);
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

    /// Test hook. The next `n` `persist_sync` calls fail and leave the in-memory watermark unchanged.
    pub fn testing_fail_next_persists(&self, n: u32) {
        self.fail_persists.store(n, Ordering::Relaxed);
    }

    /// True when the on-disk state (not the in-memory cache) records `request_uuid`.
    pub fn durable_grant_on_disk(&self, request_uuid: u128) -> Result<bool> {
        let path = self.state_dir.join("ID_ALLOCATOR_STATE");
        if !path.exists() {
            return Ok(false);
        }
        let mut raw = Vec::new();
        File::open(&path)?.read_to_end(&mut raw)?;
        let body = verify_crc32c_le(&raw, "ID_ALLOCATOR_STATE")?;
        let st = decode_allocator_state(body)?;
        Ok(st.recent_grants.contains_key(&request_uuid))
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
            if let Some(cached) = guard.recent_grants.get(&request_uuid) {
                if cached.kind != kind {
                    return Err(MetaDbError::StorageIoError(format!(
                        "request uuid {request_uuid:#x} already granted {:?} ids, refused for {kind:?}",
                        cached.kind
                    )));
                }
                if cached.count != count {
                    return Err(MetaDbError::StorageIoError(format!(
                        "request uuid {request_uuid:#x} already granted {} ids, refused for count {count}",
                        cached.count
                    )));
                }
                return Ok((cached.start, cached.count));
            }
        }

        let mut next = guard.clone();
        let start =
            match kind {
                AllocIdKind::FileInode => {
                    let s = next.next_file_inode_seq;
                    next.next_file_inode_seq = next
                        .next_file_inode_seq
                        .checked_add(count)
                        .filter(|end| *end <= metadb_core::FILE_INODE_TAG_BIT)
                        .ok_or_else(|| {
                            MetaDbError::StorageIoError("file inode bitspace exhausted".to_string())
                        })?;
                    make_file_inode_id(s)
                }
                AllocIdKind::DirInode => {
                    let s = next.next_dir_inode_id;
                    let advanced = next.next_dir_inode_id.checked_add(count).ok_or_else(|| {
                        MetaDbError::StorageIoError("dir inode overflow".to_string())
                    })?;
                    if (advanced & metadb_core::FILE_INODE_TAG_BIT) != 0 {
                        return Err(MetaDbError::StorageIoError(
                            "directory PID bitspace exhausted".to_string(),
                        ));
                    }
                    next.next_dir_inode_id = advanced;
                    s
                }
                AllocIdKind::FileId => {
                    let s = next.next_file_id;
                    next.next_file_id = next.next_file_id.checked_add(count).ok_or_else(|| {
                        MetaDbError::StorageIoError("file_id overflow".to_string())
                    })?;
                    s
                }
                AllocIdKind::ShardId => {
                    let s = next.next_shard_id;
                    next.next_shard_id =
                        next.next_shard_id.checked_add(count).ok_or_else(|| {
                            MetaDbError::StorageIoError("shard_id overflow".to_string())
                        })?;
                    s
                }
            };

        let grant = CachedIdGrant { kind, start, count };
        if request_uuid != 0 {
            if next.grant_fifo.len() >= MAX_CACHED_GRANT_UUIDS {
                if let Some(old_uuid) = next.grant_fifo.pop_front() {
                    next.recent_grants.remove(&old_uuid);
                }
            }
            next.grant_fifo.push_back(request_uuid);
            next.recent_grants.insert(request_uuid, grant);
        }

        // Cache the grant and bump the watermark only after the new state is durable.
        self.persist_sync(&next)?;
        *guard = next;
        Ok((start, count))
    }

    /// Durable request UUID for `LocalIdBuffer`. The counter survives reopen, so a new
    /// buffer cannot reuse the UUID of a directory grant and receive directory IDs for a file.
    pub fn next_client_request_uuid(&self, node_id: u64) -> Result<u128> {
        let mut guard = self.state.lock().unwrap();
        let mut next = guard.clone();
        let seq = next.next_client_seq;
        next.next_client_seq = seq.checked_add(1).ok_or_else(|| {
            MetaDbError::StorageIoError("client request uuid sequence overflow".to_string())
        })?;
        self.persist_sync(&next)?;
        *guard = next;
        Ok(((node_id as u128) << 64) | (seq as u128))
    }

    /// Acquires a monotonic fenced `DirRenameLease` (`Resolution B13`), rejecting concurrent leases
    /// until the active lease is released or expired.
    ///
    /// `max_lifetime_ms` is the hard cap measured from this grant. Renewal can push
    /// `expire_hlc` forward, but not past `now + max_lifetime_ms`.
    pub fn acquire_dir_rename_lease(
        &self,
        now_hlc: HlcTimestamp,
        ttl_ms: u64,
        max_lifetime_ms: u64,
    ) -> Result<(u64, HlcTimestamp)> {
        let mut guard = self.state.lock().unwrap();
        if let Some(active) = &guard.active_rename_lease {
            if now_hlc <= active.expire_hlc {
                return Err(MetaDbError::TransactionAborted(
                    "another cross-directory directory rename lease is currently active"
                        .to_string(),
                ));
            }
        }
        // A caller-supplied TTL must not raise the hard cap. Reject it instead
        // of granting a longer fence. The renewal deadline is the same cap.
        let cap = max_lifetime_ms.max(100);
        let ttl = ttl_ms.max(100);
        if ttl > cap {
            return Err(MetaDbError::StorageIoError(
                "rename lease TTL exceeds the lifetime cap".to_string(),
            ));
        }
        let expire_ms = now_hlc
            .physical_ms()
            .checked_add(ttl)
            .ok_or_else(|| MetaDbError::StorageIoError("rename lease TTL overflow".to_string()))?;
        let deadline_ms = now_hlc.physical_ms().checked_add(cap).ok_or_else(|| {
            MetaDbError::StorageIoError("rename lease lifetime overflow".to_string())
        })?;
        let mut next = guard.clone();
        next.rename_fence_token = next.rename_fence_token.saturating_add(1);
        let token = next.rename_fence_token;
        let expire_hlc = HlcTimestamp::new(expire_ms, 0);
        next.active_rename_lease = Some(ActiveRenameLease {
            token,
            expire_hlc,
            deadline_hlc: HlcTimestamp::new(deadline_ms, 0),
        });
        self.persist_sync(&next)?;
        *guard = next;
        Ok((token, expire_hlc))
    }

    /// Extends the active lease for `token` only. A different token, an expired
    /// lease, or a deadline that has already passed is rejected and does not
    /// move `expire_hlc`.
    pub fn extend_dir_rename_lease(
        &self,
        token: u64,
        now_hlc: HlcTimestamp,
        ttl_ms: u64,
    ) -> Result<HlcTimestamp> {
        let mut guard = self.state.lock().unwrap();
        let Some(active) = guard.active_rename_lease.clone() else {
            return Err(MetaDbError::LeaseRenewRejected(
                LeaseRenewReject::RenameLeaseNotActive,
            ));
        };
        if active.token != token {
            return Err(MetaDbError::LeaseRenewRejected(
                LeaseRenewReject::RenameLeaseTokenMismatch,
            ));
        }
        if now_hlc > active.expire_hlc {
            return Err(MetaDbError::LeaseRenewRejected(
                LeaseRenewReject::RenameLeaseExpired,
            ));
        }
        let deadline_ms = active.deadline_hlc.physical_ms();
        if now_hlc.physical_ms() >= deadline_ms {
            return Err(MetaDbError::LeaseRenewRejected(
                LeaseRenewReject::RenameLeaseCap,
            ));
        }
        let expire_ms = now_hlc
            .physical_ms()
            .saturating_add(ttl_ms.max(100))
            .min(deadline_ms);
        if expire_ms <= active.expire_hlc.physical_ms() {
            return Ok(active.expire_hlc);
        }
        let expire_hlc = HlcTimestamp::new(expire_ms, 0);
        let mut next = guard.clone();
        next.active_rename_lease = Some(ActiveRenameLease {
            expire_hlc,
            ..active
        });
        self.persist_sync(&next)?;
        *guard = next;
        Ok(expire_hlc)
    }

    pub fn release_dir_rename_lease(&self, token: u64) -> Result<()> {
        let mut guard = self.state.lock().unwrap();
        if let Some(active) = &guard.active_rename_lease {
            if active.token == token {
                let mut next = guard.clone();
                next.active_rename_lease = None;
                self.persist_sync(&next)?;
                *guard = next;
            }
        }
        Ok(())
    }

    fn persist_sync(&self, st: &PersistedAllocatorState) -> Result<()> {
        if self.fail_persists.load(Ordering::Relaxed) > 0 {
            self.fail_persists.fetch_sub(1, Ordering::Relaxed);
            return Err(MetaDbError::StorageIoError(
                "injected persist_sync failure".to_string(),
            ));
        }
        Self::write_state_file(&self.state_dir, st)
    }

    fn write_state_file(dir: &Path, st: &PersistedAllocatorState) -> Result<()> {
        let payload = codec::serialize(st).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        let mut raw = Vec::with_capacity(ALLOCATOR_HEADER_LEN + payload.len());
        raw.extend_from_slice(ALLOCATOR_FORMAT_MAGIC);
        raw.extend(std::iter::repeat_n(0u8, 32));
        raw.push(0xff);
        raw.extend_from_slice(&payload);
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
    remaining: u64,
}

/// Fast local consumer buffer leasing blocks from `DurableIdAllocator` with zero per-file disk I/O.
pub struct LocalIdBuffer {
    allocator: std::sync::Arc<DurableIdAllocator>,
    node_id: u64,
    file_inodes: Mutex<LocalBlockCursor>,
    dir_inodes: Mutex<LocalBlockCursor>,
    file_ids: Mutex<LocalBlockCursor>,
}

impl LocalIdBuffer {
    pub fn new(allocator: std::sync::Arc<DurableIdAllocator>, node_id: u64) -> Self {
        Self {
            allocator,
            node_id,
            file_inodes: Mutex::new(LocalBlockCursor::default()),
            dir_inodes: Mutex::new(LocalBlockCursor::default()),
            file_ids: Mutex::new(LocalBlockCursor::default()),
        }
    }

    fn next_uuid(&self) -> Result<u128> {
        self.allocator.next_client_request_uuid(self.node_id)
    }

    fn take_next(g: &mut LocalBlockCursor) -> Result<u64> {
        let id = g.next;
        g.remaining -= 1;
        if g.remaining > 0 {
            g.next = g.next.checked_add(1).ok_or_else(|| {
                MetaDbError::StorageIoError("local ID cursor overflow".to_string())
            })?;
        }
        Ok(id)
    }

    pub fn next_file_inode_id(&self) -> Result<InodeId> {
        let mut g = self.file_inodes.lock().unwrap();
        if g.remaining == 0 {
            let (start, count) = self.allocator.allocate_block(
                self.next_uuid()?,
                AllocIdKind::FileInode,
                CELL_FILE_ID_BLOCK_SIZE,
            )?;
            g.next = start;
            g.remaining = count;
        }
        Self::take_next(&mut g)
    }

    pub fn next_dir_inode_id(&self) -> Result<InodeId> {
        let mut g = self.dir_inodes.lock().unwrap();
        if g.remaining == 0 {
            let (start, count) = self.allocator.allocate_block(
                self.next_uuid()?,
                AllocIdKind::DirInode,
                CELL_DIR_AFFINITY_BLOCK_SIZE,
            )?;
            g.next = start;
            g.remaining = count;
        }
        Self::take_next(&mut g)
    }

    pub fn next_file_id(&self) -> Result<FileId> {
        let mut g = self.file_ids.lock().unwrap();
        if g.remaining == 0 {
            let (start, count) = self.allocator.allocate_block(
                self.next_uuid()?,
                AllocIdKind::FileId,
                CELL_FILE_ID_BLOCK_SIZE,
            )?;
            g.next = start;
            g.remaining = count;
        }
        Self::take_next(&mut g)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metadb_core::{is_dir_inode_id, is_file_inode_id};
    use tempfile::tempdir;

    #[test]
    fn reopened_local_buffer_does_not_reuse_directory_grant_as_file_inode() {
        let dir = tempdir().unwrap();
        let alloc = std::sync::Arc::new(DurableIdAllocator::open(dir.path()).unwrap());
        let dir_id = {
            let buf = LocalIdBuffer::new(alloc.clone(), 7);
            let id = buf.next_dir_inode_id().unwrap();
            assert!(
                is_dir_inode_id(id),
                "directory id {id:#x} is tagged as a file"
            );
            id
        };
        let buf = LocalIdBuffer::new(alloc, 7);
        let file_id = buf.next_file_inode_id().unwrap();
        assert!(
            is_file_inode_id(file_id),
            "reopened buffer returned a directory inode {file_id:#x} for a file"
        );
        assert_ne!(file_id, dir_id);
    }

    #[test]
    fn request_uuid_cannot_change_id_kind() {
        let dir = tempdir().unwrap();
        let alloc = DurableIdAllocator::open(dir.path()).unwrap();
        alloc.allocate_block(9, AllocIdKind::DirInode, 4).unwrap();
        let err = alloc
            .allocate_block(9, AllocIdKind::FileInode, 4)
            .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("already granted"),
            "unexpected error: {message}"
        );
    }
}
