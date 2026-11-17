use std::sync::{Arc, Mutex};

use metadb_core::schema::validate_user_entry_name;
use metadb_core::{
    negate_accounted_size, validate_dir_inode_id, validate_file_size, EntryType, FileId,
    HlcTimestamp, InodeAttr, InodeId, MetaDbError, MetaEntry, MetaKey, Result, UserId,
    ATTR_ENTRY_NAME,
};
use metadb_storage::LifecycleStorageEngine;

use crate::attr_aggregator::AsyncAttrAggregator;
use crate::aux_sink::AsyncAuxiliarySink;
use crate::rename_cycle::RenameCycleWalker;

/// Single-Shard Transaction Logic Pushdown Executor (§4.2 of `arXiv:2610.32247v1`).
///
/// Because MetaDB's sharding policy keeps `(PID, "_attr_")` and all child entries `(PID, name)`
/// colocated in the same shard, single-directory Read-Modify-Write (RMW) operations execute
/// directly on the backend shard leader in a single network hop (`1 RTT`), bypassing
/// multi-round proxy coordination.
pub struct PushdownExecutor {
    attr_aggregator: Arc<AsyncAttrAggregator>,
    aux_sink: Arc<AsyncAuxiliarySink>,
    /// Serializes mkdir's inode claim and both namespace writes.
    mkdir_gate: Mutex<()>,
}

impl PushdownExecutor {
    pub fn new(
        attr_aggregator: Arc<AsyncAttrAggregator>,
        aux_sink: Arc<AsyncAuxiliarySink>,
    ) -> Self {
        Self {
            attr_aggregator,
            aux_sink,
            mkdir_gate: Mutex::new(()),
        }
    }

    pub fn attr_aggregator(&self) -> &Arc<AsyncAttrAggregator> {
        &self.attr_aggregator
    }

    pub fn aux_sink(&self) -> &Arc<AsyncAuxiliarySink> {
        &self.aux_sink
    }

    fn validate_parent_delta(
        &self,
        engine: &LifecycleStorageEngine,
        pid: InodeId,
        children: i64,
        bytes: i64,
        hlc: HlcTimestamp,
    ) -> Result<()> {
        let raw = engine
            .get(&MetaKey::dir_attr(pid).encode(), hlc)?
            .ok_or(MetaDbError::DirectoryNotFound(pid))?;
        self.attr_aggregator
            .validate_delta(pid, &InodeAttr::decode(&raw)?, children, bytes)
    }

    /// Initializes the root directory `(ROOT_INODE_ID, "_attr_")` on the shard owning `ROOT_INODE_ID`.
    pub fn init_directory_attr(
        &self,
        engine: &LifecycleStorageEngine,
        dir_id: InodeId,
        parent_pid: InodeId,
        uid: UserId,
        mode: u32,
        hlc: HlcTimestamp,
    ) -> Result<()> {
        validate_dir_inode_id(dir_id)?;
        let attr_key = MetaKey::dir_attr(dir_id).encode();
        if engine.get(&attr_key, hlc)?.is_some() {
            return Err(MetaDbError::AlreadyExists {
                pid: dir_id,
                name: ATTR_ENTRY_NAME.to_string(),
            });
        }
        let attr = InodeAttr {
            inode_id: dir_id,
            parent_pid,
            entry_type: EntryType::DirectoryAttr,
            uid,
            mode,
            link_count: 1,
            num_children: 0,
            size_bytes: 0,
            create_time: hlc,
            modify_time: hlc,
            fid: None,
            applied_lsn: 0,
        };
        engine.put(attr_key, attr.encode(), hlc);
        Ok(())
    }

    /// Single-shard pushdown `create` operation (`1 RTT`):
    /// 1. Validates user entry name (rejecting reserved `"_attr_"`).
    /// 2. Checks parent directory `(pid, "_attr_")` existence and child uniqueness locally.
    /// 3. Writes the new file entry `(pid, name)`.
    /// 4. Enqueues conflict-free parent attribute delta (`+1` child, `+size_bytes`, `modify_time`).
    /// 5. Enqueues asynchronous quota and secondary index updates off the critical path.
    #[allow(clippy::too_many_arguments)]
    pub fn create_file(
        &self,
        engine: &LifecycleStorageEngine,
        pid: InodeId,
        name: &str,
        new_inode_id: InodeId,
        fid: FileId,
        uid: UserId,
        mode: u32,
        size_bytes: i64,
        hlc: HlcTimestamp,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(name)?;
        validate_file_size(size_bytes)?;

        // Verify parent directory exists on this shard.
        let parent_attr_key = MetaKey::dir_attr(pid).encode();
        if engine.get(&parent_attr_key, hlc)?.is_none() {
            return Err(MetaDbError::DirectoryNotFound(pid));
        }

        let child_key = MetaKey::child(pid, name);
        let child_key_bytes = child_key.encode();
        if engine.get(&child_key_bytes, hlc)?.is_some() {
            return Err(MetaDbError::AlreadyExists {
                pid,
                name: name.to_string(),
            });
        }

        let attr = InodeAttr {
            inode_id: new_inode_id,
            parent_pid: pid,
            entry_type: EntryType::File,
            uid,
            mode,
            link_count: 1,
            num_children: 0,
            size_bytes,
            create_time: hlc,
            modify_time: hlc,
            fid: Some(fid),
            applied_lsn: 0,
        };

        engine.put(child_key_bytes, attr.encode(), hlc);

        // Off-critical-path 1: Conflict-free parent attribute aggregation (§5.2).
        self.attr_aggregator
            .record_delta(engine, pid, 1, size_bytes, hlc);

        // Off-critical-path 2: Decoupled quota & secondary index updates (§5.1).
        self.aux_sink
            .enqueue_quota_delta(engine, uid, size_bytes, 1, hlc);
        self.aux_sink
            .enqueue_index_upsert(engine, fid, pid, name.to_string(), hlc, hlc);

        Ok(MetaEntry {
            key: child_key,
            attr,
        })
    }

    /// Single-shard pushdown `mkdir` operation (`1 RTT` for parent entry creation).
    /// Also initializes `(new_dir_id, "_attr_")` on the target shard owning `new_dir_id`.
    #[allow(clippy::too_many_arguments)]
    pub fn mkdir(
        &self,
        parent_engine: &LifecycleStorageEngine,
        new_dir_engine: &LifecycleStorageEngine,
        pid: InodeId,
        name: &str,
        new_dir_id: InodeId,
        uid: UserId,
        mode: u32,
        hlc: HlcTimestamp,
    ) -> Result<MetaEntry> {
        let _gate = self.mkdir_gate.lock().unwrap();
        validate_user_entry_name(name)?;
        validate_dir_inode_id(new_dir_id)?;

        let parent_attr_key = MetaKey::dir_attr(pid).encode();
        if parent_engine.get(&parent_attr_key, hlc)?.is_none() {
            return Err(MetaDbError::DirectoryNotFound(pid));
        }
        let new_attr_key = MetaKey::dir_attr(new_dir_id).encode();
        if new_dir_engine.get(&new_attr_key, hlc)?.is_some() {
            return Err(MetaDbError::AlreadyExists {
                pid: new_dir_id,
                name: ATTR_ENTRY_NAME.to_string(),
            });
        }

        let child_key = MetaKey::child(pid, name);
        let child_key_bytes = child_key.encode();
        if parent_engine.get(&child_key_bytes, hlc)?.is_some() {
            return Err(MetaDbError::AlreadyExists {
                pid,
                name: name.to_string(),
            });
        }

        let dir_entry_attr = InodeAttr {
            inode_id: new_dir_id,
            parent_pid: pid,
            entry_type: EntryType::Directory,
            uid,
            mode,
            link_count: 1,
            num_children: 0,
            size_bytes: 0,
            create_time: hlc,
            modify_time: hlc,
            fid: None,
            applied_lsn: 0,
        };

        // Claim the inode before publishing the parent dentry. A failed claim leaves no alias.
        self.init_directory_attr(new_dir_engine, new_dir_id, pid, uid, mode, hlc)?;
        parent_engine.put(child_key_bytes, dir_entry_attr.encode(), hlc);

        self.attr_aggregator
            .record_delta(parent_engine, pid, 1, 0, hlc);
        self.aux_sink
            .enqueue_quota_delta(parent_engine, uid, 0, 1, hlc);

        Ok(MetaEntry {
            key: child_key,
            attr: dir_entry_attr,
        })
    }

    /// Single-shard pushdown `unlink` operation (`1 RTT`):
    /// Removes a regular file entry and emits off-critical-path parent attribute, quota,
    /// and secondary index deletion updates.
    pub fn unlink(
        &self,
        engine: &LifecycleStorageEngine,
        pid: InodeId,
        name: &str,
        hlc: HlcTimestamp,
    ) -> Result<InodeAttr> {
        validate_user_entry_name(name)?;

        let child_key = MetaKey::child(pid, name).encode();
        let Some(raw) = engine.get(&child_key, hlc)? else {
            return Err(MetaDbError::NotFound {
                pid,
                name: name.to_string(),
            });
        };
        let attr = InodeAttr::decode(&raw)?;
        if attr.entry_type != EntryType::File {
            return Err(MetaDbError::InvalidEntryType {
                expected: "File".to_string(),
                actual: "Directory".to_string(),
            });
        }

        crate::validation::validate_size(attr.size_bytes)?;
        self.validate_parent_delta(engine, pid, -1, -attr.size_bytes, hlc)?;

        engine.delete(child_key, hlc);

        let released = negate_accounted_size(attr.size_bytes);
        self.attr_aggregator
            .record_delta(engine, pid, -1, released, hlc);
        self.aux_sink
            .enqueue_quota_delta(engine, attr.uid, released, -1, hlc);
        if let Some(fid) = attr.fid {
            self.aux_sink.enqueue_index_delete(engine, fid, hlc);
        }

        Ok(attr)
    }

    /// Authoritative `rmdir` operation (resolving Goldfish Finding F2):
    /// Rather than trusting the asynchronously aggregated `num_children` counter, `rmdir`
    /// scans the target directory's live key range `[dir_id, dir_id + 1)` (excluding `"_attr_"`)
    /// to verify that zero live child entries remain before deleting the directory.
    pub fn rmdir(
        &self,
        parent_engine: &LifecycleStorageEngine,
        dir_engine: &LifecycleStorageEngine,
        pid: InodeId,
        name: &str,
        hlc: HlcTimestamp,
    ) -> Result<InodeAttr> {
        validate_user_entry_name(name)?;

        let child_key = MetaKey::child(pid, name).encode();
        let Some(raw) = parent_engine.get(&child_key, hlc)? else {
            return Err(MetaDbError::NotFound {
                pid,
                name: name.to_string(),
            });
        };
        let dir_entry_attr = InodeAttr::decode(&raw)?;
        if dir_entry_attr.entry_type != EntryType::Directory {
            return Err(MetaDbError::InvalidEntryType {
                expected: "Directory".to_string(),
                actual: "File".to_string(),
            });
        }

        let target_dir_id = dir_entry_attr.inode_id;
        let start = MetaKey::encode_pid_prefix_start(target_dir_id);
        let end = MetaKey::encode_pid_prefix_end(target_dir_id);
        let rows = dir_engine.scan_range(&start, &end, hlc)?;

        let mut live_children = 0usize;
        for (k_bytes, _) in rows {
            let decoded_key = MetaKey::decode(&k_bytes)?;
            if !decoded_key.is_attr_key() {
                live_children += 1;
            }
        }
        if live_children > 0 {
            return Err(MetaDbError::DirectoryNotEmpty {
                dir_id: target_dir_id,
                child_count: live_children,
            });
        }

        self.validate_parent_delta(parent_engine, pid, -1, 0, hlc)?;

        parent_engine.delete(child_key, hlc);
        let target_attr_key = MetaKey::dir_attr(target_dir_id).encode();
        dir_engine.delete(target_attr_key, hlc);

        self.attr_aggregator
            .record_delta(parent_engine, pid, -1, 0, hlc);
        self.aux_sink
            .enqueue_quota_delta(parent_engine, dir_entry_attr.uid, 0, -1, hlc);

        Ok(dir_entry_attr)
    }

    /// Point lookup of a child entry `(pid, name)`.
    pub fn lookup(
        &self,
        engine: &LifecycleStorageEngine,
        pid: InodeId,
        name: &str,
        read_hlc: HlcTimestamp,
    ) -> Result<Option<MetaEntry>> {
        validate_user_entry_name(name)?;
        let key = MetaKey::child(pid, name);
        let Some(raw) = engine.get(&key.encode(), read_hlc)? else {
            return Ok(None);
        };
        let attr = InodeAttr::decode(&raw)?;
        Ok(Some(MetaEntry { key, attr }))
    }

    /// Reads the directory attributes `(pid, "_attr_")` and performs Read-Path Reconciliation
    /// with `AsyncAttrAggregator` so the caller always receives linearizable `num_children`,
    /// `size_bytes`, and `modify_time` (§5.2).
    pub fn dirstat(
        &self,
        engine: &LifecycleStorageEngine,
        pid: InodeId,
        read_hlc: HlcTimestamp,
    ) -> Result<InodeAttr> {
        let attr_key = MetaKey::dir_attr(pid).encode();
        let Some(raw) = engine.get(&attr_key, read_hlc)? else {
            return Err(MetaDbError::DirectoryNotFound(pid));
        };
        let persisted = InodeAttr::decode(&raw)?;
        Ok(self.attr_aggregator.reconcile_attr(pid, persisted))
    }

    /// Scans all live child entries in directory `pid` (excluding the internal `"_attr_"` entry).
    pub fn listdir(
        &self,
        engine: &LifecycleStorageEngine,
        pid: InodeId,
        read_hlc: HlcTimestamp,
    ) -> Result<Vec<MetaEntry>> {
        let start = MetaKey::encode_pid_prefix_start(pid);
        let end = MetaKey::encode_pid_prefix_end(pid);
        let rows = engine.scan_range(&start, &end, read_hlc)?;

        let mut entries = Vec::with_capacity(rows.len());
        for (k_bytes, v_bytes) in rows {
            let key = MetaKey::decode(&k_bytes)?;
            if key.is_attr_key() {
                continue;
            }
            let attr = InodeAttr::decode(&v_bytes)?;
            entries.push(MetaEntry { key, attr });
        }
        Ok(entries)
    }
}

/// Two-Phase Commit (2PC) Coordinator for cross-directory `rename` operations (§2.3.1, §7.6).
///
/// While `>96%` of metadata operations are single-directory operations handled via `1-RTT`
/// pushdown, cross-directory `rename` (`<3%` of workload) spans two `PID`s that may reside
/// on different shards and is executed atomically via `TwoPhaseCommitCoordinator`.
pub struct TwoPhaseCommitCoordinator {
    pushdown: Arc<PushdownExecutor>,
}

impl TwoPhaseCommitCoordinator {
    pub fn new(pushdown: Arc<PushdownExecutor>) -> Self {
        Self { pushdown }
    }

    /// Atomically moves `(src_pid, src_name)` to `(dst_pid, dst_name)` across shards,
    /// updating parent attribute deltas and the `SecondaryIndexTable` (`FID -> (dst_pid, dst_name)`).
    #[allow(clippy::too_many_arguments)]
    pub fn rename(
        &self,
        src_engine: &LifecycleStorageEngine,
        dst_engine: &LifecycleStorageEngine,
        inode_engine: &LifecycleStorageEngine,
        src_pid: InodeId,
        src_name: &str,
        dst_pid: InodeId,
        dst_name: &str,
        commit_hlc: HlcTimestamp,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(src_name)?;
        validate_user_entry_name(dst_name)?;

        // Phase 1 (Prepare / Validate on both source and destination shards):
        let src_key = MetaKey::child(src_pid, src_name).encode();
        let Some(src_raw) = src_engine.get(&src_key, commit_hlc)? else {
            return Err(MetaDbError::NotFound {
                pid: src_pid,
                name: src_name.to_string(),
            });
        };
        let mut attr = InodeAttr::decode(&src_raw)?;

        let dst_parent_attr_key = MetaKey::dir_attr(dst_pid).encode();
        if dst_engine.get(&dst_parent_attr_key, commit_hlc)?.is_none() {
            return Err(MetaDbError::DirectoryNotFound(dst_pid));
        }

        let dst_key = MetaKey::child(dst_pid, dst_name);
        let dst_key_bytes = dst_key.encode();
        if dst_engine.get(&dst_key_bytes, commit_hlc)?.is_some() {
            return Err(MetaDbError::AlreadyExists {
                pid: dst_pid,
                name: dst_name.to_string(),
            });
        }

        if attr.entry_type == EntryType::Directory && src_pid != dst_pid {
            let lookup_engines = [inode_engine, dst_engine, src_engine];
            RenameCycleWalker::new(attr.inode_id, dst_pid).walk(|cursor| {
                let key = MetaKey::dir_attr(cursor).encode();
                for engine in lookup_engines {
                    if let Some(raw) = engine.get(&key, commit_hlc)? {
                        return Ok(Some(InodeAttr::decode(&raw)?.parent_pid));
                    }
                }
                Ok(None)
            })?;
        }

        // Phase 2 (Commit on source and destination shards at `commit_hlc`):
        attr.parent_pid = dst_pid;
        attr.modify_time = commit_hlc;
        let accounted = attr.size_bytes.max(0);
        src_engine.delete(src_key, commit_hlc);
        dst_engine.put(dst_key_bytes, attr.encode(), commit_hlc);

        if attr.entry_type == EntryType::Directory && src_pid != dst_pid {
            let dir_key = MetaKey::dir_attr(attr.inode_id).encode();
            for engine in [inode_engine, dst_engine, src_engine] {
                if let Some(raw) = engine.get(&dir_key, commit_hlc)? {
                    let mut dir_self = InodeAttr::decode(&raw)?;
                    dir_self.parent_pid = dst_pid;
                    dir_self.modify_time = commit_hlc;
                    engine.put(dir_key.clone(), dir_self.encode(), commit_hlc);
                    break;
                }
            }
        }

        if src_pid != dst_pid {
            self.pushdown.attr_aggregator().record_delta(
                src_engine,
                src_pid,
                -1,
                negate_accounted_size(accounted),
                commit_hlc,
            );
            self.pushdown
                .attr_aggregator()
                .record_delta(dst_engine, dst_pid, 1, accounted, commit_hlc);
        }

        if let Some(fid) = attr.fid {
            self.pushdown.aux_sink().enqueue_index_upsert(
                dst_engine,
                fid,
                dst_pid,
                dst_name.to_string(),
                attr.create_time,
                commit_hlc,
            );
        }

        Ok(MetaEntry { key: dst_key, attr })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metadb_core::{EntryType, HlcTimestamp, InodeAttr, MetaKey};
    use metadb_storage::LifecycleStorageEngine;

    use super::*;

    #[test]
    fn rename_updates_directory_attr_on_its_own_shard() {
        let src = LifecycleStorageEngine::new(1, 5_000, 128, false);
        let dst = LifecycleStorageEngine::new(2, 5_000, 128, false);
        let inode_engine = LifecycleStorageEngine::new(3, 5_000, 128, false);
        let hlc = HlcTimestamp::new(10, 0);
        let push = Arc::new(PushdownExecutor::new(
            Arc::new(AsyncAttrAggregator::new()),
            Arc::new(AsyncAuxiliarySink::new()),
        ));
        push.init_directory_attr(&src, 1, 1, 0, 0o755, hlc).unwrap();
        push.init_directory_attr(&dst, 20, 1, 0, 0o755, hlc)
            .unwrap();
        push.init_directory_attr(&inode_engine, 30, 1, 1, 0o755, hlc)
            .unwrap();
        let dentry = InodeAttr {
            inode_id: 30,
            parent_pid: 1,
            entry_type: EntryType::Directory,
            uid: 1,
            mode: 0o755,
            link_count: 1,
            num_children: 0,
            size_bytes: 0,
            create_time: hlc,
            modify_time: hlc,
            fid: None,
            applied_lsn: 0,
        };
        src.put(MetaKey::child(1, "child").encode(), dentry.encode(), hlc);
        TwoPhaseCommitCoordinator::new(push)
            .rename(&src, &dst, &inode_engine, 1, "child", 20, "moved", hlc)
            .unwrap();
        let raw = inode_engine
            .get(&MetaKey::dir_attr(30).encode(), hlc)
            .unwrap()
            .unwrap();
        assert_eq!(InodeAttr::decode(&raw).unwrap().parent_pid, 20);
        assert!(src
            .get(&MetaKey::child(1, "child").encode(), hlc)
            .unwrap()
            .is_none());
        assert!(dst
            .get(&MetaKey::child(20, "moved").encode(), hlc)
            .unwrap()
            .is_some());
    }
}
