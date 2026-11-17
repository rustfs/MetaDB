//! Unified Replicated Transaction Router (`DistributedTxRouter`), Linearizable Read Engine
//! (including `DualModeFineGrained` shared-`barrier_hlc` scatter-gather), and Crash-Recoverable
//! Primary-Anchored Two-Phase Commit (`2PC`) Coordinator.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use metadb_core::schema::validate_user_entry_name;
use metadb_core::{
    negate_accounted_size, saturating_add_i64, validate_dir_inode_id, validate_file_size,
    AttrDeltaSpec, AuxIntentSpec, DirShardDeltaKey, DirShardDeltaRecord, EntryType, FileId,
    HlcTimestamp, HybridLogicalClock, InodeAttr, InodeId, IntentOp, MetaDbError, MetaEntry,
    MetaKey, QuotaKey, QuotaRecord, Result, SecondaryIndexKey, SecondaryIndexRecord, ShardEpoch,
    ShardId, TxnId, UserId, WriteIntentKey, WriteIntentRecord, ATTR_ENTRY_NAME, TABLE_PREFIX_QUOTA,
    TABLE_PREFIX_SECONDARY_INDEX,
};
use metadb_raft::ShardRaftGroup;
use metadb_storage::SnapshotGuard;

use crate::recovery_worker::TxnRecoveryWorker;
use crate::rename_cycle::RenameCycleWalker;
use crate::state_machine_impl::{SetAttrPatch, TxRaftCmd, TxRaftOutput, TxStateMachineHandler};

pub const DEFAULT_2PC_TTL_MS: u64 = 3_000;

fn publish_quota(mut record: QuotaRecord) -> QuotaRecord {
    record.used_bytes = record.used_bytes.max(0);
    record.used_inodes = record.used_inodes.max(0);
    record
}

// A split fences its parent before booting and publishing its child. That
// interval includes Raft recovery and can outlast a short count-based retry loop.
const ROUTE_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const ROUTE_RETRY_INTERVAL: Duration = Duration::from_millis(25);
/// A follower that already knows another leader must return `NotLeader` at once.
/// Only an election with no hint is waited out, and only briefly.
const ELECTION_RETRY_TIMEOUT: Duration = Duration::from_secs(2);

async fn retry_routing<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = tokio::time::Instant::now() + ROUTE_RETRY_TIMEOUT;
    retry_routing_until(deadline, operation).await
}

async fn retry_routing_until<T, F, Fut>(
    deadline: tokio::time::Instant,
    mut operation: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let election_deadline = tokio::time::Instant::now() + ELECTION_RETRY_TIMEOUT;
    loop {
        match operation().await {
            // Split publication is local and can take the full routing budget.
            // A known leader cannot appear by retrying on this follower.
            Err(
                error @ MetaDbError::NotLeader {
                    leader_hint: Some(_),
                    ..
                },
            ) => return Err(error),
            Err(error @ MetaDbError::NotLeader { .. }) => {
                let now = tokio::time::Instant::now();
                let limit = deadline.min(election_deadline);
                if now >= limit {
                    return Err(error);
                }
                tokio::time::sleep(ROUTE_RETRY_INTERVAL.min(limit - now)).await;
            }
            Err(
                error @ (MetaDbError::EpochMismatch { .. } | MetaDbError::ShardRoutingError(_)),
            ) => {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(error);
                }
                tokio::time::sleep(ROUTE_RETRY_INTERVAL.min(deadline - now)).await;
            }
            result => return result,
        }
    }
}

/// Fault-injection hook allowing tests to simulate coordinator crashes mid-2PC (`T-2PC-1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TwoPcCrashStage {
    None,
    StopAfterPrepareBeforePrimaryCommit,
    StopAfterPrimaryCommitBeforeSecondary,
}

/// Fenced directory-rename lease issued by the Control Plane (`Resolution B13`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirRenameLease {
    pub rename_fence_token: u64,
    pub expire_hlc: HlcTimestamp,
}

pub type RoutedShardHandle = (ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>);
type PreparedShardBatch = (ShardEpoch, Arc<ShardRaftGroup>, Vec<Vec<u8>>);
type ShardIntentGroup = (ShardEpoch, Arc<ShardRaftGroup>, Vec<(Vec<u8>, IntentOp)>);

/// Routing & lease provider implemented by `metadb-cluster` (and lightweight test clusters).
#[tonic::async_trait]
pub trait ShardDirectory: Send + Sync + 'static {
    /// Resolves `(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)` for `user_key`.
    fn route_user_key(&self, user_key: &[u8]) -> Result<RoutedShardHandle>;

    /// Allocates an identity that must never be reused, including after coordinator restart.
    /// Implementations must reserve the sequence durably before returning it.
    fn allocate_txn_id(&self, _coord_node_id: u64) -> Result<TxnId> {
        Err(MetaDbError::TransactionAborted(
            "ShardDirectory does not provide durable transaction IDs".to_string(),
        ))
    }

    /// Resolves all shards intersecting `[start_user_key, end_user_key)` in ascending key order.
    fn route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> Result<Vec<RoutedShardHandle>>;

    /// Acquires a monotonic fenced `DirRenameLease` for cross-parent directory renames.
    async fn acquire_dir_rename_lease(
        &self,
        moved_dir_id: InodeId,
        ttl_ms: u64,
    ) -> Result<DirRenameLease>;

    /// Releases a `DirRenameLease` on completion.
    async fn release_dir_rename_lease(&self, token: u64);
}

/// Executes both `1-RTT` single-shard pushdown and Primary-Anchored `2PC` across `ShardRaftGroup`s.
pub struct DistributedTxRouter {
    coord_node_id: u64,
    clock: Arc<HybridLogicalClock>,
    directory: Arc<dyn ShardDirectory>,
    ttl_ms: u64,
}

impl DistributedTxRouter {
    pub fn new(
        coord_node_id: u64,
        clock: Arc<HybridLogicalClock>,
        directory: Arc<dyn ShardDirectory>,
    ) -> Self {
        Self {
            coord_node_id,
            clock,
            directory,
            ttl_ms: DEFAULT_2PC_TTL_MS,
        }
    }

    pub fn with_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.ttl_ms = ttl_ms.max(10);
        self
    }

    pub fn directory(&self) -> &Arc<dyn ShardDirectory> {
        &self.directory
    }

    pub fn clock(&self) -> &Arc<HybridLogicalClock> {
        &self.clock
    }

    fn route_user_key_once(&self, user_key: &[u8]) -> Result<RoutedShardHandle> {
        let (epoch, group, handler) = self.directory.route_user_key(user_key)?;
        if group.current_epoch() != epoch || !handler.key_range().contains_key(user_key) {
            return Err(MetaDbError::EpochMismatch {
                shard_id: group.shard_id(),
                expected: epoch,
                actual: group.current_epoch(),
            });
        }
        Ok((epoch, group, handler))
    }

    async fn route_user_key(&self, user_key: &[u8]) -> Result<RoutedShardHandle> {
        retry_routing(|| std::future::ready(self.route_user_key_once(user_key))).await
    }

    async fn route_user_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<RoutedShardHandle>> {
        retry_routing(|| std::future::ready(self.directory.route_user_range(start, end))).await
    }

    fn validate_read_epoch(group: &ShardRaftGroup, epoch: ShardEpoch) -> Result<()> {
        let actual = group.current_epoch();
        if actual != epoch {
            return Err(MetaDbError::EpochMismatch {
                shard_id: group.shard_id(),
                expected: epoch,
                actual,
            });
        }
        Ok(())
    }

    async fn read_user_key(&self, user_key: &[u8]) -> Result<(Arc<ShardRaftGroup>, SnapshotGuard)> {
        let (epoch, group, handler) = self.route_user_key_once(user_key)?;
        let (_, snapshot) = group.linearizable_read(epoch).await?;
        // A range change can occur after routing and before the read cut.
        // Never turn a fenced parent's missing key into a successful miss.
        if group.current_epoch() != epoch || !handler.key_range().contains_key(user_key) {
            return Err(MetaDbError::EpochMismatch {
                shard_id: group.shard_id(),
                expected: epoch,
                actual: group.current_epoch(),
            });
        }
        Ok((group, snapshot))
    }

    async fn propose_cmd(
        group: &ShardRaftGroup,
        epoch: ShardEpoch,
        hlc: HlcTimestamp,
        cmd: &TxRaftCmd,
    ) -> Result<(HlcTimestamp, TxRaftOutput)> {
        let payload =
            bincode::serialize(cmd).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        let resp = group.propose(epoch, hlc, payload).await?;
        let bytes = resp.result?;
        let decoded: TxRaftOutput =
            bincode::deserialize(&bytes).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        Ok((resp.applied_hlc, decoded))
    }

    async fn propose_key_with_retry(
        &self,
        user_key: &[u8],
        cmd: &TxRaftCmd,
    ) -> Result<TxRaftOutput> {
        let deadline = tokio::time::Instant::now() + ROUTE_RETRY_TIMEOUT;
        for attempt in 0..12usize {
            let result = retry_routing_until(deadline, || async {
                let (epoch, group, _) = self.route_user_key_once(user_key)?;
                Self::propose_cmd(&group, epoch, self.clock.now(), cmd).await
            })
            .await;
            match result {
                Ok((_, out)) => return Ok(out),
                Err(MetaDbError::WriteIntentConflict {
                    conflicting_txn_id,
                    primary_key,
                    expire_hlc,
                }) if attempt < 11 => {
                    if self.clock.now().0 > expire_hlc {
                        let _ = TxnRecoveryWorker::resolve_single_txn(
                            self.directory.as_ref(),
                            &self.clock,
                            conflicting_txn_id,
                            &primary_key,
                        )
                        .await;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt as u64 + 1)))
                        .await;
                }
                Err(e) => return Err(e),
            }
        }
        Err(MetaDbError::ShardRoutingError(
            "exceeded max route/conflict retries".to_string(),
        ))
    }

    /// Initializes `(dir_id, "_attr_")` (e.g., for `ROOT_INODE_ID = 1` at cluster bootstrap).
    pub async fn init_dir_attr(
        &self,
        dir_id: InodeId,
        parent_pid: InodeId,
        uid: UserId,
        mode: u32,
    ) -> Result<InodeAttr> {
        let key = MetaKey::dir_attr(dir_id).encode();
        let cmd = TxRaftCmd::InitDirAttr {
            dir_id,
            parent_pid,
            uid,
            mode,
        };
        match self.propose_key_with_retry(&key, &cmd).await? {
            TxRaftOutput::Attr(a) => Ok(a),
            _ => Err(MetaDbError::CodecError(
                "unexpected InitDirAttr output".to_string(),
            )),
        }
    }

    /// `1-RTT` single-shard file creation (`100%` single-shard even in `DualModeFineGrained`!).
    #[allow(clippy::too_many_arguments)]
    pub async fn create_file(
        &self,
        pid: InodeId,
        name: &str,
        inode_id: InodeId,
        fid: FileId,
        uid: UserId,
        mode: u32,
        size_bytes: i64,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(name)?;
        validate_file_size(size_bytes)?;
        let key = MetaKey::child(pid, name).encode();
        let cmd = TxRaftCmd::CreateFile {
            pid,
            name: name.to_string(),
            inode_id,
            fid,
            uid,
            mode,
            size_bytes,
        };
        match self.propose_key_with_retry(&key, &cmd).await? {
            TxRaftOutput::Entry(e) => Ok(e),
            _ => Err(MetaDbError::CodecError(
                "unexpected CreateFile output".to_string(),
            )),
        }
    }

    /// `1-RTT` single-shard file unlink.
    pub async fn unlink(&self, pid: InodeId, name: &str) -> Result<InodeAttr> {
        validate_user_entry_name(name)?;
        let key = MetaKey::child(pid, name).encode();
        let cmd = TxRaftCmd::Unlink {
            pid,
            name: name.to_string(),
        };
        match self.propose_key_with_retry(&key, &cmd).await? {
            TxRaftOutput::Attr(a) => Ok(a),
            _ => Err(MetaDbError::CodecError(
                "unexpected Unlink output".to_string(),
            )),
        }
    }

    /// Creates a subdirectory `name` (`new_dir_id`):
    /// - Executes `1-RTT` `MkdirSingleShard` when `Shard(pid, name) == Shard(new_dir_id, "_attr_")`.
    /// - Automatically executes crash-recoverable Primary-Anchored `2PC` across shards otherwise.
    pub async fn mkdir(
        &self,
        pid: InodeId,
        name: &str,
        new_dir_id: InodeId,
        uid: UserId,
        mode: u32,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(name)?;
        validate_dir_inode_id(new_dir_id)?;
        match self.dirstat(new_dir_id).await {
            Ok(_) => {
                return Err(MetaDbError::AlreadyExists {
                    pid: new_dir_id,
                    name: ATTR_ENTRY_NAME.to_string(),
                });
            }
            Err(MetaDbError::DirectoryNotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let child_meta_key = MetaKey::child(pid, name);
        let child_bytes = child_meta_key.encode();
        let new_attr_bytes = MetaKey::dir_attr(new_dir_id).encode();

        let (_, g_parent, _) = self.route_user_key(&child_bytes).await?;
        let (_, g_new_dir, _) = self.route_user_key(&new_attr_bytes).await?;

        if g_parent.shard_id() == g_new_dir.shard_id() {
            let cmd = TxRaftCmd::MkdirSingleShard {
                pid,
                name: name.to_string(),
                new_dir_id,
                uid,
                mode,
            };
            return match self.propose_key_with_retry(&child_bytes, &cmd).await? {
                TxRaftOutput::Entry(e) => Ok(e),
                _ => Err(MetaDbError::CodecError(
                    "unexpected Mkdir output".to_string(),
                )),
            };
        }

        // Cross-shard 2PC `Mkdir`:
        if self.lookup(pid, name).await?.is_some() {
            return Err(MetaDbError::AlreadyExists {
                pid,
                name: name.to_string(),
            });
        }

        let start_hlc = self.clock.now();
        let dentry_attr = InodeAttr {
            inode_id: new_dir_id,
            parent_pid: pid,
            entry_type: EntryType::Directory,
            uid,
            mode,
            link_count: 1,
            num_children: 0,
            size_bytes: 0,
            create_time: start_hlc,
            modify_time: start_hlc,
            fid: None,
            applied_lsn: 0,
        };
        let self_attr = InodeAttr {
            entry_type: EntryType::DirectoryAttr,
            ..dentry_attr.clone()
        };

        let parent_attr_fence = MetaKey::dir_attr(pid).encode();
        let intents_by_key: Vec<(Vec<u8>, IntentOp)> = vec![
            (
                parent_attr_fence,
                IntentOp::DirExistsFenceSet {
                    holders: BTreeMap::new(),
                },
            ),
            (
                child_bytes.clone(),
                IntentOp::Insert {
                    value: dentry_attr.encode(),
                    attr_deltas: vec![AttrDeltaSpec {
                        pid,
                        delta_children: 1,
                        delta_bytes: 0,
                    }],
                    aux_specs: vec![AuxIntentSpec::QuotaDelta {
                        uid,
                        delta_bytes: 0,
                        delta_inodes: 1,
                    }],
                },
            ),
            (
                new_attr_bytes.clone(),
                IntentOp::Insert {
                    value: self_attr.encode(),
                    attr_deltas: vec![],
                    aux_specs: vec![],
                },
            ),
        ];

        let commit_hlc = self
            .execute_two_pc(
                &child_bytes,
                intents_by_key,
                vec![(child_bytes.clone(), None), (new_attr_bytes, None)],
                None,
                TwoPcCrashStage::None,
            )
            .await?;

        let mut committed_attr = dentry_attr;
        committed_attr.modify_time = commit_hlc;
        Ok(MetaEntry {
            key: child_meta_key,
            attr: committed_attr,
        })
    }

    /// Removes an empty directory `(pid, name)` with authoritative emptiness verification:
    /// - `1-RTT` `RmdirSingleShard` when colocated on a single shard.
    /// - Crash-recoverable `2PC` with `DeletingDir` intents across all sub-shards otherwise.
    pub async fn rmdir(&self, pid: InodeId, name: &str) -> Result<InodeAttr> {
        validate_user_entry_name(name)?;
        let Some(entry) = self.lookup(pid, name).await? else {
            return Err(MetaDbError::NotFound {
                pid,
                name: name.to_string(),
            });
        };
        if entry.attr.entry_type != EntryType::Directory {
            return Err(MetaDbError::InvalidEntryType {
                expected: "Directory".to_string(),
                actual: "File".to_string(),
            });
        }
        let target_dir_id = entry.attr.inode_id;
        let child_bytes = MetaKey::child(pid, name).encode();
        let dir_start = MetaKey::encode_pid_prefix_start(target_dir_id);
        let dir_end = MetaKey::encode_pid_prefix_end(target_dir_id);

        let (_, g_parent, _) = self.route_user_key(&child_bytes).await?;
        let dir_shards = self.route_user_range(&dir_start, &dir_end).await?;

        if dir_shards.len() == 1 && dir_shards[0].1.shard_id() == g_parent.shard_id() {
            let cmd = TxRaftCmd::RmdirSingleShard {
                pid,
                name: name.to_string(),
            };
            return match self.propose_key_with_retry(&child_bytes, &cmd).await? {
                TxRaftOutput::Attr(a) => Ok(a),
                _ => Err(MetaDbError::CodecError(
                    "unexpected Rmdir output".to_string(),
                )),
            };
        }

        // Cross-shard or `DualModeFineGrained` 2PC `Rmdir` (`DeletingDir` locks every sub-shard!).
        let mut intents_by_key: Vec<(Vec<u8>, IntentOp)> = Vec::new();
        intents_by_key.push((
            child_bytes.clone(),
            IntentOp::Delete {
                attr_deltas: vec![AttrDeltaSpec {
                    pid,
                    delta_children: -1,
                    delta_bytes: 0,
                }],
                aux_specs: vec![AuxIntentSpec::QuotaDelta {
                    uid: entry.attr.uid,
                    delta_bytes: 0,
                    delta_inodes: -1,
                }],
            },
        ));

        let anchor_attr_key = MetaKey::dir_attr(target_dir_id).encode();
        for (_, _, handler) in &dir_shards {
            let r = handler.key_range();
            let lock_key = if r.contains_key(&anchor_attr_key) {
                anchor_attr_key.clone()
            } else {
                r.start_key.clone()
            };
            intents_by_key.push((
                lock_key,
                IntentOp::DeletingDir {
                    dir_id: target_dir_id,
                    attr_deltas: vec![],
                    aux_specs: vec![],
                },
            ));
        }

        self.execute_two_pc(
            &child_bytes,
            intents_by_key,
            vec![(child_bytes.clone(), Some(entry.attr.encode()))],
            None,
            TwoPcCrashStage::None,
        )
        .await?;
        Ok(entry.attr)
    }

    /// Updates file or directory attributes (`Resolution B10`: directory `SetAttr` updates
    /// authoritative `(dir_id, "_attr_")` and any `[0x04][dir_id]` sub-shards via 2PC if cross-shard).
    pub async fn setattr(
        &self,
        pid: InodeId,
        name: &str,
        patch: SetAttrPatch,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(name)?;
        if let Some(sz) = patch.size_bytes {
            validate_file_size(sz)?;
        }
        let Some(existing) = self.lookup(pid, name).await? else {
            return Err(MetaDbError::NotFound {
                pid,
                name: name.to_string(),
            });
        };

        let child_bytes = existing.key.encode();
        if existing.attr.entry_type == EntryType::File {
            let cmd = TxRaftCmd::SetAttrSingleShard {
                pid,
                name: name.to_string(),
                patch,
            };
            return match self.propose_key_with_retry(&child_bytes, &cmd).await? {
                TxRaftOutput::Entry(e) => Ok(e),
                _ => Err(MetaDbError::CodecError(
                    "unexpected SetAttr output".to_string(),
                )),
            };
        }

        if patch.size_bytes.is_some() || patch.fid.is_some() {
            return Err(MetaDbError::CodecError(
                "directory size and fid are derived attributes".into(),
            ));
        }

        // Directory `SetAttr`: check if single-shard colocated or multi-shard (`Resolution B10`).
        let dir_id = existing.attr.inode_id;
        let dir_start = MetaKey::encode_pid_prefix_start(dir_id);
        let dir_end = MetaKey::encode_pid_prefix_end(dir_id);
        let (_, g_parent, _) = self.route_user_key(&child_bytes).await?;
        let dir_shards = self.route_user_range(&dir_start, &dir_end).await?;

        if dir_shards.len() == 1 && dir_shards[0].1.shard_id() == g_parent.shard_id() {
            let cmd = TxRaftCmd::SetAttrSingleShard {
                pid,
                name: name.to_string(),
                patch,
            };
            return match self.propose_key_with_retry(&child_bytes, &cmd).await? {
                TxRaftOutput::Entry(e) => Ok(e),
                _ => Err(MetaDbError::CodecError(
                    "unexpected SetAttr output".to_string(),
                )),
            };
        }

        let mut new_dentry = existing.attr.clone();
        let old_uid = new_dentry.uid;
        if let Some(m) = patch.mode {
            new_dentry.mode = m;
        }
        if let Some(u) = patch.uid {
            new_dentry.uid = u;
        }
        let mut aux_specs = Vec::new();
        if new_dentry.uid != old_uid {
            aux_specs.push(AuxIntentSpec::QuotaDelta {
                uid: old_uid,
                delta_bytes: 0,
                delta_inodes: -1,
            });
            aux_specs.push(AuxIntentSpec::QuotaDelta {
                uid: new_dentry.uid,
                delta_bytes: 0,
                delta_inodes: 1,
            });
        }

        let dir_attr_key = MetaKey::dir_attr(dir_id).encode();
        let dir_self_raw = self
            .read_raw_user_value(&dir_attr_key)
            .await?
            .ok_or(MetaDbError::DirectoryNotFound(dir_id))?;
        let mut dir_self = InodeAttr::decode(&dir_self_raw)?;
        dir_self.mode = new_dentry.mode;
        dir_self.uid = new_dentry.uid;

        let intents = vec![
            (
                child_bytes.clone(),
                IntentOp::Put {
                    value: new_dentry.encode(),
                    attr_deltas: vec![],
                    aux_specs,
                },
            ),
            (
                dir_attr_key.clone(),
                IntentOp::Put {
                    value: dir_self.encode(),
                    attr_deltas: vec![],
                    aux_specs: vec![],
                },
            ),
        ];
        let commit_hlc = self
            .execute_two_pc(
                &child_bytes,
                intents,
                vec![
                    (child_bytes.clone(), Some(existing.attr.encode())),
                    (dir_attr_key, Some(dir_self_raw)),
                ],
                None,
                TwoPcCrashStage::None,
            )
            .await?;
        new_dentry.modify_time = commit_hlc;
        Ok(MetaEntry {
            key: existing.key,
            attr: new_dentry,
        })
    }

    async fn ensure_directory_rename_acyclic(
        &self,
        src_dir_id: InodeId,
        dst_pid: InodeId,
    ) -> Result<()> {
        let mut walker = RenameCycleWalker::new(src_dir_id, dst_pid);
        let mut cursor = dst_pid;
        loop {
            if !walker.observe(cursor)? {
                return Ok(());
            }
            cursor = self.dirstat(cursor).await?.parent_pid;
        }
    }

    /// Atomic `Rename` (`1-RTT` within one shard, or crash-recoverable Primary-Anchored `2PC` across shards).
    pub async fn rename(
        &self,
        src_pid: InodeId,
        src_name: &str,
        dst_pid: InodeId,
        dst_name: &str,
        overwrite: bool,
    ) -> Result<MetaEntry> {
        self.rename_with_fault_injection(
            src_pid,
            src_name,
            dst_pid,
            dst_name,
            overwrite,
            TwoPcCrashStage::None,
        )
        .await
    }

    pub async fn rename_with_fault_injection(
        &self,
        src_pid: InodeId,
        src_name: &str,
        dst_pid: InodeId,
        dst_name: &str,
        overwrite: bool,
        crash_stage: TwoPcCrashStage,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(src_name)?;
        validate_user_entry_name(dst_name)?;

        let src_meta_key = MetaKey::child(src_pid, src_name);
        let dst_meta_key = MetaKey::child(dst_pid, dst_name);
        let src_bytes = src_meta_key.encode();
        let dst_bytes = dst_meta_key.encode();

        let Some(src_entry) = self.lookup(src_pid, src_name).await? else {
            return Err(MetaDbError::NotFound {
                pid: src_pid,
                name: src_name.to_string(),
            });
        };

        let (_, g_src, _) = self.route_user_key(&src_bytes).await?;
        let (_, g_dst, _) = self.route_user_key(&dst_bytes).await?;

        let single_shard_eligible = if src_entry.attr.entry_type == EntryType::Directory {
            let d_start = MetaKey::encode_pid_prefix_start(src_entry.attr.inode_id);
            let d_end = MetaKey::encode_pid_prefix_end(src_entry.attr.inode_id);
            let d_shards = self.route_user_range(&d_start, &d_end).await?;
            g_src.shard_id() == g_dst.shard_id()
                && d_shards.len() == 1
                && d_shards[0].1.shard_id() == g_src.shard_id()
                && crash_stage == TwoPcCrashStage::None
        } else {
            g_src.shard_id() == g_dst.shard_id() && crash_stage == TwoPcCrashStage::None
        };

        if single_shard_eligible {
            let cmd = TxRaftCmd::RenameSingleShard {
                src_pid,
                src_name: src_name.to_string(),
                dst_pid,
                dst_name: dst_name.to_string(),
                overwrite,
            };
            return match self.propose_key_with_retry(&src_bytes, &cmd).await? {
                TxRaftOutput::Entry(e) => Ok(e),
                _ => Err(MetaDbError::CodecError(
                    "unexpected Rename output".to_string(),
                )),
            };
        }

        crate::validation::validate_size(src_entry.attr.size_bytes)?;
        // Cross-shard 2PC `Rename`:
        let lease_opt = if src_entry.attr.entry_type == EntryType::Directory && src_pid != dst_pid {
            let lease = self
                .directory
                .acquire_dir_rename_lease(src_entry.attr.inode_id, self.ttl_ms)
                .await?;
            // Walk the full ancestor chain. A truncated walk must not allow the rename.
            if let Err(e) = self
                .ensure_directory_rename_acyclic(src_entry.attr.inode_id, dst_pid)
                .await
            {
                self.directory
                    .release_dir_rename_lease(lease.rename_fence_token)
                    .await;
                return Err(e);
            }
            Some(lease)
        } else {
            None
        };

        // Check destination existence & victim accounting (`Resolution B11`).
        let dst_existing = self.lookup(dst_pid, dst_name).await?;
        let mut expected_values = vec![
            (src_bytes.clone(), Some(src_entry.attr.encode())),
            (
                dst_bytes.clone(),
                dst_existing.as_ref().map(|entry| entry.attr.encode()),
            ),
        ];
        let mut dst_aux_specs = Vec::new();
        let src_accounted = src_entry.attr.size_bytes.max(0);
        let mut dst_delta_children = 1i64;
        let mut dst_delta_bytes = src_accounted;

        if let Some(victim_entry) = dst_existing {
            crate::validation::validate_size(victim_entry.attr.size_bytes)?;
            if !overwrite {
                if let Some(l) = lease_opt {
                    self.directory
                        .release_dir_rename_lease(l.rename_fence_token)
                        .await;
                }
                return Err(MetaDbError::AlreadyExists {
                    pid: dst_pid,
                    name: dst_name.to_string(),
                });
            }
            if victim_entry.attr.entry_type != EntryType::File
                || src_entry.attr.entry_type != EntryType::File
            {
                if let Some(l) = lease_opt {
                    self.directory
                        .release_dir_rename_lease(l.rename_fence_token)
                        .await;
                }
                return Err(MetaDbError::InvalidEntryType {
                    expected: "File".to_string(),
                    actual: "Directory".to_string(),
                });
            }
            dst_delta_children = 0; // +1 - 1 = 0 net child delta on destination!
            dst_delta_bytes = saturating_add_i64(
                src_accounted,
                negate_accounted_size(victim_entry.attr.size_bytes),
            );
            dst_aux_specs.push(AuxIntentSpec::QuotaDelta {
                uid: victim_entry.attr.uid,
                delta_bytes: negate_accounted_size(victim_entry.attr.size_bytes),
                delta_inodes: -1,
            });
            if let Some(vfid) = victim_entry.attr.fid {
                dst_aux_specs.push(AuxIntentSpec::IndexDelete { fid: vfid });
            }
        }

        if let Some(fid) = src_entry.attr.fid {
            dst_aux_specs.push(AuxIntentSpec::IndexUpsert {
                fid,
                pid: dst_pid,
                name: dst_name.to_string(),
                create_time: src_entry.attr.create_time,
            });
        }

        let mut moved_attr = src_entry.attr.clone();
        moved_attr.parent_pid = dst_pid;

        let mut intents: Vec<(Vec<u8>, IntentOp)> = Vec::new();
        let dst_dir_attr_key = MetaKey::dir_attr(dst_pid).encode();
        intents.push((
            dst_dir_attr_key,
            IntentOp::DirExistsFenceSet {
                holders: BTreeMap::new(),
            },
        ));
        intents.push((
            src_bytes.clone(),
            IntentOp::Delete {
                attr_deltas: if src_pid != dst_pid {
                    vec![AttrDeltaSpec {
                        pid: src_pid,
                        delta_children: -1,
                        delta_bytes: negate_accounted_size(src_accounted),
                    }]
                } else {
                    vec![]
                },
                aux_specs: vec![],
            },
        ));
        intents.push((
            dst_bytes,
            IntentOp::Put {
                value: moved_attr.encode(),
                attr_deltas: if src_pid != dst_pid {
                    vec![AttrDeltaSpec {
                        pid: dst_pid,
                        delta_children: dst_delta_children,
                        delta_bytes: dst_delta_bytes,
                    }]
                } else {
                    vec![AttrDeltaSpec {
                        pid: dst_pid,
                        delta_children: dst_delta_children - 1,
                        delta_bytes: saturating_add_i64(
                            dst_delta_bytes,
                            negate_accounted_size(src_accounted),
                        ),
                    }]
                },
                aux_specs: dst_aux_specs,
            },
        ));

        if moved_attr.entry_type == EntryType::Directory && src_pid != dst_pid {
            let dir_self_key = MetaKey::dir_attr(moved_attr.inode_id).encode();
            let dir_self_raw = self
                .read_raw_user_value(&dir_self_key)
                .await?
                .ok_or(MetaDbError::DirectoryNotFound(moved_attr.inode_id))?;
            let mut dir_self = InodeAttr::decode(&dir_self_raw)?;
            dir_self.parent_pid = dst_pid;
            expected_values.push((dir_self_key.clone(), Some(dir_self_raw)));
            intents.push((
                dir_self_key,
                IntentOp::Put {
                    value: dir_self.encode(),
                    attr_deltas: vec![],
                    aux_specs: vec![],
                },
            ));
        }

        let res = self
            .execute_two_pc(&src_bytes, intents, expected_values, lease_opt, crash_stage)
            .await;

        if let Some(l) = lease_opt {
            self.directory
                .release_dir_rename_lease(l.rename_fence_token)
                .await;
        }

        let commit_hlc = res?;
        moved_attr.modify_time = commit_hlc;
        Ok(MetaEntry {
            key: dst_meta_key,
            attr: moved_attr,
        })
    }

    async fn execute_two_pc(
        &self,
        primary_key: &[u8],
        intents: Vec<(Vec<u8>, IntentOp)>,
        expected_values: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        lease_opt: Option<DirRenameLease>,
        crash_stage: TwoPcCrashStage,
    ) -> Result<HlcTimestamp> {
        let txn_id = self.directory.allocate_txn_id(self.coord_node_id)?;
        let all_participant_keys: Vec<Vec<u8>> = intents.iter().map(|(k, _)| k.clone()).collect();

        // Group intents by routed `ShardId`.
        let (primary_epoch, primary_group, _) = self.route_user_key(primary_key).await?;
        let primary_shard_id = primary_group.shard_id();

        let mut grouped: HashMap<ShardId, ShardIntentGroup> = HashMap::new();
        for (uk, op) in intents {
            let (ep, grp, _) = self.route_user_key(&uk).await?;
            grouped
                .entry(grp.shard_id())
                .or_insert_with(|| (ep, grp, Vec::new()))
                .2
                .push((uk, op));
        }

        // Waiting for split publication must not consume a transaction's lease
        // before its first prepare. An external rename lease cannot be extended
        // here, so fail explicitly if that lease expired while routing.
        let start_hlc = self.clock.now();
        let expire_hlc = lease_opt
            .map(|l| l.expire_hlc)
            .unwrap_or_else(|| HlcTimestamp::new(start_hlc.physical_ms() + self.ttl_ms, 0));
        if expire_hlc <= start_hlc {
            return Err(MetaDbError::TransactionAborted(
                "directory rename lease expired while waiting for shard routing".into(),
            ));
        }
        let rename_fence_token = lease_opt.map(|l| l.rename_fence_token);

        // Phase 1: Prepare on Secondary Shards then Primary Shard (or parallel).
        let mut prepared_shards: Vec<PreparedShardBatch> = Vec::new();
        let mut max_prepared_hlc = start_hlc;

        // Prepare secondaries first so if any secondary rejects, Primary never even creates `Pending`.
        let mut ordered_groups: Vec<(ShardId, ShardIntentGroup)> = grouped.into_iter().collect();
        ordered_groups.sort_by_key(|(sid, _)| *sid == primary_shard_id);

        for (sid, (ep, grp, shard_intents)) in ordered_groups {
            let shard_keys: Vec<Vec<u8>> = shard_intents.iter().map(|(k, _)| k.clone()).collect();
            let shard_expected_values = expected_values
                .iter()
                .filter(|(key, _)| shard_keys.contains(key))
                .cloned()
                .collect();
            let prep_cmd = TxRaftCmd::Prepare2pcChecked {
                txn_id,
                is_primary: sid == primary_shard_id,
                primary_key: primary_key.to_vec(),
                participant_keys: all_participant_keys.clone(),
                start_hlc,
                expire_hlc,
                rename_fence_token,
                intents: shard_intents,
                expected_values: shard_expected_values,
            };
            match Self::propose_cmd(&grp, ep, start_hlc, &prep_cmd).await {
                Ok((_, TxRaftOutput::Prepared { prepared_hlc })) => {
                    max_prepared_hlc = max_prepared_hlc.max(prepared_hlc);
                    prepared_shards.push((ep, grp, shard_keys));
                }
                Ok(_) => {
                    self.abort_prepared_shards(txn_id, &prepared_shards).await;
                    return Err(MetaDbError::CodecError(
                        "unexpected Prepare2pc output".to_string(),
                    ));
                }
                Err(err) => {
                    self.abort_prepared_shards(txn_id, &prepared_shards).await;
                    return Err(err);
                }
            }
        }

        if crash_stage == TwoPcCrashStage::StopAfterPrepareBeforePrimaryCommit {
            return Err(MetaDbError::RpcError(
                "injected crash after Prepare before CommitPrimary2pc".to_string(),
            ));
        }

        // Phase 2a: Atomic Commit on Primary Shard (`[0x06] TxnRecord -> Committed`).
        let commit_hlc = self.clock.update(max_prepared_hlc);
        let commit_primary_cmd = TxRaftCmd::CommitPrimary2pc {
            txn_id,
            commit_hlc,
            rename_fence_token,
        };
        let (_, primary_out) = Self::propose_cmd(
            &primary_group,
            primary_epoch,
            commit_hlc,
            &commit_primary_cmd,
        )
        .await?;
        let final_commit_hlc = match primary_out {
            TxRaftOutput::Committed { commit_hlc: c, .. } => c,
            _ => {
                return Err(MetaDbError::CodecError(
                    "unexpected CommitPrimary2pc output".to_string(),
                ))
            }
        };

        if crash_stage == TwoPcCrashStage::StopAfterPrimaryCommitBeforeSecondary {
            return Err(MetaDbError::RpcError(
                "injected crash after CommitPrimary2pc before CommitSecondary2pc".to_string(),
            ));
        }

        // Phase 2b: Roll forward all Secondary Shards and mark durable ACKs on Primary (`Resolution B12`).
        let mut acked_secondary_keys = Vec::new();
        for (ep, grp, keys) in &prepared_shards {
            if grp.shard_id() == primary_shard_id {
                continue;
            }
            let sec_cmd = TxRaftCmd::CommitSecondary2pc {
                txn_id,
                commit_hlc: final_commit_hlc,
                target_user_keys: keys.clone(),
            };
            if Self::propose_cmd(grp, *ep, final_commit_hlc, &sec_cmd)
                .await
                .is_ok()
            {
                acked_secondary_keys.extend(keys.clone());
            }
        }

        if !acked_secondary_keys.is_empty() {
            let ack_cmd = TxRaftCmd::MarkParticipantAcked {
                txn_id,
                acked_user_keys: acked_secondary_keys,
            };
            let _ =
                Self::propose_cmd(&primary_group, primary_epoch, final_commit_hlc, &ack_cmd).await;
        }

        Ok(final_commit_hlc)
    }

    async fn abort_prepared_shards(&self, txn_id: TxnId, prepared: &[PreparedShardBatch]) {
        let now = self.clock.now();
        for (ep, grp, keys) in prepared {
            let cmd = TxRaftCmd::Abort2pc {
                txn_id,
                target_user_keys: keys.clone(),
            };
            let _ = Self::propose_cmd(grp, *ep, now, &cmd).await;
        }
    }

    /// Linearizable point `Lookup(pid, name)` resolving any encountered `[0x05]` `WriteIntent`s.
    pub async fn lookup(&self, pid: InodeId, name: &str) -> Result<Option<MetaEntry>> {
        retry_routing(|| self.lookup_once(pid, name)).await
    }

    async fn lookup_once(&self, pid: InodeId, name: &str) -> Result<Option<MetaEntry>> {
        validate_user_entry_name(name)?;
        let mk = MetaKey::child(pid, name);
        let key_bytes = mk.encode();

        for _ in 0..8 {
            let (group, snap) = self.read_user_key(&key_bytes).await?;
            let engine = group.engine();

            // Check `[0x05]` intent at `snap.cut()`.
            let ik = WriteIntentKey::new(key_bytes.clone()).encode();
            if let Some(raw_intent) = engine.get_with_snapshot(&ik, &snap)? {
                let intent = WriteIntentRecord::decode(&raw_intent)?;
                if !matches!(intent.op, IntentOp::DirExistsFenceSet { .. })
                    && intent.start_hlc <= snap.read_hlc()
                {
                    drop(snap);
                    TxnRecoveryWorker::resolve_single_txn(
                        self.directory.as_ref(),
                        &self.clock,
                        intent.txn_id,
                        &intent.primary_key,
                    )
                    .await?;
                    continue;
                }
            }

            let Some(raw_val) = engine.get_with_snapshot(&key_bytes, &snap)? else {
                return Ok(None);
            };
            return Ok(Some(MetaEntry {
                key: mk,
                attr: InodeAttr::decode(&raw_val)?,
            }));
        }
        Err(MetaDbError::ShardRoutingError(
            "lookup exceeded max intent/route retries".to_string(),
        ))
    }

    /// Reads committed bytes used as the predicate for an anchor attribute mutation.
    async fn read_raw_user_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        retry_routing(|| self.read_raw_user_value_once(key)).await
    }

    async fn read_raw_user_value_once(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for _ in 0..12 {
            let (group, snap) = self.read_user_key(key).await?;
            let engine = group.engine();
            let intent_key = WriteIntentKey::new(key.to_vec()).encode();
            if let Some(raw) = engine.get_with_snapshot(&intent_key, &snap)? {
                let intent = WriteIntentRecord::decode(&raw)?;
                if !matches!(intent.op, IntentOp::DirExistsFenceSet { .. }) {
                    drop(snap);
                    TxnRecoveryWorker::resolve_single_txn(
                        self.directory.as_ref(),
                        &self.clock,
                        intent.txn_id,
                        &intent.primary_key,
                    )
                    .await?;
                    continue;
                }
            }
            return engine.get_with_snapshot(key, &snap);
        }
        Err(MetaDbError::ShardRoutingError(
            "anchor read exceeded max intent/route retries".to_string(),
        ))
    }

    /// Linearizable `DirStat(pid)`:
    /// - Fast path (`1` shard covers `[pid, pid+1)`): single `ReadIndex` + `reconcile_inode_attr`.
    /// - `DualModeFineGrained` (`K` sub-shards): parallel `ReadIndex` + shared `barrier_hlc`
    ///   scatter-gather across `S_1 .. S_K` (`Resolution B1` / `G-P1`).
    pub async fn dirstat(&self, pid: InodeId) -> Result<InodeAttr> {
        retry_routing(|| self.dirstat_once(pid)).await
    }

    async fn dirstat_once(&self, pid: InodeId) -> Result<InodeAttr> {
        let start = MetaKey::encode_pid_prefix_start(pid);
        let end = MetaKey::encode_pid_prefix_end(pid);

        for _ in 0..8 {
            let shards = self.directory.route_user_range(&start, &end)?;
            if shards.len() == 1 {
                let (epoch, group, handler) = &shards[0];
                let (_, snap) = match group.linearizable_read(*epoch).await {
                    Ok(p) => p,
                    Err(MetaDbError::EpochMismatch { .. } | MetaDbError::NotLeader { .. }) => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                Self::validate_read_epoch(group, *epoch)?;
                let attr_key = MetaKey::dir_attr(pid).encode();
                let Some(raw) = group.engine().get_with_snapshot(&attr_key, &snap)? else {
                    return Err(MetaDbError::DirectoryNotFound(pid));
                };
                let persisted = InodeAttr::decode(&raw)?;
                let reconciled = handler
                    .attr_aggregator()
                    .reconcile_inode_attr(pid, persisted, &snap);
                Self::validate_read_epoch(group, *epoch)?;
                return Ok(reconciled);
            }

            // `DualModeFineGrained` multi-shard scatter-gather with shared `barrier_hlc`:
            let mut max_hlc = self.clock.now();
            let mut retry_route = false;
            for (epoch, group, _) in &shards {
                match group.read_batcher().linearizable_token(*epoch).await {
                    Ok(tok) => max_hlc = max_hlc.max(tok.max_applied_hlc),
                    Err(MetaDbError::EpochMismatch { .. } | MetaDbError::NotLeader { .. }) => {
                        retry_route = true;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
            if retry_route {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            let barrier_hlc = self.clock.update(max_hlc);
            let attr_key = MetaKey::dir_attr(pid).encode();
            let delta_key = DirShardDeltaKey { pid }.encode();

            let mut anchor_attr: Option<InodeAttr> = None;
            let mut total_children = 0i64;
            let mut total_bytes = 0i64;
            let mut latest_mtime = HlcTimestamp::ZERO;

            for (epoch, group, handler) in &shards {
                let range = handler.key_range();
                let (_, snap) = match group.fixed_barrier_read(*epoch, barrier_hlc).await {
                    Ok(p) => p,
                    Err(MetaDbError::EpochMismatch { .. } | MetaDbError::NotLeader { .. }) => {
                        retry_route = true;
                        break;
                    }
                    Err(e) => return Err(e),
                };
                Self::validate_read_epoch(group, *epoch)?;
                let eng = group.engine();
                if range.contains_key(&attr_key) {
                    let Some(raw) = eng.get_with_snapshot(&attr_key, &snap)? else {
                        return Err(MetaDbError::DirectoryNotFound(pid));
                    };
                    let rec = handler.attr_aggregator().reconcile_inode_attr(
                        pid,
                        InodeAttr::decode(&raw)?,
                        &snap,
                    );
                    total_children = saturating_add_i64(total_children, rec.num_children);
                    total_bytes = saturating_add_i64(total_bytes, rec.size_bytes);
                    latest_mtime = latest_mtime.max(rec.modify_time);
                    anchor_attr = Some(rec);
                } else if let Some(raw_sub) = eng.get_with_snapshot(&delta_key, &snap)? {
                    let sub = handler.attr_aggregator().reconcile_dir_shard_delta(
                        pid,
                        DirShardDeltaRecord::decode(&raw_sub)?,
                        &snap,
                    );
                    total_children = saturating_add_i64(total_children, sub.num_children);
                    total_bytes = saturating_add_i64(total_bytes, sub.size_bytes);
                    latest_mtime = latest_mtime.max(sub.modify_time);
                }
            }
            if retry_route {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            for (epoch, group, _) in &shards {
                Self::validate_read_epoch(group, *epoch)?;
            }

            let mut canonical = anchor_attr.ok_or(MetaDbError::DirectoryNotFound(pid))?;
            canonical.num_children = total_children.max(0);
            canonical.size_bytes = total_bytes.max(0);
            canonical.modify_time = canonical.modify_time.max(latest_mtime);
            return Ok(canonical);
        }

        Err(MetaDbError::ShardRoutingError(
            "dirstat exceeded max route retries".to_string(),
        ))
    }

    /// Keyset-paginated `ListDir(pid, after_name, prefix, limit)` unconditionally excluding `"_attr_"`.
    pub async fn listdir_paginated(
        &self,
        pid: InodeId,
        after_name: Option<&str>,
        name_prefix: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<MetaEntry>, Option<String>)> {
        retry_routing(|| self.listdir_paginated_once(pid, after_name, name_prefix, limit)).await
    }

    async fn listdir_paginated_once(
        &self,
        pid: InodeId,
        after_name: Option<&str>,
        name_prefix: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<MetaEntry>, Option<String>)> {
        let effective_limit = if limit == 0 { 10_000 } else { limit };
        let scan_start = listdir_scan_start(pid, after_name, name_prefix);
        let scan_end = MetaKey::encode_pid_prefix_end(pid);
        if scan_start >= scan_end {
            return Ok((Vec::new(), None));
        }

        let shards = self.directory.route_user_range(&scan_start, &scan_end)?;
        let mut collected: Vec<MetaEntry> = Vec::new();

        for (epoch, group, handler) in shards {
            if collected.len() >= effective_limit {
                break;
            }
            let r = handler.key_range();
            let sub_start = scan_start.as_slice().max(r.start_key.as_slice());
            let sub_end = scan_end.as_slice().min(r.end_key.as_slice());
            if sub_start >= sub_end {
                continue;
            }

            let (_, snap) = group.linearizable_read(epoch).await?;
            Self::validate_read_epoch(&group, epoch)?;
            let eng = group.engine();

            // Resolve any visible active `[0x05]` child write intents in `[sub_start, sub_end)`.
            let i_start = WriteIntentKey::encode_range_bound(sub_start);
            let i_end = WriteIntentKey::encode_range_bound(sub_end);
            let intents = eng.scan_with_snapshot(&i_start, &i_end, &snap)?;
            let mut resolved_any = false;
            for (_, raw_i) in &intents {
                let irec = WriteIntentRecord::decode(raw_i)?;
                if !matches!(irec.op, IntentOp::DirExistsFenceSet { .. })
                    && irec.start_hlc <= snap.read_hlc()
                {
                    resolved_any = true;
                    TxnRecoveryWorker::resolve_single_txn(
                        self.directory.as_ref(),
                        &self.clock,
                        irec.txn_id,
                        &irec.primary_key,
                    )
                    .await?;
                }
            }

            let final_snap = if resolved_any {
                drop(snap);
                group.linearizable_read(epoch).await?.1
            } else {
                snap
            };
            Self::validate_read_epoch(&group, epoch)?;

            let rows = eng.scan_with_snapshot(sub_start, sub_end, &final_snap)?;
            for (k_bytes, v_bytes) in rows {
                let mk = MetaKey::decode(&k_bytes)?;
                // Strict `_attr_` isolation rule (`Resolution N2` / `G-P1`): never expose `_attr_`!
                if mk.is_attr_key() {
                    continue;
                }
                if let Some(pfx) = name_prefix {
                    if !mk.name.starts_with(pfx) {
                        return Ok((collected.clone(), None));
                    }
                }
                collected.push(MetaEntry {
                    key: mk,
                    attr: InodeAttr::decode(&v_bytes)?,
                });
                if collected.len() >= effective_limit {
                    let next_cursor = collected.last().map(|e| e.key.name.clone());
                    return Ok((collected, next_cursor));
                }
            }
        }

        Ok((collected, None))
    }

    pub async fn set_quota_limit(
        &self,
        uid: UserId,
        quota_limit_bytes: u64,
    ) -> Result<QuotaRecord> {
        let qk = QuotaKey { uid }.encode();
        let cmd = TxRaftCmd::SetQuotaLimit {
            uid,
            quota_limit_bytes,
        };
        match self.propose_key_with_retry(&qk, &cmd).await? {
            TxRaftOutput::Quota(q) => Ok(q),
            _ => Err(MetaDbError::CodecError(
                "unexpected SetQuotaLimit output".to_string(),
            )),
        }
    }

    pub async fn upsert_quota_limit(
        &self,
        uid: UserId,
        insert_quota_limit_bytes: u64,
        update_quota_limit_bytes: u64,
    ) -> Result<QuotaRecord> {
        let qk = QuotaKey { uid }.encode();
        let cmd = TxRaftCmd::UpsertQuotaLimit {
            uid,
            insert_quota_limit_bytes,
            update_quota_limit_bytes,
        };
        match self.propose_key_with_retry(&qk, &cmd).await? {
            TxRaftOutput::Quota(q) => Ok(q),
            _ => Err(MetaDbError::CodecError(
                "unexpected UpsertQuotaLimit output".into(),
            )),
        }
    }

    pub async fn get_quota(&self, uid: UserId) -> Result<Option<QuotaRecord>> {
        retry_routing(|| self.get_quota_once(uid)).await
    }

    async fn get_quota_once(&self, uid: UserId) -> Result<Option<QuotaRecord>> {
        let qk = QuotaKey { uid }.encode();
        let (group, snap) = self.read_user_key(&qk).await?;
        let Some(raw) = group.engine().get_with_snapshot(&qk, &snap)? else {
            return Ok(None);
        };
        Ok(Some(publish_quota(QuotaRecord::decode(&raw)?)))
    }

    pub async fn list_quotas(&self, start_uid: UserId, limit: usize) -> Result<Vec<QuotaRecord>> {
        retry_routing(|| self.list_quotas_once(start_uid, limit)).await
    }

    async fn list_quotas_once(&self, start_uid: UserId, limit: usize) -> Result<Vec<QuotaRecord>> {
        let lim = if limit == 0 { 1000 } else { limit };
        let start = QuotaKey { uid: start_uid }.encode();
        let end = vec![TABLE_PREFIX_SECONDARY_INDEX];
        let shards = self.directory.route_user_range(&start, &end)?;
        let mut out = Vec::new();
        for (epoch, group, handler) in shards {
            let r = handler.key_range();
            let s = start.as_slice().max(r.start_key.as_slice());
            let e = end.as_slice().min(r.end_key.as_slice());
            if s >= e {
                continue;
            }
            let (_, snap) = group.linearizable_read(epoch).await?;
            Self::validate_read_epoch(&group, epoch)?;
            for (_, v) in group.engine().scan_with_snapshot(s, e, &snap)? {
                out.push(publish_quota(QuotaRecord::decode(&v)?));
                if out.len() >= lim {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    pub async fn get_secondary_index(&self, fid: FileId) -> Result<Option<SecondaryIndexRecord>> {
        retry_routing(|| self.get_secondary_index_once(fid)).await
    }

    async fn get_secondary_index_once(&self, fid: FileId) -> Result<Option<SecondaryIndexRecord>> {
        let ik = SecondaryIndexKey { fid }.encode();
        let (group, snap) = self.read_user_key(&ik).await?;
        let Some(raw) = group.engine().get_with_snapshot(&ik, &snap)? else {
            return Ok(None);
        };
        let rec = SecondaryIndexRecord::decode(&raw)?;
        if rec.is_deleted {
            Ok(None)
        } else {
            Ok(Some(rec))
        }
    }

    pub async fn scan_secondary_index(
        &self,
        after_fid: FileId,
        limit: usize,
    ) -> Result<Vec<SecondaryIndexRecord>> {
        retry_routing(|| self.scan_secondary_index_once(after_fid, limit)).await
    }

    async fn scan_secondary_index_once(
        &self,
        after_fid: FileId,
        limit: usize,
    ) -> Result<Vec<SecondaryIndexRecord>> {
        let lim = if limit == 0 { 1000 } else { limit };
        let Some(next_fid) = after_fid.checked_add(1) else {
            return Ok(Vec::new());
        };
        let start = SecondaryIndexKey { fid: next_fid }.encode();
        let end = vec![TABLE_PREFIX_SECONDARY_INDEX + 1];
        let shards = self.directory.route_user_range(&start, &end)?;
        let mut out = Vec::new();
        for (epoch, group, handler) in shards {
            let r = handler.key_range();
            let s = start.as_slice().max(r.start_key.as_slice());
            let e = end.as_slice().min(r.end_key.as_slice());
            if s >= e {
                continue;
            }
            let (_, snap) = group.linearizable_read(epoch).await?;
            Self::validate_read_epoch(&group, epoch)?;
            for (_, v) in group.engine().scan_with_snapshot(s, e, &snap)? {
                let rec = SecondaryIndexRecord::decode(&v)?;
                if !rec.is_deleted {
                    out.push(rec);
                    if out.len() >= lim {
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    }
}

#[allow(dead_code)]
const _ASSERT_QUOTA_PREFIX: u8 = TABLE_PREFIX_QUOTA;

/// Lower bound for a keyset `listdir`. When both a cursor and a prefix are set, the scan
/// starts at the later of the two so names before the prefix are not treated as the end
/// of the page.
fn listdir_scan_start(
    pid: InodeId,
    after_name: Option<&str>,
    name_prefix: Option<&str>,
) -> Vec<u8> {
    let mut start = match name_prefix {
        Some(pfx) if !pfx.is_empty() => MetaKey::child(pid, pfx).encode(),
        _ => MetaKey::encode_pid_prefix_start(pid),
    };
    if let Some(cursor) = after_name {
        if !cursor.is_empty() {
            let mut exclusive = MetaKey::child(pid, cursor).encode();
            // Names cannot contain NUL, so `cursor || 0x00` is the first key strictly after it.
            exclusive.push(0x00);
            if exclusive > start {
                start = exclusive;
            }
        }
    }
    start
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listdir_prefix_is_not_skipped_when_cursor_is_before_it() {
        let prefixed = MetaKey::child(1, "b").encode();
        let start = listdir_scan_start(1, Some("a"), Some("b"));
        assert!(
            start >= prefixed,
            "cursor 'a' must not pull the scan below prefix 'b'"
        );
        let within = listdir_scan_start(1, Some("ba"), Some("b"));
        let mut after_ba = MetaKey::child(1, "ba").encode();
        after_ba.push(0x00);
        assert_eq!(within, after_ba);
    }
}
