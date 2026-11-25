//! 3-Stage Recoverable `ExecuteSplit` Worker (`PID`-Boundary & `DualModeFineGrained` Sub-Shards,
//! `Resolutions B14, B15, B17`).

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use metadb_core::{
    affinity_key_for_raw_entry, lease_preimage_blocks_split, saturating_add_i64, CompositeKeyRange,
    DirShardDeltaKey, DirShardDeltaRecord, EntryType, HlcTimestamp, InodeAttr, InodeId,
    MetaDbError, MetaKey, Result, ShardEpoch, ShardId, TxnRecordKey, TxnStatusRecord,
    WriteIntentKey, WriteIntentRecord, ATTR_ENTRY_NAME, SYSTEM_PREFIX_LEASE_PREIMAGE,
    TABLE_PREFIX_META,
};
use metadb_raft::{
    RaftNodeOptions, RegistryShardState, ShardGroupHost, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::sstable::sync_dir;
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, PersistentEntry,
};
use metadb_tx::{TxRaftCmd, TxStateMachineHandler};

use crate::meta_cache::CompositeShardRoute;
use crate::shard::ShardSplitMode;

/// Crash points for split recovery tests. Production passes `None`.
///
/// Each point is after the named step has been committed to disk (Raft log, LSM
/// rename, or parent-key delete) and before `activate_split` publishes routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitCrashPoint {
    AfterFence,
    AfterFlush,
    AfterChildDirRename,
    AfterParentKeyDelete,
}

/// Split boundary decision computed by `RaftShardSplitWorker::discover_split_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitCandidate {
    /// Clean split at binary `split_key` (`[0x01][pid: 8B BE]` or `[0x01][pid: 8B BE][split_name]`).
    SplitAt {
        split_key: Vec<u8>,
        dual_mode_pid: Option<InodeId>,
    },
    /// Dominated (`>80%`) by a single `PID` in `DirectoryColocated` mode.
    Jumbo { dominant_pid: InodeId },
}

pub struct RaftShardSplitWorker;

impl RaftShardSplitWorker {
    /// Discovers a `PID`-boundary or `DualModeFineGrained` `(PID, name)` split point.
    pub async fn discover_split_key(
        route: &CompositeShardRoute,
        group: &ShardRaftGroup,
    ) -> Result<Option<SplitCandidate>> {
        let (_, snap) = group.linearizable_read(group.current_epoch()).await?;
        let rows = group.engine().scan_with_snapshot(
            &route.range.start_key,
            &route.range.end_key,
            &snap,
        )?;
        if rows.len() < 2 {
            return Ok(None);
        }

        // A bootstrap shard can contain several user tables. Split at a real
        // table boundary before applying any table-specific row codec.
        let first_table = rows[0].0.first().copied();
        if let Some((key, _)) = rows
            .iter()
            .find(|(key, _)| key.first().copied() != first_table)
        {
            return Ok(Some(SplitCandidate::SplitAt {
                split_key: vec![key[0]],
                dual_mode_pid: None,
            }));
        }

        // Non-MetaTable shards (`0x02 QuotaTable`, `0x03 SecondaryIndexTable`) split at byte midpoint.
        if first_table != Some(TABLE_PREFIX_META) {
            let total: usize = rows.iter().map(|(k, v)| k.len() + v.len()).sum();
            let half = total / 2;
            let mut acc = 0usize;
            for (idx, (k, v)) in rows.iter().enumerate() {
                acc += k.len() + v.len();
                if acc >= half {
                    let pick = (idx + 1).min(rows.len() - 1);
                    return Ok(Some(SplitCandidate::SplitAt {
                        split_key: rows[pick].0.clone(),
                        dual_mode_pid: None,
                    }));
                }
            }
            return Ok(None);
        }

        // `MetaTable` (`0x01`): aggregate byte distribution by `group_key = PID`.
        let mut bytes_by_pid: BTreeMap<InodeId, usize> = BTreeMap::new();
        let mut total_bytes = 0usize;
        for (k, v) in &rows {
            let mk = MetaKey::decode(k)?;
            let sz = k.len() + v.len();
            *bytes_by_pid.entry(mk.pid).or_insert(0) += sz;
            total_bytes += sz;
        }
        if total_bytes == 0 {
            return Ok(None);
        }

        let (&dominant_pid, &dom_bytes) = bytes_by_pid.iter().max_by_key(|&(_, &b)| b).unwrap();

        // Check if `route` is in `DualModeFineGrained` for `dominant_pid` (or single-PID sub-shard):
        if route.split_mode == ShardSplitMode::DualModeFineGrained
            && (route.dual_mode_pid == Some(dominant_pid) || bytes_by_pid.len() == 1)
        {
            let pid_rows: Vec<&(Vec<u8>, Vec<u8>)> = rows
                .iter()
                .filter(|(k, _)| {
                    MetaKey::decode(k)
                        .map(|m| m.pid == dominant_pid && m.name != ATTR_ENTRY_NAME)
                        .unwrap_or(false)
                })
                .collect();
            if pid_rows.len() < 2 {
                return Ok(None);
            }
            let mid = pid_rows.len() / 2;
            return Ok(Some(SplitCandidate::SplitAt {
                split_key: pid_rows[mid].0.clone(),
                dual_mode_pid: Some(dominant_pid),
            }));
        }

        // `DirectoryColocated` mode: check if `dominant_pid` exceeds 85% with no balanced PID boundary.
        if bytes_by_pid.len() == 1 || dom_bytes * 100 > total_bytes * 85 {
            return Ok(Some(SplitCandidate::Jumbo { dominant_pid }));
        }

        let pids: Vec<InodeId> = bytes_by_pid.keys().copied().collect();
        let target_half = total_bytes / 2;
        let mut acc = 0usize;
        for (idx, &pid) in pids.iter().enumerate() {
            acc += bytes_by_pid[&pid];
            if acc >= target_half {
                let split_idx = if idx + 1 < pids.len() { idx + 1 } else { idx };
                let split_pid = pids[split_idx];
                let split_key = MetaKey::encode_pid_prefix_start(split_pid);
                if split_key > route.range.start_key && split_key < route.range.end_key {
                    return Ok(Some(SplitCandidate::SplitAt {
                        split_key,
                        dual_mode_pid: None,
                    }));
                }
            }
        }
        Ok(None)
    }

    /// Executes Stages A (Intent/DeletingDir check + Flush) & B (Child LSM materialization via
    /// `affinity_key` + `NODE_SHARD_REGISTRY` + Child Raft boot + Parent `UpdateShardRangeOnSplit`).
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_local_split(
        host: &ShardGroupHost,
        parent_route: &CompositeShardRoute,
        parent_group: &ShardRaftGroup,
        parent_handler: &TxStateMachineHandler,
        child_shard_id: ShardId,
        split_key: Vec<u8>,
        dual_mode_pid: Option<InodeId>,
        lsm_config: LsmEngineConfig,
        raft_opts: RaftNodeOptions,
        target_version: u64,
        crash_at: Option<SplitCrashPoint>,
    ) -> Result<(
        CompositeShardRoute,
        CompositeShardRoute,
        Arc<ShardRaftGroup>,
        Arc<TxStateMachineHandler>,
    )> {
        let old_end_key = parent_route.range.end_key.clone();
        let now_hlc = host.clock().now();
        let left_range =
            CompositeKeyRange::new(parent_route.range.start_key.clone(), split_key.clone())?;
        let right_range = CompositeKeyRange::new(split_key.clone(), old_end_key.clone())?;
        if parent_group.current_leader() != Some(host.node_id()) {
            return Err(MetaDbError::NotLeader {
                shard_id: parent_route.shard_id,
                leader_hint: parent_group.current_leader(),
            });
        }
        // Never publish an epoch older than the one already applied. A stale plan must
        // not rewind `version` and then re-fence a range that a newer split already owns.
        let current_version = parent_group.current_epoch().version;
        if current_version > target_version {
            return Err(MetaDbError::ShardRoutingError(
                "split plan is older than the current parent epoch".into(),
            ));
        }
        let already_fenced = current_version == target_version;
        if already_fenced && parent_handler.key_range() != left_range {
            return Err(MetaDbError::ShardRoutingError(
                "split plan does not match the committed parent fence".into(),
            ));
        }

        // Stage A: Drain all shard-local intents and decisions (`Resolution B14`)
        // and verify no active `deleting_fence` (`Resolution B17`) before fencing.
        // Once the atomic fence has committed, any new transaction can touch
        // only the retained range. It must not prevent finishing publication of
        // the child after a crash, and its state remains on the original parent.
        if !already_fenced {
            let (_, snap) = parent_group
                .linearizable_read(parent_group.current_epoch())
                .await?;
            let eng = parent_group.engine();
            // Expiration only permits recovery; it never proves that a pending
            // intent is aborted. Drain every transaction before copying a shard.
            let mut first_intent = None;
            for (key, value) in eng.scan_with_snapshot(&[0x05], &[0x06], &snap)? {
                WriteIntentKey::decode(&key)?;
                let record = WriteIntentRecord::decode(&value)?;
                first_intent.get_or_insert(record);
            }
            let decisions = eng.scan_with_snapshot(&[0x06], &[0x07], &snap)?;
            // Only valid transaction state is retryable. Corrupt records must
            // remain fatal even when another valid intent would defer the split.
            for (key, value) in &decisions {
                TxnRecordKey::decode(key)?;
                TxnStatusRecord::decode(value)?;
            }
            if let Some(irec) = first_intent {
                return Err(MetaDbError::WriteIntentConflict {
                    conflicting_txn_id: irec.txn_id,
                    primary_key: irec.primary_key,
                    expire_hlc: irec.expire_hlc.0,
                });
            }
            if !decisions.is_empty() {
                return Err(MetaDbError::SplitDeferred(
                    "transaction decisions must be acknowledged before fencing".into(),
                ));
            }
            for (key, value) in eng.scan_with_snapshot(
                &[SYSTEM_PREFIX_LEASE_PREIMAGE],
                &[SYSTEM_PREFIX_LEASE_PREIMAGE.saturating_add(1)],
                &snap,
            )? {
                lease_preimage_blocks_split(&key, &value)?;
            }
            if let Some(dpid) = dual_mode_pid {
                let dk = DirShardDeltaKey { pid: dpid }.encode();
                if let Some(raw_sub) = eng.get_with_snapshot(&dk, &snap)? {
                    if DirShardDeltaRecord::decode(&raw_sub)?.deleting_fence {
                        return Err(MetaDbError::SplitDeferred(
                            "cannot split DualMode directory while Rmdir deleting_fence is active"
                                .to_string(),
                        ));
                    }
                }
                let anchor_ik = WriteIntentKey::new(MetaKey::dir_attr(dpid).encode()).encode();
                if eng.get_with_snapshot(&anchor_ik, &snap)?.is_some() {
                    return Err(MetaDbError::SplitDeferred(
                        "cannot split DualMode directory while intent is held on _attr_"
                            .to_string(),
                    ));
                }
            }
        }

        let new_parent_epoch = ShardEpoch {
            conf_ver: parent_group.current_epoch().conf_ver,
            version: target_version.max(parent_group.current_epoch().version),
        };
        let peer_addrs: BTreeMap<u64, String> = parent_route
            .peers
            .iter()
            .filter(|(_, (_, is_voter))| *is_voter)
            .map(|(&nid, (addr, _))| (nid, addr.clone()))
            .collect();

        // Pre-flight voter reachability check BEFORE fencing `S_parent`: a multi-voter child
        // shard must onboard all parent voters (`add_learners_and_promote_voters`), so deferring
        // cleanly while any voter is partitioned avoids fencing `S_parent` or churning epochs!
        if !already_fenced && peer_addrs.len() > 1 {
            let local_id = host.node_id();
            for (&peer_id, peer_addr) in &peer_addrs {
                if peer_id == local_id {
                    continue;
                }
                if host
                    .channel_pool()
                    .probe_transfer_leader(peer_addr, parent_route.shard_id, peer_id, 0)
                    .await
                    .is_err()
                {
                    return Err(MetaDbError::SplitDeferred(format!(
                        "peer voter {peer_id} ({peer_addr}) unreachable for online split"
                    )));
                }
            }
        }

        // Atomically fence `S_parent` to `left_range` + `new_parent_epoch` BEFORE flushing & scanning
        // so zero concurrent writes or `AttrDelta`s can land in `right_range` on `S_parent` during split!
        if !already_fenced {
            let fence_cmd = TxRaftCmd::FenceShardRangeForSplit {
                new_range: left_range.clone(),
                new_epoch: new_parent_epoch,
            };
            let fence_payload = metadb_tx::command_payload(&fence_cmd)?;
            parent_group
                .propose(
                    parent_group.current_epoch(),
                    host.clock().now(),
                    fence_payload,
                )
                .await?;
        }
        Self::bail_crash(crash_at, SplitCrashPoint::AfterFence)?;

        // Trigger Coordinated Flush Barrier on `S_parent` so 100% of pre-fence `AttrDelta`s are folded!
        parent_group.state_machine().flush_to_lsm()?;
        Self::bail_crash(crash_at, SplitCrashPoint::AfterFlush)?;

        // Stage B.1: Record `PendingChildBoot` in `NODE_SHARD_REGISTRY` (`Resolution B15`).
        host.record_registry_state(
            child_shard_id,
            RegistryShardState::PendingChildBoot {
                parent_shard_id: parent_route.shard_id,
                peers: peer_addrs.clone(),
                split_key: split_key.clone(),
                target_version,
                parent_end_key: old_end_key.clone(),
                dual_mode_pid,
            },
        )?;

        // Stage B.2: Partition all `0x01..0x06` entries by `affinity_key_for_raw_entry` (`0x07` stays on parent!).
        let (_, snap_full) = parent_group
            .linearizable_read(parent_group.current_epoch())
            .await?;
        let all_raw = parent_group
            .engine()
            .scan_with_snapshot(&[0x01], &[0x08], &snap_full)?;
        drop(snap_full);

        let mut child_kv: BTreeMap<Vec<u8>, PersistentEntry> = BTreeMap::new();
        let mut keys_to_delete_from_parent: Vec<Vec<u8>> = Vec::new();

        for (raw_k, raw_v) in &all_raw {
            if let Some(aff_key) = affinity_key_for_raw_entry(raw_k, raw_v)? {
                if aff_key >= split_key {
                    if matches!(raw_k.first(), Some(0x05 | 0x06)) {
                        return Err(MetaDbError::ShardRoutingError(
                            "transaction state exists outside the committed parent fence".into(),
                        ));
                    }
                    // Reset `applied_lsn` / `applied_raft_index` to `0` on migrated directory attribute
                    // records because pre-split deltas were just flushed on `S_parent` and `S_child`'s
                    // brand-new OpenRaft log starts at index `1`!
                    let normalized_v = if raw_k.first().copied() == Some(TABLE_PREFIX_META)
                        && MetaKey::decode(raw_k)
                            .map(|m| m.is_attr_key())
                            .unwrap_or(false)
                    {
                        let mut attr = InodeAttr::decode(raw_v)?;
                        attr.applied_lsn = 0;
                        attr.encode()
                    } else if raw_k.first().copied()
                        == Some(metadb_core::SYSTEM_PREFIX_DIR_SHARD_DELTA)
                    {
                        let mut sub = DirShardDeltaRecord::decode(raw_v)?;
                        sub.applied_raft_index = 0;
                        sub.encode()
                    } else {
                        raw_v.clone()
                    };
                    child_kv.insert(raw_k.clone(), PersistentEntry::Value(normalized_v));
                    keys_to_delete_from_parent.push(raw_k.clone());
                }
            }
        }

        // Stage B.3: Handle `DualModeFineGrained` directory `P` (including re-splits of non-anchor sub-shards, `Resolution B17`)!
        let mut updated_parent_anchor: Option<(InodeId, InodeAttr)> = None;
        let mut updated_parent_delta: Option<DirShardDeltaRecord> = None;

        if let Some(p) = dual_mode_pid {
            let anchor_key = MetaKey::dir_attr(p).encode();
            let delta_key = DirShardDeltaKey { pid: p }.encode().to_vec();

            // Read base directory metadata from either `(P, "_attr_")` (if parent was Anchor) or `[0x04][P]` (if non-anchor)!
            let base_meta: (InodeId, u64, u32, HlcTimestamp, HlcTimestamp) =
                if let Some((_, v)) = all_raw.iter().find(|(k, _)| k == &anchor_key) {
                    let a = InodeAttr::decode(v)?;
                    (a.parent_pid, a.uid, a.mode, a.create_time, a.modify_time)
                } else if let Some((_, v)) = all_raw.iter().find(|(k, _)| k == &delta_key) {
                    let d = DirShardDeltaRecord::decode(v)?;
                    (d.parent_pid, d.uid, d.mode, d.create_time, d.modify_time)
                } else {
                    (1, 0, 0o755, now_hlc, now_hlc)
                };

            let mut left_children = 0i64;
            let mut left_bytes = 0i64;
            let mut left_mtime = base_meta.4;
            let mut right_children = 0i64;
            let mut right_bytes = 0i64;
            let mut right_mtime = base_meta.4;

            for (raw_k, raw_v) in &all_raw {
                if raw_k.first().copied() != Some(TABLE_PREFIX_META) {
                    continue;
                }
                let mk = MetaKey::decode(raw_k)?;
                if mk.pid != p || mk.is_attr_key() {
                    continue;
                }
                let entry_attr = InodeAttr::decode(raw_v)?;
                if raw_k.as_slice() < split_key.as_slice() {
                    left_children = saturating_add_i64(left_children, 1);
                    left_bytes = saturating_add_i64(left_bytes, entry_attr.size_bytes.max(0));
                    left_mtime = left_mtime.max(entry_attr.modify_time);
                } else {
                    right_children = saturating_add_i64(right_children, 1);
                    right_bytes = saturating_add_i64(right_bytes, entry_attr.size_bytes.max(0));
                    right_mtime = right_mtime.max(entry_attr.modify_time);
                }
            }

            let left_is_anchor = left_range.contains_key(&anchor_key);
            let right_is_anchor = right_range.contains_key(&anchor_key);

            if left_is_anchor {
                updated_parent_anchor = Some((
                    p,
                    InodeAttr {
                        inode_id: p,
                        parent_pid: base_meta.0,
                        entry_type: EntryType::DirectoryAttr,
                        uid: base_meta.1,
                        mode: base_meta.2,
                        link_count: 1,
                        num_children: left_children,
                        size_bytes: left_bytes,
                        create_time: base_meta.3,
                        modify_time: left_mtime,
                        fid: None,
                        applied_lsn: 0,
                    },
                ));
            } else {
                updated_parent_delta = Some(DirShardDeltaRecord {
                    inode_id: p,
                    parent_pid: base_meta.0,
                    uid: base_meta.1,
                    mode: base_meta.2,
                    create_time: base_meta.3,
                    num_children: left_children,
                    size_bytes: left_bytes,
                    modify_time: left_mtime,
                    applied_raft_index: 0,
                    deleting_fence: false,
                });
            }

            if right_is_anchor {
                let right_anchor = InodeAttr {
                    inode_id: p,
                    parent_pid: base_meta.0,
                    entry_type: EntryType::DirectoryAttr,
                    uid: base_meta.1,
                    mode: base_meta.2,
                    link_count: 1,
                    num_children: right_children,
                    size_bytes: right_bytes,
                    create_time: base_meta.3,
                    modify_time: right_mtime,
                    fid: None,
                    applied_lsn: 0,
                };
                child_kv.remove(&delta_key);
                child_kv.insert(anchor_key, PersistentEntry::Value(right_anchor.encode()));
            } else {
                let right_delta = DirShardDeltaRecord {
                    inode_id: p,
                    parent_pid: base_meta.0,
                    uid: base_meta.1,
                    mode: base_meta.2,
                    create_time: base_meta.3,
                    num_children: right_children,
                    size_bytes: right_bytes,
                    modify_time: right_mtime,
                    applied_raft_index: 0,
                    deleting_fence: false,
                };
                child_kv.insert(delta_key, PersistentEntry::Value(right_delta.encode()));
            }
        }

        // Stage B.4: Crash-safe `.splitting` materialization & atomic rename to `shards/<child_shard_id:020>`.
        let child_final_dir = host.shard_dir(child_shard_id);
        let child_tmp_dir = child_final_dir.with_extension("splitting");
        if child_tmp_dir.exists() {
            fs::remove_dir_all(&child_tmp_dir)?;
        }
        if !child_final_dir.exists() {
            let child_epoch_init = ShardEpoch {
                conf_ver: 1,
                version: target_version,
            };
            let child_wm_tmp = Arc::new(ShardWatermarkTracker::default());
            let child_handler_tmp = TxStateMachineHandler::new(
                child_shard_id,
                child_epoch_init,
                right_range.clone(),
                child_wm_tmp,
            );
            let child_lsm = LifecycleStorageEngine::open_lsm(
                child_shard_id,
                child_tmp_dir.join("lsm"),
                host.block_cache().clone(),
                lsm_config.clone(),
            )?;
            let sorted_vec: Vec<(Vec<u8>, PersistentEntry)> = child_kv.into_iter().collect();
            child_lsm.backend().flush_l0_batch(
                &sorted_vec,
                DurableShardCheckpoint {
                    flushed_raft_index: 0,
                    flushed_max_hlc: now_hlc,
                    contiguous_aux_acked_index: 0,
                    membership_bytes: Vec::new(),
                    descriptor_bytes: ShardApplyHandler::encode_descriptor_bytes(
                        &child_handler_tmp,
                    ),
                    ..Default::default()
                },
            )?;
            drop(child_lsm);
            let _shard_set = metadb_raft::backup::lock_be_shard_set(host.data_dir())?;
            if !child_final_dir.exists() {
                fs::rename(&child_tmp_dir, &child_final_dir)?;
                if let Some(parent_fs_dir) = child_final_dir.parent() {
                    sync_dir(parent_fs_dir)?;
                }
            }
        }
        Self::bail_crash(crash_at, SplitCrashPoint::AfterChildDirRename)?;

        // Boot `S_child` OpenRaft group and transition `NODE_SHARD_REGISTRY` -> `Active`.
        // When resuming an interrupted split whose `S_child` group is already running in `host`,
        // reuse the exact `Arc<TxStateMachineHandler>` bound to `existing.state_machine()` so
        // `ReplicatedAttrAggregator` and `ShardAuxOutbox` remain unified across retries!
        let child_epoch = ShardEpoch {
            conf_ver: 1,
            version: target_version,
        };
        let (child_group, child_handler) = if let Some(existing) = host.get_group(child_shard_id) {
            let any_handler: Arc<dyn std::any::Any + Send + Sync> =
                existing.state_machine().handler().clone();
            let reused = any_handler
                .downcast::<TxStateMachineHandler>()
                .map_err(|_| {
                    MetaDbError::ShardRoutingError(format!(
                        "existing child shard {child_shard_id} has non-tx state machine handler"
                    ))
                })?;
            (existing, reused)
        } else {
            let child_wm = Arc::new(ShardWatermarkTracker::default());
            let fresh_handler = Arc::new(TxStateMachineHandler::new(
                child_shard_id,
                child_epoch,
                right_range.clone(),
                child_wm.clone(),
            ));
            fresh_handler.set_control_plane_validator(Arc::new(
                crate::replicated_control::validate_replicated_record,
            ));
            let started = ShardRaftGroup::start(
                child_shard_id,
                host.node_id(),
                &child_final_dir,
                host.block_cache().clone(),
                lsm_config,
                raft_opts,
                fresh_handler.clone(),
                child_wm,
                host.clock().clone(),
                host.router(),
                host.channel_pool().clone(),
            )
            .await?;
            host.register_running_group(started.clone());
            (started, fresh_handler)
        };

        if peer_addrs.len() <= 1 {
            child_group.initialize_if_needed(peer_addrs.clone()).await?;
            tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        } else {
            let local_id = host.node_id();
            let local_addr = peer_addrs.get(&local_id).cloned().ok_or_else(|| {
                MetaDbError::ConsensusError(format!(
                    "local node {local_id} is not a voter in parent shard {}",
                    parent_route.shard_id
                ))
            })?;
            child_group
                .initialize_if_needed(BTreeMap::from([(local_id, local_addr)]))
                .await?;
            child_group
                .wait_for_leader(std::time::Duration::from_secs(10))
                .await?;

            let target_voters: std::collections::BTreeSet<u64> =
                peer_addrs.keys().copied().collect();
            let current_voters: std::collections::BTreeSet<u64> = child_group
                .uniform_voter_addrs()
                .unwrap_or_default()
                .into_keys()
                .collect();
            if current_voters != target_voters {
                for (&peer_id, peer_addr) in &peer_addrs {
                    if peer_id == local_id {
                        continue;
                    }
                    host.channel_pool()
                        .open_joining_shard(
                            peer_addr,
                            metadb_proto::OpenJoiningShardRequest {
                                cell_id: parent_route.cell_id,
                                shard_id: child_shard_id,
                                start_key: right_range.start_key.clone(),
                                end_key: right_range.end_key.clone(),
                                initial_epoch: Some(metadb_proto::ShardEpochProto {
                                    conf_ver: 1,
                                    version: target_version,
                                }),
                                use_jumbo_dedicated_root: false,
                            },
                        )
                        .await?;
                }
                let min_matched = child_group.prepare_learner_snapshot().await?;
                let learners: Vec<(u64, String)> = peer_addrs
                    .iter()
                    .filter(|(&nid, _)| nid != local_id && !current_voters.contains(&nid))
                    .map(|(&nid, addr)| (nid, addr.clone()))
                    .collect();
                if !learners.is_empty() {
                    metadb_raft::ConfChangeOrchestrator::add_learners_and_promote_voters(
                        child_group.raft(),
                        &learners,
                        target_voters,
                        64,
                        std::time::Duration::from_secs(20),
                        min_matched,
                    )
                    .await?;
                } else {
                    metadb_raft::ConfChangeOrchestrator::change_voters(
                        child_group.raft(),
                        target_voters,
                    )
                    .await?;
                }
                let _ = child_group.wait_uniform_voter_addrs().await?;
            }
        }

        host.record_registry_state(
            child_shard_id,
            RegistryShardState::Active { peers: peer_addrs },
        )?;

        // Commit `UpdateShardRangeOnSplit` cleanup on `S_parent` (`epoch.version = target_version`).
        let update_cmd = TxRaftCmd::UpdateShardRangeOnSplit {
            new_range: left_range.clone(),
            new_epoch: new_parent_epoch,
            keys_to_delete: keys_to_delete_from_parent,
            updated_anchor_attr: updated_parent_anchor,
            updated_shard_delta: updated_parent_delta,
        };
        let payload = metadb_tx::command_payload(&update_cmd)?;
        parent_group
            .propose(parent_group.current_epoch(), host.clock().now(), payload)
            .await?;
        metadb_core::code_probe("probe.cluster.online_split_completed");
        Self::bail_crash(crash_at, SplitCrashPoint::AfterParentKeyDelete)?;

        let _ = parent_handler;

        let post_split_health = if dual_mode_pid.is_some() {
            crate::shard::ShardHealthState::Normal
        } else {
            parent_route.health_state
        };

        let mut new_parent_route = parent_route.clone();
        new_parent_route.range = left_range;
        new_parent_route.epoch = new_parent_epoch;
        new_parent_route.health_state = post_split_health;

        let child_route = CompositeShardRoute {
            cell_id: parent_route.cell_id,
            shard_id: child_shard_id,
            range: right_range,
            epoch: child_group.current_epoch(),
            leader_node: child_group
                .current_leader()
                .unwrap_or(parent_route.leader_node),
            peers: parent_route.peers.clone(),
            split_mode: parent_route.split_mode,
            health_state: post_split_health,
            dual_mode_pid,
        };

        Ok((new_parent_route, child_route, child_group, child_handler))
    }

    fn bail_crash(crash_at: Option<SplitCrashPoint>, here: SplitCrashPoint) -> Result<()> {
        if crash_at == Some(here) {
            return Err(MetaDbError::StorageIoError(format!(
                "injected split crash at {here:?}"
            )));
        }
        Ok(())
    }
}

use metadb_raft::ShardApplyHandler;
