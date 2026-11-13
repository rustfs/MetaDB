//! 3-Stage Recoverable `ExecuteSplit` Worker (`PID`-Boundary & `DualModeFineGrained` Sub-Shards,
//! `Resolutions B14, B15, B17`).

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use metadb_core::{
    affinity_key_for_raw_entry, CompositeKeyRange, DirShardDeltaKey, DirShardDeltaRecord,
    EntryType, HlcTimestamp, InodeAttr, InodeId, MetaDbError, MetaKey, Result, ShardEpoch, ShardId,
    WriteIntentKey, WriteIntentRecord, ATTR_ENTRY_NAME, TABLE_PREFIX_META,
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

        // Non-MetaTable shards (`0x02 QuotaTable`, `0x03 SecondaryIndexTable`) split at byte midpoint.
        if route.range.start_key.first().copied() != Some(TABLE_PREFIX_META) {
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
    ) -> Result<(
        CompositeShardRoute,
        CompositeShardRoute,
        Arc<ShardRaftGroup>,
        Arc<TxStateMachineHandler>,
    )> {
        let old_end_key = parent_route.range.end_key.clone();
        let target_version = parent_route.epoch.version + 1;

        // Stage A: Scan `concat([0x05], split_key) .. concat([0x05], old_end_key)` (`Resolution B14`)
        // and verify no active `deleting_fence` (`Resolution B17`) before splitting!
        let now_hlc = host.clock().now();
        {
            let (_, snap) = parent_group
                .linearizable_read(parent_group.current_epoch())
                .await?;
            let eng = parent_group.engine();
            let i_start = WriteIntentKey::encode_range_bound(&split_key);
            let i_end = WriteIntentKey::encode_range_bound(&old_end_key);
            for (_, iv) in eng.scan_with_snapshot(&i_start, &i_end, &snap)? {
                let irec = WriteIntentRecord::decode(&iv)?;
                if irec.expire_hlc >= now_hlc {
                    return Err(MetaDbError::WriteIntentConflict {
                        conflicting_txn_id: irec.txn_id,
                        primary_key: irec.primary_key,
                        expire_hlc: irec.expire_hlc.0,
                    });
                }
            }
            if let Some(dpid) = dual_mode_pid {
                let dk = DirShardDeltaKey { pid: dpid }.encode();
                if let Some(raw_sub) = eng.get_with_snapshot(&dk, &snap)? {
                    if DirShardDeltaRecord::decode(&raw_sub)?.deleting_fence {
                        return Err(MetaDbError::TransactionAborted(
                            "cannot split DualMode directory while Rmdir deleting_fence is active"
                                .to_string(),
                        ));
                    }
                }
                let anchor_ik = WriteIntentKey::new(MetaKey::dir_attr(dpid).encode()).encode();
                if eng.get_with_snapshot(&anchor_ik, &snap)?.is_some() {
                    return Err(MetaDbError::TransactionAborted(
                        "cannot split DualMode directory while intent is held on _attr_"
                            .to_string(),
                    ));
                }
            }
        }

        let left_range =
            CompositeKeyRange::new(parent_route.range.start_key.clone(), split_key.clone())?;
        let right_range = CompositeKeyRange::new(split_key.clone(), old_end_key.clone())?;
        let new_parent_epoch = ShardEpoch {
            conf_ver: parent_group.current_epoch().conf_ver,
            version: target_version,
        };

        // Atomically fence `S_parent` to `left_range` + `new_parent_epoch` BEFORE flushing & scanning
        // so zero concurrent writes or `AttrDelta`s can land in `right_range` on `S_parent` during split!
        if parent_group.current_epoch().version < target_version {
            let fence_cmd = TxRaftCmd::FenceShardRangeForSplit {
                new_range: left_range.clone(),
                new_epoch: new_parent_epoch,
            };
            let fence_payload = bincode::serialize(&fence_cmd)
                .map_err(|e| MetaDbError::CodecError(e.to_string()))?;
            parent_group
                .propose(
                    parent_group.current_epoch(),
                    host.clock().now(),
                    fence_payload,
                )
                .await?;
        }

        // Trigger Coordinated Flush Barrier on `S_parent` so 100% of pre-fence `AttrDelta`s are folded!
        parent_group.state_machine().flush_to_lsm()?;

        // Stage B.1: Record `PendingChildBoot` in `NODE_SHARD_REGISTRY` (`Resolution B15`).
        let peer_addrs: BTreeMap<u64, String> = parent_route
            .peers
            .iter()
            .filter(|(_, (_, is_voter))| *is_voter)
            .map(|(&nid, (addr, _))| (nid, addr.clone()))
            .collect();
        host.record_registry_state(
            child_shard_id,
            RegistryShardState::PendingChildBoot {
                parent_shard_id: parent_route.shard_id,
                peers: peer_addrs.clone(),
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
                    left_children += 1;
                    left_bytes += entry_attr.size_bytes;
                    left_mtime = left_mtime.max(entry_attr.modify_time);
                } else {
                    right_children += 1;
                    right_bytes += entry_attr.size_bytes;
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
            fs::rename(&child_tmp_dir, &child_final_dir)?;
            if let Some(parent_fs_dir) = child_final_dir.parent() {
                sync_dir(parent_fs_dir)?;
            }
        }

        // Boot `S_child` OpenRaft group and transition `NODE_SHARD_REGISTRY` -> `Active`.
        let child_epoch = ShardEpoch {
            conf_ver: 1,
            version: target_version,
        };
        let child_wm = Arc::new(ShardWatermarkTracker::default());
        let child_handler = Arc::new(TxStateMachineHandler::new(
            child_shard_id,
            child_epoch,
            right_range.clone(),
            child_wm.clone(),
        ));
        let child_group = ShardRaftGroup::start(
            child_shard_id,
            host.node_id(),
            &child_final_dir,
            host.block_cache().clone(),
            lsm_config,
            raft_opts,
            child_handler.clone(),
            child_wm,
            host.clock().clone(),
            host.router(),
            host.channel_pool().clone(),
        )
        .await?;
        child_group.initialize_if_needed(peer_addrs.clone()).await?;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        host.register_running_group(child_group.clone());
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
        let payload =
            bincode::serialize(&update_cmd).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        parent_group
            .propose(parent_group.current_epoch(), host.clock().now(), payload)
            .await?;

        let _ = parent_handler;

        let mut new_parent_route = parent_route.clone();
        new_parent_route.range = left_range;
        new_parent_route.epoch = new_parent_epoch;

        let child_route = CompositeShardRoute {
            cell_id: parent_route.cell_id,
            shard_id: child_shard_id,
            range: right_range,
            epoch: child_group.current_epoch(),
            leader_node: parent_route.leader_node,
            peers: parent_route.peers.clone(),
            split_mode: parent_route.split_mode,
            health_state: parent_route.health_state,
            dual_mode_pid,
        };

        Ok((new_parent_route, child_route, child_group, child_handler))
    }
}

use metadb_raft::ShardApplyHandler;
