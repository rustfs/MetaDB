use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use metadb_core::{
    AuxOutboxEntry, AuxTaskPayload, CompositeKeyRange, HlcTimestamp, HybridLogicalClock, InodeId,
    MetaDbError, Result, ShardEpoch, SpilledAuxOutboxKey, ROOT_INODE_ID,
    SYSTEM_PREFIX_SPILLED_AUX_OUTBOX,
};
use metadb_raft::{
    ChannelPool, MultiRaftRouter, RaftNodeOptions, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{
    AuxDispatcher, DirRenameLease, DistributedTxRouter, ShardDirectory, TxRaftCmd,
    TxStateMachineHandler,
};
use tempfile::tempdir;

struct ThreeTableDirectory {
    shards: Vec<(
        CompositeKeyRange,
        Arc<ShardRaftGroup>,
        Arc<TxStateMachineHandler>,
    )>,
    fence_seq: AtomicU64,
    clock: Arc<HybridLogicalClock>,
}

#[tonic::async_trait]
impl ShardDirectory for ThreeTableDirectory {
    fn route_user_key(
        &self,
        user_key: &[u8],
    ) -> Result<(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)> {
        for (r, g, h) in &self.shards {
            if r.contains_key(user_key) {
                return Ok((g.current_epoch(), g.clone(), h.clone()));
            }
        }
        Err(MetaDbError::ShardRoutingError("unrouted key".to_string()))
    }

    fn route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> Result<Vec<(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)>> {
        Ok(self
            .shards
            .iter()
            .filter(|(r, _, _)| r.overlaps(start_user_key, end_user_key))
            .map(|(_, g, h)| (g.current_epoch(), g.clone(), h.clone()))
            .collect())
    }

    async fn acquire_dir_rename_lease(
        &self,
        _moved_dir_id: InodeId,
        ttl_ms: u64,
    ) -> Result<DirRenameLease> {
        Ok(DirRenameLease {
            rename_fence_token: self.fence_seq.fetch_add(1, Ordering::AcqRel),
            expire_hlc: HlcTimestamp::new(self.clock.now().physical_ms() + ttl_ms, 0),
        })
    }

    async fn release_dir_rename_lease(&self, _token: u64) {}
}

/// T-TX-2: Verifies:
/// 1. Unspilled tasks keep `aux_outbox_durable_index` pinned below unacked Raft index (`Alignment #2`).
/// 2. Exceeding `spill_threshold` spills multi-task `[0x07][origin_raft_index][task_ordinal]` entries
///    (`Resolution B7`), advancing `aux_outbox_durable_index` so Raft logs can purge!
/// 3. `AuxOutOfOrderGap` rejects gapped quota batches (`Resolution B8`).
/// 4. Duplicate replay is suppressed (`applied_by_shard: (raft_idx, ord)`), `AckAuxTasks` deletes
///    `[0x07]` keys (`Resolution B9`), and unlink tombstones block stale pre-unlink index upserts!
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_aux_outbox_spill_contiguous_frontiers_gap_rejection_and_cleanup() {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let router = MultiRaftRouter::new();

    let boot_shard = |sid: u64, range: CompositeKeyRange, spill_thresh: usize| {
        let base = dir.path().join(format!("shard_{}", sid));
        let cache = cache.clone();
        let clock = clock.clone();
        let router = router.clone();
        async move {
            let wm = Arc::new(ShardWatermarkTracker::default());
            let h = Arc::new(TxStateMachineHandler::with_spill_threshold(
                sid,
                ShardEpoch::initial(),
                range.clone(),
                spill_thresh,
                wm.clone(),
            ));
            let g = ShardRaftGroup::start(
                sid,
                1,
                base,
                cache,
                LsmEngineConfig::default(),
                RaftNodeOptions::default(),
                h.clone(),
                wm,
                clock,
                &router,
                ChannelPool::default(),
            )
            .await
            .unwrap();
            g.initialize_if_needed(BTreeMap::from([(1u64, "127.0.0.1:0".to_string())]))
                .await
                .unwrap();
            (range, g, h)
        }
    };

    // Shard 1: MetaTable (`spill_threshold = 6` tasks = 3 files * 2 tasks/file).
    // Shard 2: QuotaTable.
    // Shard 3: SecondaryIndexTable.
    let s1 = boot_shard(1, CompositeKeyRange::meta_table_root(), 6).await;
    let s2 = boot_shard(2, CompositeKeyRange::quota_table_root(), 64).await;
    let s3 = boot_shard(3, CompositeKeyRange::secondary_index_table_root(), 64).await;
    tokio::time::sleep(Duration::from_millis(80)).await;

    let meta_grp = s1.1.clone();
    let meta_handler = s1.2.clone();
    let quota_grp = s2.1.clone();
    let idx_grp = s3.1.clone();

    let directory = Arc::new(ThreeTableDirectory {
        shards: vec![s1, s2, s3],
        fence_seq: AtomicU64::new(1),
        clock: clock.clone(),
    });
    let tx = DistributedTxRouter::new(1, clock.clone(), directory.clone());

    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();

    // 1. Create 1 file (2 outbox tasks < spill_threshold 6) and flush L0 WITHOUT draining outbox:
    //    `aux_outbox_durable_index` MUST NOT advance to `state_flushed_index` because tasks are unspilled!
    tx.create_file(ROOT_INODE_ID, "f1.bin", 101, 5001, 42, 0o644, 4096)
        .await
        .unwrap();
    let (state_flushed_1, _) = meta_grp.state_machine().flush_to_lsm().unwrap();
    let (_, aux_durable_1, _) = meta_grp.watermarks().snapshot_values();
    assert!(
        aux_durable_1 < state_flushed_1,
        "unspilled/unacked tasks must hold aux_outbox_durable_index ({}) below state_flushed_index ({})",
        aux_durable_1,
        state_flushed_1
    );

    // 2. Create 3 more files (total 8 outbox tasks >= spill_threshold 6) and flush L0:
    //    All 8 tasks (`ordinal=0` QuotaDelta + `ordinal=1` IndexUpsert per file) spill to `[0x07]`,
    //    advancing `aux_outbox_durable_index == state_flushed_2`!
    for i in 2..=4u64 {
        tx.create_file(
            ROOT_INODE_ID,
            &format!("f{}.bin", i),
            100 + i,
            5000 + i,
            42,
            0o644,
            4096,
        )
        .await
        .unwrap();
    }
    let (state_flushed_2, _) = meta_grp.state_machine().flush_to_lsm().unwrap();
    let (_, aux_durable_2, _) = meta_grp.watermarks().snapshot_values();
    assert_eq!(aux_durable_2, state_flushed_2);

    // Verify `[0x07]` contains all 8 spilled entries (`2` distinct ordinals per `CreateFile`!).
    let (_, snap_spilled) = meta_grp
        .linearizable_read(meta_grp.current_epoch())
        .await
        .unwrap();
    let spilled_rows = meta_grp
        .engine()
        .scan_with_snapshot(
            &[SYSTEM_PREFIX_SPILLED_AUX_OUTBOX],
            &SpilledAuxOutboxKey::prefix_end(),
            &snap_spilled,
        )
        .unwrap();
    assert_eq!(spilled_rows.len(), 8);
    drop(snap_spilled);

    // 3. Verify `AuxOutOfOrderGap` rejection when sending a gapped `prev_uid_cursor` to `QuotaTable`!
    let gapped_cmd = TxRaftCmd::ApplyAuxBatch {
        entries: vec![AuxOutboxEntry {
            origin_shard_id: 1,
            origin_raft_index: 99,
            task_ordinal: 0,
            hlc: clock.now(),
            payload: AuxTaskPayload::QuotaDelta {
                uid: 42,
                delta_bytes: 100,
                delta_inodes: 1,
                prev_uid_cursor: (50, 0), // Gap: QuotaTable has (0, 0) so far!
            },
        }],
    };
    let gap_err = quota_grp
        .propose(
            quota_grp.current_epoch(),
            clock.now(),
            bincode::serialize(&gapped_cmd).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(gap_err, MetaDbError::AuxOutOfOrderGap { .. }));

    // 4. Drain outbox via `AuxDispatcher` -> verify exact quota (`4 * 4096` bytes, `4` inodes),
    //    replay same batch idempotently, and verify `[0x07]` spilled keys were cleaned up!
    let pending_snapshot = meta_handler.outbox().pending_entries();
    let drained =
        AuxDispatcher::drain_shard_outbox(&meta_grp, &meta_handler, directory.as_ref(), &clock)
            .await
            .unwrap();
    assert_eq!(drained, 8);
    assert_eq!(meta_handler.outbox().pending_count(), 0);

    // Replay duplicate quota entries -> must not double-add!
    let quota_only: Vec<_> = pending_snapshot
        .into_iter()
        .filter(|e| matches!(e.payload, AuxTaskPayload::QuotaDelta { .. }))
        .collect();
    quota_grp
        .propose(
            quota_grp.current_epoch(),
            clock.now(),
            bincode::serialize(&TxRaftCmd::ApplyAuxBatch {
                entries: quota_only,
            })
            .unwrap(),
        )
        .await
        .unwrap();

    let q42 = tx.get_quota(42).await.unwrap().unwrap();
    assert_eq!(q42.used_bytes, 4 * 4096);
    assert_eq!(q42.used_inodes, 4);

    let (_, snap_after_ack) = meta_grp
        .linearizable_read(meta_grp.current_epoch())
        .await
        .unwrap();
    let remaining_spills = meta_grp
        .engine()
        .scan_with_snapshot(
            &[SYSTEM_PREFIX_SPILLED_AUX_OUTBOX],
            &SpilledAuxOutboxKey::prefix_end(),
            &snap_after_ack,
        )
        .unwrap();
    assert_eq!(
        remaining_spills.len(),
        0,
        "AckAuxTasks must delete acknowledged [0x07] spill keys"
    );
    drop(snap_after_ack);

    // 5. Unlink `f1.bin` (FID 5001), drain outbox, and verify stale pre-unlink upsert is suppressed by tombstone!
    let pre_unlink_hlc = clock.now();
    tx.unlink(ROOT_INODE_ID, "f1.bin").await.unwrap();
    AuxDispatcher::drain_shard_outbox(&meta_grp, &meta_handler, directory.as_ref(), &clock)
        .await
        .unwrap();
    assert!(tx.get_secondary_index(5001).await.unwrap().is_none());

    idx_grp
        .propose(
            idx_grp.current_epoch(),
            clock.now(),
            bincode::serialize(&TxRaftCmd::ApplyAuxBatch {
                entries: vec![AuxOutboxEntry {
                    origin_shard_id: 1,
                    origin_raft_index: 1,
                    task_ordinal: 1,
                    hlc: pre_unlink_hlc,
                    payload: AuxTaskPayload::IndexUpsert {
                        fid: 5001,
                        pid: ROOT_INODE_ID,
                        name: "f1.bin".to_string(),
                        create_time: pre_unlink_hlc,
                    },
                }],
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        tx.get_secondary_index(5001).await.unwrap().is_none(),
        "stale pre-unlink IndexUpsert must not resurrect unlinked FID"
    );

    // 6. Out-of-order `IndexDelete` arriving BEFORE `IndexUpsert` at identical HLC (`task_ordinal` tie-breaker):
    //    `tombstone_ver = (same_hlc, 1, 50, 1) > incoming_ver = (same_hlc, 1, 50, 0)` must suppress resurrection!
    let same_hlc = clock.now();
    idx_grp
        .propose(
            idx_grp.current_epoch(),
            clock.now(),
            bincode::serialize(&TxRaftCmd::ApplyAuxBatch {
                entries: vec![AuxOutboxEntry {
                    origin_shard_id: 1,
                    origin_raft_index: 50,
                    task_ordinal: 1,
                    hlc: same_hlc,
                    payload: AuxTaskPayload::IndexDelete { fid: 9999 },
                }],
            })
            .unwrap(),
        )
        .await
        .unwrap();
    idx_grp
        .propose(
            idx_grp.current_epoch(),
            clock.now(),
            bincode::serialize(&TxRaftCmd::ApplyAuxBatch {
                entries: vec![AuxOutboxEntry {
                    origin_shard_id: 1,
                    origin_raft_index: 50,
                    task_ordinal: 0,
                    hlc: same_hlc,
                    payload: AuxTaskPayload::IndexUpsert {
                        fid: 9999,
                        pid: ROOT_INODE_ID,
                        name: "reordered.bin".to_string(),
                        create_time: same_hlc,
                    },
                }],
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        tx.get_secondary_index(9999).await.unwrap().is_none(),
        "out-of-order IndexDelete persisted before IndexUpsert must suppress lower-tuple resurrection"
    );

    meta_grp.shutdown().await.unwrap();
    quota_grp.shutdown().await.unwrap();
    idx_grp.shutdown().await.unwrap();
}
