use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use metadb_core::{
    CompositeKeyRange, HlcTimestamp, HybridLogicalClock, InodeId, MetaDbError, Result, ShardEpoch,
    ROOT_INODE_ID, SYSTEM_PREFIX_TXN_RECORD,
};
use metadb_raft::{
    ChannelPool, MultiRaftRouter, RaftNodeOptions, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{
    AuxDispatcher, DirRenameLease, DistributedTxRouter, SetAttrPatch, ShardDirectory,
    TwoPcCrashStage, TxStateMachineHandler, TxnRecoveryWorker,
};
use tempfile::tempdir;

struct MultiShardTestCluster {
    shards: Vec<(
        CompositeKeyRange,
        Arc<ShardRaftGroup>,
        Arc<TxStateMachineHandler>,
    )>,
    fence_seq: AtomicU64,
    clock: Arc<HybridLogicalClock>,
}

#[tonic::async_trait]
impl ShardDirectory for MultiShardTestCluster {
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

    fn allocate_txn_id(&self, coord_node_id: u64) -> Result<metadb_core::TxnId> {
        static NEXT_TXN: AtomicU64 = AtomicU64::new(1);
        Ok(((coord_node_id as u128) << 64) | NEXT_TXN.fetch_add(1, Ordering::Relaxed) as u128)
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

/// T-2PC-1: Verifies:
/// 1. Crash after Prepare before Primary Commit -> expired pending 2PC aborted cleanly by `TxnRecoveryWorker`.
/// 2. Crash after Primary Commit before Secondary Commit -> `[0x06] TxnRecord` stays retained while
///    secondary is unacked (`Resolution B12`), concurrent `Lookup` / `TxnRecoveryWorker` rolls forward
///    secondary idempotently, and `[0x06] TxnRecord` is garbage-collected once all participants ACK.
/// 3. Cross-shard POSIX `Rename { overwrite = true }` -> subtracts victim's quota & deletes victim's `FID`
///    from `SecondaryIndexTable` (`Resolution B11`).
/// 4. Cross-shard directory `SetAttr` (`Resolution B10`), cross-shard `Rmdir`, and directory cycle rejection
///    `rename(/a/b, /a/b/c/b)` (`InvalidRenameCycle`, `Resolution B13`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_pc_crash_recovery_posix_overwrite_and_cycle_fencing() {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let router = MultiRaftRouter::new();

    let boot = |sid: u64, range: CompositeKeyRange| {
        let base = dir.path().join(format!("shard_{}", sid));
        let cache = cache.clone();
        let clock = clock.clone();
        let router = router.clone();
        async move {
            let wm = Arc::new(ShardWatermarkTracker::default());
            let h = Arc::new(TxStateMachineHandler::new(
                sid,
                ShardEpoch::initial(),
                range.clone(),
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

    // Split MetaTable across two shards at PID=500 (`[0..500)` on Shard 1, `[500..MAX)` on Shard 2),
    // plus Shard 3 (`QuotaTable`) and Shard 4 (`SecondaryIndexTable`).
    let s1 = boot(1, CompositeKeyRange::meta_pid_range(0, 500).unwrap()).await;
    let s2 = boot(2, CompositeKeyRange::meta_pid_range(500, u64::MAX).unwrap()).await;
    let s3 = boot(3, CompositeKeyRange::quota_table_root()).await;
    let s4 = boot(4, CompositeKeyRange::secondary_index_table_root()).await;
    tokio::time::sleep(Duration::from_millis(80)).await;

    let shard1_grp = s1.1.clone();
    let shard1_h = s1.2.clone();
    let shard2_grp = s2.1.clone();
    let shard2_h = s2.2.clone();

    let cluster = Arc::new(MultiShardTestCluster {
        shards: vec![s1, s2, s3, s4],
        fence_seq: AtomicU64::new(1),
        clock: clock.clone(),
    });
    // 2s covers a slow CI schedule between `start_hlc` and Prepare. Case A still
    // waits past this TTL before recovery aborts the orphaned transaction.
    let tx = DistributedTxRouter::new(1, clock.clone(), cluster.clone()).with_ttl_ms(2_000);

    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();

    // Create `/src` (PID=100 on Shard 1) and `/dst` (PID=600 on Shard 2 -> exercises cross-shard 2PC Mkdir!).
    tx.mkdir(ROOT_INODE_ID, "src", 100, 10, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "dst", 600, 10, 0o755)
        .await
        .unwrap();

    // Case A: Crash AFTER Prepare BEFORE Primary Commit (`StopAfterPrepareBeforePrimaryCommit`).
    tx.create_file(100, "abort_me.txt", 1001, 8001, 10, 0o644, 2048)
        .await
        .unwrap();
    let err_a = tx
        .rename_with_fault_injection(
            100,
            "abort_me.txt",
            600,
            "aborted_dst.txt",
            false,
            TwoPcCrashStage::StopAfterPrepareBeforePrimaryCommit,
        )
        .await
        .unwrap_err();
    assert!(matches!(err_a, MetaDbError::RpcError(_)));

    // Sleep past the 2s 2PC TTL so `TxnRecoveryWorker` aborts the orphaned pending transaction.
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    TxnRecoveryWorker::run_recovery_pass_on_shard(&shard1_grp, &shard1_h, cluster.as_ref(), &clock)
        .await
        .unwrap();
    TxnRecoveryWorker::run_recovery_pass_on_shard(&shard2_grp, &shard2_h, cluster.as_ref(), &clock)
        .await
        .unwrap();

    // Original file must remain intact in `/src`, and `/dst/aborted_dst.txt` must not exist!
    assert!(tx.lookup(100, "abort_me.txt").await.unwrap().is_some());
    assert!(tx.lookup(600, "aborted_dst.txt").await.unwrap().is_none());

    // Case B: Crash AFTER Primary Commit BEFORE Secondary Commit (`StopAfterPrimaryCommitBeforeSecondary`),
    //         AND POSIX Overwrite (`overwrite = true`) over an existing victim file owned by User 20!
    tx.create_file(600, "target.txt", 2001, 9001, 20, 0o644, 5000)
        .await
        .unwrap();
    let err_b = tx
        .rename_with_fault_injection(
            100,
            "abort_me.txt", // owned by User 10, size 2048, FID 8001
            600,
            "target.txt", // victim owned by User 20, size 5000, FID 9001
            true,
            TwoPcCrashStage::StopAfterPrimaryCommitBeforeSecondary,
        )
        .await
        .unwrap_err();
    assert!(matches!(err_b, MetaDbError::RpcError(_)));

    // Verify `[0x06] TxnRecord` IS retained on Primary Shard 1 before Secondary Shard 2 ACKs (`Resolution B12`)!
    {
        let (_, snap1) = shard1_grp
            .linearizable_read(shard1_grp.current_epoch())
            .await
            .unwrap();
        let tx_records = shard1_grp
            .engine()
            .scan_with_snapshot(
                &[SYSTEM_PREFIX_TXN_RECORD],
                &[SYSTEM_PREFIX_TXN_RECORD + 1],
                &snap1,
            )
            .unwrap();
        assert_eq!(
            tx_records.len(),
            1,
            "Primary TxnRecord must be retained until Secondary Shard 2 ACKs"
        );
    }

    // Now perform a linearizable `lookup(600, "target.txt")` on Secondary Shard 2:
    // Reader encounters `[0x05]` WriteIntent, resolves via Primary `TxnRecord::Committed`, rolls forward,
    // and deletes `[0x06] TxnRecord` once all secondaries ACK!
    let resolved_entry = tx
        .lookup(600, "target.txt")
        .await
        .unwrap()
        .expect("committed 2PC rename must roll forward on Secondary Shard 2");
    assert_eq!(resolved_entry.attr.inode_id, 1001);
    assert_eq!(resolved_entry.attr.uid, 10);
    assert!(tx.lookup(100, "abort_me.txt").await.unwrap().is_none());

    {
        let (_, snap1) = shard1_grp
            .linearizable_read(shard1_grp.current_epoch())
            .await
            .unwrap();
        let tx_records = shard1_grp
            .engine()
            .scan_with_snapshot(
                &[SYSTEM_PREFIX_TXN_RECORD],
                &[SYSTEM_PREFIX_TXN_RECORD + 1],
                &snap1,
            )
            .unwrap();
        assert_eq!(
            tx_records.len(),
            0,
            "Primary TxnRecord must be cleaned up once all secondaries durably ACK"
        );
    }

    // Drain outboxes across Shard 1 & Shard 2 and verify POSIX overwrite victim accounting (`Resolution B11`)!
    AuxDispatcher::drain_shard_outbox(&shard1_grp, &shard1_h, cluster.as_ref(), &clock)
        .await
        .unwrap();
    AuxDispatcher::drain_shard_outbox(&shard2_grp, &shard2_h, cluster.as_ref(), &clock)
        .await
        .unwrap();

    // User 20's overwritten victim file (`5000` bytes, `1` inode, `FID = 9001`) must be reclaimed!
    let q20 = tx.get_quota(20).await.unwrap().unwrap();
    assert_eq!(q20.used_bytes, 0);
    assert_eq!(q20.used_inodes, 0);
    assert!(tx.get_secondary_index(9001).await.unwrap().is_none());

    // Source `FID = 8001` must point to `(600, "target.txt")`, and `dirstat(600)` must have 1 child, 2048 bytes!
    let idx_8001 = tx.get_secondary_index(8001).await.unwrap().unwrap();
    assert_eq!(idx_8001.pid, 600);
    assert_eq!(idx_8001.name, "target.txt");
    let dst_stat = tx.dirstat(600).await.unwrap();
    assert_eq!(dst_stat.num_children, 1);
    assert_eq!(dst_stat.size_bytes, 2048);

    // Case C: Cross-shard Directory `SetAttr` (`Resolution B10`) & Directory Cycle Rejection (`Resolution B13`)!
    // `/dst` has dentry `(1, "dst")` on Shard 1 and authoritative `(600, "_attr_")` on Shard 2!
    tx.setattr(
        ROOT_INODE_ID,
        "dst",
        SetAttrPatch {
            mode: Some(0o700),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(tx.dirstat(600).await.unwrap().mode, 0o700);

    // Create `/dst/child_dir` (PID=200 on Shard 1, parent=600 on Shard 2) and attempt `rename(/dst, /dst/child_dir/loop)`!
    tx.mkdir(600, "child_dir", 200, 10, 0o755).await.unwrap();
    let cycle_err = tx
        .rename(ROOT_INODE_ID, "dst", 200, "loop", false)
        .await
        .unwrap_err();
    assert_eq!(
        cycle_err,
        MetaDbError::InvalidRenameCycle {
            dir_id: 600,
            dst_pid: 200
        }
    );

    // Cross-shard `rmdir(600, "child_dir")` succeeds once empty!
    tx.rmdir(600, "child_dir").await.unwrap();
    assert!(tx.lookup(600, "child_dir").await.unwrap().is_none());

    for (_, g, _) in &cluster.shards {
        g.shutdown().await.unwrap();
    }
}
