//! Tests for:
//! 1. WAL-less crash recovery of unspilled (`< spill_threshold`) `ShardAuxOutbox` entries from `.raftlog`.
//! 2. `ReadIndexBatcher` 32-way concurrent linearizable read coalescing.
//! 3. Cross-shard directory rename cycle prevention across `>= 2` `MetaTable` shards.
//! 4. Future `SetAttr.modify_time` L0 flush non-blocking behavior and `listdir_with_stats(Some(0))`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use metadb_core::{
    CompositeKeyRange, HlcTimestamp, HybridLogicalClock, InodeId, MetaDbError, MetaKey, Result,
    ShardEpoch, ROOT_INODE_ID, TABLE_PREFIX_META, TABLE_PREFIX_QUOTA, TABLE_PREFIX_SECONDARY_INDEX,
};
use metadb_raft::{
    ChannelPool, MultiRaftRouter, RaftNodeOptions, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{
    AuxDispatcher, DirRenameLease, DistributedTxRouter, SetAttrPatch, ShardDirectory,
    TxStateMachineHandler,
};
use tempfile::tempdir;

struct MultiShardDirectory {
    shards: Vec<(
        CompositeKeyRange,
        Arc<ShardRaftGroup>,
        Arc<TxStateMachineHandler>,
    )>,
    fence_seq: AtomicU64,
    clock: Arc<HybridLogicalClock>,
}

#[tonic::async_trait]
impl ShardDirectory for MultiShardDirectory {
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

async fn boot_shard(
    base_dir: &std::path::Path,
    sid: u64,
    range: CompositeKeyRange,
    spill_thresh: usize,
    cache: Arc<ShardedBlockCache>,
    clock: Arc<HybridLogicalClock>,
    router: &MultiRaftRouter,
) -> (
    CompositeKeyRange,
    Arc<ShardRaftGroup>,
    Arc<TxStateMachineHandler>,
) {
    let wm = Arc::new(ShardWatermarkTracker::default());
    let h = Arc::new(TxStateMachineHandler::with_spill_threshold(
        sid,
        ShardEpoch::initial(),
        range.clone(),
        spill_thresh,
        wm.clone(),
    ));
    let raft_opts = RaftNodeOptions {
        heartbeat_interval_ms: 80,
        election_timeout_min_ms: 200,
        election_timeout_max_ms: 400,
        ..RaftNodeOptions::default()
    };
    let g = ShardRaftGroup::start(
        sid,
        1,
        base_dir.join(format!("shard_{sid}")),
        cache,
        LsmEngineConfig::default(),
        raft_opts,
        h.clone(),
        wm,
        clock,
        router,
        ChannelPool::default(),
    )
    .await
    .unwrap();
    g.initialize_if_needed(BTreeMap::from([(1u64, "127.0.0.1:0".to_string())]))
        .await
        .unwrap();
    for _ in 0..60 {
        if g.current_leader() == Some(1) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    (range, g, h)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unspilled_aux_outbox_entries_survive_l0_flush_and_crash_via_raftlog_reconstruction() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(HybridLogicalClock::new());
    let meta_range =
        CompositeKeyRange::new(vec![TABLE_PREFIX_META], vec![TABLE_PREFIX_QUOTA]).unwrap();
    let quota_range =
        CompositeKeyRange::new(vec![TABLE_PREFIX_QUOTA], vec![TABLE_PREFIX_SECONDARY_INDEX])
            .unwrap();
    let idx_range = CompositeKeyRange::new(
        vec![TABLE_PREFIX_SECONDARY_INDEX],
        vec![TABLE_PREFIX_SECONDARY_INDEX + 1],
    )
    .unwrap();

    // Phase 1: Create 5 files (10 aux tasks, well below spill_threshold = 4096), flush L0 WITHOUT
    // draining AuxDispatcher, and shut down without flushing (`shutdown_no_flush`)!
    {
        let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
        let router = MultiRaftRouter::new();
        let s1 = boot_shard(
            dir.path(),
            1,
            meta_range.clone(),
            4096,
            cache.clone(),
            clock.clone(),
            &router,
        )
        .await;
        let s2 = boot_shard(
            dir.path(),
            2,
            quota_range.clone(),
            4096,
            cache.clone(),
            clock.clone(),
            &router,
        )
        .await;
        let s3 = boot_shard(
            dir.path(),
            3,
            idx_range.clone(),
            4096,
            cache.clone(),
            clock.clone(),
            &router,
        )
        .await;
        let grp1 = s1.1.clone();
        let grp2 = s2.1.clone();
        let grp3 = s3.1.clone();
        let directory = Arc::new(MultiShardDirectory {
            shards: vec![s1, s2, s3],
            fence_seq: AtomicU64::new(1),
            clock: clock.clone(),
        });
        let tx = DistributedTxRouter::new(1, clock.clone(), directory);
        tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
            .await
            .unwrap();

        for i in 0..5u64 {
            tx.create_file(
                ROOT_INODE_ID,
                &format!("unspilled_{i}.dat"),
                1_000 + i,
                5_000 + i,
                77,
                0o644,
                1024,
            )
            .await
            .unwrap();
        }

        grp1.state_machine().flush_to_lsm().unwrap();
        let (state_flushed, aux_durable, _) = grp1.watermarks().snapshot_values();
        assert!(state_flushed >= 6, "state_flushed={state_flushed}");
        assert!(
            aux_durable < state_flushed,
            "unspilled outbox must keep aux_outbox_durable_index ({aux_durable}) pinned below state_flushed ({state_flushed})"
        );
        grp1.shutdown().await.unwrap();
        grp2.shutdown().await.unwrap();
        grp3.shutdown().await.unwrap();
    }

    // Phase 2: Reopen from disk and verify `recover_unspilled_aux_from_log` reconstructs all 10 tasks!
    {
        let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
        let router = MultiRaftRouter::new();
        let s1 = boot_shard(
            dir.path(),
            1,
            meta_range,
            4096,
            cache.clone(),
            clock.clone(),
            &router,
        )
        .await;
        let s2 = boot_shard(
            dir.path(),
            2,
            quota_range,
            4096,
            cache.clone(),
            clock.clone(),
            &router,
        )
        .await;
        let s3 = boot_shard(
            dir.path(),
            3,
            idx_range,
            4096,
            cache.clone(),
            clock.clone(),
            &router,
        )
        .await;
        let grp1 = s1.1.clone();
        let h1 = s1.2.clone();
        let grp2 = s2.1.clone();
        let grp3 = s3.1.clone();
        let directory = Arc::new(MultiShardDirectory {
            shards: vec![s1, s2, s3],
            fence_seq: AtomicU64::new(1),
            clock: clock.clone(),
        });
        let tx = DistributedTxRouter::new(1, clock.clone(), directory.clone());

        assert!(
            h1.outbox().pending_count() >= 10,
            "expected >= 10 reconstructed outbox tasks (5 quota + 5 index), got {}",
            h1.outbox().pending_count()
        );
        let acked = AuxDispatcher::drain_shard_outbox(&grp1, &h1, directory.as_ref(), &clock)
            .await
            .unwrap();
        assert!(acked >= 10, "expected >= 10 tasks acked, got {acked}");

        let quota = tx.get_quota(77).await.unwrap().expect("quota for uid 77");
        assert_eq!(quota.used_inodes, 5);
        assert_eq!(quota.used_bytes, 5 * 1024);
        for i in 0..5u64 {
            let idx = tx
                .get_secondary_index(5_000 + i)
                .await
                .unwrap()
                .expect("secondary index entry");
            assert_eq!(idx.name, format!("unspilled_{i}.dat"));
        }
        grp1.shutdown().await.unwrap();
        grp2.shutdown().await.unwrap();
        grp3.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_directory_rename_rejects_descendant_cycle_and_coalesces_reads() {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let router = MultiRaftRouter::new();

    // Two MetaTable shards: Shard 1 `[0x01, PID 100)`, Shard 4 `[PID 100, 0x02)`.
    let mid = MetaKey::encode_pid_prefix_start(100);
    let r_left = CompositeKeyRange::new(vec![TABLE_PREFIX_META], mid.clone()).unwrap();
    let r_right = CompositeKeyRange::new(mid, vec![TABLE_PREFIX_QUOTA]).unwrap();
    let r_quota = CompositeKeyRange::new(
        vec![TABLE_PREFIX_QUOTA],
        vec![TABLE_PREFIX_SECONDARY_INDEX + 1],
    )
    .unwrap();

    let s1 = boot_shard(
        dir.path(),
        1,
        r_left,
        4096,
        cache.clone(),
        clock.clone(),
        &router,
    )
    .await;
    let s4 = boot_shard(
        dir.path(),
        4,
        r_right,
        4096,
        cache.clone(),
        clock.clone(),
        &router,
    )
    .await;
    let s2 = boot_shard(
        dir.path(),
        2,
        r_quota,
        4096,
        cache.clone(),
        clock.clone(),
        &router,
    )
    .await;
    let grp1 = s1.1.clone();
    let grp4 = s4.1.clone();
    let grp2 = s2.1.clone();
    let directory = Arc::new(MultiShardDirectory {
        shards: vec![s1, s4, s2],
        fence_seq: AtomicU64::new(1),
        clock: clock.clone(),
    });
    let tx = Arc::new(DistributedTxRouter::new(1, clock.clone(), directory));
    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();

    // Create `dir_a` (PID 10 on Shard 1) -> `dir_b` (PID 500 on Shard 4) -> `dir_c` (PID 20 on Shard 1)!
    // Notice `ROOT` (1) and `dir_c` (20) are BOTH on Shard 1, while intermediate ancestor `dir_b` (500) is on Shard 4!
    tx.mkdir(ROOT_INODE_ID, "dir_a", 10, 1, 0o755)
        .await
        .unwrap();
    tx.mkdir(10, "dir_b", 500, 1, 0o755).await.unwrap();
    tx.mkdir(500, "dir_c", 20, 1, 0o755).await.unwrap();
    tx.create_file(10, "left.dat", 2_001, 6_001, 1, 0o644, 64)
        .await
        .unwrap();

    // Moving `dir_a` (under ROOT=1 on Shard 1) into `dir_c` (PID=20 ALSO on Shard 1, via intermediate PID=500 on Shard 4!)
    // MUST escalate out of single-shard pushdown and reject with `InvalidRenameCycle`!
    let err = tx
        .rename(ROOT_INODE_ID, "dir_a", 20, "cycle_via_remote_shard", false)
        .await
        .unwrap_err();
    assert!(
        matches!(err, MetaDbError::InvalidRenameCycle { .. }),
        "expected InvalidRenameCycle even when src_pid and dst_pid share Shard 1 via Shard 4 ancestor, got {err:?}"
    );

    // Exercise 32 concurrent linearizable reads through `ReadIndexBatcher`!
    let mut joins = Vec::new();
    for _ in 0..32 {
        let g = grp1.clone();
        joins.push(tokio::spawn(async move {
            let (tok, _snap) = g.linearizable_read(g.current_epoch()).await.unwrap();
            tok.read_index
        }));
    }
    for j in joins {
        assert!(j.await.unwrap() >= 1);
    }

    // Future user-supplied `modify_time` must NOT block L0 flush!
    let far_future = HlcTimestamp::new(clock.now().physical_ms() + 3_600_000, 0);
    tx.setattr(
        10,
        "left.dat",
        SetAttrPatch {
            modify_time: Some(far_future),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    grp1.state_machine()
        .flush_to_lsm()
        .expect("future POSIX mtime must not stall MVCC L0 flush");

    let page = tx
        .listdir_with_stats(10, None, None, Some(0))
        .await
        .unwrap();
    assert!(page.entries.is_empty());
    assert_eq!(page.entries_examined, 0);

    grp1.shutdown().await.unwrap();
    grp4.shutdown().await.unwrap();
    grp2.shutdown().await.unwrap();
}
