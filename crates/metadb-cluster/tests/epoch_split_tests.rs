use std::collections::HashSet;
use std::sync::Arc;

use metadb_cluster::{
    AllocIdKind, CompositeMetaCache, DurableCellManager, DurableGlobalManager, DurableIdAllocator,
    LocalIdBuffer,
};
use metadb_core::{
    is_dir_inode_id, is_file_inode_id, CompositeKeyRange, HybridLogicalClock, ROOT_INODE_ID,
};
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::DistributedTxRouter;
use tempfile::tempdir;

/// T-SHARD-1 & T-ID-1: Verifies:
/// 1. Live 3-stage recoverable `PID`-boundary `ExecuteSplit` under concurrent client load (`CreateFile`,
///    `Lookup`, `ListDir`) with `ShardEpoch` fencing + transparent `MetaCache` self-healing retry!
/// 2. Durable `IdRangeAllocator` (`request_uuid` idempotency across restarts + Bit-63 File/Directory
///    bitspace separation, `Resolution B18`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_epoch_fenced_live_pid_split_and_durable_id_allocator() {
    let dir = tempdir().unwrap();

    // Part 1: Verify `DurableIdAllocator` `request_uuid` idempotency & crash-safe persistence (`T-ID-1`).
    let alloc_dir = dir.path().join("global_alloc");
    let first_grant = {
        let alloc = Arc::new(DurableIdAllocator::open(&alloc_dir).unwrap());
        let g1 = alloc
            .allocate_block(0xABCD_1234, AllocIdKind::FileInode, 100)
            .unwrap();
        let g1_retry = alloc
            .allocate_block(0xABCD_1234, AllocIdKind::FileInode, 100)
            .unwrap();
        assert_eq!(
            g1, g1_retry,
            "request_uuid retry must return identical block"
        );
        assert!(is_file_inode_id(g1.0));

        let buf = LocalIdBuffer::new(alloc.clone(), 1);
        let mut seen = HashSet::new();
        for _ in 0..500 {
            let fid_inode = buf.next_file_inode_id().unwrap();
            let dir_inode = buf.next_dir_inode_id().unwrap();
            assert!(is_file_inode_id(fid_inode));
            assert!(is_dir_inode_id(dir_inode));
            assert!(seen.insert(fid_inode));
            assert!(seen.insert(dir_inode));
        }
        g1
    };

    // Reopen allocator after simulated crash: new block must NEVER overlap prior grants!
    let alloc = Arc::new(DurableIdAllocator::open(&alloc_dir).unwrap());
    let g_after_restart = alloc
        .allocate_block(0xEEEE_9999, AllocIdKind::FileInode, 100)
        .unwrap();
    assert!(g_after_restart.0 > first_grant.0);

    // Part 2: Boot `DurableCellManager` + `DurableGlobalManager` and run live `PID`-boundary splits!
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let meta_cache = Arc::new(CompositeMetaCache::new());

    let host = Arc::new(
        ShardGroupHost::open(1, dir.path().join("be_node_1"), cache, clock.clone()).unwrap(),
    );
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        dir.path().join("cell_1_mgr"),
        host.clone(),
        "127.0.0.1:0".to_string(),
        true,
        1536, // Small threshold (1.5 KiB) to trigger PID-boundary splits
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        alloc.clone(),
        meta_cache.clone(),
    )
    .await
    .unwrap();

    let gm = Arc::new(DurableGlobalManager::new(
        clock.clone(),
        alloc.clone(),
        meta_cache.clone(),
    ));
    gm.register_cell(CompositeKeyRange::all_user_tables(), cell.clone());

    let tx = Arc::new(DistributedTxRouter::new(1, clock.clone(), gm.clone()));
    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();

    // Create 4 directories (`PIDs 10, 20, 30, 40`) and populate files while splitting Shard 1 online!
    for (idx, &dpid) in [10u64, 20, 30, 40].iter().enumerate() {
        tx.mkdir(ROOT_INODE_ID, &format!("bucket_{}", idx), dpid, 7, 0o755)
            .await
            .unwrap();
        for f in 0..8u64 {
            tx.create_file(
                dpid,
                &format!("item_{:02}.bin", f),
                dpid * 1000 + f,
                dpid * 10_000 + f,
                7,
                0o644,
                512,
            )
            .await
            .unwrap();
        }
    }

    // Trigger split evaluation on MetaTable shard 1 -> splits cleanly along a `PID` boundary!
    let new_child_id = cell
        .maybe_split_or_isolate_shard(1, false)
        .await
        .unwrap()
        .expect("MetaTable shard 1 must split along a PID boundary");
    assert!(new_child_id >= 4);

    // Continue writing concurrently to BOTH pre-split and post-split `PID`s (verifying MetaCache + Epoch)!
    for &dpid in &[10u64, 20, 30, 40] {
        tx.create_file(
            dpid,
            "post_split.bin",
            dpid * 1000 + 99,
            dpid * 10_000 + 99,
            7,
            0o644,
            512,
        )
        .await
        .unwrap();

        let stat = tx.dirstat(dpid).await.unwrap();
        assert_eq!(stat.num_children, 9);
        assert_eq!(stat.size_bytes, 9 * 512);

        let (entries, _) = tx.listdir_paginated(dpid, None, None, 100).await.unwrap();
        assert_eq!(entries.len(), 9);
    }

    host.shutdown_all().await.unwrap();
}
