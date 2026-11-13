use std::sync::Arc;

use metadb_cluster::{
    CompositeMetaCache, DurableCellManager, DurableGlobalManager, DurableIdAllocator,
    ShardHealthState,
};
use metadb_core::{CompositeKeyRange, HybridLogicalClock, ROOT_INODE_ID};
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::DistributedTxRouter;
use tempfile::tempdir;

/// T-SHARD-2: Verifies:
/// 1. Single-directory Jumbo Shard detection & dedicated-node isolation (`JumboIsolated`).
/// 2. Enabling `DualModeFineGrained` and **re-splitting** a single directory across **3 sub-shards**
///    (`Resolution B17`, testing both Anchor and non-anchor `[0x04] DirShardDelta` re-splits!).
/// 3. Lock-free `1-RTT` `CreateFile`/`Unlink` on sub-shards, shared-`barrier_hlc` scatter-gather
///    `DirStat`, cursor-paginated `ListDir` across sub-shards, and 2PC `Rmdir` across all 3 sub-shards!
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_jumbo_isolation_and_three_way_dual_mode_scatter_gather_and_rmdir() {
    let dir = tempdir().unwrap();
    let alloc = Arc::new(DurableIdAllocator::open(dir.path().join("alloc")).unwrap());
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let meta_cache = Arc::new(CompositeMetaCache::new());

    let host =
        Arc::new(ShardGroupHost::open(1, dir.path().join("be_1"), cache, clock.clone()).unwrap());
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        dir.path().join("cell_1"),
        host.clone(),
        "127.0.0.1:0".to_string(),
        true,
        1200,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        alloc.clone(),
        meta_cache.clone(),
    )
    .await
    .unwrap();

    // Register spare nodes 2 and 3 so `JumboIsolationPlanner` isolates onto an unloaded dedicated node.
    cell.register_node(2, "127.0.0.1:9002".to_string()).unwrap();
    cell.register_node(3, "127.0.0.1:9003".to_string()).unwrap();

    let gm = Arc::new(DurableGlobalManager::new(
        clock.clone(),
        alloc,
        meta_cache.clone(),
    ));
    gm.register_cell(CompositeKeyRange::all_user_tables(), cell.clone());

    let tx = DistributedTxRouter::new(1, clock.clone(), gm);
    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "jumbo_dir", 800, 5, 0o755)
        .await
        .unwrap();

    // Insert 24 files into `PID = 800` (>90% of shard 1 bytes!).
    for i in 0..24u64 {
        tx.create_file(
            800,
            &format!("part_{:04}.parquet", i),
            5000 + i,
            6000 + i,
            5,
            0o644,
            100,
        )
        .await
        .unwrap();
    }

    // 1. In `DirectoryColocated` mode, Shard 1 refuses mid-directory split and enters `JumboIsolated`!
    let split_res = cell.maybe_split_or_isolate_shard(1, true).await.unwrap();
    assert!(split_res.is_none());
    let routes_1 = cell.list_routes();
    let s1_route = routes_1.iter().find(|r| r.shard_id == 1).unwrap();
    assert!(matches!(
        s1_route.health_state,
        ShardHealthState::JumboIsolated {
            dominant_pid: 800,
            ..
        }
    ));

    // 2. Operator enables `DualModeFineGrained` on Shard 1 (`PID = 800`) and splits TWICE -> 3 sub-shards!
    cell.enable_dual_mode_split(1, 800).unwrap();
    let child_a = cell
        .maybe_split_or_isolate_shard(1, true)
        .await
        .unwrap()
        .expect("first DualMode split must succeed");
    // Re-split the newly created non-anchor right sub-shard `child_a` (`Resolution B17`)!
    let child_b = cell
        .maybe_split_or_isolate_shard(child_a, true)
        .await
        .unwrap()
        .expect("re-splitting DualMode non-anchor sub-shard must succeed");
    assert!(child_b > child_a);

    // 3. Perform `1-RTT` `create_file` and `unlink` across all 3 sub-shards!
    tx.create_file(800, "aaa_early.parquet", 5100, 6100, 5, 0o644, 100)
        .await
        .unwrap();
    tx.create_file(800, "zzz_late.parquet", 5101, 6101, 5, 0o644, 100)
        .await
        .unwrap();
    tx.unlink(800, "part_0012.parquet").await.unwrap();

    // Total live files in `PID = 800`: `24 + 2 - 1 = 25` files, `25 * 100 = 2500` bytes!
    let stat = tx.dirstat(800).await.unwrap();
    assert_eq!(stat.num_children, 25);
    assert_eq!(stat.size_bytes, 2500);

    // 4. Paginate `ListDir` across the 3 sub-shards in pages of 7 entries (`keyset continuation`)!
    let mut all_names = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let (page, next) = tx
            .listdir_paginated(800, cursor.as_deref(), None, 7)
            .await
            .unwrap();
        for e in page {
            all_names.push(e.key.name);
        }
        if next.is_none() {
            break;
        }
        cursor = next;
    }
    assert_eq!(all_names.len(), 25);
    let mut sorted_check = all_names.clone();
    sorted_check.sort();
    assert_eq!(all_names, sorted_check);

    // 5. Delete all 25 files across the 3 sub-shards, then execute 2PC `rmdir(1, "jumbo_dir")`!
    for name in &all_names {
        tx.unlink(800, name).await.unwrap();
    }
    assert_eq!(tx.dirstat(800).await.unwrap().num_children, 0);
    tx.rmdir(ROOT_INODE_ID, "jumbo_dir").await.unwrap();
    assert!(tx
        .lookup(ROOT_INODE_ID, "jumbo_dir")
        .await
        .unwrap()
        .is_none());

    host.shutdown_all().await.unwrap();
}
