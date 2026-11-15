//! Crash-injection coverage for recoverable `ExecuteSplit` (audit F1) and
//! write-before-grant ID / split-plan persistence (audit F4).

use std::sync::Arc;

use metadb_cluster::{
    AllocIdKind, CompositeMetaCache, DurableCellManager, DurableGlobalManager, DurableIdAllocator,
    SplitCrashPoint,
};
use metadb_core::{CompositeKeyRange, HybridLogicalClock, MetaKey, ROOT_INODE_ID};
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::DistributedTxRouter;
use tempfile::tempdir;

#[test]
fn persist_failure_does_not_cache_id_grant_until_disk_has_it() {
    let dir = tempdir().unwrap();
    let alloc = DurableIdAllocator::open(dir.path()).unwrap();
    let uuid = 0xF4_u128;
    alloc.testing_fail_next_persists(2);
    assert!(alloc
        .allocate_block(uuid, AllocIdKind::FileInode, 8)
        .is_err());
    assert!(!alloc.durable_grant_on_disk(uuid).unwrap());
    assert!(
        alloc
            .allocate_block(uuid, AllocIdKind::FileInode, 8)
            .is_err(),
        "retry while fsync is failing must not return a cached grant"
    );
    assert!(!alloc.durable_grant_on_disk(uuid).unwrap());

    alloc.testing_fail_next_persists(0);
    let grant = alloc
        .allocate_block(uuid, AllocIdKind::FileInode, 8)
        .unwrap();
    assert!(alloc.durable_grant_on_disk(uuid).unwrap());
    assert_eq!(
        alloc
            .allocate_block(uuid, AllocIdKind::FileInode, 8)
            .unwrap(),
        grant
    );

    let reopened = DurableIdAllocator::open(dir.path()).unwrap();
    assert_eq!(
        reopened
            .allocate_block(uuid, AllocIdKind::FileInode, 8)
            .unwrap(),
        grant
    );
    let later = reopened
        .allocate_block(0xF4_BEEF, AllocIdKind::FileInode, 8)
        .unwrap();
    assert!(later.0 > grant.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_plan_persist_failure_is_not_cached() {
    let dir = tempdir().unwrap();
    let alloc = Arc::new(DurableIdAllocator::open(dir.path().join("alloc")).unwrap());
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let meta_cache = Arc::new(CompositeMetaCache::new());
    let host =
        Arc::new(ShardGroupHost::open(1, dir.path().join("be"), cache, clock.clone()).unwrap());
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        dir.path().join("cell"),
        host.clone(),
        "127.0.0.1:0".to_string(),
        true,
        4 * 1024 * 1024,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        alloc,
        meta_cache,
    )
    .await
    .unwrap();

    let uuid = 0x5A_u128;
    cell.testing_fail_next_persists(2);
    assert!(cell
        .alloc_split_plan(uuid, 1, vec![0x01, 0x00], 2, None)
        .is_err());
    assert!(!cell.durable_split_plan_on_disk(uuid).unwrap());
    assert!(
        cell.alloc_split_plan(uuid, 1, vec![0x01, 0x00], 2, None)
            .is_err(),
        "retry must not succeed while the plan is absent from disk"
    );
    assert!(!cell.durable_split_plan_on_disk(uuid).unwrap());

    cell.testing_fail_next_persists(0);
    let child = cell
        .alloc_split_plan(uuid, 1, vec![0x01, 0x00], 2, None)
        .unwrap();
    assert!(cell.durable_split_plan_on_disk(uuid).unwrap());
    assert_eq!(
        cell.alloc_split_plan(uuid, 1, vec![0x01, 0x00], 2, None)
            .unwrap(),
        child
    );
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_crash_before_activate_is_recovered_on_reopen() {
    let points = [
        SplitCrashPoint::AfterFence,
        SplitCrashPoint::AfterFlush,
        SplitCrashPoint::AfterChildDirRename,
        SplitCrashPoint::AfterParentKeyDelete,
    ];
    for point in points {
        recover_after_split_crash(point).await;
    }
}

async fn recover_after_split_crash(point: SplitCrashPoint) {
    let dir = tempdir().unwrap();
    let be = dir.path().join("be");
    let cell_dir = dir.path().join("cell");
    let alloc_dir = dir.path().join("alloc");

    let pids = [10u64, 20, 30, 40];
    {
        let alloc = Arc::new(DurableIdAllocator::open(&alloc_dir).unwrap());
        let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
        let clock = Arc::new(HybridLogicalClock::new());
        let meta_cache = Arc::new(CompositeMetaCache::new());
        let host = Arc::new(ShardGroupHost::open(1, &be, cache, clock.clone()).unwrap());
        let cell = DurableCellManager::open_or_bootstrap(
            1,
            &cell_dir,
            host.clone(),
            "127.0.0.1:0".to_string(),
            true,
            64 * 1024 * 1024,
            LsmEngineConfig::default(),
            RaftNodeOptions::default(),
            alloc.clone(),
            meta_cache.clone(),
        )
        .await
        .unwrap();
        let gm = Arc::new(DurableGlobalManager::new(clock.clone(), alloc, meta_cache));
        gm.register_cell(CompositeKeyRange::all_user_tables(), cell.clone());
        let tx = Arc::new(DistributedTxRouter::new(1, clock, gm));
        tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
            .await
            .unwrap();
        for (idx, &dpid) in pids.iter().enumerate() {
            tx.mkdir(ROOT_INODE_ID, &format!("bucket_{idx}"), dpid, 7, 0o755)
                .await
                .unwrap();
            for f in 0..8u64 {
                tx.create_file(
                    dpid,
                    &format!("item_{f:02}.bin"),
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

        cell.set_split_crash_point(Some(point));
        let err = cell
            .maybe_split_or_isolate_shard(1, true)
            .await
            .expect_err("injected crash must surface");
        let msg = err.to_string();
        assert!(
            msg.contains("injected split crash"),
            "crash point {point:?} produced unexpected error: {msg}"
        );
        host.shutdown_all_no_flush().await.unwrap();
    }

    let alloc = Arc::new(DurableIdAllocator::open(&alloc_dir).unwrap());
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let meta_cache = Arc::new(CompositeMetaCache::new());
    let host = Arc::new(ShardGroupHost::open(1, &be, cache, clock.clone()).unwrap());
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        &cell_dir,
        host.clone(),
        "127.0.0.1:0".to_string(),
        true,
        64 * 1024 * 1024,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        alloc.clone(),
        meta_cache.clone(),
    )
    .await
    .expect("reopen must recover the interrupted split");
    let gm = Arc::new(DurableGlobalManager::new(clock.clone(), alloc, meta_cache));
    gm.register_cell(CompositeKeyRange::all_user_tables(), cell.clone());
    let tx = Arc::new(DistributedTxRouter::new(1, clock, gm));

    for &dpid in &pids {
        for f in 0..8u64 {
            let name = format!("item_{f:02}.bin");
            let found = tx
                .lookup(dpid, &name)
                .await
                .unwrap_or_else(|e| panic!("{point:?} lookup {dpid}/{name} failed: {e}"));
            assert!(
                found.is_some(),
                "{point:?}: {dpid}/{name} missing after recovery"
            );
            let key = MetaKey::child(dpid, &name).encode();
            let owners: Vec<_> = cell
                .list_routes()
                .into_iter()
                .filter(|r| r.range.contains_key(&key))
                .collect();
            assert_eq!(
                owners.len(),
                1,
                "{point:?}: key must have exactly one route owner"
            );
            let owner = owners[0].shard_id;
            assert!(
                host.get_group(owner).is_some(),
                "{point:?}: owner shard {owner} was not booted"
            );
            let (_, _, handler) = cell.lookup_local_handle(owner).unwrap();
            assert!(
                handler.key_range().contains_key(&key),
                "{point:?}: booted shard {owner} does not own the key"
            );
        }
        let stat = tx.dirstat(dpid).await.unwrap();
        assert_eq!(stat.num_children, 8, "{point:?}: dirstat drifted");
        tx.create_file(
            dpid,
            "after_recover.bin",
            dpid * 1000 + 77,
            dpid * 10_000 + 77,
            7,
            0o644,
            128,
        )
        .await
        .unwrap_or_else(|e| panic!("{point:?}: write after recovery failed: {e}"));
        assert_eq!(tx.dirstat(dpid).await.unwrap().num_children, 9);
    }

    host.shutdown_all().await.unwrap();
}
