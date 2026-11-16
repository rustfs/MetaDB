use metadb_cluster::{
    CompositeMetaCache, DurableCellManager, DurableGlobalManager, DurableIdAllocator,
    RaftShardSplitWorker, SplitCrashPoint,
};
use metadb_core::{CompositeKeyRange, HybridLogicalClock, MetaDbError, MetaKey};
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{DistributedTxRouter, TwoPcCrashStage, TxnRecoveryWorker};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_secondary_must_survive_split_copy_and_recovery_interleaving() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(HybridLogicalClock::new());
    let allocator = Arc::new(DurableIdAllocator::open(dir.path().join("allocator")).unwrap());
    let cache = Arc::new(CompositeMetaCache::new());
    let host = Arc::new(
        ShardGroupHost::open(
            1,
            dir.path().join("be"),
            Arc::new(ShardedBlockCache::new(1 << 20)),
            clock.clone(),
        )
        .unwrap(),
    );
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        dir.path().join("cell"),
        host.clone(),
        "127.0.0.1:0".into(),
        true,
        usize::MAX,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        allocator.clone(),
        cache.clone(),
    )
    .await
    .unwrap();
    let directory = Arc::new(DurableGlobalManager::new(clock.clone(), allocator, cache));
    directory.register_cell(CompositeKeyRange::all_user_tables(), cell.clone());
    let tx = DistributedTxRouter::new(1, clock.clone(), directory.clone()).with_ttl_ms(500);
    tx.init_dir_attr(1, 1, 7, 0o755).await.unwrap();
    tx.mkdir(1, "source_dir", 100, 7, 0o755).await.unwrap();
    tx.mkdir(1, "target_dir", 900, 7, 0o755).await.unwrap();

    let route = cell
        .list_routes()
        .into_iter()
        .find(|r| r.shard_id == 1)
        .unwrap();
    let (_, group, handler) = cell.lookup_local_handle(1).unwrap();
    let boundary = MetaKey::encode_pid_prefix_start(500);
    let secondary_id = cell
        .alloc_split_plan(111, 1, boundary.clone(), 2, None)
        .unwrap();
    let (left, right, _, right_handler) = RaftShardSplitWorker::execute_local_split(
        &host,
        &route,
        &group,
        &handler,
        secondary_id,
        boundary,
        None,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        2,
        None,
    )
    .await
    .unwrap();
    cell.activate_split(111, left, right, Some(right_handler))
        .unwrap();
    tx.create_file(100, "src", (1 << 63) + 77, 77, 7, 0o644, 100)
        .await
        .unwrap();
    let interrupted = tx
        .rename_with_fault_injection(
            100,
            "src",
            900,
            "dst",
            false,
            TwoPcCrashStage::StopAfterPrimaryCommitBeforeSecondary,
        )
        .await;
    assert!(interrupted.is_err());
    tokio::time::sleep(Duration::from_millis(600)).await;

    let route = cell
        .list_routes()
        .into_iter()
        .find(|r| r.shard_id == secondary_id)
        .unwrap();
    let (_, secondary, secondary_handler) = cell.lookup_local_handle(secondary_id).unwrap();
    let boundary = MetaKey::encode_pid_prefix_start(800);
    let child_id = cell
        .alloc_split_plan(222, secondary_id, boundary.clone(), 3, None)
        .unwrap();
    let paused = RaftShardSplitWorker::execute_local_split(
        &host,
        &route,
        &secondary,
        &secondary_handler,
        child_id,
        boundary.clone(),
        None,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        3,
        Some(SplitCrashPoint::AfterChildDirRename),
    )
    .await;
    assert!(
        matches!(paused, Err(MetaDbError::WriteIntentConflict { .. })),
        "expired committed intents must defer the split before copying"
    );
    assert_eq!(
        secondary_handler.key_range(),
        route.range,
        "rejected split must leave the original range writable for recovery"
    );
    assert!(
        !host.shard_dir(child_id).exists(),
        "rejected split must not publish a stale child image"
    );

    // Recover the committed transaction against the original route, then retry
    // the deferred split. No stale intent or decision may enter the child image.
    TxnRecoveryWorker::run_recovery_pass_on_shard(&group, &handler, directory.as_ref(), &clock)
        .await
        .unwrap();
    let (_, primary_snapshot) = group
        .linearizable_read(group.current_epoch())
        .await
        .unwrap();
    let remaining = group
        .engine()
        .scan_with_snapshot(&[0x06], &[0x07], &primary_snapshot)
        .unwrap();
    drop(primary_snapshot);
    assert!(
        remaining.is_empty(),
        "primary decision was not GC'd, so the intended interleaving did not occur"
    );
    let (_, snap) = secondary
        .linearizable_read(secondary.current_epoch())
        .await
        .unwrap();
    let recovered_on_parent = secondary
        .engine()
        .get_with_snapshot(&MetaKey::child(900, "dst").encode(), &snap)
        .unwrap();
    drop(snap);
    assert!(
        recovered_on_parent.is_some(),
        "recovery failed to materialize on old parent"
    );

    let (left, right, _, right_handler) = RaftShardSplitWorker::execute_local_split(
        &host,
        &route,
        &secondary,
        &secondary_handler,
        child_id,
        boundary,
        None,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        3,
        None,
    )
    .await
    .unwrap();
    cell.activate_split(222, left, right, Some(right_handler))
        .unwrap();
    let (_, child_group, child_handler) = cell.lookup_local_handle(child_id).unwrap();
    TxnRecoveryWorker::run_recovery_pass_on_shard(
        &child_group,
        &child_handler,
        directory.as_ref(),
        &clock,
    )
    .await
    .unwrap();
    let destination = tx.lookup(900, "dst").await.unwrap();
    let source = tx.lookup(100, "src").await.unwrap();
    host.shutdown_all().await.unwrap();
    assert!(source.is_none() && destination.as_ref().map(|e|e.attr.fid)==Some(Some(77)),
        "committed rename lost its destination after split/recovery: src={source:?}, dst={destination:?}");
}
