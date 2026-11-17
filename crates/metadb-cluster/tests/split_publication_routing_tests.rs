use std::sync::Arc;
use std::time::Duration;

use metadb_cluster::{
    CompositeMetaCache, DurableCellManager, DurableGlobalManager, DurableIdAllocator,
    RaftShardSplitWorker,
};
use metadb_core::{CompositeKeyRange, HybridLogicalClock, MetaKey, ROOT_INODE_ID};
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{DistributedTxRouter, ShardDirectory};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reads_and_writes_wait_for_split_route_publication_without_losing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(HybridLogicalClock::new());
    let allocator = Arc::new(DurableIdAllocator::open(dir.path().join("allocator")).unwrap());
    let cache = Arc::new(CompositeMetaCache::new());
    let host = Arc::new(
        ShardGroupHost::open(
            1,
            dir.path().join("be"),
            Arc::new(ShardedBlockCache::new(8 * 1024 * 1024)),
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
    let tx = Arc::new(DistributedTxRouter::new(1, clock, directory.clone()));
    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 7, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "left_dir", 10, 7, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "right_dir", 900, 7, 0o755)
        .await
        .unwrap();
    tx.create_file(900, "existing", (1 << 63) + 901, 901, 7, 0o644, 20)
        .await
        .unwrap();
    tx.create_file(900, "second", (1 << 63) + 902, 902, 7, 0o644, 40)
        .await
        .unwrap();

    let original_route = cell
        .list_routes()
        .into_iter()
        .find(|route| route.shard_id == 1)
        .unwrap();
    let (_, parent, handler) = cell.lookup_local_handle(1).unwrap();
    let boundary = MetaKey::encode_pid_prefix_start(500);
    let request_uuid = 0x0F3C_E900;
    let child_id = cell
        .alloc_split_plan(request_uuid, 1, boundary.clone(), 2, None)
        .unwrap();
    // Complete the data movement but deliberately hold route publication. This
    // is the exact interval after parent deletion and before activate_split.
    let (left, right, child, child_handler) = RaftShardSplitWorker::execute_local_split(
        &host,
        &original_route,
        &parent,
        &handler,
        child_id,
        boundary,
        None,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        2,
        None,
    )
    .await
    .unwrap();
    let existing_key = MetaKey::child(900, "existing").encode();
    let (_, parent_snapshot) = parent
        .linearizable_read(parent.current_epoch())
        .await
        .unwrap();
    let removed_from_parent = parent
        .engine()
        .get_with_snapshot(&existing_key, &parent_snapshot)
        .unwrap()
        .is_none();
    drop(parent_snapshot);
    let (_, child_snapshot) = child
        .linearizable_read(child.current_epoch())
        .await
        .unwrap();
    let copied_to_child = child
        .engine()
        .get_with_snapshot(&existing_key, &child_snapshot)
        .unwrap()
        .is_some();
    drop(child_snapshot);
    let stale_point_was_rejected = directory.route_user_key(&existing_key).is_err();
    let stale_range_was_rejected = directory
        .route_user_range(
            &MetaKey::encode_pid_prefix_start(900),
            &MetaKey::encode_pid_prefix_end(900),
        )
        .is_err();

    let mut lookup = tokio::spawn({
        let tx = tx.clone();
        async move { tx.lookup(900, "existing").await }
    });
    let mut missing_lookup = tokio::spawn({
        let tx = tx.clone();
        async move { tx.lookup(900, "missing").await }
    });
    let mut listing = tokio::spawn({
        let tx = tx.clone();
        async move { tx.listdir_paginated(900, None, None, 100).await }
    });
    let mut stat = tokio::spawn({
        let tx = tx.clone();
        async move { tx.dirstat(900).await }
    });
    let mut create = tokio::spawn({
        let tx = tx.clone();
        async move {
            tx.create_file(900, "created", (1 << 63) + 903, 903, 7, 0o644, 11)
                .await
        }
    });
    // The former write retry budget was about one second, while child boot can
    // leave this window open for several seconds. Reads must not return absent
    // or partial results simply because the old parent no longer owns the keys.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let completed_before_publication = [
        lookup.is_finished(),
        missing_lookup.is_finished(),
        listing.is_finished(),
        stat.is_finished(),
        create.is_finished(),
    ];
    let activation = cell.activate_split(request_uuid, left, right, Some(child_handler));
    let completed = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            &mut lookup,
            &mut missing_lookup,
            &mut listing,
            &mut stat,
            &mut create
        )
    })
    .await;
    // Preserve handles across the deadline so failed regressions still cancel
    // every waiter and shut down the real Raft groups before reporting errors.
    lookup.abort();
    missing_lookup.abort();
    listing.abort();
    stat.abort();
    create.abort();
    tokio::task::yield_now().await;
    let shutdown = host.shutdown_all().await;

    shutdown.unwrap();
    activation.unwrap();
    assert!(removed_from_parent && copied_to_child);
    assert!(stale_point_was_rejected, "stale point route was served");
    assert!(
        stale_range_was_rejected,
        "incomplete range route was served"
    );
    assert_eq!(
        completed_before_publication, [false; 5],
        "operations completed against a fenced, unpublished route"
    );
    let (lookup, missing_lookup, listing, stat, created) =
        completed.expect("operations did not resume after route publication");
    assert_eq!(lookup.unwrap().unwrap().unwrap().attr.fid, Some(901));
    assert!(missing_lookup.unwrap().unwrap().is_none());
    let (entries, cursor) = listing.unwrap().unwrap();
    let names: Vec<_> = entries
        .iter()
        .map(|entry| entry.key.name.as_str())
        .collect();
    assert!(names.contains(&"existing") && names.contains(&"second"));
    assert!(names
        .iter()
        .all(|name| ["created", "existing", "second"].contains(name)));
    assert!(cursor.is_none());
    let stat = stat.unwrap().unwrap();
    assert!(
        [(2, 60), (3, 71)].contains(&(stat.num_children, stat.size_bytes)),
        "inconsistent directory snapshot: {stat:?}"
    );
    assert_eq!(created.unwrap().unwrap().attr.fid, Some(903));
}
