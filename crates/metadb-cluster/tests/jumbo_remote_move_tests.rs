//! Loopback Jumbo placement and the spare-voter record that follows `MOVE_LIVE`.
//!
//! Two listeners in this process. No cloud VM. The destination does not open
//! Raft and the source does not call `transfer_leader`.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use metadb_cluster::{
    automatic_leadership_transfer_allowed, CompositeMetaCache, DurableCellManager,
    DurableIdAllocator, PlacementPhase, RemoteJumboDisk, SpareDrainRequest,
};
use metadb_core::HybridLogicalClock;
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use tempfile::tempdir;

async fn open_cell(dir: &Path) -> (Arc<ShardGroupHost>, Arc<DurableCellManager>) {
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let host = Arc::new(ShardGroupHost::open(1, dir.join("be"), cache, clock).unwrap());
    let alloc = Arc::new(DurableIdAllocator::open(dir.join("alloc")).unwrap());
    let cell = DurableCellManager::open_or_bootstrap_in_range(
        1,
        dir.join("cell_1"),
        host.clone(),
        "127.0.0.1:1".to_string(),
        true,
        4 * 1024 * 1024 * 1024,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        alloc,
        Arc::new(CompositeMetaCache::new()),
        None,
        None,
    )
    .await
    .unwrap();
    (host, cell)
}

fn drain_request() -> SpareDrainRequest {
    SpareDrainRequest {
        shard_id: 1,
        voters: BTreeSet::from([1, 2, 3]),
        remove: 3,
        add: 4,
        leader: 1,
        dedicated_node: 2,
        proven_voters: BTreeSet::from([1, 2, 3, 4]),
        reserved_dedicated_nodes: BTreeSet::new(),
    }
}

async fn listen(root: &Path) -> (Arc<RemoteJumboDisk>, tokio::task::JoinHandle<()>) {
    let data = root.join("remote-be");
    let disk_root = root.join("remote-disk");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&disk_root).unwrap();
    let disk = Arc::new(RemoteJumboDisk::bind(2, &data, &disk_root).await.unwrap());
    let task = tokio::spawn(disk.clone().serve());
    (disk, task)
}

async fn stop_listener(task: tokio::task::JoinHandle<()>) {
    task.abort();
    let _ = task.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_live_cut_then_spare_drain_keeps_the_placement_row() {
    let dir = tempdir().unwrap();
    let remote = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let (disk, task) = listen(remote.path()).await;
    let source = host.shard_dir(1);
    std::fs::write(source.join("remote-marker"), b"jumbo-remote-marker").unwrap();
    let early = disk.read_placed_file(1, "remote-marker").unwrap_err();
    assert!(early.to_string().contains("not serving"), "{early}");

    cell.testing_mark_jumbo(1, 2, 9).unwrap();
    let blocked = cell.reconcile_jumbo_placement(1, 9, 2).await.unwrap();
    assert_eq!(blocked.phase, PlacementPhase::Blocked);
    assert!(blocked.committed_moves.is_empty(), "{blocked:?}");

    let outcome = cell
        .relocate_jumbo_shard_to_remote(1, 2, disk.local_addr(), &drain_request())
        .await
        .unwrap();
    assert_eq!(outcome.voters_after, BTreeSet::from([1, 2, 4]));
    assert_eq!(outcome.removed, 3);
    assert_eq!(outcome.added, 4);
    assert!(!outcome.raft_membership_changed);
    assert!(!outcome.destination_raft_opened);
    assert!(!outcome.transfer_leader_called);
    assert!(!automatic_leadership_transfer_allowed());
    assert!(disk.is_serving(1));
    assert_eq!(
        disk.read_placed_file(1, "remote-marker").unwrap(),
        b"jumbo-remote-marker"
    );
    assert!(
        metadb_cluster::jumbo_files::load_live_dir(disk.dedicated_root(), 1)
            .unwrap()
            .is_some()
    );
    assert!(
        !source.exists(),
        "source directory is removed only after the live cut"
    );
    assert!(cell.lookup_local_handle(1).is_err());
    assert!(cell.lookup_local_handle(2).is_ok());
    assert_eq!(
        cell.raft_voter_ids(2).unwrap(),
        BTreeSet::from([1]),
        "the one-voter raft set is not rewritten by the protocol record"
    );
    let drains = cell.stored_spare_drains().unwrap();
    assert_eq!(drains.len(), 1);
    assert_eq!(drains[0].voters_after, vec![1, 2, 4]);
    assert!(!drains[0].raft_membership_changed);
    assert!(!drains[0].transfer_leader_called);
    let handoffs = cell.stored_remote_handoffs().unwrap();
    assert_eq!(handoffs.len(), 1);
    assert_eq!(handoffs[0].dest_node, 2);
    assert!(!handoffs[0].destination_raft_opened);
    assert!(!handoffs[0].transfer_leader_called);
    let placement = cell.stored_placement(1).unwrap();
    assert_eq!(placement.phase, PlacementPhase::Blocked);
    assert!(placement.committed_moves.is_empty());

    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(host);
    stop_listener(task).await;

    let (host, cell) = open_cell(dir.path()).await;
    assert!(
        cell.lookup_local_handle(1).is_err(),
        "restart must not open an empty group for a handed-off shard"
    );
    assert!(!host.shard_dir(1).exists());
    assert!(cell.lookup_local_handle(2).is_ok());
    assert_eq!(cell.stored_remote_handoffs().unwrap().len(), 1);
    assert!(cell.stored_placement(1).is_some());
    host.shutdown_all().await.unwrap();

    let disk = RemoteJumboDisk::bind(
        2,
        &remote.path().join("remote-be"),
        &remote.path().join("remote-disk"),
    )
    .await
    .unwrap();
    assert!(disk.is_serving(1));
    assert_eq!(
        disk.read_placed_file(1, "remote-marker").unwrap(),
        b"jumbo-remote-marker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_before_the_live_cut_keeps_the_source_and_the_spare() {
    let dir = tempdir().unwrap();
    let remote = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let (disk, task) = listen(remote.path()).await;
    let source = host.shard_dir(1);
    cell.testing_mark_jumbo(1, 2, 9).unwrap();
    disk.testing_fail_before_next_live_cut();
    let failed = cell
        .relocate_jumbo_shard_to_remote(1, 2, disk.local_addr(), &drain_request())
        .await
        .unwrap_err();
    let text = failed.to_string();
    assert!(text.contains("failed before the live cut"), "{text}");
    assert!(text.contains("spare voter was not replaced"), "{text}");
    assert!(text.contains("reopened"), "{text}");
    assert!(text.contains("transfer_leader was not called"), "{text}");
    assert!(source.exists());
    assert!(cell.lookup_local_handle(1).is_ok());
    assert!(!disk.is_serving(1));
    assert!(disk.read_placed_file(1, "raft").is_err());
    assert!(
        metadb_cluster::jumbo_files::load_live_dir(disk.dedicated_root(), 1)
            .unwrap()
            .is_none()
    );
    let published = disk
        .dedicated_root()
        .join("shards")
        .join(format!("{:020}", 1u64));
    assert!(
        !published.exists(),
        "a partial destination tree must not remain"
    );
    assert!(cell.stored_spare_drains().unwrap().is_empty());
    assert!(cell.stored_remote_handoffs().unwrap().is_empty());
    assert_eq!(cell.raft_voter_ids(1).unwrap(), BTreeSet::from([1]));
    assert_eq!(cell.raft_voter_ids(2).unwrap(), BTreeSet::from([1]));
    assert!(!automatic_leadership_transfer_allowed());
    host.shutdown_all().await.unwrap();
    stop_listener(task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn draining_the_leader_does_not_copy_or_call_transfer() {
    let dir = tempdir().unwrap();
    let remote = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let (disk, task) = listen(remote.path()).await;
    cell.testing_mark_jumbo(1, 2, 9).unwrap();
    let mut drain = drain_request();
    drain.remove = 1;
    let refused = cell
        .relocate_jumbo_shard_to_remote(1, 2, disk.local_addr(), &drain)
        .await
        .unwrap_err();
    let text = refused.to_string();
    assert!(text.contains("transfer_leader was not called"), "{text}");
    assert!(text.contains("no files were copied"), "{text}");
    assert!(host.shard_dir(1).exists());
    assert!(cell.lookup_local_handle(1).is_ok());
    assert!(!disk.is_serving(1));
    assert!(cell.stored_spare_drains().unwrap().is_empty());
    assert!(cell.stored_remote_handoffs().unwrap().is_empty());

    let remote_addr = "8.8.8.8:9".parse().unwrap();
    let blocked = cell
        .relocate_jumbo_shard_to_remote(1, 2, remote_addr, &drain_request())
        .await
        .unwrap_err();
    assert!(blocked.to_string().contains("127.0.0.1"), "{blocked}");
    assert!(cell.lookup_local_handle(1).is_ok());
    host.shutdown_all().await.unwrap();
    stop_listener(task).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_dirsync_failure_does_not_delete_the_only_copy_or_drain() {
    let dir = tempdir().unwrap();
    let remote = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let (disk, task) = listen(remote.path()).await;
    let source = host.shard_dir(1);
    cell.testing_mark_jumbo(1, 2, 9).unwrap();
    disk.testing_report_visible_dirsync_failure();
    let failed = cell
        .relocate_jumbo_shard_to_remote(1, 2, disk.local_addr(), &drain_request())
        .await
        .unwrap_err();
    let text = failed.to_string();
    assert!(text.contains("directory fsync failed"), "{text}");
    assert!(text.contains("not reopened"), "{text}");
    assert!(text.contains("not deleted"), "{text}");
    assert!(text.contains("spare voter was not replaced"), "{text}");
    assert!(source.exists());
    assert!(cell.lookup_local_handle(1).is_err());
    assert!(!disk.is_serving(1));
    assert!(
        metadb_cluster::jumbo_files::load_live_dir(disk.dedicated_root(), 1)
            .unwrap()
            .is_some(),
        "MOVE_LIVE stays visible"
    );
    assert!(cell.stored_spare_drains().unwrap().is_empty());
    assert!(cell.stored_remote_handoffs().unwrap().is_empty());
    assert_eq!(cell.raft_voter_ids(2).unwrap(), BTreeSet::from([1]));
    host.shutdown_all().await.unwrap();
    stop_listener(task).await;
}
