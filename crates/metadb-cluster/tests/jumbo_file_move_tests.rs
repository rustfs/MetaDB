//! The local jumbo shard directory can move onto a dedicated disk root.
//!
//! This does not copy files onto another machine and does not call
//! `transfer_leader`.

use std::sync::Arc;

use metadb_cluster::{CompositeMetaCache, DurableCellManager, DurableIdAllocator};
use metadb_core::HybridLogicalClock;
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use tempfile::tempdir;

async fn open_cell(
    dir: &std::path::Path,
    dedicated_root: Option<&std::path::Path>,
) -> (Arc<ShardGroupHost>, Arc<DurableCellManager>) {
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
        dedicated_root.map(|path| path.to_path_buf()),
    )
    .await
    .unwrap();
    (host, cell)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_jumbo_files_move_onto_a_dedicated_root_and_reopen_there() {
    let dir = tempdir().unwrap();
    let disk = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path(), None).await;
    let source = host.shard_dir(1);
    assert!(source.join("raft").is_dir());
    let before = std::fs::read_dir(source.join("raft")).unwrap().count();
    assert!(before > 0);
    let plain = cell
        .relocate_jumbo_shard_files(1, disk.path())
        .await
        .unwrap_err();
    assert!(plain.to_string().contains("not jumbo-isolated"), "{plain}");
    assert!(source.exists());

    cell.testing_mark_local_jumbo(1, 9).unwrap();
    let moved = cell
        .relocate_jumbo_shard_files(1, disk.path())
        .await
        .unwrap();
    assert!(!source.exists(), "source shard directory must be removed");
    assert!(moved.join("raft").is_dir());
    assert!(moved.join("JUMBO_FILE_MOVE").is_file());
    assert!(cell.lookup_local_handle(1).is_ok());
    let again = cell
        .relocate_jumbo_shard_files(1, disk.path())
        .await
        .unwrap();
    assert_eq!(again, moved);
    assert!(!metadb_cluster::automatic_leadership_transfer_allowed());
    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(host);

    let (host, cell) = open_cell(dir.path(), Some(disk.path())).await;
    assert!(cell.lookup_local_handle(1).is_ok());
    assert!(!host.shard_dir(1).exists());
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_move_live_does_not_open_the_old_directory() {
    let dir = tempdir().unwrap();
    let disk = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path(), None).await;
    let source = host.shard_dir(1);
    cell.testing_mark_local_jumbo(1, 9).unwrap();
    cell.testing_stop_before_jumbo_source_removal();
    let stopped = cell
        .relocate_jumbo_shard_files(1, disk.path())
        .await
        .unwrap_err();
    assert!(
        stopped
            .to_string()
            .contains("will not be opened on restart"),
        "{stopped}"
    );
    assert!(source.exists(), "old directory remains until a later open");
    assert!(cell.lookup_local_handle(1).is_ok());
    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(host);

    let (host, cell) = open_cell(dir.path(), Some(disk.path())).await;
    assert!(
        !host.shard_dir(1).exists(),
        "restart must remove the old directory after MOVE_LIVE"
    );
    assert!(cell.lookup_local_handle(1).is_ok());
    let published = disk.path().join("shards").join(format!("{:020}", 1u64));
    assert!(published.join("JUMBO_FILE_MOVE").is_file());
    assert!(published.join("raft").is_dir());
    host.shutdown_all().await.unwrap();
}
