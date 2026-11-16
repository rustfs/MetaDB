use metadb_core::{EntryType, ROOT_INODE_ID};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn opposing_directory_renames_remain_reachable_with_eight_clients() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let tx = cluster.tx_router().clone();
    tx.mkdir(ROOT_INODE_ID, "A", 10, 5, 0o755).await.unwrap();
    tx.mkdir(ROOT_INODE_ID, "B", 600, 5, 0o755).await.unwrap();
    assert!(cluster
        .cell_manager()
        .maybe_split_or_isolate_shard(1, true)
        .await
        .unwrap()
        .is_some());
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut clients = tokio::task::JoinSet::new();
    for i in 0..8 {
        let tx = tx.clone();
        let barrier = barrier.clone();
        clients.spawn(async move {
            barrier.wait().await;
            if i % 2 == 0 {
                tx.rename(1, "A", 600, "A", false).await
            } else {
                tx.rename(1, "B", 10, "B", false).await
            }
        });
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while let Some(result) = clients.join_next().await {
            // Contention, vanished source, or cycle rejection are valid results.
            let _ = result.expect("rename task panicked");
        }
    })
    .await
    .expect("opposing directory renames deadlocked");
    cluster.run_maintenance_tick().await.unwrap();
    let mut seen = BTreeSet::new();
    let mut pending = VecDeque::from([ROOT_INODE_ID]);
    while let Some(pid) = pending.pop_front() {
        assert!(seen.insert(pid), "directory appears twice or forms a cycle");
        let (children, _) = tx.listdir_paginated(pid, None, None, 100).await.unwrap();
        for child in children {
            if child.attr.entry_type == EntryType::Directory {
                assert_eq!(
                    tx.dirstat(child.attr.inode_id).await.unwrap().parent_pid,
                    pid
                );
                pending.push_back(child.attr.inode_id);
            }
        }
    }
    cluster.shutdown().await.unwrap();
    assert_eq!(
        seen,
        BTreeSet::from([ROOT_INODE_ID, 10, 600]),
        "live directory became unreachable"
    );
}
