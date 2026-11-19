//! Durable control-plane heartbeat and the single-voter jumbo placement refusal.
//!
//! These tests use one `DurableCellManager` and its Raft groups. They do not start
//! three voters, and they do not claim a leadership transfer or a peer drain ran.

use std::sync::Arc;

use metadb_cluster::{CompositeMetaCache, DurableCellManager, DurableIdAllocator, PlacementPhase};
use metadb_core::HybridLogicalClock;
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use tempfile::tempdir;

async fn open_cell(dir: &std::path::Path) -> (Arc<ShardGroupHost>, Arc<DurableCellManager>) {
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let host = Arc::new(ShardGroupHost::open(1, dir.join("be"), cache, clock).unwrap());
    let alloc = Arc::new(DurableIdAllocator::open(dir.join("alloc")).unwrap());
    let cell = DurableCellManager::open_or_bootstrap(
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
    )
    .await
    .unwrap();
    (host, cell)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_survives_reopen_and_a_lower_sequence_does_not_replace_it() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let first = cell.publish_local_heartbeat().unwrap();
    assert_eq!(first.seq, 1);
    assert!(first.disk_free_bytes.is_none());
    assert!(first.local_hosted_shard_ids.contains(&1));
    assert!(first.reported_shard_ids.is_empty());
    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(host);

    let (host, cell) = open_cell(dir.path()).await;
    let reloaded = cell
        .stored_heartbeat(1)
        .expect("heartbeat reloads from disk");
    assert_eq!(reloaded.seq, 1);
    assert_eq!(
        reloaded.local_hosted_shard_ids,
        first.local_hosted_shard_ids
    );
    let second = cell.publish_local_heartbeat().unwrap();
    assert_eq!(second.seq, 2);
    let stale = cell.accept_remote_heartbeat(1, 1, None, vec![999]);
    assert!(
        stale.is_err(),
        "the local node does not accept its own remote heartbeat"
    );
    cell.register_node(4, "127.0.0.1:4".to_string()).unwrap();
    cell.accept_remote_heartbeat(4, 3, Some(8), vec![999, 999])
        .unwrap();
    let peer = cell.stored_heartbeat(4).unwrap();
    assert_eq!(peer.reported_shard_ids, vec![999]);
    assert!(cell.list_routes().iter().all(|route| route.shard_id != 999));
    let conflict = cell.accept_remote_heartbeat(4, 3, Some(9), vec![999]);
    assert!(conflict.unwrap_err().to_string().contains("conflicts"));
    assert_eq!(cell.stored_heartbeat(4).unwrap().disk_free_bytes, Some(8));
    let older = cell.accept_remote_heartbeat(4, 2, Some(8), vec![999]);
    assert!(older.unwrap_err().to_string().contains("stale"));
    cell.testing_fail_next_control_persists(1);
    let failed = cell.accept_remote_heartbeat(4, 4, Some(8), vec![999]);
    assert!(failed.is_err());
    assert_eq!(cell.stored_heartbeat(4).unwrap().seq, 3);

    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_jumbo_placement_does_not_change_membership() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let before: Vec<_> = cell.raft_voter_ids(1).unwrap().into_iter().collect();
    assert_eq!(before, vec![1]);

    let blocked = cell.reconcile_jumbo_placement(1, 800, 3).await.unwrap();
    assert_eq!(blocked.phase, PlacementPhase::Blocked);
    assert!(blocked.committed_moves.is_empty(), "{blocked:?}");
    assert!(blocked.blocked_reason.contains("not a current voter"));
    assert_eq!(
        cell.raft_voter_ids(1).unwrap(),
        cell.raft_voter_ids(2).unwrap()
    );
    assert_eq!(
        cell.raft_voter_ids(1)
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![1]
    );

    let local = cell.reconcile_jumbo_placement(1, 800, 1).await.unwrap();
    assert_eq!(local.phase, PlacementPhase::LeaderOnDedicated);
    assert_ne!(local.phase, PlacementPhase::DedicatedNodeNotShared);
    assert!(local.committed_moves.is_empty(), "{local:?}");
    assert!(
        local.blocked_reason.contains("proven voter")
            || local.blocked_reason.contains("fault tolerance"),
        "{}",
        local.blocked_reason
    );
    for shard_id in [1, 2, 3] {
        assert_eq!(
            cell.raft_voter_ids(shard_id)
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![1],
            "shard {shard_id}"
        );
    }

    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(host);

    let (host, cell) = open_cell(dir.path()).await;
    let reloaded = cell.stored_placement(1).expect("placement reloads");
    assert_eq!(reloaded.phase, PlacementPhase::LeaderOnDedicated);
    assert!(reloaded.committed_moves.is_empty());
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corrupt_control_plane_file_refuses_to_open() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    cell.publish_local_heartbeat().unwrap();
    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(host);

    let path = dir.path().join("cell_1").join("CELL_CONTROL_PLANE");
    let mut raw = std::fs::read(&path).unwrap();
    raw.extend_from_slice(&[1, 2, 3, 4]);
    std::fs::write(&path, raw).unwrap();

    let cache = Arc::new(ShardedBlockCache::new(1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let host = ShardGroupHost::open(1, dir.path().join("be"), cache, clock).unwrap();
    let alloc = Arc::new(DurableIdAllocator::open(dir.path().join("alloc")).unwrap());
    let opened = DurableCellManager::open_or_bootstrap(
        1,
        dir.path().join("cell_1"),
        Arc::new(host),
        "127.0.0.1:1".to_string(),
        true,
        4 * 1024 * 1024 * 1024,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        alloc,
        Arc::new(CompositeMetaCache::new()),
    )
    .await;
    let error = match opened {
        Ok(_) => panic!("a torn control-plane file must not open"),
        Err(error) => error,
    };
    let text = error.to_string();
    assert!(
        text.contains("CELL_CONTROL_PLANE") || text.contains("checksum"),
        "{text}"
    );
}
