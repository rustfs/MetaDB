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

async fn open_receiver(dir: &std::path::Path) -> (Arc<ShardGroupHost>, Arc<DurableCellManager>) {
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let host = Arc::new(ShardGroupHost::open(2, dir.join("be"), cache, clock).unwrap());
    let alloc = Arc::new(DurableIdAllocator::open(dir.join("alloc")).unwrap());
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        dir.join("cell_1"),
        host.clone(),
        "127.0.0.1:2".to_string(),
        true,
        4 * 1024 * 1024 * 1024,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        alloc,
        Arc::new(CompositeMetaCache::new()),
    )
    .await
    .unwrap();
    cell.register_node(1, "127.0.0.1:1".to_string()).unwrap();
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
    let stale = cell.accept_remote_heartbeat(1, 1, None, vec![999], None, 11);
    assert!(
        stale.is_err(),
        "the local node does not accept its own remote heartbeat"
    );
    cell.register_node(4, "127.0.0.1:4".to_string()).unwrap();
    cell.accept_remote_heartbeat(4, 3, Some(8), vec![999, 999], None, 11)
        .unwrap();
    let peer = cell.stored_heartbeat(4).unwrap();
    assert_eq!(peer.reported_shard_ids, vec![999]);
    assert!(cell.list_routes().iter().all(|route| route.shard_id != 999));
    let conflict = cell.accept_remote_heartbeat(4, 3, Some(9), vec![999], None, 11);
    assert!(conflict.unwrap_err().to_string().contains("conflicts"));
    assert_eq!(cell.stored_heartbeat(4).unwrap().disk_free_bytes, Some(8));
    let older = cell.accept_remote_heartbeat(4, 2, Some(8), vec![999], None, 11);
    assert!(older.unwrap_err().to_string().contains("stale"));
    cell.testing_age_stored_heartbeat(4, 5_000);
    cell.testing_fail_next_control_persists(1);
    let failed = cell.accept_remote_heartbeat(4, 4, Some(8), vec![999], None, 11);
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
        !metadb_cluster::leadership_move_recorded(false, true),
        "an already-correct leader is not this process's LeadershipTransferred"
    );
    assert!(!metadb_cluster::automatic_leadership_transfer_allowed());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_sync_failure_after_rename_keeps_the_new_sequence() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    assert_eq!(cell.publish_local_heartbeat().unwrap().seq, 1);
    cell.testing_fail_next_control_dirsync(1);
    let failed = cell.publish_local_heartbeat().unwrap_err();
    assert!(failed.to_string().contains("directory sync"), "{failed}");
    assert_eq!(
        cell.stored_heartbeat(1).unwrap().seq,
        2,
        "a renamed control file must stay in memory"
    );
    assert_eq!(cell.publish_local_heartbeat().unwrap().seq, 3);
    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(host);

    let (host, cell) = open_cell(dir.path()).await;
    assert_eq!(cell.stored_heartbeat(1).unwrap().seq, 3);
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_heartbeat_rejects_a_poison_sequence_and_a_fast_repeat() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    cell.register_node(4, "127.0.0.1:4".to_string()).unwrap();
    cell.accept_remote_heartbeat(4, 1, None, Vec::new(), None, 11)
        .unwrap();
    let too_soon = cell
        .accept_remote_heartbeat(4, 2, None, Vec::new(), None, 11)
        .unwrap_err();
    assert!(too_soon.to_string().contains("rate"), "{too_soon}");
    assert_eq!(cell.stored_heartbeat(4).unwrap().seq, 1);
    let poisoned = cell
        .accept_remote_heartbeat(4, u64::MAX, None, Vec::new(), None, 11)
        .unwrap_err();
    assert!(
        poisoned.to_string().contains("jumps") || poisoned.to_string().contains("exceeds"),
        "{poisoned}"
    );
    assert_eq!(cell.stored_heartbeat(4).unwrap().seq, 1);
    cell.testing_age_stored_heartbeat(4, 5_000);
    cell.accept_remote_heartbeat(4, 2, None, Vec::new(), None, 11)
        .unwrap();
    assert_eq!(cell.stored_heartbeat(4).unwrap().seq, 2);
    assert!(!metadb_cluster::heartbeat_is_scheduling_truth());
    assert!(!metadb_cluster::automatic_leadership_transfer_allowed());
    assert!(!metadb_cluster::leadership_move_recorded(false, true));
    assert!(!metadb_cluster::transfer_poll_covers_election(3_000, 5_000));
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_receiver_accepts_a_sender_sequence_above_64() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    cell.register_node(5, "127.0.0.1:5".to_string()).unwrap();
    cell.accept_remote_heartbeat(5, 80, None, Vec::new(), None, 41)
        .unwrap();
    assert_eq!(cell.stored_heartbeat(5).unwrap().seq, 80);
    assert_eq!(cell.stored_heartbeat(5).unwrap().observation_generation, 41);
    cell.testing_age_stored_heartbeat(5, 5_000);
    let jumped = cell
        .accept_remote_heartbeat(5, 80 + 65, None, Vec::new(), None, 41)
        .unwrap_err();
    assert!(jumped.to_string().contains("jumps"), "{jumped}");
    assert_eq!(cell.stored_heartbeat(5).unwrap().seq, 80);
    cell.testing_age_stored_heartbeat(5, 5_000);
    cell.accept_remote_heartbeat(5, 1, None, Vec::new(), None, 42)
        .unwrap();
    let recovered = cell.stored_heartbeat(5).unwrap();
    assert_eq!(recovered.observation_generation, 42);
    assert_eq!(recovered.seq, 1);
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_sequence_generation_spam_is_rate_limited_and_a_retired_generation_is_not_remembered()
{
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    cell.register_node(4, "127.0.0.1:4".to_string()).unwrap();
    cell.accept_remote_heartbeat(4, 5, None, Vec::new(), None, 11)
        .unwrap();
    let spam = cell
        .accept_remote_heartbeat(4, 5, None, Vec::new(), None, 12)
        .unwrap_err();
    assert!(spam.to_string().contains("rate"), "{spam}");
    assert_eq!(cell.stored_heartbeat(4).unwrap().observation_generation, 11);
    cell.testing_age_stored_heartbeat(4, 5_000);
    cell.accept_remote_heartbeat(4, 5, None, Vec::new(), None, 12)
        .unwrap();
    assert_eq!(cell.stored_heartbeat(4).unwrap().observation_generation, 12);
    cell.testing_age_stored_heartbeat(4, 5_000);
    // No retired-generation set. A delayed older generation replaces the row
    // once the rate window allows another state change. It does not schedule.
    cell.accept_remote_heartbeat(4, 5, None, Vec::new(), None, 11)
        .unwrap();
    assert_eq!(cell.stored_heartbeat(4).unwrap().observation_generation, 11);
    assert!(!metadb_cluster::heartbeat_is_scheduling_truth());
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sender_file_loss_remints_generation_and_restarts_the_sequence() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let first = cell.publish_local_heartbeat().unwrap();
    assert_ne!(first.observation_generation, 0);
    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(host);
    std::fs::remove_file(dir.path().join("cell_1").join("CELL_CONTROL_PLANE")).unwrap();

    let (host, cell) = open_cell(dir.path()).await;
    let restarted = cell.publish_local_heartbeat().unwrap();
    assert_eq!(restarted.seq, 1);
    assert_ne!(
        restarted.observation_generation,
        first.observation_generation
    );
    assert_ne!(restarted.observation_generation, 0);
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_file_loss_accepts_sender_sequence_above_64() {
    let sender_dir = tempdir().unwrap();
    let (sender_host, sender) = open_cell(sender_dir.path()).await;
    let mut published = sender.publish_local_heartbeat().unwrap();
    for _ in 0..64 {
        published = sender.publish_local_heartbeat().unwrap();
    }
    assert!(published.seq > 64, "{}", published.seq);
    let generation = published.observation_generation;
    let seq = published.seq;
    sender_host.shutdown_all().await.unwrap();

    let receiver_dir = tempdir().unwrap();
    let (host, cell) = open_receiver(receiver_dir.path()).await;
    cell.accept_remote_heartbeat(1, seq, None, Vec::new(), None, generation)
        .unwrap();
    assert_eq!(cell.stored_heartbeat(1).unwrap().seq, seq);
    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(host);
    std::fs::remove_file(
        receiver_dir
            .path()
            .join("cell_1")
            .join("CELL_CONTROL_PLANE"),
    )
    .unwrap();

    let (host, cell) = open_receiver(receiver_dir.path()).await;
    cell.accept_remote_heartbeat(1, seq, None, Vec::new(), None, generation)
        .unwrap();
    assert_eq!(cell.stored_heartbeat(1).unwrap().seq, seq);
    assert!(cell.stored_heartbeat(1).unwrap().seq > 64);
    host.shutdown_all().await.unwrap();
}
