//! A purged or missing Raft snapshot file must not stop the read path.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use metadb_core::codec;
use metadb_core::{HlcTimestamp, HybridLogicalClock, MetaKey, ShardEpoch};
use metadb_proto::raft_transport_service_server::RaftTransportServiceServer;
use metadb_raft::{
    BasicKvApplyHandler, BasicKvCommand, ChannelPool, ConfChangeOrchestrator, MultiRaftRouter,
    MultiRaftTransportServer, RaftNodeOptions, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use openraft::storage::RaftStateMachine;
use tempfile::tempdir;

async fn boot(
    shard_dir: &std::path::Path,
    cache: Arc<ShardedBlockCache>,
    clock: Arc<HybridLogicalClock>,
) -> Arc<ShardRaftGroup> {
    let watermarks = Arc::new(ShardWatermarkTracker::new(0, 0, 0));
    let handler = Arc::new(BasicKvApplyHandler::new(
        ShardEpoch::initial(),
        watermarks.clone(),
    ));
    let router = MultiRaftRouter::new();
    let group = ShardRaftGroup::start(
        1,
        1,
        shard_dir,
        cache,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        handler,
        watermarks,
        clock,
        &router,
        ChannelPool::default(),
    )
    .await
    .unwrap();
    group
        .initialize_if_needed(BTreeMap::from([(1u64, "127.0.0.1:0".to_string())]))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    group
}

async fn assert_keys(group: &ShardRaftGroup) {
    let epoch = group.current_epoch();
    let (_, snap) = group
        .linearizable_read(epoch)
        .await
        .unwrap_or_else(|error| {
            panic!("read path died after snapshot loss: {error}");
        });
    let start = MetaKey::child(1, "alpha").encode();
    let end = MetaKey::child(1, "zzz").encode();
    let listed = group
        .engine()
        .scan_with_snapshot(&start, &end, &snap)
        .unwrap_or_else(|error| panic!("list died after snapshot loss: {error}"));
    assert_eq!(listed.len(), 3, "list missed committed keys: {listed:?}");
    for name in ["alpha", "beta", "gamma"] {
        let key = MetaKey::child(1, name).encode();
        assert_eq!(
            group.engine().get_with_snapshot(&key, &snap).unwrap(),
            Some(name.as_bytes().to_vec()),
            "{name}"
        );
    }
}

#[tokio::test]
async fn missing_snapshot_after_flush_and_purge_does_not_kill_reads() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());

    {
        let group = boot(&shard_dir, cache.clone(), clock.clone()).await;
        let epoch = group.current_epoch();
        for name in ["alpha", "beta", "gamma"] {
            let cmd = BasicKvCommand::Put {
                key: MetaKey::child(1, name).encode(),
                value: name.as_bytes().to_vec(),
            };
            group
                .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
                .await
                .unwrap();
        }
        let purged = group.flush_snapshot_and_purge().await.unwrap();
        assert!(purged > 0);
        assert_eq!(group.log_store().retained_entry_count(), 0);
        assert_keys(&group).await;

        let snaps = shard_dir.join("raft/snapshots");
        let mut removed = false;
        for entry in fs::read_dir(&snaps).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with("snap_") && !name.contains('.') {
                fs::remove_file(entry.path().join("SNAPSHOT_META")).unwrap();
                removed = true;
            }
        }
        assert!(removed, "snapshot directory had no SNAPSHOT_META to remove");

        let mut state_machine = group.state_machine().clone();
        let loaded = state_machine.get_current_snapshot().await.unwrap();
        assert!(
            loaded.is_some(),
            "a live process must rewrite the missing snapshot from the LSM"
        );
        assert!(
            group.storage_unrecoverable().is_none(),
            "a rebuilt snapshot must still be reported as a live voter"
        );
        assert!(
            shard_dir
                .join("raft/snapshots")
                .read_dir()
                .unwrap()
                .any(|entry| {
                    entry
                        .unwrap()
                        .file_name()
                        .to_str()
                        .unwrap_or("")
                        .starts_with("snap_")
                }),
            "rewrite must put a snapshot directory back on disk"
        );
        assert_keys(&group).await;
        group.shutdown().await.unwrap();
    }

    let group = boot(&shard_dir, cache, clock).await;
    let mut state_machine = group.state_machine().clone();
    let rebuilt = state_machine.get_current_snapshot().await.unwrap();
    assert!(
        rebuilt.is_some(),
        "restart must rebuild a readable snapshot from the flushed LSM"
    );
    assert_keys(&group).await;
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn unpurged_raft_log_reconstructs_keys_without_a_snapshot() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    {
        let group = boot(&shard_dir, cache.clone(), clock.clone()).await;
        let epoch = group.current_epoch();
        for name in ["alpha", "beta", "gamma"] {
            let cmd = BasicKvCommand::Put {
                key: MetaKey::child(1, name).encode(),
                value: name.as_bytes().to_vec(),
            };
            group
                .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
                .await
                .unwrap();
        }
        assert!(
            group.log_store().retained_entry_count() > 0,
            "the segmented log is the WAL until purge"
        );
        let _ = fs::remove_dir_all(shard_dir.join("raft/snapshots"));
        group.shutdown().await.unwrap();
    }
    let group = boot(&shard_dir, cache, clock).await;
    assert!(
        group.storage_unrecoverable().is_none(),
        "an unpurged log must not be reported as an unrecoverable voter"
    );
    assert_keys(&group).await;
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_lsm_and_snapshot_after_log_purge_is_not_a_live_voter() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let group = boot(&shard_dir, cache, clock).await;
    let epoch = group.current_epoch();
    for name in ["alpha", "beta", "gamma"] {
        let cmd = BasicKvCommand::Put {
            key: MetaKey::child(1, name).encode(),
            value: name.as_bytes().to_vec(),
        };
        group
            .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
            .await
            .unwrap();
    }
    let purged = group.flush_snapshot_and_purge().await.unwrap();
    assert!(purged > 0);
    assert_eq!(group.log_store().retained_entry_count(), 0);

    let mut removed_sst = 0usize;
    for entry in fs::read_dir(shard_dir.join("lsm")).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.ends_with(".sst") {
            fs::remove_file(entry.path()).unwrap();
            removed_sst += 1;
        }
    }
    assert!(removed_sst > 0, "flush did not create an SSTable to remove");
    let _ = fs::remove_dir_all(shard_dir.join("raft/snapshots"));

    let mut state_machine = group.state_machine().clone();
    let error = state_machine
        .get_current_snapshot()
        .await
        .expect_err("missing LSM and snapshot must not look like a healthy snapshot");
    let message = error.to_string();
    assert!(
        message.contains("snapshot unrecoverable"),
        "missing durable copies must be an explicit error, not ENOENT: {message}"
    );
    assert!(
        message.contains("not reconstructed"),
        "the error must say committed data is not reconstructed: {message}"
    );
    let reason = group
        .storage_unrecoverable()
        .expect("cluster status must be able to see that this process is not a live voter");
    assert!(reason.contains("not reconstructed"), "{reason}");
    let read_error = match group.linearizable_read(group.current_epoch()).await {
        Ok(_) => panic!("reads must not succeed once storage is unrecoverable"),
        Err(error) => error,
    };
    assert!(
        read_error.to_string().contains("snapshot unrecoverable"),
        "{read_error}"
    );
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn forwarded_propose_local_rewrites_a_missing_snapshot() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let group = boot(&shard_dir, cache, clock).await;
    let epoch = group.current_epoch();
    for name in ["alpha", "beta", "gamma"] {
        let cmd = BasicKvCommand::Put {
            key: MetaKey::child(1, name).encode(),
            value: name.as_bytes().to_vec(),
        };
        group
            .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
            .await
            .unwrap();
    }
    group.flush_snapshot_and_purge().await.unwrap();
    let published = group.watermarks().snapshot_values().2;
    let meta = shard_dir.join(format!("raft/snapshots/snap_{published:020}/SNAPSHOT_META"));
    fs::remove_file(&meta).unwrap();
    let cmd = BasicKvCommand::Put {
        key: MetaKey::child(1, "delta").encode(),
        value: b"delta".to_vec(),
    };
    group
        .propose_local(
            epoch,
            HlcTimestamp::ZERO,
            codec::serialize(&cmd).unwrap(),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    let rewritten = shard_dir
        .join("raft/snapshots")
        .read_dir()
        .unwrap()
        .any(|entry| {
            entry
                .ok()
                .and_then(|entry| entry.file_name().into_string().ok())
                .is_some_and(|name| name.starts_with("snap_") && !name.contains('.'))
        });
    assert!(rewritten, "propose_local must rewrite the missing snapshot");
    assert!(group.storage_unrecoverable().is_none());
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn older_snapshot_does_not_satisfy_a_purged_prefix() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let group = boot(&shard_dir, cache, clock).await;
    let epoch = group.current_epoch();
    for name in ["alpha", "beta", "gamma"] {
        let cmd = BasicKvCommand::Put {
            key: MetaKey::child(1, name).encode(),
            value: name.as_bytes().to_vec(),
        };
        group
            .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
            .await
            .unwrap();
    }
    let purged = group.flush_snapshot_and_purge().await.unwrap();
    assert!(purged > 1);
    for entry in fs::read_dir(shard_dir.join("lsm")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_str().unwrap_or("").ends_with(".sst") {
            fs::remove_file(entry.path()).unwrap();
        }
    }
    let _ = fs::remove_dir_all(shard_dir.join("raft/snapshots"));
    metadb_raft::state_machine::write_complete_snapshot_marker(&shard_dir.join("raft"), 1).unwrap();

    let mut state_machine = group.state_machine().clone();
    let first = state_machine.get_current_snapshot().await.unwrap_err();
    assert!(
        first.to_string().contains("snapshot unrecoverable"),
        "{first}"
    );
    let second = state_machine.get_current_snapshot().await.unwrap_err();
    assert!(
        second.to_string().contains("snapshot unrecoverable"),
        "a later read must not accept the older snapshot: {second}"
    );
    assert!(group.storage_unrecoverable().is_some());
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_after_purged_log_missing_lsm_and_snapshot_is_unrecoverable_before_serving() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    {
        let group = boot(&shard_dir, cache.clone(), clock.clone()).await;
        let epoch = group.current_epoch();
        for name in ["alpha", "beta", "gamma"] {
            let cmd = BasicKvCommand::Put {
                key: MetaKey::child(1, name).encode(),
                value: name.as_bytes().to_vec(),
            };
            group
                .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
                .await
                .unwrap();
        }
        let purged = group.flush_snapshot_and_purge().await.unwrap();
        assert!(purged > 0);
        for entry in fs::read_dir(shard_dir.join("lsm")).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_str().unwrap_or("").ends_with(".sst") {
                fs::remove_file(entry.path()).unwrap();
            }
        }
        let _ = fs::remove_dir_all(shard_dir.join("raft/snapshots"));
        group.shutdown().await.unwrap();
    }

    let watermarks = Arc::new(ShardWatermarkTracker::new(0, 0, 0));
    let handler = Arc::new(BasicKvApplyHandler::new(
        ShardEpoch::initial(),
        watermarks.clone(),
    ));
    let router = MultiRaftRouter::new();
    let started = ShardRaftGroup::start(
        1,
        1,
        &shard_dir,
        cache,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        handler,
        watermarks,
        clock,
        &router,
        ChannelPool::default(),
    )
    .await;
    match started {
        Err(error) => {
            let text = error.to_string();
            assert!(
                text.contains("snapshot unrecoverable"),
                "restart must fail closed before serving: {text}"
            );
            assert!(
                text.contains("not reconstructed"),
                "an empty replacement LSM must not be treated as a new shard: {text}"
            );
        }
        Ok(group) => {
            let latched = group.storage_unrecoverable().is_some();
            let rejected = group
                .propose_local(
                    group.current_epoch(),
                    HlcTimestamp::ZERO,
                    codec::serialize(&BasicKvCommand::Put {
                        key: MetaKey::child(1, "fresh").encode(),
                        value: b"nope".to_vec(),
                    })
                    .unwrap(),
                    tokio::time::Instant::now() + Duration::from_secs(5),
                )
                .await
                .is_err();
            let _ = group.shutdown().await;
            assert!(
                latched && rejected,
                "restart must be unrecoverable and must not accept a write onto an empty LSM"
            );
        }
    }
}

#[tokio::test]
async fn restart_rebuilds_snapshot_from_lsm_without_a_prior_live_read() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    {
        let group = boot(&shard_dir, cache.clone(), clock.clone()).await;
        let epoch = group.current_epoch();
        for name in ["alpha", "beta", "gamma"] {
            let cmd = BasicKvCommand::Put {
                key: MetaKey::child(1, name).encode(),
                value: name.as_bytes().to_vec(),
            };
            group
                .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
                .await
                .unwrap();
        }
        group.flush_snapshot_and_purge().await.unwrap();
        let _ = fs::remove_dir_all(shard_dir.join("raft/snapshots"));
        group.shutdown().await.unwrap();
    }

    let group = boot(&shard_dir, cache, clock).await;
    let rewritten = fs::read_dir(shard_dir.join("raft/snapshots"))
        .unwrap()
        .flatten()
        .any(|entry| {
            let name = entry.file_name();
            let name = name.to_str().unwrap_or("");
            name.starts_with("snap_") && !name.contains('.')
        });
    assert!(
        rewritten,
        "start must rewrite the snapshot from the LSM before a client read"
    );
    assert!(group.storage_unrecoverable().is_none());
    assert_keys(&group).await;
    group.shutdown().await.unwrap();
}

async fn spawn_transport(router: MultiRaftRouter) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let svc = RaftTransportServiceServer::new(MultiRaftTransportServer::new(router));
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    addr
}

async fn start_voter(
    node_id: u64,
    shard_dir: &std::path::Path,
    router: &MultiRaftRouter,
    cache: Arc<ShardedBlockCache>,
) -> Arc<ShardRaftGroup> {
    let watermarks = Arc::new(ShardWatermarkTracker::new(0, 0, 0));
    let handler = Arc::new(BasicKvApplyHandler::new(
        ShardEpoch::initial(),
        watermarks.clone(),
    ));
    ShardRaftGroup::start(
        1,
        node_id,
        shard_dir,
        cache,
        LsmEngineConfig::default(),
        RaftNodeOptions {
            heartbeat_interval_ms: 80,
            election_timeout_min_ms: 250,
            election_timeout_max_ms: 450,
            ..RaftNodeOptions::default()
        },
        handler,
        watermarks,
        Arc::new(HybridLogicalClock::new()),
        router,
        ChannelPool::default(),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_install_succeeds_after_leader_snapshot_file_deleted() {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let router1 = MultiRaftRouter::new();
    let router2 = MultiRaftRouter::new();
    let addr1 = spawn_transport(router1.clone()).await;
    let addr2 = spawn_transport(router2.clone()).await;
    let leader = start_voter(1, &dir.path().join("node1"), &router1, cache.clone()).await;
    leader
        .initialize_if_needed(BTreeMap::from([(1u64, addr1.clone())]))
        .await
        .unwrap();
    let epoch = leader.current_epoch();
    let cmd = BasicKvCommand::Put {
        key: MetaKey::child(1, "alpha").encode(),
        value: b"alpha".to_vec(),
    };
    leader
        .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
        .await
        .unwrap();
    let purged = leader.flush_snapshot_and_purge().await.unwrap();
    assert!(purged > 0);
    let _ = fs::remove_dir_all(dir.path().join("node1/raft/snapshots"));

    let learner = start_voter(2, &dir.path().join("node2"), &router2, cache).await;
    ConfChangeOrchestrator::add_learners_and_promote_voters(
        leader.raft(),
        &[(2, addr2)],
        BTreeSet::from([1, 2]),
        64,
        Duration::from_secs(10),
        purged,
    )
    .await
    .expect("learner install must succeed after the leader rewrites its snapshot");
    let snap = learner
        .state_machine()
        .acquire_snapshot(learner.current_epoch(), purged, None)
        .expect("installed learner state must be readable without being the leader");
    let key = MetaKey::child(1, "alpha").encode();
    assert_eq!(
        learner.engine().get_with_snapshot(&key, &snap).unwrap(),
        Some(b"alpha".to_vec())
    );
    leader.shutdown().await.unwrap();
    learner.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_install_fails_closed_when_leader_is_unrecoverable() {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let router1 = MultiRaftRouter::new();
    let router2 = MultiRaftRouter::new();
    let addr1 = spawn_transport(router1.clone()).await;
    let addr2 = spawn_transport(router2.clone()).await;
    let leader = start_voter(1, &dir.path().join("node1"), &router1, cache.clone()).await;
    leader
        .initialize_if_needed(BTreeMap::from([(1u64, addr1)]))
        .await
        .unwrap();
    let epoch = leader.current_epoch();
    let cmd = BasicKvCommand::Put {
        key: MetaKey::child(1, "alpha").encode(),
        value: b"alpha".to_vec(),
    };
    leader
        .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
        .await
        .unwrap();
    leader.flush_snapshot_and_purge().await.unwrap();
    for entry in fs::read_dir(dir.path().join("node1/lsm")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_str().unwrap_or("").ends_with(".sst") {
            fs::remove_file(entry.path()).unwrap();
        }
    }
    let _ = fs::remove_dir_all(dir.path().join("node1/raft/snapshots"));
    let _learner = start_voter(2, &dir.path().join("node2"), &router2, cache).await;
    let installed = ConfChangeOrchestrator::add_learners_and_promote_voters(
        leader.raft(),
        &[(2, addr2)],
        BTreeSet::from([1, 2]),
        64,
        Duration::from_secs(3),
        1,
    )
    .await;
    assert!(
        installed.is_err(),
        "an unrecoverable leader must not install a learner from a missing snapshot and LSM"
    );
    let write = leader
        .propose_local(
            leader.current_epoch(),
            HlcTimestamp::ZERO,
            codec::serialize(&BasicKvCommand::Put {
                key: MetaKey::child(1, "later").encode(),
                value: b"no".to_vec(),
            })
            .unwrap(),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await;
    assert!(
        write.is_err(),
        "the leader must not keep accepting writes after it cannot rebuild the snapshot"
    );
    leader.shutdown().await.ok();
}
