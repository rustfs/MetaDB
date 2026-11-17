use std::collections::{BTreeMap, BTreeSet};
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
use tempfile::tempdir;

async fn spawn_raft_grpc_server(
    router: MultiRaftRouter,
    authenticated: bool,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let svc = RaftTransportServiceServer::with_interceptor(
        MultiRaftTransportServer::new(router),
        move |req: tonic::Request<()>| {
            if authenticated
                && req
                    .metadata()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    != Some("Bearer raft-regression-secret")
            {
                return Err(tonic::Status::unauthenticated("missing shared secret"));
            }
            Ok(req)
        },
    );
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (addr, handle)
}

/// T-RAFT-2: Boots a `1-Voter` shard over real `tonic` TCP/gRPC, writes data + builds SSTable snapshot +
/// purges logs (forcing `InstallSnapshot` on learners!), promotes online via `AddLearner -> 3-Voter`,
/// kills original Node 1 leader, and verifies failover election + `ReadIndex` linearizable reads!
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_online_1_to_3_voter_snapshot_promotion_and_leader_failover() {
    run_promotion_and_failover(false).await;
}

/// Exercise authenticated Vote, AppendEntries and InstallSnapshot over real TCP.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_authenticated_raft_snapshot_promotion_and_leader_failover() {
    run_promotion_and_failover(true).await;
}

async fn run_promotion_and_failover(authenticated: bool) {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let opts = RaftNodeOptions {
        heartbeat_interval_ms: 80,
        election_timeout_min_ms: 250,
        election_timeout_max_ms: 450,
        max_in_snapshot_log_to_keep: 0,
        max_segment_bytes: 64 * 1024,
    };

    // Start gRPC servers for Node 1, Node 2, and Node 3.
    let router1 = MultiRaftRouter::new();
    let router2 = MultiRaftRouter::new();
    let router3 = MultiRaftRouter::new();
    let (addr1, srv1) = spawn_raft_grpc_server(router1.clone(), authenticated).await;
    let (addr2, srv2) = spawn_raft_grpc_server(router2.clone(), authenticated).await;
    let (addr3, srv3) = spawn_raft_grpc_server(router3.clone(), authenticated).await;

    let start_node = |nid: u64, rtr: MultiRaftRouter| {
        let base = dir.path().join(format!("node_{}/shard_1", nid));
        let cache = cache.clone();
        let opts = opts.clone();
        async move {
            let wm = Arc::new(ShardWatermarkTracker::new(0, 0, 0));
            let handler = Arc::new(BasicKvApplyHandler::new(ShardEpoch::initial(), wm.clone()));
            let pool = ChannelPool::default();
            if authenticated {
                pool.set_shared_secret(Some("raft-regression-secret".to_string()));
            }
            ShardRaftGroup::start(
                1,
                nid,
                base,
                cache,
                LsmEngineConfig::default(),
                opts,
                handler,
                wm,
                Arc::new(HybridLogicalClock::new()),
                &rtr,
                pool,
            )
            .await
            .unwrap()
        }
    };

    // 1. Start Node 1 as `1-Voter` Single-Node group, write 25 keys, and compact into a Raft snapshot
    //    (purging `.raftlog` so Learners MUST receive `InstallSnapshot` over gRPC!).
    let node1 = start_node(1, router1.clone()).await;
    node1
        .initialize_if_needed(BTreeMap::from([(1u64, addr1.clone())]))
        .await
        .unwrap();
    node1.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    let ep1 = node1.current_epoch();
    for i in 0..25u64 {
        let cmd = BasicKvCommand::Put {
            key: MetaKey::child(100, format!("snap_file_{:03}", i)).encode(),
            value: format!("val_{}", i).into_bytes(),
        };
        node1
            .propose(ep1, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
            .await
            .unwrap();
    }
    node1.flush_snapshot_and_purge().await.unwrap();

    // Write 5 additional tail log entries after the snapshot.
    for i in 25..30u64 {
        let cmd = BasicKvCommand::Put {
            key: MetaKey::child(100, format!("snap_file_{:03}", i)).encode(),
            value: format!("val_{}", i).into_bytes(),
        };
        node1
            .propose(ep1, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
            .await
            .unwrap();
    }

    // 2. Start Node 2 and Node 3 and execute online `1-Voter -> Learner -> 3-Voter` promotion!
    let node2 = start_node(2, router2.clone()).await;
    let node3 = start_node(3, router3.clone()).await;

    ConfChangeOrchestrator::add_learners_and_promote_voters(
        node1.raft(),
        &[(2, addr2.clone()), (3, addr3.clone())],
        BTreeSet::from([1, 2, 3]),
        5,
        Duration::from_secs(10),
        0,
    )
    .await
    .unwrap();
    node1.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Write 1 more entry committed across the 3-Voter quorum!
    let ep_3voter = node1.current_epoch();
    let post_promote_cmd = BasicKvCommand::Put {
        key: MetaKey::child(100, "quorum_3voter.txt").encode(),
        value: b"committed_on_3_voters".to_vec(),
    };
    node1
        .propose(
            ep_3voter,
            HlcTimestamp::ZERO,
            codec::serialize(&post_promote_cmd).unwrap(),
        )
        .await
        .unwrap();

    // 3. Kill original Leader (Node 1) and verify Node 2 or Node 3 wins election and serves linearizable reads!
    router1.remove_group(1);
    node1.shutdown().await.unwrap();
    srv1.abort();

    let mut new_leader = None;
    for _ in 0..80 {
        for candidate in [&node2, &node3] {
            let ep = candidate.current_epoch();
            if candidate.linearizable_read(ep).await.is_ok() {
                new_leader = Some(candidate.clone());
                break;
            }
        }
        if new_leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let leader = new_leader.expect("Node 2 or Node 3 must elect a new leader after Node 1 failure");
    let ep_after = leader.current_epoch();
    let (_, snap) = leader.linearizable_read(ep_after).await.unwrap();

    // Verify both snapshot-transferred keys (`0..25`), tail log keys (`25..30`), and 3-voter key!
    for i in 0..30u64 {
        let k = MetaKey::child(100, format!("snap_file_{:03}", i)).encode();
        assert_eq!(
            leader.engine().get_with_snapshot(&k, &snap).unwrap(),
            Some(format!("val_{}", i).into_bytes())
        );
    }
    assert_eq!(
        leader
            .engine()
            .get_with_snapshot(&MetaKey::child(100, "quorum_3voter.txt").encode(), &snap)
            .unwrap(),
        Some(b"committed_on_3_voters".to_vec())
    );
    drop(snap);

    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
    srv2.abort();
    srv3.abort();
}
