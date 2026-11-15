//! Three-voter membership on the real OpenRaft metadata state machine.
//!
//! `metadbd --role be` is rejected by `PersistentMetaDbCluster::open_and_serve`, and a second
//! all-in-one process bootstraps its own cell instead of joining. This test uses the membership
//! path the product does implement: `DurableCellManager::scale_out_replicas` adds learners over
//! localhost `RaftTransportService`, promotes them to voters, and serves `DistributedTxRouter`
//! metadata commands. It then stops the original leader and restarts that voter from its
//! `data_dir`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metadb_cluster::{
    CompositeMetaCache, DurableCellManager, DurableGlobalManager, DurableIdAllocator,
};
use metadb_core::{
    CompositeKeyRange, HlcTimestamp, HybridLogicalClock, InodeId, MetaDbError, MetaKey, ShardEpoch,
    ROOT_INODE_ID,
};
use metadb_proto::raft_transport_service_server::RaftTransportServiceServer;
use metadb_raft::{
    ChannelPool, MultiRaftRouter, MultiRaftTransportServer, RaftNodeOptions, ShardGroupHost,
    ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{
    DirRenameLease, DistributedTxRouter, RoutedShardHandle, ShardDirectory, TxStateMachineHandler,
};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn raft_options() -> RaftNodeOptions {
    RaftNodeOptions {
        heartbeat_interval_ms: 80,
        election_timeout_min_ms: 250,
        election_timeout_max_ms: 450,
        max_in_snapshot_log_to_keep: 0,
        max_segment_bytes: 64 * 1024,
    }
}

fn spawn_raft(router: MultiRaftRouter, listener: TcpListener) -> tokio::task::JoinHandle<()> {
    let svc = RaftTransportServiceServer::new(MultiRaftTransportServer::new(router));
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    })
}

struct RaftPeer {
    addr: String,
    group: Arc<ShardRaftGroup>,
    handler: Arc<TxStateMachineHandler>,
    server: tokio::task::JoinHandle<()>,
}

async fn start_learner(
    node_id: u64,
    shard_dir: &std::path::Path,
    opts: &RaftNodeOptions,
    lsm: &LsmEngineConfig,
    clock: Arc<HybridLogicalClock>,
    range: &CompositeKeyRange,
) -> RaftPeer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let router = MultiRaftRouter::new();
    let server = spawn_raft(router.clone(), listener);
    let watermarks = Arc::new(ShardWatermarkTracker::default());
    let handler = Arc::new(TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        range.clone(),
        watermarks.clone(),
    ));
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let group = ShardRaftGroup::start(
        1,
        node_id,
        shard_dir,
        cache,
        lsm.clone(),
        opts.clone(),
        handler.clone(),
        watermarks,
        clock,
        &router,
        ChannelPool::default(),
    )
    .await
    .unwrap();
    RaftPeer {
        addr,
        group,
        handler,
        server,
    }
}

struct LeaderDirectory {
    range: CompositeKeyRange,
    current: Mutex<(Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)>,
    clock: Arc<HybridLogicalClock>,
    next_lease: std::sync::atomic::AtomicU64,
}

impl LeaderDirectory {
    fn new(
        range: CompositeKeyRange,
        group: Arc<ShardRaftGroup>,
        handler: Arc<TxStateMachineHandler>,
        clock: Arc<HybridLogicalClock>,
    ) -> Self {
        Self {
            range,
            current: Mutex::new((group, handler)),
            clock,
            next_lease: std::sync::atomic::AtomicU64::new(1),
        }
    }

    fn handle(&self) -> RoutedShardHandle {
        let (group, handler) = self.current.lock().unwrap().clone();
        (group.current_epoch(), group, handler)
    }
}

#[tonic::async_trait]
impl ShardDirectory for LeaderDirectory {
    fn route_user_key(&self, user_key: &[u8]) -> metadb_core::Result<RoutedShardHandle> {
        if !self.range.contains_key(user_key) {
            return Err(MetaDbError::ShardRoutingError(
                "key is outside the test cluster range".to_string(),
            ));
        }
        Ok(self.handle())
    }

    fn route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> metadb_core::Result<Vec<RoutedShardHandle>> {
        if start_user_key < end_user_key
            && start_user_key < self.range.end_key.as_slice()
            && end_user_key > self.range.start_key.as_slice()
        {
            Ok(vec![self.handle()])
        } else {
            Ok(Vec::new())
        }
    }

    async fn acquire_dir_rename_lease(
        &self,
        _moved_dir_id: InodeId,
        ttl_ms: u64,
    ) -> metadb_core::Result<DirRenameLease> {
        let token = self
            .next_lease
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = self.clock.now();
        Ok(DirRenameLease {
            rename_fence_token: token,
            expire_hlc: HlcTimestamp::new(now.physical_ms().saturating_add(ttl_ms), 0),
        })
    }

    async fn release_dir_rename_lease(&self, _token: u64) {}
}

async fn wait_leader(
    candidates: &[(Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)],
) -> (Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>) {
    for _ in 0..160 {
        for (group, handler) in candidates {
            if group.linearizable_read(group.current_epoch()).await.is_ok() {
                return (group.clone(), handler.clone());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for a Raft leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voter_membership_metadata_leader_change_and_restart() {
    let dir = tempdir().unwrap();
    let clock = Arc::new(HybridLogicalClock::new());
    let opts = raft_options();
    let lsm = LsmEngineConfig::default();
    let range = CompositeKeyRange::all_user_tables();

    let host = Arc::new(
        ShardGroupHost::open(
            1,
            dir.path().join("node_1"),
            Arc::new(ShardedBlockCache::new(4 * 1024 * 1024)),
            clock.clone(),
        )
        .unwrap(),
    );
    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap().to_string();
    let srv1 = spawn_raft(host.router().clone(), listener1);

    let alloc = Arc::new(DurableIdAllocator::open(dir.path().join("alloc")).unwrap());
    let meta_cache = Arc::new(CompositeMetaCache::new());
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        dir.path().join("cell_1"),
        host.clone(),
        addr1.clone(),
        false,
        4 * 1024 * 1024 * 1024,
        lsm.clone(),
        opts.clone(),
        alloc.clone(),
        meta_cache.clone(),
    )
    .await
    .unwrap();

    let gm = Arc::new(DurableGlobalManager::new(clock.clone(), alloc, meta_cache));
    gm.register_cell(range.clone(), cell.clone());
    let tx = DistributedTxRouter::new(1, clock.clone(), gm);

    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();
    let bucket = tx
        .mkdir(ROOT_INODE_ID, "bucket", 10, 7, 0o755)
        .await
        .unwrap();
    let bucket_id = bucket.attr.inode_id;
    tx.create_file(bucket_id, "before_scale.dat", 100, 500, 7, 0o644, 64)
        .await
        .unwrap();

    let peer2 = start_learner(
        2,
        &dir.path().join("node_2"),
        &opts,
        &lsm,
        clock.clone(),
        &range,
    )
    .await;
    let peer3 = start_learner(
        3,
        &dir.path().join("node_3"),
        &opts,
        &lsm,
        clock.clone(),
        &range,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    cell.register_node(2, peer2.addr.clone()).unwrap();
    cell.register_node(3, peer3.addr.clone()).unwrap();
    let upgraded = cell.scale_out_replicas(3).await.unwrap();
    assert!(
        upgraded >= 1,
        "expected at least the meta shard to gain voters"
    );

    let route = cell
        .list_routes()
        .into_iter()
        .find(|route| route.shard_id == 1)
        .expect("shard 1");
    assert_eq!(route.peers.len(), 3);
    assert_eq!(
        route
            .peers
            .values()
            .filter(|(_, is_voter)| *is_voter)
            .count(),
        3
    );

    tx.create_file(bucket_id, "quorum.dat", 101, 501, 7, 0o644, 64)
        .await
        .unwrap();
    assert!(tx
        .lookup(bucket_id, "before_scale.dat")
        .await
        .unwrap()
        .is_some());
    assert!(tx.lookup(bucket_id, "quorum.dat").await.unwrap().is_some());
    assert_eq!(tx.dirstat(bucket_id).await.unwrap().num_children, 2);

    // Drop the leader's `LsmStorageBackend` so its `lsm/LOCK` is released before restart.
    host.shutdown_all().await.unwrap();

    let survivors = [
        (peer2.group.clone(), peer2.handler.clone()),
        (peer3.group.clone(), peer3.handler.clone()),
    ];
    let (leader, handler) = wait_leader(&survivors).await;
    let leader_id = leader.node_id();
    assert_ne!(leader_id, 1, "the stopped node must not remain leader");
    let failover_tx = DistributedTxRouter::new(
        leader_id,
        clock.clone(),
        Arc::new(LeaderDirectory::new(
            range.clone(),
            leader,
            handler,
            clock.clone(),
        )),
    );
    assert!(failover_tx
        .lookup(bucket_id, "before_scale.dat")
        .await
        .unwrap()
        .is_some());
    assert!(failover_tx
        .lookup(bucket_id, "quorum.dat")
        .await
        .unwrap()
        .is_some());
    failover_tx
        .create_file(bucket_id, "after_failover.dat", 102, 502, 7, 0o644, 32)
        .await
        .unwrap();
    failover_tx
        .rename(
            bucket_id,
            "quorum.dat",
            bucket_id,
            "quorum_moved.dat",
            false,
        )
        .await
        .unwrap();
    assert!(failover_tx
        .lookup(bucket_id, "quorum.dat")
        .await
        .unwrap()
        .is_none());
    assert!(failover_tx
        .lookup(bucket_id, "quorum_moved.dat")
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        failover_tx.dirstat(bucket_id).await.unwrap().num_children,
        3
    );

    let restart_cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let restart_wm = Arc::new(ShardWatermarkTracker::default());
    let restart_handler = Arc::new(TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        range.clone(),
        restart_wm.clone(),
    ));
    let restarted = ShardRaftGroup::start(
        1,
        1,
        host.shard_dir(1),
        restart_cache,
        lsm,
        opts,
        restart_handler,
        restart_wm,
        clock.clone(),
        host.router(),
        host.channel_pool().clone(),
    )
    .await
    .unwrap();
    restarted
        .initialize_if_needed(BTreeMap::from([
            (1u64, addr1.clone()),
            (2u64, peer2.addr.clone()),
            (3u64, peer3.addr.clone()),
        ]))
        .await
        .unwrap();
    host.register_running_group(restarted.clone());

    let new_key = MetaKey::child(bucket_id, "after_failover.dat").encode();
    let old_key = MetaKey::child(bucket_id, "before_scale.dat").encode();
    let mut caught_up = false;
    for _ in 0..160 {
        let now = clock.now();
        let has_new = matches!(restarted.engine().get(&new_key, now), Ok(Some(_)));
        let has_old = matches!(restarted.engine().get(&old_key, now), Ok(Some(_)));
        if has_new && has_old {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        caught_up,
        "restarted voter did not apply the pre-scale key and the post-failover key"
    );

    host.shutdown_all().await.unwrap();
    peer2.group.shutdown().await.unwrap();
    peer3.group.shutdown().await.unwrap();
    srv1.abort();
    peer2.server.abort();
    peer3.server.abort();
}
