//! Three-voter membership on the real OpenRaft metadata state machine.
//!
//! `metadbd --role be` is rejected by `PersistentMetaDbCluster::open_and_serve`, and a second
//! all-in-one process bootstraps its own cell instead of joining. This test uses the membership
//! path the product does implement: `DurableCellManager::scale_out_replicas` adds learners over
//! localhost `RaftTransportService`, promotes them to voters, and serves `DistributedTxRouter`
//! metadata commands. It then stops the original leader and restarts that voter from its
//! `data_dir`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use metadb_cluster::{
    CompositeMetaCache, DurableCellManager, DurableGlobalManager, DurableIdAllocator,
};
use metadb_core::{
    make_file_inode_id, CompositeKeyRange, FileId, HlcTimestamp, HybridLogicalClock, InodeId,
    MetaDbError, MetaKey, ShardEpoch, ROOT_INODE_ID,
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

const LOAD_FILE_BYTES: i64 = 128;
const LOAD_MIN_ACKED: usize = 16;

struct BurstReport {
    acked: HashMap<String, (InodeId, FileId)>,
    attempted: HashSet<String>,
    crash_window_errors: u64,
    latencies_us: Vec<u64>,
}

struct LiveBurst {
    acked: Arc<Mutex<HashMap<String, (InodeId, FileId)>>>,
    attempted: Arc<Mutex<HashSet<String>>>,
    in_flight: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    uncertain: Arc<AtomicBool>,
    pre_crash_error: Arc<Mutex<Option<String>>>,
    crash_errors: Arc<AtomicU64>,
    latencies: Arc<Mutex<Vec<u64>>>,
    joins: Vec<tokio::task::JoinHandle<()>>,
}

impl LiveBurst {
    fn start(
        tx: Arc<DistributedTxRouter>,
        pid: InodeId,
        prefix: &'static str,
        ids: Arc<AtomicU64>,
        max_per_worker: u64,
        deadline: Option<Instant>,
    ) -> Self {
        let acked = Arc::new(Mutex::new(HashMap::new()));
        let attempted = Arc::new(Mutex::new(HashSet::new()));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let uncertain = Arc::new(AtomicBool::new(false));
        let pre_crash_error = Arc::new(Mutex::new(None));
        let crash_errors = Arc::new(AtomicU64::new(0));
        let latencies = Arc::new(Mutex::new(Vec::new()));
        let mut joins = Vec::with_capacity(4);
        for tid in 0..4 {
            let tx = tx.clone();
            let ids = ids.clone();
            let acked = acked.clone();
            let attempted = attempted.clone();
            let in_flight = in_flight.clone();
            let stop = stop.clone();
            let uncertain = uncertain.clone();
            let pre_crash_error = pre_crash_error.clone();
            let crash_errors = crash_errors.clone();
            let latencies = latencies.clone();
            joins.push(tokio::spawn(async move {
                let mut local = 0u64;
                while local < max_per_worker {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if deadline.is_some_and(|limit| Instant::now() >= limit) {
                        break;
                    }
                    local += 1;
                    let seq = ids.fetch_add(1, Ordering::Relaxed);
                    let name = format!("{prefix}{tid:02}_{seq:05}.dat");
                    attempted.lock().unwrap().insert(name.clone());
                    in_flight.fetch_add(1, Ordering::Relaxed);
                    let started = Instant::now();
                    let result = tokio::time::timeout(
                        Duration::from_secs(5),
                        tx.create_file(
                            pid,
                            &name,
                            make_file_inode_id(seq),
                            seq,
                            7,
                            0o644,
                            LOAD_FILE_BYTES,
                        ),
                    )
                    .await;
                    in_flight.fetch_sub(1, Ordering::Relaxed);
                    latencies
                        .lock()
                        .unwrap()
                        .push(started.elapsed().as_micros() as u64);
                    match result {
                        Ok(Ok(entry)) => {
                            let fid = entry.attr.fid.expect("file id");
                            acked
                                .lock()
                                .unwrap()
                                .insert(name, (entry.attr.inode_id, fid));
                        }
                        Ok(Err(err)) => note_failure(
                            &uncertain,
                            &pre_crash_error,
                            &crash_errors,
                            err.to_string(),
                        ),
                        Err(_) => note_failure(
                            &uncertain,
                            &pre_crash_error,
                            &crash_errors,
                            format!("create {name} timed out"),
                        ),
                    }
                }
            }));
        }
        Self {
            acked,
            attempted,
            in_flight,
            stop,
            uncertain,
            pre_crash_error,
            crash_errors,
            latencies,
            joins,
        }
    }

    async fn wait_until_overlapping(&self, min_acked: usize) {
        let started = Instant::now();
        loop {
            let n = self.acked.lock().unwrap().len();
            let busy = self.in_flight.load(Ordering::Relaxed);
            if n >= min_acked && busy >= 1 {
                return;
            }
            if started.elapsed() > Duration::from_secs(20) {
                panic!("create load did not overlap a propose (acked={n} in_flight={busy})");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    fn allow_errors(&self) {
        self.uncertain.store(true, Ordering::Relaxed);
    }

    fn request_stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    async fn finish(self) -> BurstReport {
        for join in self.joins {
            join.await.expect("create worker panicked");
        }
        if let Some(err) = self.pre_crash_error.lock().unwrap().clone() {
            panic!("create failed before the injected failure window: {err}");
        }
        BurstReport {
            acked: std::mem::take(&mut *self.acked.lock().unwrap()),
            attempted: std::mem::take(&mut *self.attempted.lock().unwrap()),
            crash_window_errors: self.crash_errors.load(Ordering::Relaxed),
            latencies_us: std::mem::take(&mut *self.latencies.lock().unwrap()),
        }
    }
}

fn note_failure(
    uncertain: &AtomicBool,
    pre_crash_error: &Mutex<Option<String>>,
    crash_errors: &AtomicU64,
    message: String,
) {
    if uncertain.load(Ordering::Relaxed) {
        crash_errors.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let mut guard = pre_crash_error.lock().unwrap();
    if guard.is_none() {
        *guard = Some(message);
    }
}

/// Concurrent creates on a 3-voter shard, then stop the original leader without flushing.
/// Acknowledged names must be visible on the new leader. The stopped voter is restarted
/// while a second create wave is still running and must catch up to the committed listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_voter_leader_loss_under_load() {
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
    let tx = Arc::new(DistributedTxRouter::new(1, clock.clone(), gm));
    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();
    let bucket = tx
        .mkdir(ROOT_INODE_ID, "bucket", 10, 7, 0o755)
        .await
        .unwrap();
    let bucket_id = bucket.attr.inode_id;
    let sentinel = tx
        .create_file(
            bucket_id,
            "before_scale.dat",
            make_file_inode_id(1),
            1,
            7,
            0o644,
            LOAD_FILE_BYTES,
        )
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
    assert!(upgraded >= 1, "expected the meta shard to gain voters");

    let ids = Arc::new(AtomicU64::new(2));
    let phase_started = Instant::now();
    let burst = LiveBurst::start(tx.clone(), bucket_id, "a", ids.clone(), 40, None);
    burst.wait_until_overlapping(LOAD_MIN_ACKED).await;
    let in_flight = burst.in_flight();
    burst.allow_errors();
    burst.request_stop();
    tokio::time::timeout(Duration::from_secs(15), host.shutdown_all_no_flush())
        .await
        .expect("leader shutdown_all_no_flush timed out")
        .unwrap();
    let phase_a = burst.finish().await;

    let survivors = [
        (peer2.group.clone(), peer2.handler.clone()),
        (peer3.group.clone(), peer3.handler.clone()),
    ];
    let (leader, handler) = wait_leader(&survivors).await;
    let leader_id = leader.node_id();
    assert_ne!(leader_id, 1, "the stopped node must not remain leader");
    let failover = Arc::new(DistributedTxRouter::new(
        leader_id,
        clock.clone(),
        Arc::new(LeaderDirectory::new(
            range.clone(),
            leader,
            handler,
            clock.clone(),
        )),
    ));

    let mut acked = phase_a.acked.clone();
    acked.insert(
        "before_scale.dat".to_string(),
        (
            sentinel.attr.inode_id,
            sentinel.attr.fid.expect("sentinel fid"),
        ),
    );
    let mut attempted = phase_a.attempted.clone();
    attempted.insert("before_scale.dat".to_string());
    let listing = assert_create_set(&failover, bucket_id, &acked, &attempted)
        .await
        .expect("leader-loss listing")
        .len();
    emit_failure(
        "quorum-leader-loss",
        phase_started.elapsed(),
        acked.len(),
        attempted.len(),
        listing,
        phase_a.crash_window_errors,
        in_flight,
        &phase_a.latencies_us,
    );

    let restart_started = Instant::now();
    // Keep the overlap short: enough in-flight creates to hit the restarting voter,
    // not a multi-thousand-op soak. The ignored stress_local profile covers duration.
    let phase_b = LiveBurst::start(
        failover.clone(),
        bucket_id,
        "b",
        ids,
        40,
        Some(Instant::now() + Duration::from_secs(1)),
    );
    phase_b.wait_until_overlapping(8).await;
    let restart_in_flight = phase_b.in_flight();
    phase_b.allow_errors();
    let restart_cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let restart_wm = Arc::new(ShardWatermarkTracker::default());
    let restart_handler = Arc::new(TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        range.clone(),
        restart_wm.clone(),
    ));
    let restarted = tokio::time::timeout(
        Duration::from_secs(20),
        ShardRaftGroup::start(
            1,
            1,
            host.shard_dir(1),
            restart_cache,
            lsm,
            opts,
            restart_handler.clone(),
            restart_wm,
            clock.clone(),
            host.router(),
            host.channel_pool().clone(),
        ),
    )
    .await
    .expect("restarting the stopped voter timed out")
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
    phase_b.request_stop();
    let phase_b = phase_b.finish().await;

    acked.extend(phase_b.acked);
    attempted.extend(phase_b.attempted);
    // Restarting the stopped voter can move the Raft leader. Follow whoever wins
    // before checking the committed listing.
    let candidates = [
        (peer2.group.clone(), peer2.handler.clone()),
        (peer3.group.clone(), peer3.handler.clone()),
        (restarted.clone(), restart_handler),
    ];
    let check_deadline = Instant::now() + Duration::from_secs(10);
    let (listing, committed, leader_after_restart) = loop {
        let (leader, handler) = wait_leader(&candidates).await;
        let leader_id = leader.node_id();
        let reader = Arc::new(DistributedTxRouter::new(
            leader_id,
            clock.clone(),
            Arc::new(LeaderDirectory::new(
                range.clone(),
                leader,
                handler,
                clock.clone(),
            )),
        ));
        match assert_create_set(&reader, bucket_id, &acked, &attempted).await {
            Ok(names) => break (names.len(), names, leader_id),
            Err(err) if is_transient(&err) && Instant::now() < check_deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(err) => panic!("post-restart correctness check failed: {err}"),
        }
    };
    let catch_started = Instant::now();
    wait_engine_has_names(&restarted, &clock, bucket_id, &committed).await;
    println!("metadb failure profile=quorum-restart-leader node={leader_after_restart}");
    emit_failure(
        "quorum-restart-under-load",
        restart_started.elapsed(),
        acked.len(),
        attempted.len(),
        listing,
        phase_b.crash_window_errors,
        restart_in_flight,
        &phase_b.latencies_us,
    );
    println!(
        "metadb failure profile=quorum-restart-catchup elapsed_ms={} names={}",
        catch_started.elapsed().as_millis(),
        committed.len()
    );

    host.shutdown_all().await.unwrap();
    peer2.group.shutdown().await.unwrap();
    peer3.group.shutdown().await.unwrap();
    srv1.abort();
    peer2.server.abort();
    peer3.server.abort();
}

fn is_transient(err: &MetaDbError) -> bool {
    matches!(
        err,
        MetaDbError::NotLeader { .. }
            | MetaDbError::EpochMismatch { .. }
            | MetaDbError::ShardRoutingError(_)
            | MetaDbError::ConsensusError(_)
    )
}

async fn assert_create_set(
    tx: &DistributedTxRouter,
    pid: InodeId,
    acked: &HashMap<String, (InodeId, FileId)>,
    attempted: &HashSet<String>,
) -> Result<HashSet<String>, MetaDbError> {
    let listing = list_names(tx, pid).await?;
    let missing: Vec<_> = acked
        .keys()
        .filter(|name| !listing.contains(*name))
        .take(3)
        .cloned()
        .collect();
    assert!(
        missing.is_empty(),
        "acknowledged creates missing after leader loss: {missing:?} listing={} acked={}",
        listing.len(),
        acked.len()
    );
    let unexpected: Vec<_> = listing.difference(attempted).take(3).cloned().collect();
    assert!(
        unexpected.is_empty(),
        "listing contains names no client attempted: {unexpected:?}"
    );
    let stat = tx.dirstat(pid).await?;
    assert_eq!(stat.num_children, listing.len() as i64, "dirstat children");
    assert_eq!(
        stat.size_bytes,
        listing.len() as i64 * LOAD_FILE_BYTES,
        "dirstat bytes"
    );
    for (name, (inode, fid)) in acked {
        let got = tx
            .lookup(pid, name)
            .await?
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(got.attr.inode_id, *inode, "{name}");
        assert_eq!(got.attr.fid, Some(*fid), "{name}");
        assert_eq!(got.attr.size_bytes, LOAD_FILE_BYTES, "{name}");
    }
    Ok(listing)
}

async fn list_names(
    tx: &DistributedTxRouter,
    pid: InodeId,
) -> Result<HashSet<String>, MetaDbError> {
    let mut names = HashSet::new();
    let mut cursor = None;
    loop {
        let (page, next) = tx
            .listdir_paginated(pid, cursor.as_deref(), None, 256)
            .await?;
        if page.is_empty() {
            break;
        }
        for entry in page {
            names.insert(entry.key.name);
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(names)
}

async fn wait_engine_has_names(
    group: &ShardRaftGroup,
    clock: &HybridLogicalClock,
    pid: InodeId,
    names: &HashSet<String>,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut missing = names.len();
    while Instant::now() < deadline {
        missing = 0;
        let now = clock.now();
        for name in names {
            let key = MetaKey::child(pid, name).encode();
            if !matches!(group.engine().get(&key, now), Ok(Some(_))) {
                missing += 1;
            }
        }
        if missing == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "restarted voter missing {missing} of {} committed names",
        names.len()
    );
}

#[allow(clippy::too_many_arguments)]
fn emit_failure(
    profile: &str,
    elapsed: Duration,
    acked: usize,
    attempted: usize,
    listing: usize,
    crash_window_errors: u64,
    in_flight: usize,
    samples: &[u64],
) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    println!(
        "metadb failure profile={profile} acked={acked} attempted={attempted} listing={listing} crash_window_errors={crash_window_errors} in_flight_at_stop={in_flight} elapsed_ms={} latency_us p50={} p99={} max={}",
        elapsed.as_millis(),
        percentile(&sorted, 50),
        percentile(&sorted, 99),
        sorted.last().copied().unwrap_or(0),
    );
}

fn percentile(sorted: &[u64], pct: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as u64 * pct / 100) as usize;
    sorted[index]
}
