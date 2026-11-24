//! Multi-voter (`3-Replica`) online shard split, `ProbeTransferLeader`-guarded leadership transfer,
//! and `CompositeMetaCache` overlap eviction integration tests.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use metadb_cluster::meta_cache::{CompositeMetaCache, CompositeShardRoute};
use metadb_cluster::{
    DurableCellManager, DurableGlobalManager, DurableIdAllocator, ShardHealthState, ShardSplitMode,
};
use metadb_core::{CompositeKeyRange, HybridLogicalClock, MetaKey, ShardEpoch, ROOT_INODE_ID};
use metadb_raft::{MultiRaftTransportServer, RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::DistributedTxRouter;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct RaftTestNode {
    host: Arc<ShardGroupHost>,
    cell: Arc<DurableCellManager>,
    gm: Arc<DurableGlobalManager>,
    tx: Arc<DistributedTxRouter>,
    shutdown_tx: watch::Sender<bool>,
    srv_handle: tokio::task::JoinHandle<()>,
    addr: String,
}

impl RaftTestNode {
    async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.srv_handle.await;
        let _ = self.host.shutdown_all().await;
    }

    async fn crash(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.srv_handle.await;
        let _ = self.host.shutdown_all_no_flush().await;
    }
}

#[derive(Clone)]
struct MiniControlPlane {
    cell: Arc<DurableCellManager>,
}

#[tonic::async_trait]
impl metadb_proto::control_plane_service_server::ControlPlaneService for MiniControlPlane {
    async fn register_node(
        &self,
        _req: tonic::Request<metadb_proto::RegisterNodeRequest>,
    ) -> Result<tonic::Response<metadb_proto::RegisterNodeResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn store_heartbeat(
        &self,
        _req: tonic::Request<metadb_proto::StoreHeartbeatRequest>,
    ) -> Result<tonic::Response<metadb_proto::StoreHeartbeatResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn get_routes(
        &self,
        _req: tonic::Request<metadb_proto::GetRoutesRequest>,
    ) -> Result<tonic::Response<metadb_proto::GetRoutesResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn allocate_id_block(
        &self,
        _req: tonic::Request<metadb_proto::AllocateIdBlockRequest>,
    ) -> Result<tonic::Response<metadb_proto::AllocateIdBlockResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn alloc_split_plan(
        &self,
        _req: tonic::Request<metadb_proto::AllocSplitPlanRequest>,
    ) -> Result<tonic::Response<metadb_proto::AllocSplitPlanResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn activate_split(
        &self,
        _req: tonic::Request<metadb_proto::ActivateSplitRequest>,
    ) -> Result<tonic::Response<metadb_proto::ActivateSplitResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn acquire_dir_rename_lease(
        &self,
        _req: tonic::Request<metadb_proto::AcquireDirRenameLeaseRequest>,
    ) -> Result<tonic::Response<metadb_proto::AcquireDirRenameLeaseResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn extend_dir_rename_lease(
        &self,
        _req: tonic::Request<metadb_proto::ExtendDirRenameLeaseRequest>,
    ) -> Result<tonic::Response<metadb_proto::ExtendDirRenameLeaseResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn release_dir_rename_lease(
        &self,
        _req: tonic::Request<metadb_proto::ReleaseDirRenameLeaseRequest>,
    ) -> Result<tonic::Response<metadb_proto::ReleaseDirRenameLeaseResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn trigger_shard_split(
        &self,
        _req: tonic::Request<metadb_proto::TriggerShardSplitRequest>,
    ) -> Result<tonic::Response<metadb_proto::TriggerShardSplitResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn enable_dual_mode_split(
        &self,
        _req: tonic::Request<metadb_proto::EnableDualModeSplitRequest>,
    ) -> Result<tonic::Response<metadb_proto::EnableDualModeSplitResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn scale_shard_replicas(
        &self,
        _req: tonic::Request<metadb_proto::ScaleShardReplicasRequest>,
    ) -> Result<tonic::Response<metadb_proto::ScaleShardReplicasResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn register_cell(
        &self,
        _req: tonic::Request<metadb_proto::RegisterCellRequest>,
    ) -> Result<tonic::Response<metadb_proto::RegisterCellResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn transfer_shard_leader(
        &self,
        _req: tonic::Request<metadb_proto::TransferShardLeaderRequest>,
    ) -> Result<tonic::Response<metadb_proto::TransferShardLeaderResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn remove_shard_voter(
        &self,
        _req: tonic::Request<metadb_proto::RemoveShardVoterRequest>,
    ) -> Result<tonic::Response<metadb_proto::RemoveShardVoterResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unused in mini test"))
    }
    async fn open_joining_shard(
        &self,
        req: tonic::Request<metadb_proto::OpenJoiningShardRequest>,
    ) -> Result<tonic::Response<metadb_proto::OpenJoiningShardResponse>, tonic::Status> {
        let r = req.into_inner();
        let range = CompositeKeyRange::new(r.start_key, r.end_key).unwrap();
        let epoch: ShardEpoch = r.initial_epoch.unwrap_or_default().into();
        let res = self
            .cell
            .open_joining_shard(r.shard_id, range, epoch, r.use_jumbo_dedicated_root)
            .await;
        Ok(tonic::Response::new(
            metadb_proto::OpenJoiningShardResponse {
                error: res.err().map(|e| metadb_proto::ErrorDetailProto::from(&e)),
            },
        ))
    }
    async fn close_removed_shard(
        &self,
        req: tonic::Request<metadb_proto::CloseRemovedShardRequest>,
    ) -> Result<tonic::Response<metadb_proto::CloseRemovedShardResponse>, tonic::Status> {
        let r = req.into_inner();
        let res = self
            .cell
            .close_removed_shard(r.shard_id, r.delete_files)
            .await;
        Ok(tonic::Response::new(
            metadb_proto::CloseRemovedShardResponse {
                error: res.err().map(|e| metadb_proto::ErrorDetailProto::from(&e)),
            },
        ))
    }
}

async fn boot_three_voter_cell_with_cp(
    root: &std::path::Path,
) -> (RaftTestNode, RaftTestNode, RaftTestNode) {
    let raft_opts = RaftNodeOptions {
        heartbeat_interval_ms: 80,
        election_timeout_min_ms: 200,
        election_timeout_max_ms: 400,
        ..RaftNodeOptions::default()
    };

    let mut listeners = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..3 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(l.local_addr().unwrap().to_string());
        listeners.push(l);
    }

    let n1 = {
        let clock = Arc::new(HybridLogicalClock::new());
        let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
        let alloc = Arc::new(DurableIdAllocator::open_for_node(root.join("n1/alloc"), 1).unwrap());
        let meta_cache = Arc::new(CompositeMetaCache::new());
        let host =
            Arc::new(ShardGroupHost::open(1, root.join("n1/be"), cache, clock.clone()).unwrap());
        let cell = DurableCellManager::open_or_bootstrap(
            1,
            root.join("n1/cell_1"),
            host.clone(),
            addrs[0].clone(),
            true,
            64 * 1024 * 1024,
            LsmEngineConfig::default(),
            raft_opts.clone(),
            alloc.clone(),
            meta_cache.clone(),
        )
        .await
        .unwrap();
        let gm = Arc::new(DurableGlobalManager::new(clock.clone(), alloc, meta_cache));
        gm.register_cell(CompositeKeyRange::all_user_tables(), cell.clone());
        let tx = Arc::new(DistributedTxRouter::new(1, clock, gm.clone()));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let listener = listeners.remove(0);
        let router = host.router().clone();
        let cp_cell = cell.clone();
        let srv_handle = tokio::spawn(async move {
            let fut = Server::builder()
                .add_service(
                    metadb_proto::raft_transport_service_server::RaftTransportServiceServer::new(
                        MultiRaftTransportServer::new(router),
                    ),
                )
                .add_service(
                    metadb_proto::control_plane_service_server::ControlPlaneServiceServer::new(
                        MiniControlPlane { cell: cp_cell },
                    ),
                )
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_rx.changed().await;
                });
            tokio::pin!(fut);
            let _ = fut.await;
        });
        RaftTestNode {
            host,
            cell,
            gm,
            tx,
            shutdown_tx,
            srv_handle,
            addr: addrs[0].clone(),
        }
    };

    let init_routes = n1.cell.list_routes();
    let cluster_id = n1.cell.cluster_id();

    let mut joiners = Vec::new();
    for (idx, nid) in [2u64, 3u64].into_iter().enumerate() {
        let addr = addrs[idx + 1].clone();
        let listener = listeners.remove(0);
        let clock = Arc::new(HybridLogicalClock::new());
        let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
        let alloc = Arc::new(
            DurableIdAllocator::open_for_node(root.join(format!("n{nid}/alloc")), nid).unwrap(),
        );
        let meta_cache = Arc::new(CompositeMetaCache::new());
        let host = Arc::new(
            ShardGroupHost::open(nid, root.join(format!("n{nid}/be")), cache, clock.clone())
                .unwrap(),
        );
        let cell = DurableCellManager::open_joining(
            1,
            root.join(format!("n{nid}/cell_1")),
            host.clone(),
            addr.clone(),
            64 * 1024 * 1024,
            LsmEngineConfig::default(),
            raft_opts.clone(),
            alloc.clone(),
            meta_cache.clone(),
        )
        .await
        .unwrap();
        cell.adopt_cluster_identity(cluster_id).unwrap();
        cell.begin_join(&init_routes).unwrap();
        cell.start_joining_replicas(&init_routes).await.unwrap();
        let gm = Arc::new(DurableGlobalManager::new(clock.clone(), alloc, meta_cache));
        let tx = Arc::new(DistributedTxRouter::new(nid, clock, gm.clone()));
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let router = host.router().clone();
        let cp_cell = cell.clone();
        let srv_handle = tokio::spawn(async move {
            let fut = Server::builder()
                .add_service(
                    metadb_proto::raft_transport_service_server::RaftTransportServiceServer::new(
                        MultiRaftTransportServer::new(router),
                    ),
                )
                .add_service(
                    metadb_proto::control_plane_service_server::ControlPlaneServiceServer::new(
                        MiniControlPlane { cell: cp_cell },
                    ),
                )
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_rx.changed().await;
                });
            tokio::pin!(fut);
            let _ = fut.await;
        });
        joiners.push(RaftTestNode {
            host,
            cell,
            gm,
            tx,
            shutdown_tx,
            srv_handle,
            addr,
        });
    }
    let n2 = joiners.remove(0);
    let n3 = joiners.remove(0);

    tokio::time::sleep(Duration::from_millis(50)).await;
    n1.cell
        .register_node_instance(2, n2.addr.clone(), n2.cell.node_instance_id())
        .await
        .unwrap();
    n1.cell
        .register_node_instance(3, n3.addr.clone(), n3.cell.node_instance_id())
        .await
        .unwrap();
    n1.cell.scale_out_replicas(3).await.unwrap();
    n1.gm.publish_replicated_directory(&n1.cell).await.unwrap();

    for peer in [&n2, &n3] {
        for _ in 0..80 {
            if peer.cell.reconcile_replicated_membership().is_ok()
                && peer
                    .cell
                    .list_routes()
                    .iter()
                    .all(|r| r.peers.values().filter(|(_, v)| *v).count() == 3)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        peer.gm
            .register_cell(CompositeKeyRange::all_user_tables(), peer.cell.clone());
    }

    (n1, n2, n3)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voter_online_shard_split_and_leader_failover() {
    let dir = tempdir().unwrap();
    let (n1, n2, n3) = boot_three_voter_cell_with_cp(dir.path()).await;

    n1.tx
        .init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();
    for (idx, pid) in [10u64, 20, 30, 40].into_iter().enumerate() {
        n1.tx
            .mkdir(ROOT_INODE_ID, &format!("d_{pid}"), pid, 1, 0o755)
            .await
            .unwrap();
        for f in 0..4u64 {
            n1.tx
                .create_file(
                    pid,
                    &format!("f_{f}.dat"),
                    1_000 + (idx as u64) * 10 + f,
                    5_000 + (idx as u64) * 10 + f,
                    1,
                    0o644,
                    256,
                )
                .await
                .unwrap();
        }
    }

    // Execute online 3-voter PID-boundary split on Shard 1!
    let child_id = n1
        .cell
        .maybe_split_or_isolate_shard(1, true)
        .await
        .unwrap()
        .expect("3-voter shard 1 must split");
    n1.gm.publish_replicated_directory(&n1.cell).await.unwrap();

    // Verify child shard has `{1, 2, 3}` uniform voters!
    let (_, child_grp_n1, _) = n1.cell.lookup_local_handle(child_id).unwrap();
    let voters = child_grp_n1.uniform_voter_addrs().unwrap();
    assert_eq!(
        voters.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 2, 3]),
        "split child shard must promote all 3 parent voters before activating split"
    );

    // Wait for replicated control snapshot to reach followers, then crash Node 1!
    tokio::time::sleep(Duration::from_millis(200)).await;
    n1.crash().await;

    // Wait for {2, 3} to elect a new leader on BOTH parent Shard 1 and child shard `child_id`!
    let mut elected_parent = None;
    let mut elected_child = None;
    for _ in 0..100 {
        for peer in [&n2, &n3] {
            let _ = peer.cell.routes_for_live_status();
            if let Ok((_, grp1, _)) = peer.cell.lookup_local_handle(1) {
                if matches!(grp1.current_leader(), Some(2 | 3)) {
                    elected_parent = grp1.current_leader();
                }
            }
            if let Ok((_, grpc, _)) = peer.cell.lookup_local_handle(child_id) {
                if matches!(grpc.current_leader(), Some(2 | 3)) {
                    elected_child = grpc.current_leader();
                }
            }
        }
        if elected_parent.is_some() && elected_child.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        elected_parent.is_some(),
        "parent shard 1 failed to elect leader on {{2, 3}}"
    );
    assert!(
        elected_child.is_some(),
        "child shard {child_id} failed to elect leader on {{2, 3}}"
    );

    // Verify reads AND new writes succeed on BOTH left-half (PID 10) and right-half (PID 40) via Node 2 / Node 3!
    let mut left_ok = false;
    let mut right_ok = false;
    for _ in 0..40 {
        for peer in [&n2, &n3] {
            let _ = peer.cell.routes_for_live_status();
            if !left_ok
                && peer.tx.lookup(10, "f_0.dat").await.ok().flatten().is_some()
                && peer
                    .tx
                    .create_file(10, "post_failover_left.dat", 9_001, 19_001, 1, 0o644, 64)
                    .await
                    .is_ok()
            {
                left_ok = true;
            }
            if !right_ok
                && peer.tx.lookup(40, "f_0.dat").await.ok().flatten().is_some()
                && peer
                    .tx
                    .create_file(40, "post_failover_right.dat", 9_002, 19_002, 1, 0o644, 64)
                    .await
                    .is_ok()
            {
                right_ok = true;
            }
        }
        if left_ok && right_ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        left_ok,
        "left-half parent shard did not serve post-failover read/write"
    );
    assert!(
        right_ok,
        "right-half split child shard did not serve post-failover read/write"
    );

    n2.stop().await;
    n3.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn safe_leadership_transfer_probes_before_dropping_lease_and_transfers_to_live_voter() {
    let dir = tempdir().unwrap();
    let (n1, n2, n3) = boot_three_voter_cell_with_cp(dir.path()).await;

    n1.tx
        .init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();

    // Stop Node 3 so it is unreachable, while {1, 2} retain quorum!
    n3.stop().await;

    // Attempting to transfer leadership of Shard 1 to dead Node 3 MUST fail closed at `ProbeTransferLeader`
    // WITHOUT calling OpenRaft `transfer_leader` or disabling Node 1's leader lease!
    let dead_outcome = n1.cell.transfer_shard_leader(1, 3).await.unwrap();
    assert!(
        !dead_outcome.openraft_called,
        "ProbeTransferLeader preflight must prevent calling openraft transfer_leader on dead target: {dead_outcome:?}"
    );
    assert!(
        dead_outcome.committed.is_none(),
        "dead target transfer must not commit: {dead_outcome:?}"
    );
    assert_eq!(dead_outcome.leader, Some(1));
    let (_, grp1, _) = n1.cell.lookup_local_handle(1).unwrap();
    assert_eq!(
        grp1.current_leader(),
        Some(1),
        "Node 1 must remain leader with its lease intact when ProbeTransferLeader rejects dead target"
    );
    n1.tx
        .create_file(ROOT_INODE_ID, "lease_intact.dat", 801, 1801, 1, 0o644, 32)
        .await
        .expect("Node 1 must immediately serve writes without waiting out an election timeout");

    // Transferring leadership of Shard 1 to healthy caught-up voter Node 2 MUST succeed!
    let outcome = n1.cell.transfer_shard_leader(1, 2).await.unwrap();
    assert!(
        outcome.refusal.is_none(),
        "unexpected refusal: {:?}",
        outcome.refusal
    );
    assert!(
        outcome.committed.is_some(),
        "expected committed LeadershipTransferred"
    );
    assert_eq!(outcome.leader, Some(2));

    n1.stop().await;
    n2.stop().await;
}

#[test]
fn composite_meta_cache_evicts_overlapping_stale_routes() {
    let cache = CompositeMetaCache::new();
    let full = CompositeShardRoute {
        cell_id: 1,
        shard_id: 1,
        range: CompositeKeyRange::new(vec![0x01], vec![0x02]).unwrap(),
        epoch: ShardEpoch {
            conf_ver: 1,
            version: 1,
        },
        leader_node: 1,
        peers: BTreeMap::new(),
        split_mode: ShardSplitMode::DirectoryColocated,
        health_state: ShardHealthState::Normal,
        dual_mode_pid: None,
    };
    cache.upsert_route(full);

    let mid = MetaKey::encode_pid_prefix_start(500);
    let left = CompositeShardRoute {
        cell_id: 1,
        shard_id: 1,
        range: CompositeKeyRange::new(vec![0x01], mid.clone()).unwrap(),
        epoch: ShardEpoch {
            conf_ver: 1,
            version: 2,
        },
        leader_node: 1,
        peers: BTreeMap::new(),
        split_mode: ShardSplitMode::DirectoryColocated,
        health_state: ShardHealthState::Normal,
        dual_mode_pid: None,
    };
    let right = CompositeShardRoute {
        cell_id: 1,
        shard_id: 4,
        range: CompositeKeyRange::new(mid, vec![0x02]).unwrap(),
        epoch: ShardEpoch {
            conf_ver: 1,
            version: 2,
        },
        leader_node: 2,
        peers: BTreeMap::new(),
        split_mode: ShardSplitMode::DirectoryColocated,
        health_state: ShardHealthState::Normal,
        dual_mode_pid: None,
    };
    cache.apply_split_routes(left, right);

    let r_left = cache.lookup_key(&MetaKey::child(10, "a").encode()).unwrap();
    assert_eq!(r_left.shard_id, 1);
    assert_eq!(
        r_left.epoch,
        ShardEpoch {
            conf_ver: 1,
            version: 2
        }
    );

    let r_right = cache
        .lookup_key(&MetaKey::child(600, "b").encode())
        .unwrap();
    assert_eq!(r_right.shard_id, 4);
    assert_eq!(r_right.leader_node, 2);
}
