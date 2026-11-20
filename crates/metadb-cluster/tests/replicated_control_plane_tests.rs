//! The cell directory and cell-manager membership are stored in the existing
//! shard Raft log. A restart reads that snapshot. A second voter of the same
//! shard reads it after the log is applied.
//!
//! This does not forward requests, allocate ids, schedule heartbeats, move a
//! jumbo shard, or transfer leadership.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use metadb_cluster::cell_directory::{decode_directory_body, store_directory, DirectoryCell};
use metadb_cluster::{
    manager_nodes_from_body, manager_voters_from_body, validate_replicated_record,
    CompositeMetaCache, CompositeShardRoute, DurableCellManager, DurableGlobalManager,
    DurableIdAllocator, ShardHealthState, ShardSplitMode,
};
use metadb_core::{CompositeKeyRange, HybridLogicalClock, ShardEpoch};
use metadb_proto::raft_transport_service_server::RaftTransportServiceServer;
use metadb_raft::{
    ChannelPool, MultiRaftRouter, MultiRaftTransportServer, RaftNodeOptions, ShardGroupHost,
    ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{read_replicated_control_record, ReplicatedControlRecord, TxStateMachineHandler};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

fn raft_options() -> RaftNodeOptions {
    RaftNodeOptions {
        heartbeat_interval_ms: 40,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        ..RaftNodeOptions::default()
    }
}

async fn spawn_raft(router: MultiRaftRouter) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let service = RaftTransportServiceServer::new(MultiRaftTransportServer::new(router));
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (address, handle)
}

async fn open_leader(
    dir: &std::path::Path,
    address: &str,
    host: Arc<ShardGroupHost>,
) -> (Arc<DurableCellManager>, Arc<DurableGlobalManager>) {
    let cell = DurableCellManager::open_or_bootstrap_in_range(
        1,
        dir.join("cell_1"),
        host,
        address.to_string(),
        true,
        4 * 1024 * 1024 * 1024,
        LsmEngineConfig::default(),
        raft_options(),
        Arc::new(DurableIdAllocator::open(dir.join("alloc")).unwrap()),
        Arc::new(CompositeMetaCache::new()),
        Some(CompositeKeyRange::meta_table_root()),
        None,
    )
    .await
    .unwrap();
    let global = Arc::new(DurableGlobalManager::new(
        Arc::new(HybridLogicalClock::new()),
        Arc::new(DurableIdAllocator::open(dir.join("global-alloc")).unwrap()),
        Arc::new(CompositeMetaCache::new()),
    ));
    global
        .load_or_create_directory(dir.join("GLOBAL_CELL_DIRECTORY"))
        .unwrap();
    global.align_directory_with_replication(&cell).unwrap();
    global
        .install_local_cell(cell.clone(), address.to_string())
        .unwrap();
    assert!(global.publish_replicated_directory(&cell).await.unwrap());
    (cell, global)
}

fn remote_cell() -> metadb_cluster::RemotePublication {
    let mut peers = BTreeMap::new();
    peers.insert(9, ("127.0.0.1:9".to_string(), true));
    metadb_cluster::RemotePublication {
        cell_id: 2,
        range: CompositeKeyRange::quota_table_root(),
        endpoints: vec!["127.0.0.1:9".to_string()],
        cluster_id: 77,
        routes: vec![CompositeShardRoute {
            cell_id: 2,
            shard_id: 2,
            range: CompositeKeyRange::quota_table_root(),
            epoch: ShardEpoch::initial(),
            leader_node: 9,
            peers,
            split_mode: ShardSplitMode::DirectoryColocated,
            health_state: ShardHealthState::Normal,
            dual_mode_pid: None,
        }],
    }
}

fn snapshot(engine: &LifecycleStorageEngine) -> ReplicatedControlRecord {
    read_replicated_control_record(engine)
        .expect("corrupt snapshot must fail closed")
        .expect("replicated snapshot")
}

fn assert_remote_cell(engine: &LifecycleStorageEngine) {
    let record = snapshot(engine);
    let cells = decode_directory_body(&record.directory_body).unwrap();
    let remote = cells.get(&2).expect("remote cell");
    assert_eq!(remote.endpoints, vec!["127.0.0.1:9".to_string()]);
    assert_eq!(remote.range, CompositeKeyRange::quota_table_root());
    assert!(!remote.local);
    assert!(cells.contains_key(&1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_and_membership_persist_recover_and_replicate() {
    let root = tempdir().unwrap();
    let leader_dir = root.path().join("leader");
    let follower_dir = root.path().join("follower");
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let host = Arc::new(
        ShardGroupHost::open(
            1,
            leader_dir.join("be"),
            cache.clone(),
            Arc::new(HybridLogicalClock::new()),
        )
        .unwrap(),
    );
    let (leader_addr, leader_rpc) = spawn_raft(host.router().clone()).await;
    let follower_router = MultiRaftRouter::new();
    let (follower_addr, follower_rpc) = spawn_raft(follower_router.clone()).await;

    let (cell, global) = open_leader(&leader_dir, &leader_addr, host.clone()).await;
    assert!(global.commit_remote_cell(remote_cell()).unwrap());
    cell.register_node(2, follower_addr.clone()).unwrap();
    assert!(global.publish_replicated_directory(&cell).await.unwrap());
    let engine = cell.host().get_group(1).unwrap().engine();
    assert_remote_cell(engine.as_ref());
    let nodes = manager_nodes_from_body(&snapshot(engine.as_ref()).manager_body).unwrap();
    assert_eq!(
        nodes.get(&2).map(String::as_str),
        Some(follower_addr.as_str())
    );
    drop(engine);

    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(global);

    let (cell, global) = open_leader(&leader_dir, &leader_addr, host.clone()).await;
    let engine = cell.host().get_group(1).unwrap().engine();
    assert_remote_cell(engine.as_ref());
    let nodes = manager_nodes_from_body(&snapshot(engine.as_ref()).manager_body).unwrap();
    assert_eq!(
        nodes.get(&2).map(String::as_str),
        Some(follower_addr.as_str()),
        "restart must keep the registered node"
    );
    drop(engine);
    let remote = global.remote_publication(2).unwrap();
    assert_eq!(remote.endpoints, vec!["127.0.0.1:9".to_string()]);

    let route = cell
        .list_routes()
        .into_iter()
        .find(|route| route.shard_id == 1)
        .unwrap();
    let follower_wm = Arc::new(ShardWatermarkTracker::default());
    let follower_handler = Arc::new(TxStateMachineHandler::new(
        route.shard_id,
        ShardEpoch::initial(),
        route.range.clone(),
        follower_wm.clone(),
    ));
    follower_handler.set_control_plane_validator(Arc::new(validate_replicated_record));
    let follower = ShardRaftGroup::start(
        route.shard_id,
        2,
        follower_dir.join("shard"),
        cache,
        LsmEngineConfig::default(),
        raft_options(),
        follower_handler,
        follower_wm,
        Arc::new(HybridLogicalClock::new()),
        &follower_router,
        ChannelPool::default(),
    )
    .await
    .unwrap();
    cell.scale_out_replicas(2).await.expect("promote follower");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let ready = read_replicated_control_record(follower.engine().as_ref())
            .ok()
            .flatten()
            .and_then(|record| manager_voters_from_body(&record.manager_body).ok())
            .is_some_and(|voters| voters.values().any(|shard| shard.contains_key(&2)));
        if ready {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("follower did not apply membership");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_remote_cell(follower.engine().as_ref());
    let voters =
        manager_voters_from_body(&snapshot(follower.engine().as_ref()).manager_body).unwrap();
    assert!(
        voters
            .values()
            .any(|shard| shard.contains_key(&1) && shard.contains_key(&2)),
        "{voters:?}"
    );

    let mut only_local = BTreeMap::new();
    only_local.insert(
        1,
        DirectoryCell {
            cell_id: 1,
            range: CompositeKeyRange::meta_table_root(),
            endpoints: vec![leader_addr.clone()],
            local: true,
            cluster_id: cell.cluster_id(),
            routes: Vec::new(),
        },
    );
    let dropped = cell.publish_control_plane(&only_local).await.unwrap_err();
    assert!(
        dropped.to_string().contains("drop replicated cell 2"),
        "{dropped}"
    );
    assert_remote_cell(follower.engine().as_ref());

    follower.state_machine().flush_to_lsm().unwrap();
    follower.shutdown().await.unwrap();
    follower_router.clear();
    drop(follower);
    let follower_wm = Arc::new(ShardWatermarkTracker::default());
    let follower_handler = Arc::new(TxStateMachineHandler::new(
        route.shard_id,
        ShardEpoch::initial(),
        route.range,
        follower_wm.clone(),
    ));
    follower_handler.set_control_plane_validator(Arc::new(validate_replicated_record));
    let follower = ShardRaftGroup::start(
        1,
        2,
        follower_dir.join("shard"),
        Arc::new(ShardedBlockCache::new(8 * 1024 * 1024)),
        LsmEngineConfig::default(),
        raft_options(),
        follower_handler,
        follower_wm,
        Arc::new(HybridLogicalClock::new()),
        &follower_router,
        ChannelPool::default(),
    )
    .await
    .unwrap();
    assert_remote_cell(follower.engine().as_ref());
    let cluster_id = cell.cluster_id();
    follower.shutdown().await.unwrap();
    follower_router.clear();
    drop(follower);

    let directory = leader_dir.join("GLOBAL_CELL_DIRECTORY");
    let mut kept = BTreeMap::new();
    kept.insert(
        1,
        DirectoryCell {
            cell_id: 1,
            range: CompositeKeyRange::meta_table_root(),
            endpoints: vec![leader_addr.clone()],
            local: true,
            cluster_id,
            routes: Vec::new(),
        },
    );
    store_directory(&directory, &kept).unwrap();
    let global = DurableGlobalManager::new(
        Arc::new(HybridLogicalClock::new()),
        Arc::new(DurableIdAllocator::open(leader_dir.join("global-alloc-2")).unwrap()),
        Arc::new(CompositeMetaCache::new()),
    );
    global.load_or_create_directory(&directory).unwrap();
    let inconsistent = global.align_directory_with_replication(&cell).unwrap_err();
    assert!(
        inconsistent
            .to_string()
            .contains("missing replicated cell 2"),
        "{inconsistent}"
    );

    host.shutdown_all().await.unwrap();
    drop(cell);
    drop(global);
    drop(host);

    let mut raw = std::fs::read(&directory).unwrap();
    raw.push(0x5A);
    std::fs::write(&directory, &raw).unwrap();
    let global = DurableGlobalManager::new(
        Arc::new(HybridLogicalClock::new()),
        Arc::new(DurableIdAllocator::open(leader_dir.join("global-alloc-3")).unwrap()),
        Arc::new(CompositeMetaCache::new()),
    );
    let corrupt = global.load_or_create_directory(&directory).unwrap_err();
    assert!(
        corrupt.to_string().contains("GLOBAL_CELL_DIRECTORY")
            || corrupt.to_string().contains("checksum"),
        "{corrupt}"
    );

    let manager = leader_dir.join("cell_1").join("CELL_MANAGER_STATE");
    let mut raw = std::fs::read(&manager).unwrap();
    raw.truncate(raw.len().saturating_sub(1));
    std::fs::write(&manager, raw).unwrap();
    let host = ShardGroupHost::open(
        1,
        leader_dir.join("be"),
        Arc::new(ShardedBlockCache::new(1024 * 1024)),
        Arc::new(HybridLogicalClock::new()),
    )
    .unwrap();
    let opened = DurableCellManager::open_or_bootstrap_in_range(
        1,
        leader_dir.join("cell_1"),
        Arc::new(host),
        leader_addr,
        true,
        4 * 1024 * 1024 * 1024,
        LsmEngineConfig::default(),
        raft_options(),
        Arc::new(DurableIdAllocator::open(leader_dir.join("alloc-corrupt")).unwrap()),
        Arc::new(CompositeMetaCache::new()),
        Some(CompositeKeyRange::meta_table_root()),
        None,
    )
    .await;
    let error = match opened {
        Ok(_) => panic!("corrupt CELL_MANAGER_STATE must not open"),
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("CELL_MANAGER_STATE") || error.to_string().contains("checksum"),
        "{error}"
    );

    leader_rpc.abort();
    follower_rpc.abort();
}
