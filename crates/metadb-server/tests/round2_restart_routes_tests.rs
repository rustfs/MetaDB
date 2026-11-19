use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use openraft::async_runtime::watch::WatchReceiver;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn advertised_local_peer_address_must_follow_listener_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    let (first, old_address) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
        .await
        .unwrap();
    first.shutdown().await.unwrap();
    drop(first);
    let _occupied_old_port = std::net::TcpListener::bind(&old_address).unwrap();
    let (second, new_address) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let advertised: Vec<_> = second
        .cell_manager()
        .list_routes()
        .iter()
        .map(|route| (route.shard_id, route.peers.get(&1).unwrap().0.clone()))
        .collect();
    for route in second.cell_manager().list_routes() {
        let (_, group, _) = second
            .cell_manager()
            .lookup_local_handle(route.shard_id)
            .unwrap();
        let membership = group
            .raft()
            .metrics()
            .borrow_watched()
            .membership_config
            .clone();
        assert_eq!(
            membership.membership().get_node(&1).unwrap().addr,
            new_address
        );
        assert_eq!(route.epoch, group.current_epoch());
    }
    second.shutdown().await.unwrap();
    assert_ne!(old_address, new_address);
    assert!(advertised.iter().all(|(_, address)| address == &new_address),
        "restarted listener {new_address}, but advertised routes point at stopped listener {old_address}: {advertised:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopening_another_nodes_directory_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    let (first, _) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
        .await
        .unwrap();
    // A registered peer is not the owner of this node's local directory.
    first
        .cell_manager()
        .register_node(2, "127.0.0.1:1".to_string())
        .unwrap();
    first.shutdown().await.unwrap();
    drop(first);
    let state_path = dir.path().join("cell_1/CELL_MANAGER_STATE");
    let before = std::fs::read(&state_path).unwrap();
    cfg.node.node_id = 2;
    let result = PersistentMetaDbCluster::open_and_serve(cfg).await;
    if let Ok((cluster, _)) = &result {
        cluster.shutdown().await.unwrap();
    }
    assert!(
        result.is_err(),
        "a different node ID silently adopted existing shard state"
    );
    assert_eq!(std::fs::read(state_path).unwrap(), before);
}
