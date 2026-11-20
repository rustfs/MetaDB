//! `be` hosts local Raft replicas and serves their metadata.
//! `proxy` forwards metadata and transactions to one upstream.
//! Neither process is a multi-cell router. The proxy does not host Raft.

use std::time::Instant;

use metadb_core::ROOT_INODE_ID;
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::raft_transport_service_client::RaftTransportServiceClient;
use metadb_proto::{
    AllocateIdBlockRequest, CreateFileRequest, GetRoutesRequest, LookupRequest, RaftRpcEnvelope,
    RegisterCellRequest,
};
use metadb_server::{PersistentMetaDbCluster, ServerRole, ServerTomlConfig};
use tempfile::tempdir;

fn loopback(dir: &std::path::Path, role: ServerRole) -> ServerTomlConfig {
    let mut cfg = ServerTomlConfig::default();
    cfg.node.role = role;
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    cfg.sharding.bootstrap_table_shards = false;
    cfg.raft_options.heartbeat_interval_ms = 80;
    cfg.raft_options.election_timeout_min_ms = 200;
    cfg.raft_options.election_timeout_max_ms = 400;
    cfg
}

#[tokio::test]
async fn misconfigured_be_and_proxy_fail_before_listen() {
    let dir = tempdir().unwrap();
    let missing = dir.path().join("absent");
    let mut proxy = loopback(&missing, ServerRole::Proxy);
    proxy.cluster.join = "127.0.0.1:1".into();
    proxy.cluster.upstream = "127.0.0.1:9".into();
    let message = match PersistentMetaDbCluster::open_and_serve(proxy).await {
        Ok(_) => panic!("proxy must not join"),
        Err(error) => error.to_string(),
    };
    assert!(message.contains("does not join"), "{message}");
    assert!(!missing.exists());

    let mut https = loopback(dir.path(), ServerRole::Proxy);
    https.cluster.upstream = "https://127.0.0.1:9".into();
    let message = match PersistentMetaDbCluster::open_and_serve(https).await {
        Ok(_) => panic!("https upstream must fail closed"),
        Err(error) => error.to_string(),
    };
    assert!(message.contains("https"), "{message}");

    let mut publish = loopback(dir.path(), ServerRole::Be);
    publish.cluster.global_manager = "127.0.0.1:9".into();
    let message = match PersistentMetaDbCluster::open_and_serve(publish).await {
        Ok(_) => panic!("be must not publish a cell"),
        Err(error) => error.to_string(),
    };
    assert!(message.contains("does not publish"), "{message}");
    assert!(!dir.path().join("be").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn be_serves_local_shards_and_proxy_forwards_one_upstream() {
    let be_dir = tempdir().unwrap();
    let (be, be_addr) =
        PersistentMetaDbCluster::open_and_serve(loopback(be_dir.path(), ServerRole::Be))
            .await
            .unwrap();
    let ready = be.readiness();
    assert!(ready.ready, "{ready:?}");
    assert_eq!(ready.reason, "ready");
    assert!(be_dir.path().join("be").join("shards").exists());

    let mut be_cp = ControlPlaneServiceClient::connect(format!("http://{be_addr}"))
        .await
        .unwrap();
    let routes = be_cp
        .get_routes(GetRoutesRequest { cell_id: 1 })
        .await
        .unwrap()
        .into_inner();
    assert!(routes.error.is_none(), "{:?}", routes.error);
    assert_eq!(routes.shards.len(), 1);

    let allocated = be_cp
        .allocate_id_block(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 1,
            count: 1,
            preferred_shard_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let allocated = allocated.error.expect("be does not mint ids").message;
    assert!(allocated.contains("does not allocate"), "{allocated}");
    let remote = be_cp
        .register_cell(RegisterCellRequest {
            cell_id: 2,
            start_key: vec![0x02],
            end_key: vec![0x03],
            cell_manager_endpoints: vec![be_addr.clone()],
        })
        .await
        .unwrap()
        .into_inner();
    let remote = remote.error.expect("be is not the directory").message;
    assert!(remote.contains("does not store a remote cell"), "{remote}");

    let proxy_dir = tempdir().unwrap();
    let mut proxy_cfg = loopback(proxy_dir.path(), ServerRole::Proxy);
    proxy_cfg.cluster.upstream = be_addr.clone();
    let (proxy, proxy_addr) = PersistentMetaDbCluster::open_and_serve(proxy_cfg)
        .await
        .unwrap();
    assert_eq!(proxy.readiness().reason, "proxy_gateway");
    assert!(proxy.readiness().ready);
    assert!(!proxy_dir.path().join("be").exists());
    assert!(!proxy_dir
        .path()
        .join("cell_1")
        .join("CELL_MANAGER_STATE")
        .exists());

    let mut proxy_meta = MetadataServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let created = proxy_meta
        .create_file(CreateFileRequest {
            header: None,
            pid: ROOT_INODE_ID,
            name: "via-proxy".into(),
            inode_id: 100,
            fid: 500,
            uid: 7,
            mode: 0o644,
            size_bytes: 4,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(created.error.is_none(), "{:?}", created.error);
    let found = proxy_meta
        .lookup(LookupRequest {
            header: None,
            pid: ROOT_INODE_ID,
            name: "via-proxy".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(found.error.is_none(), "{:?}", found.error);
    assert!(found.found);
    be.tx_router()
        .lookup(ROOT_INODE_ID, "via-proxy")
        .await
        .unwrap()
        .expect("the upstream stored the forwarded create");

    let mut proxy_cp = ControlPlaneServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let forwarded = proxy_cp
        .get_routes(GetRoutesRequest { cell_id: 1 })
        .await
        .unwrap()
        .into_inner();
    assert!(forwarded.error.is_none(), "{:?}", forwarded.error);
    assert_eq!(forwarded.shards.len(), 1);
    let rejected = proxy_cp
        .register_cell(RegisterCellRequest {
            cell_id: 9,
            start_key: vec![0x03],
            end_key: vec![0x04],
            cell_manager_endpoints: vec![proxy_addr.clone()],
        })
        .await
        .unwrap()
        .into_inner();
    let rejected = rejected.error.expect("proxy control plane").message;
    assert!(
        rejected.contains("does not host the control plane"),
        "{rejected}"
    );

    let mut raft = RaftTransportServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let vote = raft
        .append_entries(RaftRpcEnvelope {
            shard_id: 1,
            bincode_payload: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        vote.error_message.contains("does not host raft"),
        "{}",
        vote.error_message
    );

    proxy.shutdown().await.unwrap();
    be.shutdown().await.unwrap();
    drop(be);
    let (reopened, _) =
        PersistentMetaDbCluster::open_and_serve(loopback(be_dir.path(), ServerRole::Be))
            .await
            .unwrap();
    reopened
        .tx_router()
        .lookup(ROOT_INODE_ID, "via-proxy")
        .await
        .unwrap()
        .expect("restart opens the same shard files");
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_reports_a_dead_upstream_without_stopping() {
    let dir = tempdir().unwrap();
    let mut cfg = loopback(dir.path(), ServerRole::Proxy);
    cfg.cluster.upstream = "127.0.0.1:1".into();
    let (proxy, addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let mut meta = MetadataServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let started = Instant::now();
    let failed = meta
        .lookup(LookupRequest {
            header: None,
            pid: ROOT_INODE_ID,
            name: "missing".into(),
        })
        .await;
    assert!(
        started.elapsed().as_secs() < 5,
        "dead upstream hung the proxy"
    );
    let status = failed.expect_err("dead upstream must fail the call");
    assert!(status.to_string().contains("not reachable"), "{status}");
    assert_eq!(proxy.readiness().reason, "proxy_gateway");
    assert!(proxy.readiness().ready);
    proxy.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_be_joins_the_first_shard_host() {
    let dir1 = tempdir().unwrap();
    let (leader, addr) =
        PersistentMetaDbCluster::open_and_serve(loopback(dir1.path(), ServerRole::Be))
            .await
            .unwrap();
    let dir2 = tempdir().unwrap();
    let mut join = loopback(dir2.path(), ServerRole::Be);
    join.node.node_id = 2;
    join.cluster.join = addr;
    let (replica, _) = PersistentMetaDbCluster::open_and_serve(join).await.unwrap();
    let voters: Vec<u64> = replica
        .cell_manager()
        .list_routes()
        .iter()
        .flat_map(|route| route.peers.keys().copied())
        .collect();
    assert!(voters.contains(&1) && voters.contains(&2), "{voters:?}");
    assert!(replica.readiness().ready, "{:?}", replica.readiness());
    replica.shutdown().await.unwrap();
    leader.shutdown().await.unwrap();
}
