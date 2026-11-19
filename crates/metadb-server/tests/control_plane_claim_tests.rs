//! Claim checks for control-plane RPCs that previously reported success without
//! doing the documented work, or that could replace authoritative routes.

use metadb_core::ROOT_INODE_ID;
use metadb_proto::control_plane_service_server::ControlPlaneService;
use metadb_proto::{
    AcquireDirRenameLeaseRequest, GetRoutesRequest, RegisterCellRequest,
    ReleaseDirRenameLeaseRequest, ShardDescriptorProto, ShardHeartbeatStatsProto,
    StoreHeartbeatRequest,
};
use metadb_server::{MetaDbGrpcService, PersistentMetaDbCluster, ServerRole, ServerTomlConfig};
use tempfile::tempdir;
use tonic::Request;

#[tokio::test]
async fn disaggregated_roles_are_rejected_before_listen() {
    for role in [
        ServerRole::Be,
        ServerRole::Proxy,
        ServerRole::CellManager,
        ServerRole::GlobalManager,
    ] {
        let mut cfg = ServerTomlConfig::default();
        cfg.node.role = role;
        cfg.node.data_dir = tempdir().unwrap().path().to_path_buf();
        cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
        let err = match PersistentMetaDbCluster::open_and_serve(cfg).await {
            Ok(_) => panic!("disaggregated role {role:?} must not start"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("not implemented"),
            "role {role:?} error was: {msg}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_plane_does_not_pretend_missing_topology_ops_succeeded() {
    let dir = tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let svc = MetaDbGrpcService::new(
        cluster.global_manager().clone(),
        cluster.tx_router().clone(),
        cluster.proxy_router().clone(),
    );

    let before = cluster.global_manager().meta_cache().all_routes();
    assert!(!before.is_empty());
    let meta_shard = before
        .iter()
        .find(|r| r.range.start_key == vec![0x01])
        .expect("meta table route");

    let heartbeat = svc
        .store_heartbeat(Request::new(StoreHeartbeatRequest {
            cell_id: 1,
            node_id: 99,
            disk_free_bytes: 1,
            shards: vec![ShardHeartbeatStatsProto {
                descriptor: Some(ShardDescriptorProto {
                    cell_id: 1,
                    shard_id: 999,
                    start_key: meta_shard.range.start_key.clone(),
                    end_key: meta_shard.range.end_key.clone(),
                    epoch: Some(meta_shard.epoch.into()),
                    leader_node_id: 99,
                    peers: vec![],
                    dual_mode_fine_grained: false,
                    jumbo_isolated: false,
                    dominant_pid: 0,
                }),
                approximate_bytes: 1,
                qps: 1,
                unsplittable_jumbo: true,
                dominant_pid: 1,
                safe_purge_index: 0,
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    let heartbeat_err = heartbeat.error.expect("heartbeat must not succeed");
    assert!(
        heartbeat_err.message.contains("does not update routes"),
        "{}",
        heartbeat_err.message
    );
    let after = cluster.global_manager().meta_cache().all_routes();
    assert_eq!(after.len(), before.len());
    assert_eq!(
        after
            .iter()
            .find(|r| r.range.start_key == vec![0x01])
            .unwrap()
            .shard_id,
        meta_shard.shard_id
    );
    cluster
        .tx_router()
        .dirstat(ROOT_INODE_ID)
        .await
        .expect("root dirstat must still route after a rejected heartbeat");

    let registered = svc
        .register_cell(Request::new(RegisterCellRequest {
            cell_id: 2,
            start_key: vec![0x01],
            end_key: vec![0x04],
            cell_manager_endpoints: vec!["127.0.0.1:1".into()],
        }))
        .await
        .unwrap()
        .into_inner();
    let register_err = registered.error.expect("register_cell must not succeed");
    assert!(
        register_err.message.contains("not implemented"),
        "{}",
        register_err.message
    );
    assert!(cluster.global_manager().get_cell(2).is_none());
    assert_eq!(cluster.metrics_snapshot().cells_total, 1);

    let unknown = svc
        .get_routes(Request::new(GetRoutesRequest { cell_id: 99 }))
        .await
        .unwrap()
        .into_inner();
    let unknown_err = unknown.error.expect("unknown cell must not look empty");
    assert!(unknown_err.message.contains("unknown cell 99"));
    assert!(unknown.shards.is_empty());

    let known = svc
        .get_routes(Request::new(GetRoutesRequest { cell_id: 1 }))
        .await
        .unwrap()
        .into_inner();
    assert!(known.error.is_none());
    assert!(!known.shards.is_empty());

    let all = svc
        .get_routes(Request::new(GetRoutesRequest { cell_id: 0 }))
        .await
        .unwrap()
        .into_inner();
    assert!(all.error.is_none());
    assert_eq!(all.shards.len(), before.len());

    let lease = svc
        .acquire_dir_rename_lease(Request::new(AcquireDirRenameLeaseRequest {
            request_uuid_be: vec![],
            moved_dir_id: 42,
            ttl_ms: 60_000,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(lease.error.is_none());
    let token = lease.rename_fence_token;
    assert!(token > 0);

    cluster
        .global_manager()
        .id_allocator()
        .testing_fail_next_persists(1);
    let failed_release = svc
        .release_dir_rename_lease(Request::new(ReleaseDirRenameLeaseRequest {
            rename_fence_token: token,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        failed_release.error.is_some(),
        "a failed lease fsync must not be reported as released"
    );
    let blocked = svc
        .acquire_dir_rename_lease(Request::new(AcquireDirRenameLeaseRequest {
            request_uuid_be: vec![],
            moved_dir_id: 43,
            ttl_ms: 60_000,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        blocked.error.is_some(),
        "the unreleased lease must still fence the next acquire"
    );

    let released = svc
        .release_dir_rename_lease(Request::new(ReleaseDirRenameLeaseRequest {
            rename_fence_token: token,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(released.error.is_none(), "{:?}", released.error);

    cluster.shutdown().await.unwrap();
}
