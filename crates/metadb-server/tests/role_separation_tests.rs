//! `global-manager` serves the cell directory and a shared id allocator. It
//! does not host Raft. `cell-manager` hosts one local cell and can publish it.
//! `be` and `proxy` are covered in `be_proxy_tests`. The directory file is not
//! a Raft group. A metadata call whose key is owned by a published cell is
//! forwarded; the in-process router still refuses to apply that key locally.

use metadb_core::ROOT_INODE_ID;
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{
    AllocateIdBlockRequest, CreateFileRequest, GetRoutesRequest, LookupRequest, RegisterCellRequest,
};
use metadb_server::{PersistentMetaDbCluster, ServerRole, ServerTomlConfig};
use tempfile::tempdir;

fn loopback_config(dir: &std::path::Path) -> ServerTomlConfig {
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    cfg
}

#[tokio::test]
async fn all_in_one_does_not_register_with_a_global_manager() {
    let dir = tempdir().unwrap();
    let mut cfg = loopback_config(dir.path());
    cfg.cluster.global_manager = "127.0.0.1:1".into();
    let message = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok(_) => panic!("all-in-one must not register with a global-manager"),
        Err(error) => error.to_string(),
    };
    assert!(message.contains("cell-manager"), "{message}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_manager_publishes_a_cell_and_does_not_host_shards() {
    let global_dir = tempdir().unwrap();
    let mut global_cfg = loopback_config(global_dir.path());
    global_cfg.node.role = ServerRole::GlobalManager;
    global_cfg.node.node_id = 1;
    global_cfg.node.cell_id = 1;
    let (global, global_addr) = PersistentMetaDbCluster::open_and_serve(global_cfg)
        .await
        .unwrap();
    let ready = global.readiness();
    assert!(ready.ready, "{ready:?}");
    assert_eq!(ready.reason, "directory_only");
    assert_eq!(global.metrics_snapshot().shards_total, 0);
    assert!(!global_dir.path().join("be").exists());
    assert!(!global_dir
        .path()
        .join("cell_1")
        .join("CELL_MANAGER_STATE")
        .exists());

    let before = global
        .tx_router()
        .dirstat(ROOT_INODE_ID)
        .await
        .expect_err("an empty directory does not serve metadata");
    assert!(
        before.to_string().contains("published routes")
            || before.to_string().contains("no published route"),
        "{before}"
    );

    let cell_dir = tempdir().unwrap();
    let mut cell_cfg = loopback_config(cell_dir.path());
    cell_cfg.node.role = ServerRole::CellManager;
    cell_cfg.node.node_id = 2;
    cell_cfg.node.cell_id = 2;
    cell_cfg.cluster.cell_start_key = "01".into();
    cell_cfg.cluster.cell_end_key = "02".into();
    cell_cfg.cluster.global_manager = global_addr.clone();
    let (cell, cell_addr) = PersistentMetaDbCluster::open_and_serve(cell_cfg)
        .await
        .unwrap();
    cell.tx_router()
        .dirstat(ROOT_INODE_ID)
        .await
        .expect("the cell-manager still hosts its local meta shard");

    let mut global_cp = ControlPlaneServiceClient::connect(format!("http://{global_addr}"))
        .await
        .unwrap();
    let listed = global_cp
        .get_routes(GetRoutesRequest { cell_id: 2 })
        .await
        .unwrap()
        .into_inner();
    assert!(listed.error.is_none(), "{:?}", listed.error);
    assert_eq!(listed.shards.len(), 1);
    assert_eq!(listed.shards[0].cell_id, 2);
    assert_eq!(global.metrics_snapshot().cells_total, 1);
    assert_eq!(global.metrics_snapshot().shards_total, 0);

    let local_refusal = global
        .tx_router()
        .dirstat(ROOT_INODE_ID)
        .await
        .expect_err("the in-process router does not apply a remote key locally");
    assert!(
        local_refusal.to_string().contains("will not apply"),
        "{local_refusal}"
    );
    let mut meta = MetadataServiceClient::connect(format!("http://{global_addr}"))
        .await
        .unwrap();
    let created = meta
        .create_file(CreateFileRequest {
            header: None,
            pid: ROOT_INODE_ID,
            name: "not-here".into(),
            inode_id: 100,
            fid: 100,
            uid: 1,
            mode: 0o644,
            size_bytes: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        created.error.is_none(),
        "create is forwarded to the published cell: {:?}",
        created.error
    );
    let mut cell_meta = MetadataServiceClient::connect(format!("http://{cell_addr}"))
        .await
        .unwrap();
    let found = cell_meta
        .lookup(LookupRequest {
            header: None,
            pid: ROOT_INODE_ID,
            name: "not-here".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(found.error.is_none(), "{:?}", found.error);
    assert!(found.found, "the forwarded create landed on the cell");
    assert_eq!(global.metrics_snapshot().shards_total, 0);

    let allocated = global_cp
        .allocate_id_block(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 3,
            count: 4,
            preferred_shard_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(allocated.error.is_none(), "{:?}", allocated.error);
    let allocated_again = global_cp
        .allocate_id_block(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 3,
            count: 4,
            preferred_shard_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        allocated_again.error.is_none(),
        "{:?}",
        allocated_again.error
    );
    assert_eq!(
        allocated_again.start_id,
        allocated.start_id + allocated.count
    );

    let mut cell_cp = ControlPlaneServiceClient::connect(format!("http://{cell_addr}"))
        .await
        .unwrap();
    let remote = cell_cp
        .register_cell(RegisterCellRequest {
            cell_id: 3,
            start_key: vec![0x03],
            end_key: vec![0x04],
            cell_manager_endpoints: vec![cell_addr.clone()],
        })
        .await
        .unwrap()
        .into_inner();
    let remote = remote
        .error
        .expect("cell-manager is not the directory")
        .message;
    assert!(remote.contains("does not store a remote cell"), "{remote}");

    global.shutdown().await.unwrap();
    drop(global);
    drop(global_cp);
    let mut reopened_cfg = loopback_config(global_dir.path());
    reopened_cfg.node.role = ServerRole::GlobalManager;
    let (reopened, reopened_addr) = PersistentMetaDbCluster::open_and_serve(reopened_cfg)
        .await
        .unwrap();
    let mut reopened_cp = ControlPlaneServiceClient::connect(format!("http://{reopened_addr}"))
        .await
        .unwrap();
    let again = reopened_cp
        .get_routes(GetRoutesRequest { cell_id: 2 })
        .await
        .unwrap()
        .into_inner();
    assert!(again.error.is_none(), "{:?}", again.error);
    assert_eq!(again.shards.len(), 1);
    assert_eq!(reopened.metrics_snapshot().shards_total, 0);
    let resumed = reopened_cp
        .allocate_id_block(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 3,
            count: 4,
            preferred_shard_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resumed.error.is_none(), "{:?}", resumed.error);
    assert_eq!(
        resumed.start_id,
        allocated_again.start_id + allocated_again.count,
        "restart must not reissue a granted block"
    );

    reopened.shutdown().await.unwrap();
    cell.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cell_manager_without_a_global_address_stays_local() {
    let dir = tempdir().unwrap();
    let mut cfg = loopback_config(dir.path());
    cfg.node.role = ServerRole::CellManager;
    cfg.cluster.cell_start_key = "02".into();
    cfg.cluster.cell_end_key = "03".into();
    let (cell, addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    assert!(cell.readiness().ready);
    assert_ne!(cell.readiness().reason, "directory_only");
    assert!(dir.path().join("be").join("shards").exists());
    let mut client = ControlPlaneServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let listed = client
        .get_routes(GetRoutesRequest { cell_id: 1 })
        .await
        .unwrap()
        .into_inner();
    assert!(listed.error.is_none(), "{:?}", listed.error);
    assert_eq!(listed.shards.len(), 1);
    cell.shutdown().await.unwrap();
}
