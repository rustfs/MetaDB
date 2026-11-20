//! Metadata and transaction calls follow the cell that owns the key.
//! A shared id allocator on `global-manager` grants disjoint blocks.
//!
//! A bootstrap leader also stores a directory snapshot on its lowest shard log.
//! A `global-manager` with no shard group keeps only the local file.
//! This is not leader transfer, not a jumbo file move, and not a heartbeat scheduler.

use std::fs;

use metadb_core::ROOT_INODE_ID;
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{
    AllocateIdBlockRequest, CreateFileRequest, GetQuotaRequest, LookupRequest, RegisterCellRequest,
    SetQuotaLimitRequest,
};
use metadb_server::{PersistentMetaDbCluster, ServerRole, ServerTomlConfig};
use tonic::Request;

fn config(dir: &std::path::Path, node_id: u64, cell_id: u64) -> ServerTomlConfig {
    let mut cfg = ServerTomlConfig::default();
    cfg.node.node_id = node_id;
    cfg.node.cell_id = cell_id;
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.shared_secret = "cell-secret".into();
    cfg
}

fn bearer<T>(inner: T) -> Request<T> {
    let mut request = Request::new(inner);
    request
        .metadata_mut()
        .insert("authorization", "Bearer cell-secret".parse().unwrap());
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_key_stays_local_and_remote_key_is_forwarded_or_fails() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let mut meta_cfg = config(dir1.path(), 1, 1);
    meta_cfg.cluster.cell_start_key = "01".into();
    meta_cfg.cluster.cell_end_key = "02".into();
    let mut quota_cfg = config(dir2.path(), 2, 2);
    quota_cfg.cluster.cell_start_key = "02".into();
    quota_cfg.cluster.cell_end_key = "03".into();
    let (cell1, addr1) = PersistentMetaDbCluster::open_and_serve(meta_cfg)
        .await
        .unwrap();
    let (cell2, addr2) = PersistentMetaDbCluster::open_and_serve(quota_cfg)
        .await
        .unwrap();

    let mut cp = ControlPlaneServiceClient::connect(format!("http://{addr1}"))
        .await
        .unwrap();
    let registered = cp
        .register_cell(bearer(RegisterCellRequest {
            cell_id: 2,
            start_key: vec![0x02],
            end_key: vec![0x03],
            cell_manager_endpoints: vec![addr2.clone()],
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(registered.error.is_none(), "{:?}", registered.error);

    let mut meta = MetadataServiceClient::connect(format!("http://{addr1}"))
        .await
        .unwrap();
    let created = meta
        .create_file(bearer(CreateFileRequest {
            pid: ROOT_INODE_ID,
            name: "local-file".into(),
            inode_id: 100,
            fid: 100,
            uid: 1,
            mode: 0o644,
            size_bytes: 0,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(created.error.is_none(), "{:?}", created.error);
    let found = meta
        .lookup(bearer(LookupRequest {
            pid: ROOT_INODE_ID,
            name: "local-file".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(found.found, "a meta key stays on the local cell");

    let written = meta
        .set_quota_limit(bearer(SetQuotaLimitRequest {
            uid: 9,
            quota_limit_bytes: 64,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        written.error.is_none(),
        "quota is forwarded to cell 2: {:?}",
        written.error
    );
    let mut quota = MetadataServiceClient::connect(format!("http://{addr2}"))
        .await
        .unwrap();
    let remote = quota
        .get_quota(bearer(GetQuotaRequest {
            uid: 9,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(remote.found, "the forwarded quota write landed on cell 2");
    assert_eq!(remote.record.expect("quota row").quota_limit_bytes, 64);

    cell2.shutdown().await.unwrap();
    drop(cell2);
    drop(quota);
    let down = meta
        .set_quota_limit(bearer(SetQuotaLimitRequest {
            uid: 9,
            quota_limit_bytes: 128,
            ..Default::default()
        }))
        .await
        .expect_err("a dead remote cell fails the call");
    let down = down.to_string();
    assert!(
        down.contains("not reachable") && down.contains("did not apply"),
        "{down}"
    );

    let (cell2, addr2) = PersistentMetaDbCluster::open_and_serve({
        let mut cfg = config(dir2.path(), 2, 2);
        cfg.cluster.cell_start_key = "02".into();
        cfg.cluster.cell_end_key = "03".into();
        cfg
    })
    .await
    .unwrap();
    let mut quota = MetadataServiceClient::connect(format!("http://{addr2}"))
        .await
        .unwrap();
    let kept = quota
        .get_quota(bearer(GetQuotaRequest {
            uid: 9,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        kept.record.expect("original quota").quota_limit_bytes,
        64,
        "the failed forward must not replace the value"
    );
    cell2.shutdown().await.unwrap();
    cell1.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_cells_receive_disjoint_blocks_from_the_shared_allocator() {
    let global_dir = tempfile::tempdir().unwrap();
    let mut global_cfg = config(global_dir.path(), 1, 1);
    global_cfg.node.role = ServerRole::GlobalManager;
    let (global, global_addr) = PersistentMetaDbCluster::open_and_serve(global_cfg)
        .await
        .unwrap();

    let cell_a_dir = tempfile::tempdir().unwrap();
    let mut cell_a_cfg = config(cell_a_dir.path(), 2, 2);
    cell_a_cfg.node.role = ServerRole::CellManager;
    cell_a_cfg.cluster.cell_start_key = "01".into();
    cell_a_cfg.cluster.cell_end_key = "02".into();
    cell_a_cfg.cluster.global_manager = global_addr.clone();
    let (cell_a, addr_a) = PersistentMetaDbCluster::open_and_serve(cell_a_cfg)
        .await
        .unwrap();

    let cell_b_dir = tempfile::tempdir().unwrap();
    let mut cell_b_cfg = config(cell_b_dir.path(), 3, 3);
    cell_b_cfg.node.role = ServerRole::CellManager;
    cell_b_cfg.cluster.cell_start_key = "02".into();
    cell_b_cfg.cluster.cell_end_key = "03".into();
    cell_b_cfg.cluster.global_manager = global_addr.clone();
    let (cell_b, addr_b) = PersistentMetaDbCluster::open_and_serve(cell_b_cfg)
        .await
        .unwrap();

    let mut client_a = ControlPlaneServiceClient::connect(format!("http://{addr_a}"))
        .await
        .unwrap();
    let mut client_b = ControlPlaneServiceClient::connect(format!("http://{addr_b}"))
        .await
        .unwrap();
    let grant_a = client_a
        .allocate_id_block(bearer(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 3,
            count: 4,
            preferred_shard_id: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(grant_a.error.is_none(), "{:?}", grant_a.error);
    let grant_b = client_b
        .allocate_id_block(bearer(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 3,
            count: 4,
            preferred_shard_id: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(grant_b.error.is_none(), "{:?}", grant_b.error);
    assert_ne!(grant_a.start_id, grant_b.start_id);
    assert!(
        grant_a.start_id + grant_a.count <= grant_b.start_id
            || grant_b.start_id + grant_b.count <= grant_a.start_id,
        "cell blocks overlap: {grant_a:?} {grant_b:?}"
    );

    let local_allocator = cell_a_dir
        .path()
        .join("global_allocator")
        .join("ID_ALLOCATOR_STATE");
    let before = fs::read(&local_allocator).unwrap();
    global.shutdown().await.unwrap();
    drop(global);
    let down = client_a
        .allocate_id_block(bearer(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 3,
            count: 4,
            preferred_shard_id: 0,
        }))
        .await
        .expect_err("a down allocator is not replaced by a local grant");
    let down = down.to_string();
    assert!(down.contains("did not grant a local block"), "{down}");
    assert_eq!(
        fs::read(&local_allocator).unwrap(),
        before,
        "the cell allocator file must not record a fallback grant"
    );

    let mut reopened_cfg = config(global_dir.path(), 1, 1);
    reopened_cfg.node.role = ServerRole::GlobalManager;
    let (reopened, reopened_addr) = PersistentMetaDbCluster::open_and_serve(reopened_cfg)
        .await
        .unwrap();
    let mut reopened_cp = ControlPlaneServiceClient::connect(format!("http://{reopened_addr}"))
        .await
        .unwrap();
    let resumed = reopened_cp
        .allocate_id_block(bearer(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 3,
            count: 4,
            preferred_shard_id: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(resumed.error.is_none(), "{:?}", resumed.error);
    let previous_end = (grant_a.start_id + grant_a.count).max(grant_b.start_id + grant_b.count);
    assert!(
        resumed.start_id >= previous_end,
        "restart reissued a granted block: resumed {} previous_end {previous_end}",
        resumed.start_id
    );

    reopened.shutdown().await.unwrap();
    cell_a.shutdown().await.unwrap();
    cell_b.shutdown().await.unwrap();
}
