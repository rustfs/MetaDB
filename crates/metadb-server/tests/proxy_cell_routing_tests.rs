//! The proxy dials a registered cell when that cell contains the request.
//! A missing directory, a partial overlap, or a split across cells fails closed.
//! The proxy does not apply the request locally and does not invent a placement.
//! `CommitPrimary2pc` has no user key and stays on the configured upstream.
//! This is not a MetaCache of every shard and not a disaggregated deployment.

use metadb_core::{MetaKey, QuotaKey, SecondaryIndexKey, ROOT_INODE_ID};
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::transaction_service_client::TransactionServiceClient;
use metadb_proto::{
    CommitPrimary2pcRequest, CreateFileRequest, GetQuotaRequest, GetRoutesRequest,
    ListQuotasRequest, LookupRequest, Prepare2pcRequest, RegisterCellRequest, RequestRoutingHeader,
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

fn forwarded<T>(inner: T) -> Request<T> {
    let mut request = bearer(inner);
    request
        .metadata_mut()
        .insert("x-metadb-cell-forwarded", "1".parse().unwrap());
    request
}

fn create(name: &str, inode_id: u64) -> CreateFileRequest {
    CreateFileRequest {
        header: None,
        pid: ROOT_INODE_ID,
        name: name.into(),
        inode_id,
        fid: inode_id,
        uid: 7,
        mode: 0o644,
        size_bytes: 1,
    }
}

async fn register(addr: &str, cell_id: u64, start: u8, end: u8, endpoint: &str) -> String {
    let mut client = ControlPlaneServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let response = client
        .register_cell(bearer(RegisterCellRequest {
            cell_id,
            start_key: vec![start],
            end_key: vec![end],
            cell_manager_endpoints: vec![endpoint.to_string()],
        }))
        .await
        .unwrap()
        .into_inner();
    response
        .error
        .map(|error| error.message)
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_dials_the_cell_that_contains_the_request() {
    let decoy_dir = tempfile::tempdir().unwrap();
    let mut decoy_cfg = config(decoy_dir.path(), 4, 4);
    decoy_cfg.node.role = ServerRole::Be;
    decoy_cfg.sharding.bootstrap_table_shards = false;
    let (decoy, decoy_addr) = PersistentMetaDbCluster::open_and_serve(decoy_cfg)
        .await
        .unwrap();

    let meta_dir = tempfile::tempdir().unwrap();
    let mut meta_cfg = config(meta_dir.path(), 2, 2);
    meta_cfg.cluster.cell_start_key = "01".into();
    meta_cfg.cluster.cell_end_key = "02".into();
    let (meta, meta_addr) = PersistentMetaDbCluster::open_and_serve(meta_cfg)
        .await
        .unwrap();

    let quota_dir = tempfile::tempdir().unwrap();
    let mut quota_cfg = config(quota_dir.path(), 3, 3);
    quota_cfg.cluster.cell_start_key = "02".into();
    quota_cfg.cluster.cell_end_key = "03".into();
    let (quota, quota_addr) = PersistentMetaDbCluster::open_and_serve(quota_cfg)
        .await
        .unwrap();

    let proxy_dir = tempfile::tempdir().unwrap();
    let mut proxy_cfg = config(proxy_dir.path(), 5, 5);
    proxy_cfg.node.role = ServerRole::Proxy;
    proxy_cfg.cluster.upstream = decoy_addr.clone();
    let (proxy, proxy_addr) = PersistentMetaDbCluster::open_and_serve(proxy_cfg)
        .await
        .unwrap();
    assert_eq!(proxy.readiness().reason, "proxy_gateway");

    let missing = register(&proxy_addr, 2, 0x01, 0x04, &meta_addr).await;
    assert!(
        missing.contains("cover") || missing.contains("end at") || missing.contains("partial"),
        "a cell that does not cover the requested range is not stored: {missing}"
    );
    assert!(!proxy_dir.path().join("GLOBAL_CELL_DIRECTORY").exists());

    let registered = register(&proxy_addr, 2, 0x01, 0x02, &meta_addr).await;
    assert!(registered.is_empty(), "{registered}");
    let registered = register(&proxy_addr, 3, 0x02, 0x03, &quota_addr).await;
    assert!(registered.is_empty(), "{registered}");
    let overlap = register(&proxy_addr, 9, 0x01, 0x03, &meta_addr).await;
    assert!(overlap.contains("overlaps"), "{overlap}");

    let mut proxy_meta = MetadataServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let created = proxy_meta
        .create_file(bearer(create("owned", 100)))
        .await
        .unwrap()
        .into_inner();
    assert!(created.error.is_none(), "{:?}", created.error);

    let mut meta_client = MetadataServiceClient::connect(format!("http://{meta_addr}"))
        .await
        .unwrap();
    let on_meta = meta_client
        .lookup(bearer(LookupRequest {
            pid: ROOT_INODE_ID,
            name: "owned".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(on_meta.found, "the create landed on the meta cell");
    let mut quota_client = MetadataServiceClient::connect(format!("http://{quota_addr}"))
        .await
        .unwrap();
    let not_on_quota = quota_client
        .lookup(bearer(LookupRequest {
            pid: ROOT_INODE_ID,
            name: "owned".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        not_on_quota.error.is_some() || !not_on_quota.found,
        "the quota cell does not own the meta key: {not_on_quota:?}"
    );
    assert!(
        decoy
            .tx_router()
            .lookup(ROOT_INODE_ID, "owned")
            .await
            .unwrap()
            .is_none(),
        "the configured upstream is not a guessed placement"
    );

    let written = proxy_meta
        .set_quota_limit(bearer(SetQuotaLimitRequest {
            uid: 9,
            quota_limit_bytes: 64,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(written.error.is_none(), "{:?}", written.error);
    let remote_quota = quota_client
        .get_quota(bearer(GetQuotaRequest {
            uid: 9,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(
        remote_quota.found,
        "the quota write landed on the quota cell"
    );
    assert_eq!(
        remote_quota.record.expect("quota row").quota_limit_bytes,
        64
    );
    let listed = proxy_meta
        .list_quotas(bearer(ListQuotasRequest {
            start_uid: 0,
            limit: 10,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(listed.error.is_none(), "{:?}", listed.error);
    assert_eq!(listed.records.len(), 1);

    let mut proxy_tx = TransactionServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let split = proxy_tx
        .prepare2pc(bearer(Prepare2pcRequest {
            primary_key: MetaKey::child(ROOT_INODE_ID, "split").encode(),
            all_participant_keys: vec![QuotaKey { uid: 9 }.encode()],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let split = split.error.expect("keys in two cells").message;
    assert!(split.contains("more than one remote cell"), "{split}");
    assert!(
        !meta_client
            .lookup(bearer(LookupRequest {
                pid: ROOT_INODE_ID,
                name: "split".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .found
    );

    let marked = proxy_meta
        .create_file(forwarded(create("marked", 101)))
        .await
        .unwrap()
        .into_inner();
    let marked = marked.error.expect("already forwarded").message;
    assert!(marked.contains("already cell-forwarded"), "{marked}");
    assert!(
        !meta_client
            .lookup(bearer(LookupRequest {
                pid: ROOT_INODE_ID,
                name: "marked".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .found
    );

    meta.shutdown().await.unwrap();
    drop(meta);
    drop(meta_client);
    let down = proxy_meta
        .create_file(bearer(create("while-down", 102)))
        .await
        .expect_err("a dead cell fails the call");
    let down = down.to_string();
    assert!(
        down.contains("not reachable") && down.contains("did not apply"),
        "{down}"
    );

    let (meta, meta_addr) = PersistentMetaDbCluster::open_and_serve({
        let mut cfg = config(meta_dir.path(), 2, 2);
        cfg.cluster.cell_start_key = "01".into();
        cfg.cluster.cell_end_key = "02".into();
        cfg
    })
    .await
    .unwrap();
    let mut meta_client = MetadataServiceClient::connect(format!("http://{meta_addr}"))
        .await
        .unwrap();
    assert!(
        meta_client
            .lookup(bearer(LookupRequest {
                pid: ROOT_INODE_ID,
                name: "owned".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .found
    );
    assert!(
        !meta_client
            .lookup(bearer(LookupRequest {
                pid: ROOT_INODE_ID,
                name: "while-down".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .found
    );

    proxy.shutdown().await.unwrap();
    quota.shutdown().await.unwrap();
    decoy.shutdown().await.unwrap();
    meta.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_fails_closed_when_the_directory_is_missing_or_incomplete() {
    let decoy_dir = tempfile::tempdir().unwrap();
    let mut decoy_cfg = config(decoy_dir.path(), 4, 4);
    decoy_cfg.node.role = ServerRole::Be;
    let (decoy, decoy_addr) = PersistentMetaDbCluster::open_and_serve(decoy_cfg)
        .await
        .unwrap();
    let meta_dir = tempfile::tempdir().unwrap();
    let mut meta_cfg = config(meta_dir.path(), 2, 2);
    meta_cfg.cluster.cell_start_key = "01".into();
    meta_cfg.cluster.cell_end_key = "02".into();
    let (meta, meta_addr) = PersistentMetaDbCluster::open_and_serve(meta_cfg)
        .await
        .unwrap();
    let quota_dir = tempfile::tempdir().unwrap();
    let mut quota_cfg = config(quota_dir.path(), 3, 3);
    quota_cfg.cluster.cell_start_key = "02".into();
    quota_cfg.cluster.cell_end_key = "03".into();
    let (quota, quota_addr) = PersistentMetaDbCluster::open_and_serve(quota_cfg)
        .await
        .unwrap();

    let proxy_dir = tempfile::tempdir().unwrap();
    let mut proxy_cfg = config(proxy_dir.path(), 5, 5);
    proxy_cfg.node.role = ServerRole::Proxy;
    proxy_cfg.cluster.upstream = decoy_addr.clone();
    let (proxy, proxy_addr) = PersistentMetaDbCluster::open_and_serve(proxy_cfg)
        .await
        .unwrap();
    let mut proxy_meta = MetadataServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let missing = proxy_meta
        .create_file(bearer(create("nowhere", 100)))
        .await
        .unwrap()
        .into_inner();
    let missing = missing.error.expect("missing directory").message;
    assert!(
        missing.contains("cell directory is missing") && missing.contains("invent a placement"),
        "{missing}"
    );
    assert!(decoy
        .tx_router()
        .lookup(ROOT_INODE_ID, "nowhere")
        .await
        .unwrap()
        .is_none());

    let registered = register(&proxy_addr, 2, 0x01, 0x02, &meta_addr).await;
    assert!(registered.is_empty(), "{registered}");
    let quota_write = proxy_meta
        .set_quota_limit(bearer(SetQuotaLimitRequest {
            uid: 4,
            quota_limit_bytes: 8,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let quota_write = quota_write.error.expect("quota cell is absent").message;
    assert!(
        quota_write.contains("does not contain this request"),
        "{quota_write}"
    );
    let mut quota_client = MetadataServiceClient::connect(format!("http://{quota_addr}"))
        .await
        .unwrap();
    let absent = quota_client
        .get_quota(bearer(GetQuotaRequest {
            uid: 4,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!absent.found, "an unplaced quota write must not land");

    let listed = proxy_meta
        .list_quotas(bearer(ListQuotasRequest {
            start_uid: 0,
            limit: 10,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let listed = listed
        .error
        .expect("range is outside the meta cell")
        .message;
    assert!(listed.contains("does not contain this request"), "{listed}");

    let mut proxy_tx = TransactionServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let split = proxy_tx
        .prepare2pc(bearer(Prepare2pcRequest {
            primary_key: MetaKey::child(ROOT_INODE_ID, "meta-half").encode(),
            all_participant_keys: vec![SecondaryIndexKey { fid: 1 }.encode()],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let split = split
        .error
        .expect("key outside the registered cell")
        .message;
    assert!(split.contains("split between"), "{split}");
    assert!(meta
        .tx_router()
        .lookup(ROOT_INODE_ID, "meta-half")
        .await
        .unwrap()
        .is_none());

    proxy.shutdown().await.unwrap();
    quota.shutdown().await.unwrap();
    meta.shutdown().await.unwrap();
    decoy.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_marks_the_request_so_the_receiver_does_not_forward_again() {
    let meta_dir = tempfile::tempdir().unwrap();
    let mut meta_cfg = config(meta_dir.path(), 2, 2);
    meta_cfg.cluster.cell_start_key = "01".into();
    meta_cfg.cluster.cell_end_key = "02".into();
    let (meta, meta_addr) = PersistentMetaDbCluster::open_and_serve(meta_cfg)
        .await
        .unwrap();

    let global_dir = tempfile::tempdir().unwrap();
    let mut global_cfg = config(global_dir.path(), 1, 1);
    global_cfg.node.role = ServerRole::GlobalManager;
    let (global, global_addr) = PersistentMetaDbCluster::open_and_serve(global_cfg)
        .await
        .unwrap();
    let registered = register(&global_addr, 2, 0x01, 0x02, &meta_addr).await;
    assert!(registered.is_empty(), "{registered}");

    let decoy_dir = tempfile::tempdir().unwrap();
    let mut decoy_cfg = config(decoy_dir.path(), 4, 4);
    decoy_cfg.node.role = ServerRole::Be;
    let (decoy, decoy_addr) = PersistentMetaDbCluster::open_and_serve(decoy_cfg)
        .await
        .unwrap();

    let proxy_dir = tempfile::tempdir().unwrap();
    let mut proxy_cfg = config(proxy_dir.path(), 5, 5);
    proxy_cfg.node.role = ServerRole::Proxy;
    proxy_cfg.cluster.upstream = decoy_addr.clone();
    let (proxy, proxy_addr) = PersistentMetaDbCluster::open_and_serve(proxy_cfg)
        .await
        .unwrap();
    let registered = register(&proxy_addr, 2, 0x01, 0x02, &global_addr).await;
    assert!(
        registered.is_empty(),
        "the proxy stores the forwarder as the cell endpoint: {registered}"
    );

    let mut forwarder = MetadataServiceClient::connect(format!("http://{global_addr}"))
        .await
        .unwrap();
    let direct = forwarder
        .create_file(bearer(create("direct", 50)))
        .await
        .unwrap()
        .into_inner();
    assert!(
        direct.error.is_none(),
        "without the marker the forwarder writes on the owning cell: {direct:?}"
    );

    let mut proxy_meta = MetadataServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    // The forwarder keeps a marked request and retries the local routing error
    // for 30s. The proxy's dial timeout fires first.
    let created = proxy_meta.create_file(bearer(create("once", 100))).await;
    let status = created
        .expect_err("the marked request is not forwarded again")
        .to_string();
    assert!(status.contains("Timeout expired"), "{status}");

    let mut meta_client = MetadataServiceClient::connect(format!("http://{meta_addr}"))
        .await
        .unwrap();
    assert!(
        meta_client
            .lookup(bearer(LookupRequest {
                pid: ROOT_INODE_ID,
                name: "direct".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .found,
        "the unmarked forward landed on the owning cell"
    );
    assert!(
        !meta_client
            .lookup(bearer(LookupRequest {
                pid: ROOT_INODE_ID,
                name: "once".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .found,
        "a second hop would have written the file on the owning cell"
    );
    assert!(decoy
        .tx_router()
        .lookup(ROOT_INODE_ID, "once")
        .await
        .unwrap()
        .is_none());

    proxy.shutdown().await.unwrap();
    global.shutdown().await.unwrap();
    decoy.shutdown().await.unwrap();
    meta.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_primary_without_a_user_key_stays_on_the_upstream() {
    let be_dir = tempfile::tempdir().unwrap();
    let mut be_cfg = config(be_dir.path(), 2, 2);
    be_cfg.node.role = ServerRole::Be;
    be_cfg.sharding.bootstrap_table_shards = false;
    let (be, be_addr) = PersistentMetaDbCluster::open_and_serve(be_cfg)
        .await
        .unwrap();

    let proxy_dir = tempfile::tempdir().unwrap();
    let mut proxy_cfg = config(proxy_dir.path(), 5, 5);
    proxy_cfg.node.role = ServerRole::Proxy;
    proxy_cfg.cluster.upstream = be_addr.clone();
    let (proxy, proxy_addr) = PersistentMetaDbCluster::open_and_serve(proxy_cfg)
        .await
        .unwrap();

    let mut meta = MetadataServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let blocked = meta
        .create_file(bearer(create("not-placed", 100)))
        .await
        .unwrap()
        .into_inner();
    let blocked = blocked.error.expect("no directory").message;
    assert!(blocked.contains("cell directory is missing"), "{blocked}");
    assert!(be
        .tx_router()
        .lookup(ROOT_INODE_ID, "not-placed")
        .await
        .unwrap()
        .is_none());

    let mut routes = ControlPlaneServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let routes = routes
        .get_routes(bearer(GetRoutesRequest { cell_id: 2 }))
        .await
        .unwrap()
        .into_inner();
    assert!(routes.error.is_none(), "{:?}", routes.error);
    let shard = routes.shards.first().expect("upstream shard");

    let mut tx = TransactionServiceClient::connect(format!("http://{proxy_addr}"))
        .await
        .unwrap();
    let committed = tx
        .commit_primary2pc(bearer(CommitPrimary2pcRequest {
            header: Some(RequestRoutingHeader {
                shard_id: shard.shard_id,
                expected_epoch: shard.epoch,
                caller_hlc: 0,
            }),
            txn_id_be: 7u128.to_be_bytes().to_vec(),
            commit_hlc: 1,
            has_rename_fence_token: false,
            rename_fence_token: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    if let Some(error) = committed.error {
        assert!(
            !error.message.contains("cell directory")
                && !error.message.contains("invent a placement")
                && !error.message.contains("unknown shard"),
            "CommitPrimary2pc left the shard host: {}",
            error.message
        );
    }

    proxy.shutdown().await.unwrap();
    be.shutdown().await.unwrap();
}
