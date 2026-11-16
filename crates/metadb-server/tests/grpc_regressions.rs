use metadb_core::{
    HlcTimestamp, IntentOp, MetaKey, QuotaKey, QuotaRecord, WriteIntentKey, WriteIntentRecord,
};
use metadb_proto::raft_transport_service_client::RaftTransportServiceClient;
use metadb_proto::transaction_service_client::TransactionServiceClient;
use metadb_proto::{
    Prepare2pcRequest, PrepareExpectedValue, RaftRpcEnvelope, RequestRoutingHeader,
};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};

fn config(dir: &std::path::Path) -> ServerTomlConfig {
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn participant_prepare_uses_header_shard_not_primary_shard() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(config(dir.path()))
        .await
        .unwrap();
    let (_, target, _) = cluster.cell_manager().lookup_local_handle(2).unwrap();
    let key = QuotaKey { uid: 42 }.encode().to_vec();
    let primary_key = MetaKey::dir_attr(1).encode();
    let start = cluster.tx_router().clock().now();
    let expire = HlcTimestamp::new(start.physical_ms() + 3000, 0);
    let rec = WriteIntentRecord {
        txn_id: 123,
        primary_shard_hint: 1,
        primary_key: primary_key.clone(),
        start_hlc: start,
        expire_hlc: expire,
        rename_fence_token: None,
        op: IntentOp::Put {
            value: QuotaRecord::new(42, 1000).encode(),
            attr_deltas: vec![],
            aux_specs: vec![],
        },
    };
    let mut client = TransactionServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let response = client
        .prepare2pc(Prepare2pcRequest {
            header: Some(RequestRoutingHeader {
                shard_id: 2,
                expected_epoch: Some(target.current_epoch().into()),
                ..Default::default()
            }),
            txn_id_be: 123u128.to_be_bytes().to_vec(),
            is_primary: false,
            primary_key,
            all_participant_keys: vec![key.clone()],
            start_hlc: start.0,
            expire_hlc: expire.0,
            encoded_intent_keys: vec![WriteIntentKey::new(key.clone()).encode()],
            encoded_intent_records: vec![rec.encode()],
            expected_values: vec![PrepareExpectedValue {
                user_key: key.clone(),
                value: None,
            }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let (_, snap) = target
        .linearizable_read(target.current_epoch())
        .await
        .unwrap();
    let stored = target
        .engine()
        .get_with_snapshot(&WriteIntentKey::new(key).encode(), &snap)
        .unwrap();
    drop(snap);
    cluster.shutdown().await.unwrap();
    assert!(
        response.error.is_none(),
        "participant prepare failed: {:?}",
        response.error
    );
    assert!(
        stored.is_some(),
        "participant intent missing on header shard"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mismatched_prepare_vectors_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(config(dir.path()))
        .await
        .unwrap();
    let (_, target, _) = cluster.cell_manager().lookup_local_handle(1).unwrap();
    let start = cluster.tx_router().clock().now();
    let key = MetaKey::dir_attr(1).encode();
    let mut client = TransactionServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let result = client
        .prepare2pc(Prepare2pcRequest {
            header: Some(RequestRoutingHeader {
                shard_id: 1,
                expected_epoch: Some(target.current_epoch().into()),
                ..Default::default()
            }),
            txn_id_be: 123u128.to_be_bytes().to_vec(),
            primary_key: key.clone(),
            start_hlc: start.0,
            expire_hlc: HlcTimestamp::new(start.physical_ms() + 3000, 0).0,
            encoded_intent_keys: vec![WriteIntentKey::new(key).encode()],
            ..Default::default()
        })
        .await;
    cluster.shutdown().await.unwrap();
    assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_secret_protects_raft_transport() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = config(dir.path());
    cfg.security.shared_secret = "test-secret".into();
    let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = RaftTransportServiceClient::new(channel.clone());
    let result = client
        .vote(RaftRpcEnvelope {
            shard_id: 99999,
            bincode_payload: vec![],
        })
        .await;
    let mut authenticated = RaftTransportServiceClient::with_interceptor(
        channel,
        metadb_server::BearerAuthClient::new("test-secret"),
    );
    let allowed = authenticated
        .vote(RaftRpcEnvelope {
            shard_id: 99999,
            bincode_payload: vec![],
        })
        .await;
    cluster.shutdown().await.unwrap();
    assert!(
        allowed.is_ok(),
        "authenticated peer was rejected: {allowed:?}"
    );
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
}

#[test]
fn outbound_token_adds_authorization_header() {
    use tonic::service::Interceptor;
    let mut token = metadb_server::BearerAuthClient::new("test-secret");
    let request = token.call(tonic::Request::new(())).unwrap();
    assert_eq!(
        request.metadata().get("authorization").unwrap(),
        "Bearer test-secret"
    );
    assert!(metadb_server::BearerAuth::new("test-secret")
        .call(request)
        .is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_cell_registration_does_not_report_success() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(config(dir.path()))
        .await
        .unwrap();
    let mut client =
        metadb_proto::control_plane_service_client::ControlPlaneServiceClient::connect(format!(
            "http://{addr}"
        ))
        .await
        .unwrap();
    let result = client
        .register_cell(metadb_proto::RegisterCellRequest::default())
        .await;
    cluster.shutdown().await.unwrap();
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unimplemented);
}
