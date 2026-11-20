//! Focused TLS and mTLS checks for process startup.
//!
//! These tests do not skip certificate or hostname verification.

use std::io::Write;
use std::net::IpAddr;
use std::time::Duration;

use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::GetRoutesRequest;
use metadb_security::testing::{IssueRequest, TestCa};
use metadb_security::{TlsSpec, TransportSecurity};
use metadb_server::{BearerAuthClient, PersistentMetaDbCluster, ServerTomlConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

fn write_pem(dir: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(body.as_bytes()).unwrap();
    path
}

struct Material {
    ca: std::path::PathBuf,
    server_cert: std::path::PathBuf,
    server_key: std::path::PathBuf,
    client_cert: std::path::PathBuf,
    client_key: std::path::PathBuf,
}

fn material(dir: &std::path::Path) -> Material {
    let mut ca = TestCa::generate().unwrap();
    let server = ca
        .issue(IssueRequest::valid_for(
            Vec::new(),
            vec!["127.0.0.1".parse::<IpAddr>().unwrap()],
            vec!["spiffe://metadb/node/server".into()],
        ))
        .unwrap();
    let client = ca
        .issue(IssueRequest::valid_for(
            Vec::new(),
            Vec::new(),
            vec!["spiffe://metadb/node/client".into()],
        ))
        .unwrap();
    Material {
        ca: write_pem(dir, "ca.pem", ca.cert_pem()),
        server_cert: write_pem(dir, "server.pem", &server.cert_pem),
        server_key: write_pem(dir, "server.key", &server.key_pem),
        client_cert: write_pem(dir, "client.pem", &client.cert_pem),
        client_key: write_pem(dir, "client.key", &client.key_pem),
    }
}

fn apply_mtls(cfg: &mut ServerTomlConfig, files: &Material) {
    cfg.security.tls.mode = "mtls".into();
    cfg.security.tls.cert_file = files.server_cert.display().to_string();
    cfg.security.tls.key_file = files.server_key.display().to_string();
    cfg.security.tls.ca_file = files.ca.display().to_string();
    cfg.security.tls.server_name = "127.0.0.1".into();
}

async fn rpc_client(
    addr: &str,
    files: &Material,
    bearer: &str,
) -> ControlPlaneServiceClient<InterceptedService<Channel, BearerAuthClient>> {
    let security = TransportSecurity::load(
        &TlsSpec::from_client_fields(
            "mtls",
            &files.client_cert.display().to_string(),
            &files.client_key.display().to_string(),
            &files.ca.display().to_string(),
        )
        .unwrap(),
    )
    .unwrap();
    let url = security.prepare_url(addr).unwrap();
    let endpoint = Endpoint::from_shared(url)
        .unwrap()
        .connect_timeout(Duration::from_secs(3));
    let channel = security.connect(endpoint).await.unwrap();
    ControlPlaneServiceClient::with_interceptor(channel, BearerAuthClient::new(bearer))
}

#[tokio::test]
async fn missing_tls_material_does_not_create_a_data_directory() {
    let parent = tempfile::tempdir().unwrap();
    let data = parent.path().join("not-created");
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = data.clone();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.shared_secret = "test-secret".into();
    cfg.security.tls.mode = "mtls".into();
    cfg.security.tls.cert_file = parent.path().join("missing.pem").display().to_string();
    cfg.security.tls.key_file = parent.path().join("missing.key").display().to_string();
    cfg.security.tls.ca_file = parent.path().join("missing-ca.pem").display().to_string();
    cfg.security.tls.server_name = "127.0.0.1".into();
    let error = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok(_) => panic!("incomplete TLS material must not serve"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("refusing to start"), "{error}");
    assert!(!data.exists(), "TLS refusal must not create {data:?}");
    assert!(!error.contains("BEGIN"));
}

#[tokio::test]
async fn wildcard_listen_with_tls_does_not_create_a_data_directory() {
    let parent = tempfile::tempdir().unwrap();
    let files = material(parent.path());
    let data = parent.path().join("not-created");
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = data.clone();
    cfg.node.grpc_listen_addr = "0.0.0.0:7800".into();
    cfg.security.shared_secret = "test-secret".into();
    apply_mtls(&mut cfg, &files);
    let error = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok(_) => panic!("wildcard TLS bind must not serve"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("wildcard"), "{error}");
    assert!(!data.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_serves_grpc_and_metrics_and_keeps_the_bearer_check() {
    let parent = tempfile::tempdir().unwrap();
    let files = material(parent.path());
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = parent.path().join("data");
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.node.metrics_listen_addr = Some("127.0.0.1:0".into());
    cfg.security.shared_secret = "test-secret".into();
    apply_mtls(&mut cfg, &files);
    let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let metrics_addr = cluster.metrics_addr().unwrap().to_string();

    let plaintext_accepted =
        match ControlPlaneServiceClient::connect(format!("http://{grpc_addr}")).await {
            Ok(mut client) => client
                .get_routes(GetRoutesRequest { cell_id: 1 })
                .await
                .is_ok(),
            Err(_) => false,
        };
    assert!(
        !plaintext_accepted,
        "plaintext gRPC must not complete an RPC while mTLS is enabled"
    );

    let mut authed = rpc_client(&grpc_addr, &files, "test-secret").await;
    let routes = authed
        .get_routes(GetRoutesRequest { cell_id: 1 })
        .await
        .unwrap()
        .into_inner();
    assert!(routes.error.is_none(), "{routes:?}");

    let mut anonymous = rpc_client(&grpc_addr, &files, "").await;
    let denied = anonymous
        .get_routes(GetRoutesRequest { cell_id: 1 })
        .await
        .expect_err("a valid client certificate is not a bearer or a role");
    assert_eq!(denied.code(), tonic::Code::Unauthenticated);

    let security = TransportSecurity::load(
        &TlsSpec::from_client_fields(
            "mtls",
            &files.client_cert.display().to_string(),
            &files.client_key.display().to_string(),
            &files.ca.display().to_string(),
        )
        .unwrap(),
    )
    .unwrap();
    let (status, body) = metrics_get(&security, &metrics_addr, Some("Bearer test-secret")).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("ready"), "{body}");
    let (denied_status, denied_body) = metrics_get(&security, &metrics_addr, None).await;
    assert_eq!(denied_status, 401, "{denied_body}");
    assert!(!denied_body.contains("test-secret"), "{denied_body}");
    assert!(!denied_body.contains("metadb_"), "{denied_body}");
}

async fn metrics_get(
    security: &TransportSecurity,
    addr: &str,
    authorization: Option<&str>,
) -> (u16, String) {
    let mut socket = security.connect_http(addr).await.unwrap();
    let mut request = format!("GET /readyz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    if let Some(value) = authorization {
        request.push_str("Authorization: ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut body = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut body))
        .await
        .expect("metrics response")
        .unwrap();
    let text = String::from_utf8_lossy(&body).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    (status, text)
}
