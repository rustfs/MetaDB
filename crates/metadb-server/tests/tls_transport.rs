//! TLS 1.3 and mTLS on the gRPC and metrics listeners.
//!
//! These tests use the rustls ring provider. They do not skip certificate
//! checks and they do not treat a certificate name as a role.

use std::time::Duration;

use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::GetRoutesRequest;
use metadb_security::{TlsSpec, TransportSecurity};
use metadb_server::{BearerAuthClient, PersistentMetaDbCluster, ServerTomlConfig};
use tonic::transport::Endpoint;
use tonic::Code;

fn bundle(dir: &std::path::Path) -> (String, String, String) {
    let mut ca = metadb_security::testing::TestCa::new("metadb-it-ca");
    let issued = ca.issue(metadb_security::testing::IssueRequest::server_and_client(
        "127.0.0.1".parse().unwrap(),
        "spiffe://metadb/node/it",
    ));
    metadb_security::testing::write_bundle(dir, ca.pem(), &issued.cert_pem, &issued.key_pem)
}

fn apply_tls(cfg: &mut ServerTomlConfig, cert: &str, key: &str, ca: &str) {
    cfg.security.tls.mode = "mtls".into();
    cfg.security.tls.cert_file = cert.into();
    cfg.security.tls.key_file = key.into();
    cfg.security.tls.ca_file = ca.into();
    cfg.security.tls.server_name = "127.0.0.1".into();
}

#[tokio::test]
async fn missing_material_and_https_on_plaintext_fail_before_a_data_directory() {
    let parent = tempfile::tempdir().unwrap();
    let data = parent.path().join("not-created");
    let marker = "SUPER-SECRET-KEY-MATERIAL";
    let key = parent.path().join("key.pem");
    std::fs::write(&key, marker).unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = data.clone();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.shared_secret = "test-secret".into();
    cfg.security.tls.mode = "mtls".into();
    cfg.security.tls.cert_file = parent.path().join("cert.pem").display().to_string();
    cfg.security.tls.key_file = key.display().to_string();
    cfg.security.tls.ca_file = parent.path().join("ca.pem").display().to_string();
    cfg.security.tls.server_name = "127.0.0.1".into();
    let error = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok(_) => panic!("missing certificates must not serve"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("cannot read"), "{error}");
    assert!(!error.contains(marker), "{error}");
    assert!(!error.contains("BEGIN"), "{error}");
    assert!(!data.exists(), "refused TLS must not create {data:?}");

    let mut plain = ServerTomlConfig::default();
    plain.node.role = metadb_server::ServerRole::CellManager;
    plain.node.data_dir = data.clone();
    plain.node.grpc_listen_addr = "127.0.0.1:0".into();
    plain.security.allow_unauthenticated = true;
    plain.cluster.global_manager = "https://127.0.0.1:7800".into();
    let error = match PersistentMetaDbCluster::open_and_serve(plain).await {
        Ok(_) => panic!("https must not be dialed while TLS is off"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("https"), "{error}");
    assert!(!data.exists(), "scheme refusal must not create {data:?}");
}

#[tokio::test]
async fn wildcard_tls_listen_and_plaintext_url_fail_before_a_data_directory() {
    let parent = tempfile::tempdir().unwrap();
    let data = parent.path().join("not-created");
    let (cert, key, ca) = bundle(parent.path());
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = data.clone();
    cfg.node.grpc_listen_addr = "0.0.0.0:7800".into();
    cfg.security.shared_secret = "test-secret".into();
    apply_tls(&mut cfg, &cert, &key, &ca);
    let error = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok(_) => panic!("wildcard TLS listen must not serve"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("wildcard"), "{error}");
    assert!(!data.exists(), "wildcard refusal must not create {data:?}");

    let mut http = ServerTomlConfig::default();
    http.node.role = metadb_server::ServerRole::CellManager;
    http.node.data_dir = data.clone();
    http.node.grpc_listen_addr = "127.0.0.1:0".into();
    http.security.shared_secret = "test-secret".into();
    apply_tls(&mut http, &cert, &key, &ca);
    http.cluster.global_manager = "http://127.0.0.1:7800".into();
    let error = match PersistentMetaDbCluster::open_and_serve(http).await {
        Ok(_) => panic!("http must not be dialed while TLS is enabled"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("plaintext"), "{error}");
    assert!(!data.exists(), "http refusal must not create {data:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_serves_routes_and_readyz_and_still_requires_the_bearer() {
    let parent = tempfile::tempdir().unwrap();
    let (cert, key, ca) = bundle(parent.path());
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = parent.path().join("data");
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.node.metrics_listen_addr = Some("127.0.0.1:0".into());
    cfg.security.shared_secret = "test-secret".into();
    apply_tls(&mut cfg, &cert, &key, &ca);
    let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(cfg)
        .await
        .expect("mtls server");
    let security = cluster.transport_security().clone();
    let url = security.prepare_url(&grpc_addr).expect("https url");
    assert!(url.starts_with("https://"), "{url}");

    let endpoint = Endpoint::from_shared(url.clone())
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5));
    let channel = security.connect(endpoint).await.expect("mtls channel");
    let mut authed = ControlPlaneServiceClient::with_interceptor(
        channel.clone(),
        BearerAuthClient::new("test-secret"),
    );
    let routes = authed
        .get_routes(GetRoutesRequest { cell_id: 1 })
        .await
        .expect("get_routes over mtls");
    assert!(routes.into_inner().error.is_none());

    let mut bare = ControlPlaneServiceClient::with_interceptor(channel, BearerAuthClient::new(""));
    let missing = bare
        .get_routes(GetRoutesRequest { cell_id: 1 })
        .await
        .expect_err("a client certificate is not a bearer");
    assert_eq!(missing.code(), Code::Unauthenticated);

    let plain = TcpPlain::connect(&grpc_addr).await;
    assert!(
        plain.is_err(),
        "plaintext HTTP/2 completed an RPC on the TLS listener: {plain:?}"
    );

    let metrics = wait_metrics(&cluster).await;
    let denied = security
        .exchange_http1(
            &metrics,
            b"GET /readyz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        )
        .await
        .expect("tls metrics");
    let denied = String::from_utf8_lossy(&denied);
    assert!(denied.contains("401"), "{denied}");
    assert!(
        !denied.contains("ready\n") || denied.contains("401"),
        "{denied}"
    );
    assert!(!denied.contains("test-secret"), "{denied}");

    let ready = security
        .exchange_http1(
            &metrics,
            b"GET /readyz HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer test-secret\r\nConnection: close\r\n\r\n",
        )
        .await
        .expect("readyz");
    let ready = String::from_utf8_lossy(&ready);
    assert!(ready.contains("200"), "{ready}");
    assert!(ready.contains("ready"), "{ready}");

    let mut foreign = metadb_security::testing::TestCa::new("foreign");
    let foreign_cert = foreign.issue(metadb_security::testing::IssueRequest::server_and_client(
        "127.0.0.1".parse().unwrap(),
        "spiffe://metadb/node/foreign",
    ));
    let foreign_dir = parent.path().join("foreign");
    std::fs::create_dir_all(&foreign_dir).unwrap();
    let (fcert, fkey, fca) = metadb_security::testing::write_bundle(
        &foreign_dir,
        foreign.pem(),
        &foreign_cert.cert_pem,
        &foreign_cert.key_pem,
    );
    let foreign_security =
        TransportSecurity::load(&TlsSpec::from_client_fields("mtls", &fcert, &fkey, &fca).unwrap())
            .unwrap();
    let foreign_url = foreign_security.prepare_url(&grpc_addr).unwrap();
    let foreign_endpoint = Endpoint::from_shared(foreign_url)
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(3));
    let rejected = foreign_security.connect(foreign_endpoint).await;
    assert!(rejected.is_err(), "a foreign CA must not dial");

    cluster.shutdown().await.expect("shutdown");
}

struct TcpPlain;

impl TcpPlain {
    async fn connect(addr: &str) -> Result<(), String> {
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))
            .map_err(|error| error.to_string())?
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(2));
        match endpoint.connect().await {
            Ok(channel) => {
                let mut client = ControlPlaneServiceClient::new(channel);
                match client.get_routes(GetRoutesRequest { cell_id: 1 }).await {
                    Ok(_) => Ok(()),
                    Err(error) => Err(error.to_string()),
                }
            }
            Err(error) => Err(error.to_string()),
        }
    }
}

async fn wait_metrics(cluster: &PersistentMetaDbCluster) -> String {
    for _ in 0..50 {
        if let Some(addr) = cluster.metrics_addr() {
            return addr.to_string();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("metrics listener did not publish an address");
}
