//! Fail-closed checks for missing, empty, and mismatched shared secrets.
//!
//! These tests only send a missing or wrong bearer and expect rejection, or they
//! expect startup to refuse a listener. They do not attempt to bypass authentication.

use std::time::Duration;

use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::raft_transport_service_client::RaftTransportServiceClient;
use metadb_proto::{GetRoutesRequest, LookupRequest, RaftRpcEnvelope};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tonic::transport::Endpoint;
use tonic::Code;

fn loopback_dial(bound: &str) -> String {
    let port = bound.rsplit_once(':').expect("host:port").1;
    format!("127.0.0.1:{port}")
}

async fn expect_closed(addr: &str) {
    let connected = TcpStream::connect(addr).await;
    assert!(
        connected.is_err(),
        "listener {addr} accepted a connection after fail-closed startup"
    );
}

async fn http_exchange(addr: &str, authorization: Option<&str>, path: &str) -> (u16, String) {
    let mut socket = TcpStream::connect(addr).await.expect("metrics connect");
    let mut request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
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
        .expect("metrics response timed out")
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

fn assert_rejected_http(status: u16, body: &str) {
    assert_eq!(status, 401, "{body}");
    assert!(
        !body.contains("metadb_"),
        "rejected metrics response included metric text: {body}"
    );
    assert!(
        !body.contains("test-secret"),
        "rejected response echoed the configured secret"
    );
}

#[tokio::test]
async fn wildcard_without_a_secret_does_not_listen() {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = std::env::temp_dir().join(format!("metadb-fail-closed-{port}"));
    cfg.node.grpc_listen_addr = format!("0.0.0.0:{port}");
    cfg.security.shared_secret.clear();
    let err = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok(_) => panic!("empty secret on a wildcard bind must fail closed"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("refusing to serve"), "{err}");
    expect_closed(&format!("127.0.0.1:{port}")).await;

    let mut spaced = ServerTomlConfig::default();
    spaced.node.grpc_listen_addr = format!("0.0.0.0:{port}");
    spaced.security.shared_secret = "   ".into();
    let spaced_err = match PersistentMetaDbCluster::open_and_serve(spaced).await {
        Ok(_) => panic!("whitespace secret on a wildcard bind must fail closed"),
        Err(err) => err,
    };
    assert!(
        spaced_err.to_string().contains("refusing to serve"),
        "{spaced_err}"
    );

    let metrics_probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let metrics_port = metrics_probe.local_addr().unwrap().port();
    drop(metrics_probe);
    let mut metrics = ServerTomlConfig::default();
    metrics.node.grpc_listen_addr = "127.0.0.1:0".into();
    metrics.node.metrics_listen_addr = Some(format!("0.0.0.0:{metrics_port}"));
    let metrics_err = match PersistentMetaDbCluster::open_and_serve(metrics).await {
        Ok(_) => panic!("wildcard metrics bind without a secret must fail closed"),
        Err(err) => err,
    };
    assert!(
        metrics_err.to_string().contains("refusing to serve"),
        "{metrics_err}"
    );
    expect_closed(&format!("127.0.0.1:{metrics_port}")).await;
}

#[tokio::test]
async fn misspelled_secret_and_tls_options_do_not_start() {
    let typo = ServerTomlConfig::from_toml_str(
        r#"
        [node]
        grpc_listen_addr = "0.0.0.0:7800"
        [security]
        shared_secrett = "not-the-field"
        "#,
    );
    assert!(
        typo.unwrap_err().to_string().contains("refusing to start"),
        "misspelled shared_secret must not parse into an open listener"
    );

    let tls = ServerTomlConfig::from_toml_str(
        r#"
        [node]
        grpc_listen_addr = "127.0.0.1:7800"
        tls_cert_file = "/tmp/cert.pem"
        [security]
        shared_secret = "secret"
        "#,
    );
    let tls_err = tls.unwrap_err().to_string();
    assert!(tls_err.contains("does not terminate TLS"), "{tls_err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_secret_rejects_missing_and_wrong_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "0.0.0.0:0".into();
    cfg.node.metrics_listen_addr = Some("0.0.0.0:0".into());
    cfg.security.shared_secret = "test-secret".into();
    let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let grpc_dial = loopback_dial(&grpc_addr);
    let metrics_dial = loopback_dial(cluster.metrics_addr().expect("metrics listener"));

    let channel = Endpoint::from_shared(format!("http://{grpc_dial}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut status = ControlPlaneServiceClient::new(channel.clone());
    let missing_status = status.get_routes(GetRoutesRequest { cell_id: 0 }).await;
    let mut empty_status_req = tonic::Request::new(GetRoutesRequest { cell_id: 0 });
    empty_status_req
        .metadata_mut()
        .insert("authorization", "Bearer ".parse().unwrap());
    let empty_status = status.get_routes(empty_status_req).await;
    let mut typo_status_req = tonic::Request::new(GetRoutesRequest { cell_id: 0 });
    typo_status_req
        .metadata_mut()
        .insert("authorization", "Bearer test-secre".parse().unwrap());
    let typo_status = status.get_routes(typo_status_req).await;

    let mut metadata = MetadataServiceClient::new(channel.clone());
    let missing_lookup = metadata
        .lookup(LookupRequest {
            pid: 1,
            name: "no-such-entry".into(),
            ..Default::default()
        })
        .await;
    let mut wrong_lookup_req = tonic::Request::new(LookupRequest {
        pid: 1,
        name: "no-such-entry".into(),
        ..Default::default()
    });
    wrong_lookup_req
        .metadata_mut()
        .insert("authorization", "Bearer wrong-secret".parse().unwrap());
    let wrong_lookup = metadata.lookup(wrong_lookup_req).await;

    let mut raft = RaftTransportServiceClient::new(channel);
    let missing_raft = raft
        .vote(RaftRpcEnvelope {
            shard_id: 99999,
            bincode_payload: vec![],
        })
        .await;

    let missing_metrics = http_exchange(&metrics_dial, None, "/metrics").await;
    let empty_metrics = http_exchange(&metrics_dial, Some("Bearer "), "/metrics").await;
    let wrong_metrics = http_exchange(&metrics_dial, Some("Bearer wrong-secret"), "/metrics").await;
    let status_path = http_exchange(&metrics_dial, None, "/status").await;

    cluster.shutdown().await.unwrap();

    for (name, result) in [
        ("cluster status missing", missing_status.map(|_| ())),
        ("cluster status empty bearer", empty_status.map(|_| ())),
        ("cluster status typo bearer", typo_status.map(|_| ())),
        ("lookup missing", missing_lookup.map(|_| ())),
        ("lookup wrong", wrong_lookup.map(|_| ())),
        ("raft missing", missing_raft.map(|_| ())),
    ] {
        assert_eq!(
            result.unwrap_err().code(),
            Code::Unauthenticated,
            "{name} was not rejected"
        );
    }
    assert_rejected_http(missing_metrics.0, &missing_metrics.1);
    assert_rejected_http(empty_metrics.0, &empty_metrics.1);
    assert_rejected_http(wrong_metrics.0, &wrong_metrics.1);
    assert_rejected_http(status_path.0, &status_path.1);
}
