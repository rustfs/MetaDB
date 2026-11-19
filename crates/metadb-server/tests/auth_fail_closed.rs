//! Fail-closed checks for missing, empty, and mismatched shared secrets.
//!
//! These tests only send a missing or wrong credential and expect rejection, or they
//! expect startup to refuse a listener. They do not attempt to bypass authentication.
//!
//! Two cases lock current behavior that is still a gap:
//!
//! - An empty `shared_secret` on `127.0.0.1` serves metadata, transactions, the
//!   control plane, and metrics with no credentials. A post-merge run that used
//!   loopback and an empty secret did not prove fail-closed authentication.
//! - Unknown TOML keys that do not look like a secret or TLS setting are ignored.
//!   A misspelled storage or join key still starts the process.

use std::time::Duration;

use metadb_core::{make_file_inode_id, ROOT_INODE_ID};
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::raft_transport_service_client::RaftTransportServiceClient;
use metadb_proto::transaction_service_client::TransactionServiceClient;
use metadb_proto::{
    CheckTxnStatusRequest, CreateFileRequest, GetRoutesRequest, LookupRequest, RaftRpcEnvelope,
    RenameRequest,
};
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
    let authed_metrics = http_exchange(&metrics_dial, Some("Bearer test-secret"), "/metrics").await;

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
    assert_eq!(authed_metrics.0, 200, "{}", authed_metrics.1);
    assert!(
        authed_metrics.1.contains("text/plain; version=0.0.4"),
        "authenticated scrape missing Prometheus content type: {}",
        authed_metrics.1
    );
    assert!(
        authed_metrics.1.contains("metadb_shards_total"),
        "authenticated scrape missing metric text: {}",
        authed_metrics.1
    );
}

fn rejected_file(name: &str) -> CreateFileRequest {
    CreateFileRequest {
        header: None,
        pid: ROOT_INODE_ID,
        name: name.to_string(),
        inode_id: make_file_inode_id(50),
        fid: 50,
        uid: 7,
        mode: 0o644,
        size_bytes: 128,
    }
}

fn with_authorization(
    request: CreateFileRequest,
    value: &str,
) -> tonic::Request<CreateFileRequest> {
    let mut request = tonic::Request::new(request);
    request
        .metadata_mut()
        .insert("authorization", value.parse().unwrap());
    request
}

/// Loopback with an empty secret is the local-development path. It is not fail-closed.
///
/// A run that binds `127.0.0.1` and leaves `shared_secret` empty, including the
/// post-merge Mac mini run, accepts callers that send no credentials.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_secret_on_loopback_serves_without_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.node.metrics_listen_addr = Some("127.0.0.1:0".into());
    cfg.security.shared_secret.clear();
    let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    assert!(
        grpc_addr.starts_with("127.0.0.1:"),
        "empty-secret listener left loopback: {grpc_addr}"
    );
    let metrics_dial = loopback_dial(cluster.metrics_addr().expect("metrics listener"));
    let channel = Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut status = ControlPlaneServiceClient::new(channel.clone());
    let routes = status
        .get_routes(GetRoutesRequest { cell_id: 0 })
        .await
        .expect("empty secret must allow GetRoutes with no authorization");
    assert!(
        routes.into_inner().error.is_none(),
        "unauthenticated GetRoutes failed inside the handler"
    );

    let mut basic_status = tonic::Request::new(GetRoutesRequest { cell_id: 0 });
    basic_status
        .metadata_mut()
        .insert("authorization", "Basic dGVzdDpzZWNyZXQ=".parse().unwrap());
    status
        .get_routes(basic_status)
        .await
        .expect("empty secret must ignore a Basic authorization header");

    let mut metadata = MetadataServiceClient::new(channel.clone());
    let created = metadata
        .create_file(rejected_file("open-loopback.dat"))
        .await
        .expect("empty secret must allow CreateFile with no authorization")
        .into_inner();
    assert!(
        created.error.is_none(),
        "unauthenticated create was accepted by the interceptor and then failed: {:?}",
        created.error
    );
    let mut garbage = tonic::Request::new(LookupRequest {
        pid: ROOT_INODE_ID,
        name: "open-loopback.dat".into(),
        ..Default::default()
    });
    garbage
        .metadata_mut()
        .insert("authorization", "Bearer not-configured".parse().unwrap());
    let found = metadata
        .lookup(garbage)
        .await
        .expect("empty secret must ignore a bearer")
        .into_inner();
    assert!(found.found, "unauthenticated create did not become visible");

    let mut transactions = TransactionServiceClient::new(channel);
    let rename = transactions
        .rename(RenameRequest {
            src_pid: ROOT_INODE_ID,
            src_name: "missing-for-rename.dat".into(),
            dst_pid: ROOT_INODE_ID,
            dst_name: "renamed.dat".into(),
            overwrite: false,
            caller_hlc: 0,
        })
        .await
        .expect("empty secret must allow TransactionService with no authorization")
        .into_inner();
    assert!(
        rename.error.is_some(),
        "rename of a missing name should fail in the handler, not in the interceptor"
    );

    let metrics = http_exchange(&metrics_dial, None, "/metrics").await;
    let basic_metrics =
        http_exchange(&metrics_dial, Some("Basic dGVzdDpzZWNyZXQ="), "/metrics").await;
    cluster.shutdown().await.unwrap();

    assert_eq!(metrics.0, 200, "{}", metrics.1);
    assert!(
        metrics.1.contains("metadb_shards_total"),
        "anonymous metrics scrape returned no metric text: {}",
        metrics.1
    );
    assert_eq!(basic_metrics.0, 200, "{}", basic_metrics.1);
    assert!(
        basic_metrics.1.contains("metadb_shards_total"),
        "Basic credentials were treated as authentication on an open listener: {}",
        basic_metrics.1
    );
}

/// A non-empty secret on loopback must reject failed authentication and must not apply the RPC.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_secret_rejects_basic_auth_without_applying_the_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.node.metrics_listen_addr = Some("127.0.0.1:0".into());
    cfg.security.shared_secret = "test-secret".into();
    let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let metrics_dial = loopback_dial(cluster.metrics_addr().expect("metrics listener"));
    let channel = Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut metadata = MetadataServiceClient::new(channel.clone());
    let name = "must-not-fail-open.dat";
    let attempts = [
        (
            "missing",
            metadata.create_file(rejected_file(name)).await.map(|_| ()),
        ),
        (
            "basic",
            metadata
                .create_file(with_authorization(
                    rejected_file(name),
                    "Basic dGVzdDpzZWNyZXQ=",
                ))
                .await
                .map(|_| ()),
        ),
        (
            "lowercase scheme",
            metadata
                .create_file(with_authorization(
                    rejected_file(name),
                    "bearer test-secret",
                ))
                .await
                .map(|_| ()),
        ),
        (
            "secret without scheme",
            metadata
                .create_file(with_authorization(rejected_file(name), "test-secret"))
                .await
                .map(|_| ()),
        ),
        (
            "wrong bearer",
            metadata
                .create_file(with_authorization(
                    rejected_file(name),
                    "Bearer wrong-secret",
                ))
                .await
                .map(|_| ()),
        ),
    ];

    let mut transactions = TransactionServiceClient::new(channel.clone());
    let rename = transactions
        .rename(RenameRequest {
            src_pid: ROOT_INODE_ID,
            src_name: name.into(),
            dst_pid: ROOT_INODE_ID,
            dst_name: "renamed-open.dat".into(),
            overwrite: false,
            caller_hlc: 0,
        })
        .await
        .map(|_| ());
    let txn_status = transactions
        .check_txn_status(CheckTxnStatusRequest {
            header: None,
            txn_id_be: vec![0; 16],
            primary_key: vec![1, 2, 3],
            caller_hlc: 0,
        })
        .await
        .map(|_| ());

    let mut authed = tonic::Request::new(LookupRequest {
        pid: ROOT_INODE_ID,
        name: name.into(),
        ..Default::default()
    });
    authed
        .metadata_mut()
        .insert("authorization", "Bearer test-secret".parse().unwrap());
    let lookup = metadata
        .lookup(authed)
        .await
        .expect("lookup with the configured bearer");

    let mut created_req = tonic::Request::new(rejected_file(name));
    created_req
        .metadata_mut()
        .insert("authorization", "Bearer test-secret".parse().unwrap());
    let created = metadata
        .create_file(created_req)
        .await
        .expect("configured bearer must be accepted")
        .into_inner();

    let basic_metrics =
        http_exchange(&metrics_dial, Some("Basic dGVzdDpzZWNyZXQ="), "/metrics").await;
    let bearer_metrics = http_exchange(&metrics_dial, Some("Bearer test-secret"), "/metrics").await;
    cluster.shutdown().await.unwrap();

    for (label, result) in attempts {
        assert_eq!(
            result.unwrap_err().code(),
            Code::Unauthenticated,
            "{label} was not rejected"
        );
    }
    assert_eq!(
        rename.unwrap_err().code(),
        Code::Unauthenticated,
        "transaction rename failed open"
    );
    assert_eq!(
        txn_status.unwrap_err().code(),
        Code::Unauthenticated,
        "transaction status check failed open"
    );
    let lookup = lookup.into_inner();
    assert!(
        lookup.error.is_none(),
        "authenticated lookup failed: {:?}",
        lookup.error
    );
    assert!(
        !lookup.found,
        "a rejected create or rename became visible; authentication failed open"
    );
    assert!(
        created.error.is_none(),
        "configured bearer create failed: {:?}",
        created.error
    );
    assert_rejected_http(basic_metrics.0, &basic_metrics.1);
    assert_eq!(bearer_metrics.0, 200, "{}", bearer_metrics.1);
    assert!(bearer_metrics.1.contains("metadb_shards_total"));
}

/// Whitespace is not an empty secret once a listener is up. Loopback still starts,
/// and a missing bearer is rejected instead of being treated as anonymous access.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whitespace_secret_on_loopback_rejects_anonymous_calls() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.node.metrics_listen_addr = Some("127.0.0.1:0".into());
    cfg.security.shared_secret = "   ".into();
    let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let metrics_dial = loopback_dial(cluster.metrics_addr().expect("metrics listener"));
    let channel = Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut status = ControlPlaneServiceClient::new(channel);
    let anonymous = status.get_routes(GetRoutesRequest { cell_id: 0 }).await;
    let mut exact = tonic::Request::new(GetRoutesRequest { cell_id: 0 });
    exact
        .metadata_mut()
        .insert("authorization", "Bearer    ".parse().unwrap());
    let accepted = status.get_routes(exact).await;
    let metrics = http_exchange(&metrics_dial, None, "/metrics").await;
    cluster.shutdown().await.unwrap();

    assert_eq!(anonymous.unwrap_err().code(), Code::Unauthenticated);
    assert!(accepted
        .expect("exact whitespace bearer")
        .into_inner()
        .error
        .is_none());
    assert_rejected_http(metrics.0, &metrics.1);
}

/// Unknown keys that do not name a secret or TLS setting are still ignored.
///
/// This is a fail-open configuration hole: a misspelled storage or join field
/// does not fail startup, and the process serves with the defaults. Secret-like
/// and TLS-like keys remain rejected. If unknown keys start failing closed,
/// update `docs/en/configuration.md` and `docs/en/testing.md` with the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_toml_keys_are_ignored_and_loopback_still_serves() {
    let defaults = ServerTomlConfig::default();
    let mut cfg = ServerTomlConfig::from_toml_str(
        r#"
        [node]
        grpc_listen_addrr = "0.0.0.0:9"
        not_a_listener = true

        [storage]
        memtable_size_bytez = 1
        flush_threshold = "ignored"

        [cluster]
        joinn = "10.9.9.9:7800"

        [security]
        shared_secret = ""
        "#,
    )
    .expect("non-secret unknown keys are ignored and must not fail the parse");
    assert_eq!(
        cfg.node.grpc_listen_addr, defaults.node.grpc_listen_addr,
        "a misspelled listen address was applied"
    );
    assert_eq!(
        cfg.storage.memtable_size_bytes, defaults.storage.memtable_size_bytes,
        "a misspelled storage key changed the memtable size"
    );
    assert!(
        cfg.cluster.join.is_empty(),
        "a misspelled join key was applied: {}",
        cfg.cluster.join
    );
    assert!(cfg.security.shared_secret.is_empty());

    let token_file = ServerTomlConfig::from_toml_str(
        r#"
        [node]
        grpc_listen_addr = "127.0.0.1:7800"
        token_file = "/tmp/token"
        "#,
    );
    let token_err = token_file.unwrap_err().to_string();
    assert!(
        token_err.contains("refusing to start"),
        "a token key must not be ignored: {token_err}"
    );
    let tls_note = ServerTomlConfig::from_toml_str(
        r#"
        [storage]
        tls_mode = "off"
        "#,
    );
    let tls_err = tls_note.unwrap_err().to_string();
    assert!(
        tls_err.contains("does not terminate TLS"),
        "a TLS key must not be ignored: {tls_err}"
    );

    let dir = tempfile::tempdir().unwrap();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(cfg)
        .await
        .expect("ignored unknown keys must not stop the current process from serving");
    let channel = Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut status = ControlPlaneServiceClient::new(channel);
    let routes = status.get_routes(GetRoutesRequest { cell_id: 0 }).await;
    cluster.shutdown().await.unwrap();
    assert!(routes
        .expect("server started from a config with ignored keys")
        .into_inner()
        .error
        .is_none());
}
