//! Defensive checks for fail-closed restart, bearer comparison, and `uid`.
//!
//! These tests observe current behavior. They do not change it, and they do not
//! try to bypass authentication. A green run of a residual-risk case is a record
//! of that gap, not a first-ship security pass.

use metadb_core::ROOT_INODE_ID;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{CreateFileRequest, LookupRequest};
use metadb_server::{BearerAuthClient, PersistentMetaDbCluster, ServerTomlConfig};
use tonic::transport::Endpoint;

fn with_data_dir(mut cfg: ServerTomlConfig, dir: &std::path::Path) -> ServerTomlConfig {
    cfg.node.data_dir = dir.to_path_buf();
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_non_loopback_advertise_without_secret_fails_closed() {
    // Starting again on 127.0.0.1 with an empty secret does not exercise this
    // path. The data directory already saved the address from the first bind.
    let dir = tempfile::tempdir().unwrap();
    let mut first = ServerTomlConfig::default();
    first = with_data_dir(first, dir.path());
    first.node.grpc_listen_addr = "0.0.0.0:0".into();
    first.security.shared_secret = "test-secret".into();
    let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(first)
        .await
        .expect("first start with a secret must succeed");
    let persisted = grpc_addr.clone();
    cluster.shutdown().await.unwrap();
    assert!(
        !persisted.starts_with("127.")
            && !persisted.starts_with("[::1]")
            && !persisted.to_ascii_lowercase().starts_with("localhost"),
        "this case needs a persisted non-loopback advertise address, got {persisted}"
    );

    let mut restart = ServerTomlConfig::default();
    restart = with_data_dir(restart, dir.path());
    restart.node.grpc_listen_addr = "127.0.0.1:0".into();
    restart.security.shared_secret.clear();
    let err = match PersistentMetaDbCluster::open_and_serve(restart).await {
        Ok((cluster, _)) => {
            let _ = cluster.shutdown().await;
            panic!("empty secret must fail closed when a saved advertise address is {persisted}");
        }
        Err(err) => err,
    };
    let message = err.to_string();
    assert!(
        message.contains("refusing to serve"),
        "restart error was: {message}"
    );
    assert!(
        message.contains(&persisted) || message.contains("unauthenticated listener"),
        "fail-closed error did not name the persisted bind: {message}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loopback_whitespace_secret_does_not_start() {
    // A whitespace-only secret used to start on loopback and accept only the
    // literal whitespace bearer. Startup now refuses that value.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg = with_data_dir(cfg, dir.path());
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.shared_secret = "   ".into();
    let err = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok((cluster, _)) => {
            let _ = cluster.shutdown().await;
            panic!("whitespace secret must not start");
        }
        Err(err) => err,
    };
    assert!(err.to_string().contains("whitespace-only"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uid_is_not_an_authorization_boundary() {
    // One valid bearer can create and look up files that carry different `uid`
    // request fields. `uid` is stored metadata. It is not an identity or tenant
    // boundary. A green result documents that gap and is not a first-ship pass.
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg = with_data_dir(cfg, dir.path());
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.shared_secret = "test-secret".into();
    let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let channel = Endpoint::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut meta =
        MetadataServiceClient::with_interceptor(channel, BearerAuthClient::new("test-secret"));

    for (name, inode_id, fid, uid) in [
        ("one.dat", 1001u64, 5001u64, 1u64),
        ("two.dat", 1002, 5002, 2),
    ] {
        let created = meta
            .create_file(CreateFileRequest {
                header: None,
                pid: ROOT_INODE_ID,
                name: name.into(),
                inode_id,
                fid,
                uid,
                mode: 0o644,
                size_bytes: 8,
            })
            .await
            .expect("create_file rpc")
            .into_inner();
        assert!(
            created.error.is_none(),
            "same bearer created {name} as uid {uid}: {:?}",
            created.error
        );
    }

    for (name, uid) in [("one.dat", 1u64), ("two.dat", 2u64)] {
        let found = meta
            .lookup(LookupRequest {
                pid: ROOT_INODE_ID,
                name: name.into(),
                ..Default::default()
            })
            .await
            .expect("lookup rpc")
            .into_inner();
        assert!(found.error.is_none(), "lookup {name}: {:?}", found.error);
        assert!(found.found, "lookup did not find {name}");
        let attr = found.entry.expect("entry").attr.expect("attr");
        assert_eq!(attr.uid, uid, "{name} stored a different uid");
    }
    cluster.shutdown().await.unwrap();
}
