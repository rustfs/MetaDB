//! TLS modes are recognized and refused before a listener or data directory exists.
//!
//! These tests do not start a TLS stack and do not skip certificate checks.

use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};

#[tokio::test]
async fn mtls_is_refused_before_a_data_directory_and_does_not_read_the_key() {
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
        Ok(_) => panic!("tls and mtls must not serve"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("does not terminate TLS"), "{error}");
    assert!(error.contains("ISC"), "{error}");
    assert!(!error.contains(marker), "{error}");
    assert!(!error.contains("BEGIN"), "{error}");
    assert!(!data.exists(), "refused TLS must not create {data:?}");
}

#[tokio::test]
async fn https_global_manager_is_refused_on_the_plaintext_policy() {
    let parent = tempfile::tempdir().unwrap();
    let data = parent.path().join("not-created");
    let mut cfg = ServerTomlConfig::default();
    cfg.node.role = metadb_server::ServerRole::CellManager;
    cfg.node.data_dir = data.clone();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    cfg.cluster.global_manager = "https://127.0.0.1:7800".into();
    let error = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok(_) => panic!("https must not be dialed while TLS is off"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("https"), "{error}");
    assert!(!data.exists(), "scheme refusal must not create {data:?}");
}
