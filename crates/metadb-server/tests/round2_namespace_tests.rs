use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};

async fn start(dir: &std::path::Path) -> std::sync::Arc<PersistentMetaDbCluster> {
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    PersistentMetaDbCluster::open_and_serve(cfg)
        .await
        .unwrap()
        .0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mkdir_must_not_alias_an_existing_directory_inode() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = start(dir.path()).await;
    let tx = cluster.tx_router();
    tx.mkdir(1, "original", 10, 7, 0o755).await.unwrap();
    tx.create_file(10, "kept", (1 << 63) + 77, 77, 7, 0o644, 100)
        .await
        .unwrap();
    cluster.flush_all().await.unwrap();
    let result = tx.mkdir(1, "alias", 10, 8, 0o700).await;
    let stat = tx.dirstat(10).await.unwrap();
    cluster.shutdown().await.unwrap();
    assert!(
        result.is_err(),
        "mkdir accepted a second dentry for inode 10; stats afterward: {stat:?}"
    );
    assert_eq!(stat.num_children, 1);
    assert_eq!(stat.uid, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mkdir_must_not_create_a_cycle_using_root_inode() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = start(dir.path()).await;
    let result = cluster
        .tx_router()
        .mkdir(1, "root_alias", 1, 7, 0o700)
        .await;
    cluster.shutdown().await.unwrap();
    assert!(result.is_err(), "mkdir accepted root as its own child");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zero_voter_scale_request_must_not_report_success() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = start(dir.path()).await;
    let result = cluster.cell_manager().scale_out_replicas(0).await;
    cluster.shutdown().await.unwrap();
    assert!(
        result.is_err(),
        "zero-voter request was acknowledged as successful: {result:?}"
    );
}
