use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_tables_single_shard_configuration_must_support_split() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.sharding.bootstrap_table_shards = false;
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let tx = cluster.tx_router();
    for pid in [10u64, 20] {
        tx.mkdir(1, &format!("dir{pid}"), pid, 7, 0o755)
            .await
            .unwrap();
        for i in 0..4 {
            let fid = pid * 100 + i;
            tx.create_file(pid, &format!("file{i}"), (1 << 63) + fid, fid, 7, 0o644, 10)
                .await
                .unwrap();
        }
    }
    cluster.run_maintenance_tick().await.unwrap();
    let result = cluster
        .cell_manager()
        .maybe_split_or_isolate_shard(1, true)
        .await;
    cluster.shutdown().await.unwrap();
    assert!(
        result.is_ok(),
        "valid single-shard configuration cannot split after quota/index delivery: {result:?}"
    );
    assert!(
        result.unwrap().is_some(),
        "balanced multi-directory data did not split"
    );
}
