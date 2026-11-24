use std::time::Duration;

use metadb_core::ROOT_INODE_ID;
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::sql_param_value;
use metadb_proto::transaction_service_client::TransactionServiceClient;
use metadb_proto::{
    DirStatRequest, ExecuteSqlRequest, GetQuotaRequest, GetRoutesRequest, GetSecondaryIndexRequest,
    ListDirRequest, RenameRequest, SqlParamValue, TriggerShardSplitRequest,
};
use metadb_server::{
    render_prometheus_metrics, MetaDbConfig, MetaDbEngine, PersistentMetaDbCluster,
    ServerTomlConfig,
};
use tempfile::tempdir;

/// End-to-End Phase 6 Verification Suite (`e2e_cluster_evolution_tests.rs`):
/// 1. Boots a persistent `PersistentMetaDbCluster` (`1-Voter` OpenRaft + WAL-less `LsmStorageBackend`)
///    configured from TOML, serving `MetadataService`, `TransactionService`, `ControlPlaneService`,
///    and `RaftTransportService` over real TCP/gRPC.
/// 2. Creates directories/files via `mkdir_auto_id` / `create_file_auto_id` and parameterized `ExecuteSql`,
///    gracefully shuts down, restarts from the same disk directory, and verifies 100% state recovery!
/// 3. Splits shards online via `ControlPlaneService::TriggerShardSplit`, executes cross-shard 2PC
///    `TransactionService::Rename` (exclusive + overwrite), runs consecutive bulk deletions verifying
///    `LocalityCompactor` purges physical tombstones (`0` tombstones traversed on gRPC `ListDir`),
///    and verifies `QuotaTable`, `SecondaryIndexTable`, and Prometheus metrics output!
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_e2e_persistent_grpc_cluster_restart_split_2pc_and_locality_compaction() {
    let dir = tempdir().unwrap();
    let toml_text = format!(
        r#"
[node]
node_id = 1
cell_id = 1
role = "all-in-one"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[clock]
max_clock_drift_ms = 500

[storage]
memtable_size_bytes = 33554432
block_cache_bytes = 16777216
mvcc_retention_ms = 5000
bloom_bits_per_key = 10
locality_tombstone_threshold = 20
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 65536

[security]
allow_unauthenticated = true
"#,
        dir.path().display()
    );

    let cfg = ServerTomlConfig::from_toml_str(&toml_text).unwrap();

    // Stage 1: Boot persistent node, create directories & files, execute gRPC SQL, and shut down!
    let (dir_a_id, dir_b_id) = {
        let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
            .await
            .unwrap();
        let url = format!("http://{}", addr);

        let dir_a = cluster
            .mkdir_auto_id(ROOT_INODE_ID, "alpha_bucket", 50, 0o755)
            .await
            .unwrap();
        let dir_b = cluster
            .mkdir_auto_id(ROOT_INODE_ID, "beta_bucket", 50, 0o755)
            .await
            .unwrap();

        for i in 0..10 {
            cluster
                .create_file_auto_id(
                    dir_a.attr.inode_id,
                    &format!("a_{:02}.obj", i),
                    50,
                    0o644,
                    512,
                )
                .await
                .unwrap();
            cluster
                .create_file_auto_id(
                    dir_b.attr.inode_id,
                    &format!("b_{:02}.obj", i),
                    50,
                    0o644,
                    512,
                )
                .await
                .unwrap();
        }

        // Execute parameterized SQL over real gRPC `MetadataServiceClient`!
        let mut meta_client = MetadataServiceClient::connect(url).await.unwrap();
        let sql_resp = meta_client
            .execute_sql(ExecuteSqlRequest {
                sql: "SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'".to_string(),
                params: vec![SqlParamValue {
                    kind: Some(sql_param_value::Kind::U64Val(dir_a.attr.inode_id)),
                }],
                caller_hlc: 0,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(sql_resp.error.is_none());
        assert_eq!(sql_resp.single_attr.unwrap().num_children, 10);

        cluster.shutdown().await.unwrap();
        (dir_a.attr.inode_id, dir_b.attr.inode_id)
    };

    // Stage 2: Reopen from disk, verify crash-safe persistence, trigger gRPC `PID` split,
    //          run cross-shard gRPC `Rename`, and verify bulk-delete `LocalityCompactor`!
    {
        let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
        let url = format!("http://{}", addr);

        let mut cp_client = ControlPlaneServiceClient::connect(url.clone())
            .await
            .unwrap();
        let mut meta_client = MetadataServiceClient::connect(url.clone()).await.unwrap();
        let mut tx_client = TransactionServiceClient::connect(url).await.unwrap();

        // Verify persisted directory attributes survived restart!
        let stat_a = meta_client
            .dir_stat(DirStatRequest {
                header: None,
                pid: dir_a_id,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(stat_a.attr.unwrap().num_children, 10);

        // Trigger online `PID`-boundary shard split over gRPC `ControlPlaneService`!
        let split_resp = cp_client
            .trigger_shard_split(TriggerShardSplitRequest { shard_id: 1 })
            .await
            .unwrap()
            .into_inner();
        assert!(split_resp.error.is_none());
        assert!(split_resp.split_executed);

        let routes = cp_client
            .get_routes(GetRoutesRequest { cell_id: 1 })
            .await
            .unwrap()
            .into_inner();
        assert!(routes.shards.len() >= 4);

        // Execute cross-shard 2PC POSIX overwrite `Rename` over gRPC `TransactionService`!
        let rename_resp = tx_client
            .rename(RenameRequest {
                src_pid: dir_a_id,
                src_name: "a_00.obj".to_string(),
                dst_pid: dir_b_id,
                dst_name: "b_00.obj".to_string(),
                overwrite: true,
                caller_hlc: 0,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(rename_resp.error.is_none());
        let renamed_fid = rename_resp.entry.unwrap().attr.unwrap().fid;

        // One aux propose can miss while the background drain is in flight.
        // Retry until the overwrite delta is visible, then assert the final values.
        let mut used_bytes = None;
        let mut index_pid = None;
        for _ in 0..8 {
            cluster.run_maintenance_tick().await.unwrap();
            let quota_resp = meta_client
                .get_quota(GetQuotaRequest {
                    header: None,
                    uid: 50,
                })
                .await
                .unwrap()
                .into_inner();
            assert!(quota_resp.found);
            used_bytes = quota_resp.record.map(|record| record.used_bytes);
            let idx_resp = meta_client
                .get_secondary_index(GetSecondaryIndexRequest {
                    header: None,
                    fid: renamed_fid,
                })
                .await
                .unwrap()
                .into_inner();
            if idx_resp.found {
                index_pid = idx_resp.record.map(|record| record.pid);
            }
            // 20 initial files - 1 overwritten file = 19 live files * 512 bytes.
            if used_bytes == Some(19 * 512) && index_pid == Some(dir_b_id) {
                break;
            }
        }
        assert_eq!(used_bytes, Some(19 * 512));
        assert_eq!(index_pid, Some(dir_b_id));

        // Consecutive bulk TTL deletion test on `dir_b_id`: create 25 TTL files, flush to L1,
        // delete 22 of them (`>= locality_tombstone_threshold = 20`), flush, and verify 0 tombstones!
        for i in 0..25 {
            cluster
                .create_file_auto_id(dir_b_id, &format!("ttl_{:03}.tmp", i), 50, 0o644, 128)
                .await
                .unwrap();
        }
        cluster.flush_all().await.unwrap();
        for i in 0..22 {
            cluster
                .tx_router()
                .unlink(dir_b_id, &format!("ttl_{:03}.tmp", i))
                .await
                .unwrap();
        }
        cluster.flush_all().await.unwrap();

        use metadb_tx::two_pc::ShardDirectory;
        let dir_b_child_key = metadb_core::MetaKey::child(dir_b_id, "ttl_000.tmp").encode();
        let (_, dir_b_grp, _) = cluster
            .global_manager()
            .route_user_key(&dir_b_child_key)
            .unwrap();
        let tombs_before_list = dir_b_grp.engine().metrics().tombstones_traversed_on_scans;

        let list_resp = meta_client
            .list_dir(ListDirRequest {
                header: None,
                pid: dir_b_id,
                after_name: String::new(),
                name_prefix: "ttl_".to_string(),
                limit: 100,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(list_resp.entries.len(), 3);

        let tombs_after_list = dir_b_grp.engine().metrics().tombstones_traversed_on_scans;
        assert_eq!(
            tombs_after_list - tombs_before_list,
            0,
            "LocalityCompactor must purge all 22 TTL tombstones so ListDir traverses zero tombstones"
        );

        let prom = render_prometheus_metrics(&cluster.metrics_snapshot());
        assert!(prom.contains("metadb_locality_compactions_total"));
        assert!(cluster.metrics_snapshot().locality_compactions_total >= 1);

        tokio::time::sleep(Duration::from_millis(20)).await;
        cluster.shutdown().await.unwrap();
    }

    // --- Phase C: Verify `MetaDbEngine::open(MetaDbConfig::from_toml_file(...)?).await?` SDK entry point ---
    {
        let toml_path = dir.path().join("standalone.toml");
        std::fs::write(&toml_path, &toml_text).unwrap();
        let engine = MetaDbEngine::open(MetaDbConfig::from_toml_file(&toml_path).unwrap())
            .await
            .unwrap();
        let stat_b = engine.tx_router().dirstat(dir_b_id).await.unwrap();
        assert_eq!(stat_b.num_children, 13);
        tokio::time::sleep(Duration::from_millis(20)).await;
        engine.shutdown().await.unwrap();
    }
}
