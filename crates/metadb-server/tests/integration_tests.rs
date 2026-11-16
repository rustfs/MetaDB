use std::sync::Arc;
use std::thread;

use metadb_cluster::{ShardHealthState, ShardSplitMode};
use metadb_core::{
    HlcTimestamp, IntentOp, MetaDbError, MetaKey, QuotaKey, WriteIntentKey, WriteIntentRecord,
    ROOT_INODE_ID,
};
use metadb_proto::transaction_service_server::TransactionService;
use metadb_proto::{Prepare2pcRequest, RequestRoutingHeader};
use metadb_server::{
    MetaDbConfig, MetaDbEngine, MetaDbGrpcService, PersistentMetaDbCluster, ServerTomlConfig,
    SqlQueryResult,
};
use metadb_tx::AuxUpdateTask;
use tempfile::tempdir;
use tonic::Request;

// T1–T6 exercise `MetaDbEngine::new_single_node`, an in-memory simulator
// (`InMemoryBackend` + `VecDeque` log). They are not crash-recovery proof of the
// OpenRaft + LSM engine. `test_persistent_restart_lookup_dirstat_quota` is.

/// T1: Verifies that Policy/Size-Based Sharding splits shards strictly on `group_key = PID`
/// boundaries and never separates `(PID, "_attr_")` from `(PID, child_name)`.
#[test]
fn test_t1_pid_boundary_sharding_preserves_directory_colocation() {
    let config = MetaDbConfig {
        initial_node_id: 1,
        // Small threshold (1.5 KiB) so creating multiple directories triggers automatic PID-boundary splits.
        shard_split_threshold_bytes: 1536,
        mvcc_retention_ms: 5_000,
        tombstone_density_threshold: 16,
    };
    let db = MetaDbEngine::new_single_node(config);

    // Create 4 subdirectories under root (PIDs 10, 20, 30, 40) and populate files in each.
    for (idx, &dir_id) in [10u64, 20, 30, 40].iter().enumerate() {
        db.mkdir(ROOT_INODE_ID, &format!("dir_{}", idx), dir_id, 100, 0o755)
            .unwrap();
        for f in 0..8 {
            let inode_id = dir_id * 100 + f;
            let fid = dir_id * 1_000 + f;
            db.create_file(
                dir_id,
                &format!("file_{:02}.dat", f),
                inode_id,
                fid,
                100,
                0o644,
                4096,
            )
            .unwrap();
        }
    }

    let shards = db.list_all_shards();
    assert!(
        shards.len() >= 2,
        "expected shard split across multiple PIDs, got {} shard(s)",
        shards.len()
    );

    // Verify that for every directory PID, its `(PID, "_attr_")` record and all 8 child files
    // reside on the exact same shard and are returned intact by `dirstat` and `listdir`.
    for &dir_id in &[10u64, 20, 30, 40] {
        let stat = db.dirstat(dir_id).unwrap();
        assert_eq!(stat.num_children, 8);
        assert_eq!(stat.size_bytes, 8 * 4096);

        let children = db.listdir(dir_id).unwrap();
        assert_eq!(children.len(), 8);

        let route = db
            .global_manager()
            .meta_cache()
            .lookup_pid(dir_id)
            .expect("route must be cached in MetaCache");
        assert!(dir_id >= route.start_pid && dir_id < route.end_pid);
    }
}

/// T2: Verifies that an unsplittable single-directory Jumbo Shard is detected, isolated onto
/// a dedicated BE node (`ShardHealthState::JumboIsolated`), and supports `DualModeFineGrained` fallback.
#[test]
fn test_t2_jumbo_shard_isolation_and_dual_mode_fallback() {
    let config = MetaDbConfig {
        initial_node_id: 1,
        shard_split_threshold_bytes: 1200,
        mvcc_retention_ms: 5_000,
        tombstone_density_threshold: 16,
    };
    let db = MetaDbEngine::new_single_node(config);

    // Register node 2 and node 3 in Cell 1 so Jumbo Shard isolation can assign a dedicated node.
    let cell = db.global_manager().cells()[0].clone();
    cell.register_node(2);
    cell.register_node(3);

    // Insert many files into ROOT_INODE_ID (single PID = 1) so the root shard only contains PID 1.
    for i in 0..20 {
        db.create_file(
            ROOT_INODE_ID,
            &format!("model_ckpt_{:04}.pt", i),
            1000 + i,
            2000 + i,
            42,
            0o644,
            1024,
        )
        .unwrap();
    }

    let shards = cell.list_shards();
    assert_eq!(
        shards.len(),
        1,
        "single-PID shard must not split mid-directory in DirectoryColocated mode"
    );
    assert_eq!(
        shards[0].health_state,
        ShardHealthState::JumboIsolated {
            dedicated_node: 3,
            dominant_pid: ROOT_INODE_ID,
        }
    );

    // Operator fallback: enable DualModeFineGrained on the jumbo shard.
    cell.enable_dual_mode_split(shards[0].shard_id, "model_ckpt_0010.pt".to_string())
        .unwrap();
    let updated = cell.list_shards();
    assert_eq!(updated[0].split_mode, ShardSplitMode::DualModeFineGrained);
}

/// T3: Verifies conflict-free asynchronous parent attribute aggregation and linearizable
/// Read-Path Reconciliation under concurrent single-directory file creation and flushes
/// (including Goldfish Finding F1 atomic `applied_lsn` handoff).
#[test]
fn test_t3_conflict_free_async_attr_aggregation_and_read_reconciliation() {
    let db = Arc::new(MetaDbEngine::new_single_node(MetaDbConfig::default()));
    db.mkdir(ROOT_INODE_ID, "ml_checkpoints", 500, 7, 0o755)
        .unwrap();

    // Spawn 8 concurrent worker threads creating 25 files each (200 files total) in the same directory `PID = 500`.
    let mut handles = Vec::new();
    for t in 0..8u64 {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..25u64 {
                let seq = t * 100 + i;
                db_clone
                    .create_file(
                        500,
                        &format!("worker_{}_step_{}.ckpt", t, i),
                        10_000 + seq,
                        20_000 + seq,
                        7,
                        0o644,
                        512,
                    )
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // Before background flush: pending queue holds 200 attribute deltas, and `dirstat`
    // reconciles them on the read path with zero loss or double-counting.
    assert!(db.pushdown().attr_aggregator().pending_count() >= 200);
    let stat_before_flush = db.dirstat(500).unwrap();
    assert_eq!(stat_before_flush.num_children, 200);
    assert_eq!(stat_before_flush.size_bytes, 200 * 512);

    // Run background maintenance (flushes aggregated deltas into `(500, "_attr_")`) and
    // verify `dirstat` returns the exact same count after flush (no double-counting).
    db.run_background_maintenance().unwrap();
    assert_eq!(db.pushdown().attr_aggregator().pending_count(), 0);

    let stat_after_flush = db.dirstat(500).unwrap();
    assert_eq!(stat_after_flush.num_children, 200);
    assert_eq!(stat_after_flush.size_bytes, 200 * 512);
    assert!(stat_after_flush.applied_lsn > 0);
}

/// T4: Verifies decoupled asynchronous `QuotaTable` and `SecondaryIndexTable` updates,
/// exact-once idempotency under duplicate Raft log replays, and cross-shard `rename` (Goldfish Finding F3).
#[test]
fn test_t4_decoupled_async_auxiliary_idempotency_and_cross_shard_rename() {
    let config = MetaDbConfig {
        initial_node_id: 1,
        shard_split_threshold_bytes: 1024,
        mvcc_retention_ms: 5_000,
        tombstone_density_threshold: 16,
    };
    let db = MetaDbEngine::new_single_node(config);

    db.mkdir(ROOT_INODE_ID, "src_dir", 100, 99, 0o755).unwrap();
    db.mkdir(ROOT_INODE_ID, "dst_dir", 900, 99, 0o755).unwrap();

    db.create_file(100, "dataset.parquet", 1001, 7777, 99, 0o644, 8192)
        .unwrap();
    db.run_background_maintenance().unwrap();

    // Verify QuotaTable and SecondaryIndexTable were updated asynchronously.
    let quota = db.get_user_quota(99).expect("quota record must exist");
    assert_eq!(quota.used_bytes, 8192);
    // 2 directories + 1 file = 3 inodes for user 99.
    assert_eq!(quota.used_inodes, 3);

    let sec_idx = db
        .get_secondary_index(7777)
        .expect("secondary index record must exist");
    assert_eq!(sec_idx.pid, 100);
    assert_eq!(sec_idx.name, "dataset.parquet");

    // Simulate duplicate Raft log replay of an already-applied QuotaDelta (LSN = 1):
    // Must be suppressed by `applied_lsn_by_shard` watermark!
    let applied = db
        .pushdown()
        .aux_sink()
        .apply_task_idempotent(AuxUpdateTask::QuotaDelta {
            origin_shard_id: 1,
            lsn: 1,
            hlc: HlcTimestamp::new(1, 0),
            uid: 99,
            delta_bytes: 8192,
            delta_inodes: 1,
        });
    assert!(!applied, "duplicate Raft log replay must be suppressed");
    assert_eq!(db.get_user_quota(99).unwrap().used_bytes, 8192);

    // Execute cross-directory `rename` from (100, "dataset.parquet") -> (900, "renamed.parquet").
    db.rename(100, "dataset.parquet", 900, "renamed.parquet")
        .unwrap();
    db.run_background_maintenance().unwrap();

    assert!(db.lookup(100, "dataset.parquet").unwrap().is_none());
    assert!(db.lookup(900, "renamed.parquet").unwrap().is_some());
    assert_eq!(db.dirstat(100).unwrap().num_children, 0);
    assert_eq!(db.dirstat(900).unwrap().num_children, 1);

    let updated_idx = db.get_secondary_index(7777).unwrap();
    assert_eq!(updated_idx.pid, 900);
    assert_eq!(updated_idx.name, "renamed.parquet");
}

/// T5: Verifies Lifecycle-Aware Storage Engine: In-Memory MVCC double-tombstone elimination,
/// active-snapshot retention within the 5s window, and Locality-Aware Compaction during
/// consecutive bulk TTL deletions (Goldfish Finding F4).
#[test]
fn test_t5_in_memory_mvcc_and_locality_aware_compaction_on_bulk_deletes() {
    let config = MetaDbConfig {
        initial_node_id: 1,
        shard_split_threshold_bytes: 10 * 1024 * 1024,
        mvcc_retention_ms: 5_000,
        tombstone_density_threshold: 25,
    };
    let db = MetaDbEngine::new_single_node(config);
    db.mkdir(ROOT_INODE_ID, "ttl_bucket", 300, 1, 0o755)
        .unwrap();

    // Create 60 time-prefixed objects and delete the first 50 consecutively.
    for i in 0..60u64 {
        db.create_file(
            300,
            &format!("20261105_obj_{:04}", i),
            5000 + i,
            6000 + i,
            1,
            0o644,
            256,
        )
        .unwrap();
    }
    for i in 0..50u64 {
        db.unlink(300, &format!("20261105_obj_{:04}", i)).unwrap();
    }

    // Flush In-Memory MVCC to persistent storage:
    // - MVCC tombstones are pruned in memory (not written as versioned values).
    // - Physical LSM tombstones exceed `tombstone_density_threshold = 25` for PID 300,
    //   automatically triggering Locality-Aware Compaction on `[PID=300, PID=301)`!
    db.run_background_maintenance().unwrap();

    let (_, _, storage) = db.global_manager().route_pid(300).unwrap();
    let metrics = storage.metrics();
    assert!(
        metrics.pruned_mvcc_versions >= 50,
        "expected >=50 MVCC versions/tombstones pruned in memory, got {}",
        metrics.pruned_mvcc_versions
    );
    assert!(
        metrics.compaction_stats.localized_compactions_triggered >= 1,
        "expected locality-aware compaction to trigger on bulk deletion bucket"
    );
    assert_eq!(
        metrics.active_physical_tombstones, 0,
        "all physical tombstones in PID 300 should be purged by locality-aware compaction"
    );

    // Subsequent `listdir(300)` returns the remaining 10 live files and traverses 0 physical tombstones!
    let live = db.listdir(300).unwrap();
    assert_eq!(live.len(), 10);
    let metrics_after_scan = storage.metrics();
    assert_eq!(metrics_after_scan.tombstones_traversed_on_scans, 0);

    // Verify RaftLog safe truncation watermark advanced across storage, attr_aggregator, and aux_sink.
    let watermarks = storage.raft_log().watermarks();
    assert!(watermarks.safe_truncate_lsn() > 0);
    assert_eq!(storage.raft_log().retained_len(), 0);

    // Verify expired snapshot (>5s older than latest HLC) is deterministically rejected.
    let expired_hlc = HlcTimestamp::new(1, 0);
    let key_bytes = MetaKey::dir_attr(300).encode();
    let err = storage.get(&key_bytes, expired_hlc).unwrap_err();
    assert!(matches!(err, MetaDbError::TransactionTimeout { .. }));
}

/// T6: Verifies seamless three-stage evolution (Single-Node 1-Voter -> Multi-Shard ->
/// 3-Voter Distributed Multi-Cell Cluster), authoritative `rmdir` emptiness check (Goldfish Finding F2),
/// reserved `"_attr_"` protection (Goldfish Finding F6), and lightweight SQL template execution.
#[test]
fn test_t6_single_node_to_distributed_multi_cell_evolution_and_sql_proxy() {
    let db = MetaDbEngine::new_single_node(MetaDbConfig {
        initial_node_id: 1,
        shard_split_threshold_bytes: 2048,
        mvcc_retention_ms: 5_000,
        tombstone_density_threshold: 32,
    });

    // 1. Verify reserved `"_attr_"` name is rejected.
    let reserved_err = db
        .create_file(ROOT_INODE_ID, "_attr_", 99, 99, 1, 0o644, 10)
        .unwrap_err();
    assert_eq!(
        reserved_err,
        MetaDbError::ReservedEntryName("_attr_".to_string())
    );

    // 2. Execute metadata operations via the Lightweight SQL Template Proxy.
    db.execute_sql(
        "INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (1, 'logs', 200, 'DIR', 10, 493);",
    )
    .unwrap();
    db.execute_sql(
        "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (200, 'access.log', 201, 'FILE', 9001, 10, 420, 16384);",
    )
    .unwrap();

    // 3. Verify `rmdir` rejects removing non-empty directory `logs` (`PID = 200`) even before
    // background attribute aggregation flushes (Goldfish Finding F2).
    let rmdir_err = db
        .execute_sql("RMDIR FROM MetaTable WHERE pid = 1 AND name = 'logs';")
        .unwrap_err();
    assert_eq!(
        rmdir_err,
        MetaDbError::DirectoryNotEmpty {
            dir_id: 200,
            child_count: 1
        }
    );

    // 4. Online Scale-Out from Stage 1 (`1-Voter` Single-Node) to Stage 3 (`3-Voter` Multi-Raft):
    let cell_1 = db.global_manager().cells()[0].clone();
    assert_eq!(cell_1.list_shards()[0].membership.voter_count(), 1);

    cell_1.register_node(2);
    cell_1.register_node(3);
    let upgraded = cell_1.scale_out_raft_replicas(3).unwrap();
    assert!(upgraded >= 1);
    assert_eq!(cell_1.list_shards()[0].membership.voter_count(), 3);

    // 5. Query via SQL template after online 3-replica upgrade, then unlink file and rmdir directory.
    let stat_res = db
        .execute_sql("SELECT * FROM MetaTable WHERE pid = 200 AND name = '_attr_';")
        .unwrap();
    match stat_res {
        SqlQueryResult::Attr(attr) => {
            assert_eq!(attr.num_children, 1);
            assert_eq!(attr.size_bytes, 16384);
        }
        other => panic!("unexpected result: {:?}", other),
    }

    db.execute_sql("DELETE FROM MetaTable WHERE pid = 200 AND name = 'access.log';")
        .unwrap();
    db.execute_sql("RMDIR FROM MetaTable WHERE pid = 1 AND name = 'logs';")
        .unwrap();

    let lookup_res = db
        .execute_sql("SELECT * FROM MetaTable WHERE pid = 1 AND name = 'logs';")
        .unwrap();
    assert_eq!(lookup_res, SqlQueryResult::OptionalEntry(None));
}

fn persistent_cfg(dir: &std::path::Path) -> ServerTomlConfig {
    let toml_text = format!(
        r#"
[node]
node_id = 1
cell_id = 1
role = "all-in-one"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296
"#,
        dir.display()
    );
    ServerTomlConfig::from_toml_str(&toml_text).unwrap()
}

/// Opens a real data directory, kills the process, and checks lookup / dirstat / quota.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_persistent_restart_lookup_dirstat_quota() {
    let dir = tempdir().unwrap();
    let cfg = persistent_cfg(dir.path());
    let (dir_id, file_name) = {
        let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
            .await
            .unwrap();
        let dir_ent = cluster
            .mkdir_auto_id(ROOT_INODE_ID, "bucket", 9, 0o755)
            .await
            .unwrap();
        let file_name = "payload.bin";
        cluster
            .create_file_auto_id(dir_ent.attr.inode_id, file_name, 9, 0o644, 4096)
            .await
            .unwrap();
        cluster.run_maintenance_tick().await.unwrap();
        let quota = cluster.tx_router().get_quota(9).await.unwrap().unwrap();
        assert!(quota.used_bytes >= 4096);
        cluster.shutdown().await.unwrap();
        (dir_ent.attr.inode_id, file_name.to_string())
    };

    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let tx = cluster.tx_router();
    let stat = tx.dirstat(dir_id).await.unwrap();
    assert_eq!(stat.num_children, 1);
    assert_eq!(stat.size_bytes, 4096);
    let found = tx.lookup(dir_id, &file_name).await.unwrap();
    assert!(found.is_some());
    let quota = tx
        .get_quota(9)
        .await
        .unwrap()
        .expect("quota must survive restart");
    assert!(quota.used_bytes >= 4096);
    cluster.shutdown().await.unwrap();
}

/// After many committed writes, one maintenance tick must drop retained Raft entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_maintenance_tick_purges_raft_log() {
    let dir = tempdir().unwrap();
    let cfg = persistent_cfg(dir.path());
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
        .await
        .unwrap();
    for i in 0..24 {
        cluster
            .create_file_auto_id(ROOT_INODE_ID, &format!("f{i:02}.dat"), 3, 0o644, 64)
            .await
            .unwrap();
    }
    let before: usize = cluster
        .cell_manager()
        .host()
        .all_groups()
        .iter()
        .map(|g| g.log_store().retained_entry_count())
        .sum();
    assert!(
        before > 8,
        "expected a non-trivial raft log, retained {before}"
    );
    cluster.run_maintenance_tick().await.unwrap();
    let after: usize = cluster
        .cell_manager()
        .host()
        .all_groups()
        .iter()
        .map(|g| g.log_store().retained_entry_count())
        .sum();
    assert!(
        after < before,
        "maintenance must purge raft logs: before {before} after {after}"
    );
    cluster.shutdown().await.unwrap();
    drop(cluster);

    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let snapshot_floor: u64 = cluster
        .cell_manager()
        .host()
        .all_groups()
        .iter()
        .map(|g| g.watermarks().snapshot_values().2)
        .max()
        .unwrap_or(0);
    assert!(
        snapshot_floor > 0,
        "reopen must restore last_snapshot_index from the MANIFEST flushed index"
    );
    cluster.shutdown().await.unwrap();
}

/// `Prepare2pc` must propose on `header.shard_id`, not on the shard that owns `primary_key`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_prepare2pc_uses_routing_header_shard() {
    let dir = tempdir().unwrap();
    let cfg = persistent_cfg(dir.path());
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let svc = MetaDbGrpcService::new(
        cluster.global_manager().clone(),
        cluster.tx_router().clone(),
        cluster.proxy_router().clone(),
    );

    let primary_key = MetaKey::child(1, "primary.txt").encode();
    let quota_key = QuotaKey { uid: 42 }.encode();
    let (_, quota_grp, _) = cluster.cell_manager().lookup_local_handle(2).unwrap();
    let (_, meta_grp, _) = cluster.cell_manager().lookup_local_handle(1).unwrap();
    let epoch = quota_grp.current_epoch();

    let quota_intent = WriteIntentKey::new(quota_key.clone()).encode();
    let prepared = svc
        .prepare2pc(Request::new(prepare_req(
            11,
            epoch,
            primary_key.clone(),
            quota_key,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(
        prepared.error.is_none(),
        "secondary prepare must land on the quota shard: {:?}",
        prepared.error
    );
    let stored = quota_grp
        .engine()
        .get(&quota_intent, HlcTimestamp(u64::MAX))
        .unwrap()
        .expect("quota shard must store the intent");
    assert_eq!(WriteIntentRecord::decode(&stored).unwrap().txn_id, 11);
    assert!(meta_grp
        .engine()
        .get(&quota_intent, HlcTimestamp(u64::MAX))
        .unwrap()
        .is_none());

    let meta_intent = WriteIntentKey::new(primary_key.clone()).encode();
    let rejected = svc
        .prepare2pc(Request::new(prepare_req(
            12,
            epoch,
            primary_key.clone(),
            primary_key,
        )))
        .await
        .unwrap()
        .into_inner();
    assert!(
        rejected.error.is_some(),
        "a meta-table intent is outside the quota shard"
    );
    assert!(meta_grp
        .engine()
        .get(&meta_intent, HlcTimestamp(u64::MAX))
        .unwrap()
        .is_none());

    cluster.shutdown().await.unwrap();
}

fn prepare_req(
    txn_id: u128,
    epoch: metadb_core::ShardEpoch,
    primary_key: Vec<u8>,
    intent_user_key: Vec<u8>,
) -> Prepare2pcRequest {
    let record = WriteIntentRecord {
        txn_id,
        primary_shard_hint: 1,
        primary_key: primary_key.clone(),
        start_hlc: HlcTimestamp::ZERO,
        expire_hlc: HlcTimestamp(u64::MAX),
        rename_fence_token: None,
        op: IntentOp::Put {
            value: b"prepared".to_vec(),
            attr_deltas: Vec::new(),
            aux_specs: Vec::new(),
        },
    };
    Prepare2pcRequest {
        header: Some(RequestRoutingHeader {
            shard_id: 2,
            expected_epoch: Some(epoch.into()),
            caller_hlc: 0,
        }),
        txn_id_be: txn_id.to_be_bytes().to_vec(),
        is_primary: false,
        primary_key: primary_key.clone(),
        all_participant_keys: vec![primary_key, intent_user_key.clone()],
        start_hlc: 0,
        expire_hlc: u64::MAX,
        has_rename_fence_token: false,
        rename_fence_token: 0,
        encoded_intent_keys: vec![WriteIntentKey::new(intent_user_key.clone()).encode()],
        encoded_intent_records: vec![record.encode()],
        expected_values: vec![],
    }
}
