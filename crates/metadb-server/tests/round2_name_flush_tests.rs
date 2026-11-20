use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_user_name_must_be_representable_in_sstable() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let created = cluster
        .create_file_auto_id(1, &"a".repeat(70_000), 7, 0o644, 1)
        .await;
    let flush = cluster.flush_all().await;
    let shutdown = cluster.shutdown().await;
    if shutdown.is_err() {
        cluster.cell_manager().host().shutdown_all().await.unwrap();
    }
    assert!(
        created.is_err() || (flush.is_ok() && shutdown.is_ok()),
        "accepted long name cannot flush: create_ok={}, flush={flush:?}, shutdown={shutdown:?}",
        created.is_ok()
    );
}

#[test]
fn maximum_name_fits_the_unshared_write_intent_key_on_disk() {
    use metadb_core::{HlcTimestamp, MetaKey, WriteIntentKey, MAX_USER_NAME_BYTES};
    use metadb_storage::{
        DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache,
    };
    use std::sync::Arc;
    let dir = tempfile::tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(1 << 20));
    let open = || {
        LifecycleStorageEngine::open_lsm(1, dir.path(), cache.clone(), LsmEngineConfig::default())
            .unwrap()
    };
    let key =
        WriteIntentKey::new(MetaKey::child(1, "a".repeat(MAX_USER_NAME_BYTES)).encode()).encode();
    assert_eq!(key.len(), u16::MAX as usize);
    let engine = open();
    engine.apply_committed_put(key.clone(), b"payload".to_vec(), HlcTimestamp(100), 1);
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: 1,
                flushed_max_hlc: HlcTimestamp(100),
                ..Default::default()
            },
            None,
        )
        .unwrap();
    drop(engine);
    let reopened = open();
    let snapshot = reopened.acquire_linearizable_snapshot(1).unwrap();
    assert_eq!(
        reopened.get_with_snapshot(&key, &snapshot).unwrap(),
        Some(b"payload".to_vec())
    );
}
