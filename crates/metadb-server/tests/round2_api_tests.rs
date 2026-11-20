use std::sync::Arc;

use metadb_core::codec;
use metadb_core::{CompositeKeyRange, HlcTimestamp, MetaKey, ShardEpoch};
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{
    sql_param_value, CreateFileRequest, ExecuteSqlRequest, ListDirRequest, SqlParamValue,
};
use metadb_proxy::SqlTemplateParser;
use metadb_raft::{MetaDbRaftRequest, ShardApplyHandler, ShardWatermarkTracker};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use metadb_storage::{LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{TxRaftCmd, TxStateMachineHandler};

async fn start(
    dir: &std::path::Path,
) -> (
    Arc<PersistentMetaDbCluster>,
    MetadataServiceClient<tonic::transport::Channel>,
) {
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let client = MetadataServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    (cluster, client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_upsert_must_use_insert_value_for_a_missing_uid() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, mut client) = start(dir.path()).await;
    let sql = "INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (42, 100) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = 200";
    let response = client
        .execute_sql(ExecuteSqlRequest {
            sql: sql.into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let stored = cluster.tx_router().get_quota(42).await.unwrap().unwrap();
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        stored.quota_limit_bytes, 100,
        "absent UID took the ON CONFLICT branch: {response:?}"
    );
    let updated = client
        .execute_sql(ExecuteSqlRequest {
            sql: sql.into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(updated.error.is_none(), "{updated:?}");
    assert_eq!(
        cluster
            .tx_router()
            .get_quota(42)
            .await
            .unwrap()
            .unwrap()
            .quota_limit_bytes,
        200
    );
    cluster.shutdown().await.unwrap();
}

#[test]
fn sql_mode_must_reject_values_outside_the_wire_u32_domain() {
    for query in [
        "UPDATE MetaTable SET mode = 4294967296 WHERE pid = 1 AND name = 'victim'",
        "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (1, 'f', 9223372036854775809, 'FILE', 1, 7, 4294967296, 0)",
        "INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (1, 'd', 2, 'DIR', 7, 4294967296)",
    ] {
        let parsed = SqlTemplateParser::parse(query);
        assert!(parsed.is_err(), "mode silently wrapped modulo 2^32: {parsed:?}");
        assert!(SqlTemplateParser::parse(&query.replace("4294967296", "4294967295")).is_ok());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unset_sql_bind_must_not_be_removed_before_destructive_execution() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, mut client) = start(dir.path()).await;
    cluster
        .create_file_auto_id(1, "victim", 7, 0o644, 10)
        .await
        .unwrap();
    // Three wire bindings for two placeholders: the missing oneof is malformed,
    // and the third binding is extra. Neither may be silently discarded.
    let response = client
        .execute_sql(ExecuteSqlRequest {
            sql: "DELETE FROM MetaTable WHERE pid = ? AND name = ?".into(),
            params: vec![
                SqlParamValue {
                    kind: Some(sql_param_value::Kind::U64Val(1)),
                },
                SqlParamValue { kind: None },
                SqlParamValue {
                    kind: Some(sql_param_value::Kind::StrVal("victim".into())),
                },
            ],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let kept = cluster.tx_router().lookup(1, "victim").await.unwrap();
    cluster.shutdown().await.unwrap();
    assert!(
        response.error.is_some() && kept.is_some(),
        "malformed bind vector executed DELETE: response={response:?}; victim={kept:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn listdir_combined_cursor_and_prefix_must_seek_to_the_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, mut client) = start(dir.path()).await;
    for name in ["b", "z1", "z2"] {
        cluster
            .create_file_auto_id(1, name, 7, 0o644, 10)
            .await
            .unwrap();
    }
    let response = client
        .list_dir(ListDirRequest {
            pid: 1,
            after_name: "a".into(),
            name_prefix: "z".into(),
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    cluster.shutdown().await.unwrap();
    assert!(response.error.is_none(), "{response:?}");
    let names: Vec<_> = response
        .entries
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    assert_eq!(
        names,
        ["z1", "z2"],
        "scan stopped at nonmatching 'b': {response:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_create_must_reject_negative_file_size_before_persisting() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, mut client) = start(dir.path()).await;
    let response = client
        .create_file(CreateFileRequest {
            pid: 1,
            name: "negative".into(),
            inode_id: (1 << 63) | 100,
            fid: 100,
            uid: 7,
            mode: 0o644,
            size_bytes: -1,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let stored = cluster.tx_router().lookup(1, "negative").await.unwrap();
    cluster.shutdown().await.unwrap();
    assert!(
        response.error.is_some() && stored.is_none(),
        "negative file size accepted and persisted: response={response:?}; stored={stored:?}"
    );
}

fn apply(
    handler: &TxStateMachineHandler,
    engine: &LifecycleStorageEngine,
    index: u64,
    command: TxRaftCmd,
) -> metadb_core::Result<Vec<u8>> {
    let hlc = HlcTimestamp::new(1, index as u16);
    handler.apply_command(
        index,
        hlc,
        &MetaDbRaftRequest {
            expected_epoch: handler.current_epoch(),
            hlc,
            payload: codec::serialize(&command).unwrap(),
        },
        engine,
    )
}

#[test]
fn minimum_file_size_must_not_panic_after_unlink_has_already_deleted_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let engine = LifecycleStorageEngine::open_lsm(
        1,
        dir.path(),
        Arc::new(ShardedBlockCache::new(1024 * 1024)),
        LsmEngineConfig::default(),
    )
    .unwrap();
    let handler = TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        CompositeKeyRange::all_user_tables(),
        Arc::new(ShardWatermarkTracker::default()),
    );
    apply(
        &handler,
        &engine,
        1,
        TxRaftCmd::InitDirAttr {
            dir_id: 1,
            parent_pid: 1,
            uid: 7,
            mode: 0o755,
        },
    )
    .unwrap();
    let creation = apply(
        &handler,
        &engine,
        2,
        TxRaftCmd::CreateFile {
            pid: 1,
            name: "minimum".into(),
            inode_id: (1 << 63) | 100,
            fid: 100,
            uid: 7,
            mode: 0o644,
            size_bytes: i64::MIN,
        },
    );
    // Rejection at creation would satisfy the required safety property.
    if creation.is_err() {
        return;
    }
    let unlink = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        apply(
            &handler,
            &engine,
            3,
            TxRaftCmd::Unlink {
                pid: 1,
                name: "minimum".into(),
            },
        )
    }));
    let stored = engine
        .get(
            &MetaKey::child(1, "minimum").encode(),
            HlcTimestamp::new(1, 3),
        )
        .unwrap();
    assert!(
        unlink.is_ok(),
        "Unlink panicked for accepted i64::MIN size; key after panic={stored:?}"
    );
}
