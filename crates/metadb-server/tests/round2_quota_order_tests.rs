use metadb_core::{AuxOutboxEntry, AuxTaskPayload, HlcTimestamp};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use metadb_tx::TxRaftCmd;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_origin_quota_deltas_must_conserve_totals_under_reordered_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let (_, quota_group, _) = cluster.cell_manager().lookup_local_handle(2).unwrap();
    // Valid causal history: create on origin 1, rename to origin 4, unlink there.
    // Deliver the unlink's independent origin stream before the create stream.
    for entry in [
        AuxOutboxEntry {
            origin_shard_id: 4,
            origin_raft_index: 7,
            task_ordinal: 0,
            hlc: HlcTimestamp::new(200, 0),
            payload: AuxTaskPayload::QuotaDelta {
                uid: 9,
                delta_bytes: -100,
                delta_inodes: -1,
                prev_uid_cursor: (0, 0),
            },
        },
        AuxOutboxEntry {
            origin_shard_id: 1,
            origin_raft_index: 3,
            task_ordinal: 0,
            hlc: HlcTimestamp::new(100, 0),
            payload: AuxTaskPayload::QuotaDelta {
                uid: 9,
                delta_bytes: 100,
                delta_inodes: 1,
                prev_uid_cursor: (0, 0),
            },
        },
    ] {
        quota_group
            .propose(
                quota_group.current_epoch(),
                cluster.tx_router().clock().now(),
                bincode::serialize(&TxRaftCmd::ApplyAuxBatch {
                    entries: vec![entry],
                })
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let quota = cluster.tx_router().get_quota(9).await.unwrap().unwrap();
    cluster.shutdown().await.unwrap();
    assert_eq!(
        (quota.used_bytes, quota.used_inodes),
        (0, 0),
        "two valid per-origin streams have net zero, but delivery order changed quota"
    );
}
