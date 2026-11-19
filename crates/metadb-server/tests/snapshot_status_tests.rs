//! A missing snapshot must not leave cluster status advertising a live voter.

use std::fs;

use metadb_core::ROOT_INODE_ID;
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use openraft::storage::RaftStateMachine;

#[tokio::test]
async fn unrecoverable_snapshot_is_not_reported_as_a_live_voter() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.sharding.bootstrap_table_shards = false;
    let (cluster, _address) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    cluster
        .tx_router()
        .mkdir(ROOT_INODE_ID, "bucket", 100, 1, 0o755)
        .await
        .unwrap();
    cluster
        .tx_router()
        .create_file(100, "alpha.dat", 1001, 7, 1, 0o644, 4)
        .await
        .unwrap();

    let (_, group, _) = cluster.cell_manager().lookup_local_handle(1).unwrap();
    let purged = group.flush_snapshot_and_purge().await.unwrap();
    assert!(purged > 0);
    let live = cluster.cell_manager().routes_for_live_status().unwrap();
    assert!(
        live.iter()
            .any(|route| { route.shard_id == 1 && route.peers.values().any(|(_, voter)| *voter) }),
        "a healthy shard is still a live voter"
    );

    let shard_dir = cluster.cell_manager().host().shard_dir(1);
    let mut removed_sst = 0usize;
    for entry in fs::read_dir(shard_dir.join("lsm")).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_str().unwrap_or("").ends_with(".sst") {
            fs::remove_file(entry.path()).unwrap();
            removed_sst += 1;
        }
    }
    assert!(removed_sst > 0, "flush did not create an SSTable");
    let _ = fs::remove_dir_all(shard_dir.join("raft/snapshots"));

    let mut state_machine = group.state_machine().clone();
    let error = state_machine.get_current_snapshot().await.unwrap_err();
    assert!(
        error.to_string().contains("snapshot unrecoverable"),
        "{error}"
    );
    let status = cluster.cell_manager().routes_for_live_status().unwrap_err();
    let status = status.to_string();
    assert!(
        status.contains("not a live voter"),
        "cluster status must not keep advertising voters after the snapshot and LSM are gone: {status}"
    );
    cluster.shutdown().await.unwrap();
}
