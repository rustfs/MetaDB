//! A second all-in-one cell can be registered after its routes cover the
//! requested range. This test does not split roles, move shard files, or
//! forward a key to the other process.

use std::sync::Arc;

use metadb_core::{QuotaKey, ROOT_INODE_ID};
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::{GetRoutesRequest, RegisterCellRequest};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use metadb_tx::ShardDirectory;
use tempfile::tempdir;

fn config(
    dir: &std::path::Path,
    node_id: u64,
    cell_id: u64,
    start: &str,
    end: &str,
) -> ServerTomlConfig {
    let mut cfg = ServerTomlConfig::default();
    cfg.node.node_id = node_id;
    cfg.node.cell_id = cell_id;
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    cfg.cluster.cell_start_key = start.into();
    cfg.cluster.cell_end_key = end.into();
    cfg
}

async fn open(
    dir: &std::path::Path,
    node_id: u64,
    cell_id: u64,
    start: &str,
    end: &str,
) -> (Arc<PersistentMetaDbCluster>, String) {
    PersistentMetaDbCluster::open_and_serve(config(dir, node_id, cell_id, start, end))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_cell_is_durable_only_after_exact_route_coverage() {
    let dir1 = tempdir().unwrap();
    let dir2 = tempdir().unwrap();
    let (cell1, addr1) = open(dir1.path(), 1, 1, "01", "02").await;
    let (cell2, addr2) = open(dir2.path(), 2, 2, "02", "03").await;
    let mut client = ControlPlaneServiceClient::connect(format!("http://{addr1}"))
        .await
        .unwrap();

    let denied = client
        .register_cell(RegisterCellRequest {
            cell_id: 2,
            start_key: vec![0x02],
            end_key: vec![0x03],
            cell_manager_endpoints: vec!["8.8.8.8:9".into()],
        })
        .await
        .unwrap()
        .into_inner();
    let denied = denied.error.expect("non-loopback").message;
    assert!(denied.contains("loopback"), "{denied}");
    assert_eq!(cell1.metrics_snapshot().cells_total, 1);

    let down = client
        .register_cell(RegisterCellRequest {
            cell_id: 3,
            start_key: vec![0x03],
            end_key: vec![0x04],
            cell_manager_endpoints: vec!["127.0.0.1:1".into()],
        })
        .await
        .unwrap()
        .into_inner();
    let down = down.error.expect("unreachable").message;
    assert!(down.contains("not reachable"), "{down}");
    assert_eq!(cell1.metrics_snapshot().cells_total, 1);

    let partial = client
        .register_cell(RegisterCellRequest {
            cell_id: 2,
            start_key: vec![0x02],
            end_key: vec![0x04],
            cell_manager_endpoints: vec![addr2.clone()],
        })
        .await
        .unwrap()
        .into_inner();
    let partial = partial.error.expect("partial").message;
    assert!(partial.contains("partial"), "{partial}");
    assert!(cell1.global_manager().remote_publication(2).is_none());

    let registered = client
        .register_cell(RegisterCellRequest {
            cell_id: 2,
            start_key: vec![0x02],
            end_key: vec![0x03],
            cell_manager_endpoints: vec![addr2.clone()],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(registered.error.is_none(), "{:?}", registered.error);
    assert_eq!(cell1.metrics_snapshot().cells_total, 2);

    let remote = client
        .get_routes(GetRoutesRequest { cell_id: 2 })
        .await
        .unwrap()
        .into_inner();
    assert!(remote.error.is_none(), "{:?}", remote.error);
    assert_eq!(remote.shards.len(), 1);
    assert_eq!(remote.shards[0].cell_id, 2);
    let all = client
        .get_routes(GetRoutesRequest { cell_id: 0 })
        .await
        .unwrap()
        .into_inner();
    assert!(all.error.is_none(), "{:?}", all.error);
    assert!(all.shards.iter().any(|shard| shard.cell_id == 1));
    assert!(all.shards.iter().any(|shard| shard.cell_id == 2));

    let quota = QuotaKey { uid: 7 }.encode();
    cell2
        .global_manager()
        .route_user_key(&quota)
        .expect("cell 2 serves its quota range");
    let refused = match cell1.global_manager().route_user_key(&quota) {
        Ok(_) => panic!("cell 1 must not forward a remote key"),
        Err(error) => error,
    };
    assert!(
        refused.to_string().contains("does not forward"),
        "{refused}"
    );
    cell1
        .tx_router()
        .dirstat(ROOT_INODE_ID)
        .await
        .expect("meta root stays on cell 1");

    let overlap = client
        .register_cell(RegisterCellRequest {
            cell_id: 3,
            start_key: vec![0x01],
            end_key: vec![0x02],
            cell_manager_endpoints: vec![addr2.clone()],
        })
        .await
        .unwrap()
        .into_inner();
    let overlap = overlap.error.expect("overlap").message;
    assert!(overlap.contains("overlaps"), "{overlap}");
    assert_eq!(cell1.metrics_snapshot().cells_total, 2);

    cell2.shutdown().await.unwrap();
    drop(cell2);
    let replay = client
        .register_cell(RegisterCellRequest {
            cell_id: 2,
            start_key: vec![0x02],
            end_key: vec![0x03],
            cell_manager_endpoints: vec![addr2],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        replay.error.is_none(),
        "an identical registration keeps the stored publication when the peer is down: {:?}",
        replay.error
    );

    cell1.shutdown().await.unwrap();
    drop(cell1);
    drop(client);
    let (cell1, addr1) = open(dir1.path(), 1, 1, "01", "02").await;
    let mut client = ControlPlaneServiceClient::connect(format!("http://{addr1}"))
        .await
        .unwrap();
    let restored = client
        .get_routes(GetRoutesRequest { cell_id: 2 })
        .await
        .unwrap()
        .into_inner();
    assert!(restored.error.is_none(), "{:?}", restored.error);
    assert_eq!(restored.shards.len(), 1);
    assert_eq!(cell1.metrics_snapshot().cells_total, 2);
    let still_refused = match cell1.global_manager().route_user_key(&quota) {
        Ok(_) => panic!("restart must not start forwarding"),
        Err(error) => error,
    };
    assert!(still_refused.to_string().contains("does not forward"));
    assert!(!metadb_cluster::automatic_leadership_transfer_allowed());
    assert!(!metadb_cluster::heartbeat_is_scheduling_truth());
    cell1.shutdown().await.unwrap();
}
