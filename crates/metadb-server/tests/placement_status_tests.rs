//! Local placement rows on `metadb-ctl` and unlabeled Prometheus gauges.
//!
//! The rows come from `CELL_CONTROL_PLANE`. They are not routes, not a file move,
//! and not a split. Automatic leadership transfer stays off.

use std::io::{Read, Write};
use std::process::Command;

use metadb_server::{render_prometheus_metrics, PersistentMetaDbCluster, ServerTomlConfig};

async fn start(dir: &std::path::Path) -> (std::sync::Arc<PersistentMetaDbCluster>, String) {
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.node.metrics_listen_addr = Some("127.0.0.1:0".into());
    cfg.security.allow_unauthenticated = true;
    PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap()
}

fn ctl(address: &str, arguments: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_metadb-ctl"))
        .args(["--endpoint", address])
        .args(arguments)
        .output()
        .unwrap()
}

fn http_get(addr: &str, path: &str) -> String {
    let mut socket = std::net::TcpStream::connect(addr).unwrap();
    socket
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .unwrap();
    write!(
        socket,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut body = String::new();
    socket.read_to_string(&mut body).unwrap();
    body
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placement_rows_show_up_on_ctl_and_metrics_without_claiming_a_split() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, address) = start(dir.path()).await;
    let cell = cluster.cell_manager();
    let routes = cell.list_routes();
    assert!(routes.len() >= 2, "bootstrap table shards: {routes:?}");
    let leader_shard = routes[0].shard_id;
    let blocked_shard = routes[1].shard_id;

    let mut leader = None;
    for _ in 0..20 {
        let record = cell
            .reconcile_jumbo_placement(leader_shard, 800, 1)
            .await
            .unwrap();
        if record.phase.as_str() == "leader_on_dedicated" {
            leader = Some(record);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let leader = leader.expect("dedicated node 1 did not become the recorded leader");
    assert!(leader.committed_moves.is_empty(), "{leader:?}");
    assert_eq!(leader.last_transfer_attempt_unix_ms, 0);

    let blocked = cell
        .reconcile_jumbo_placement(blocked_shard, 800, 99)
        .await
        .unwrap();
    assert_eq!(
        blocked.phase.as_str(),
        "blocked",
        "{}",
        blocked.blocked_reason
    );
    assert!(blocked.blocked_reason.contains("not a current voter"));
    assert!(blocked.committed_moves.is_empty(), "{blocked:?}");

    let snap = cluster.metrics_snapshot();
    assert_eq!(snap.placement_records, 2);
    assert_eq!(snap.placement_phase_leader_on_dedicated, 1);
    assert_eq!(snap.placement_phase_blocked, 1);
    assert_eq!(snap.placement_phase_recorded, 0);
    assert_eq!(snap.placement_phase_dedicated_not_shared, 0);
    assert_eq!(snap.placement_moves_leadership, 0);
    assert_eq!(snap.placement_moves_voter_replaced, 0);
    assert_eq!(snap.placement_rows_with_transfer_attempt, 0);
    assert_eq!(snap.placement_rows_with_replace_attempt, 0);
    assert_eq!(snap.placement_automatic_transfer_enabled, 0);
    let rendered = render_prometheus_metrics(&snap);
    assert!(
        rendered.contains("metadb_placement_records 2\n"),
        "{rendered}"
    );
    assert!(
        rendered.contains("metadb_placement_phase_blocked 1\n"),
        "{rendered}"
    );
    assert!(!rendered.contains('{'), "{rendered}");

    let metrics_addr = cluster
        .metrics_addr()
        .expect("metrics listener")
        .to_string();
    let scraped = http_get(&metrics_addr, "/metrics");
    assert!(
        scraped.contains("metadb_placement_records 2\n"),
        "{scraped}"
    );
    assert!(
        scraped.contains("metadb_placement_automatic_transfer_enabled 0\n"),
        "{scraped}"
    );
    assert!(
        !scraped.contains("metadb_placement_phase_recorded 1"),
        "the reconciler does not publish Recorded: {scraped}"
    );

    let status = ctl(&address, &["cluster", "status"]);
    let status_text = String::from_utf8(status.stdout).unwrap();
    assert!(
        status.status.success(),
        "stdout={status_text} stderr={}",
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(status_text.contains("jumbo=false"), "{status_text}");
    assert!(
        status_text.contains("placement_automatic_transfer_enabled=false"),
        "{status_text}"
    );
    assert!(status_text.contains("Placements: 2"), "{status_text}");
    assert!(
        status_text.contains(&format!(
            "placement cell=1 shard={leader_shard} phase=leader_on_dedicated phase_code=2 dedicated_node=1 dominant_pid=800 move_count=0 moves= transfer_attempt_unix_ms=0"
        )),
        "{status_text}"
    );
    assert!(
        status_text.contains(&format!(
            "placement cell=1 shard={blocked_shard} phase=blocked phase_code=4 dedicated_node=99 dominant_pid=800 move_count=0 moves="
        )),
        "{status_text}"
    );
    assert!(status_text.contains("not a current voter"), "{status_text}");
    assert!(
        !status_text.contains("leadership_transferred"),
        "{status_text}"
    );

    let listed = ctl(&address, &["shard", "list", "--cell-id", "1"]);
    let listed_text = String::from_utf8(listed.stdout).unwrap();
    assert!(listed.status.success(), "{listed_text}");
    assert!(
        listed_text.contains("phase=leader_on_dedicated"),
        "{listed_text}"
    );
    assert!(listed_text.contains("phase=blocked"), "{listed_text}");

    let split = ctl(
        &address,
        &["shard", "split", "--shard-id", &leader_shard.to_string()],
    );
    let split_text = String::from_utf8(split.stdout).unwrap();
    assert!(
        split.status.success(),
        "stdout={split_text} stderr={}",
        String::from_utf8_lossy(&split.stderr)
    );
    assert!(split_text.contains("split_executed=false"), "{split_text}");
    assert!(!split_text.contains("split_executed=true"), "{split_text}");
    assert!(
        split_text.contains(&format!(
            "placement_phase=leader_on_dedicated placement_phase_code=2 placement_shard={leader_shard} dedicated_node=1 dominant_pid=800 move_count=0 placement_automatic_transfer_enabled=false"
        )),
        "{split_text}"
    );

    cluster.shutdown().await.unwrap();
}
