//! Same-machine, in-process scale-in of three localhost voters.
//!
//! Three `PersistentMetaDbCluster` servers run inside this test process and talk over
//! localhost gRPC. `DurableCellManager::scale_out_replicas` shrinks the voter set from
//! the highest ids (3→2 drops node 3, 3→1 drops nodes 3 and 2). The test then shuts the
//! servers down and reopens the same data directories.
//!
//! This is a single-process test. It does not replace three-VM scale-in evidence.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use metadb_core::{make_file_inode_id, MetaDbError, ROOT_INODE_ID};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use tempfile::tempdir;

const BUCKET_ID: u64 = 10;
const FILE_BYTES: i64 = 64;

fn fast_config(dir: &Path, node_id: u64, join: &str) -> ServerTomlConfig {
    let toml_text = format!(
        r#"
[node]
node_id = {node_id}
cell_id = 1
role = "all-in-one"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[sharding]
bootstrap_table_shards = false
shard_split_threshold_bytes = 4294967296

[cluster]
join = "{join}"
"#,
        dir.display()
    );
    let mut cfg = ServerTomlConfig::from_toml_str(&toml_text).unwrap();
    cfg.raft_options.heartbeat_interval_ms = 80;
    cfg.raft_options.election_timeout_min_ms = 200;
    cfg.raft_options.election_timeout_max_ms = 400;
    cfg
}

fn raft_voters(cluster: &PersistentMetaDbCluster) -> BTreeSet<u64> {
    let routes = cluster.cell_manager().list_routes();
    assert!(!routes.is_empty(), "cell has no shard routes");
    let mut found: Option<BTreeSet<u64>> = None;
    for route in routes {
        let voters = cluster
            .cell_manager()
            .raft_voter_ids(route.shard_id)
            .unwrap_or_else(|err| panic!("raft voters for shard {}: {err}", route.shard_id));
        match &found {
            None => found = Some(voters),
            Some(previous) => assert_eq!(previous, &voters, "shard voter sets diverged"),
        }
    }
    found.unwrap()
}

fn persisted_voters(cluster: &PersistentMetaDbCluster) -> BTreeSet<u64> {
    let routes = cluster.cell_manager().list_routes();
    assert!(!routes.is_empty(), "cell has no shard routes");
    let mut found: Option<BTreeSet<u64>> = None;
    for route in routes {
        let voters = route
            .peers
            .iter()
            .filter(|(_, (_, is_voter))| *is_voter)
            .map(|(node_id, _)| *node_id)
            .collect::<BTreeSet<_>>();
        match &found {
            None => found = Some(voters),
            Some(previous) => {
                assert_eq!(previous, &voters, "persisted shard voter sets diverged")
            }
        }
    }
    found.unwrap()
}

async fn open_node(cfg: ServerTomlConfig) -> (Arc<PersistentMetaDbCluster>, String) {
    tokio::time::timeout(
        Duration::from_secs(90),
        PersistentMetaDbCluster::open_and_serve(cfg),
    )
    .await
    .expect("open_and_serve timed out")
    .expect("open_and_serve failed")
}

async fn current_leader(nodes: &[Arc<PersistentMetaDbCluster>]) -> Arc<PersistentMetaDbCluster> {
    for _ in 0..160 {
        for node in nodes {
            let Some(route) = node.cell_manager().list_routes().into_iter().next() else {
                continue;
            };
            let Ok((_, group, _)) = node.cell_manager().lookup_local_handle(route.shard_id) else {
                continue;
            };
            if group.linearizable_read(group.current_epoch()).await.is_ok() {
                return Arc::clone(node);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for a Raft leader");
}

fn is_transient(err: &MetaDbError) -> bool {
    matches!(
        err,
        MetaDbError::NotLeader { .. } | MetaDbError::EpochMismatch { .. }
    )
}

async fn create_on_leader(
    nodes: &[Arc<PersistentMetaDbCluster>],
    name: &str,
    inode_seq: u64,
    fid: u64,
    errors: &mut Vec<String>,
) {
    let mut last_transient = None;
    for _ in 0..160 {
        for node in nodes {
            match node
                .tx_router()
                .create_file(
                    BUCKET_ID,
                    name,
                    make_file_inode_id(inode_seq),
                    fid,
                    7,
                    0o644,
                    FILE_BYTES,
                )
                .await
            {
                Ok(_) => return,
                Err(err) if is_transient(&err) => last_transient = Some(err.to_string()),
                Err(err) => {
                    errors.push(format!(
                        "create {name} on node {}: {err}",
                        node.config().node.node_id
                    ));
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    errors.push(format!(
        "create {name} was not acknowledged ({})",
        last_transient.unwrap_or_else(|| "no response".to_string())
    ));
}

async fn expect_dirstat(
    nodes: &[Arc<PersistentMetaDbCluster>],
    label: &str,
    errors: &mut Vec<String>,
) {
    let mut last_transient = None;
    for _ in 0..160 {
        for node in nodes {
            match node.tx_router().dirstat(BUCKET_ID).await {
                Ok(stat) => {
                    if stat.num_children != 2 || stat.size_bytes != FILE_BYTES * 2 {
                        errors.push(format!(
                            "dirstat {label} on node {} children={} bytes={}",
                            node.config().node.node_id,
                            stat.num_children,
                            stat.size_bytes
                        ));
                    }
                    return;
                }
                Err(err) if is_transient(&err) => last_transient = Some(err.to_string()),
                Err(err) => {
                    errors.push(format!(
                        "dirstat {label} on node {}: {err}",
                        node.config().node.node_id
                    ));
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    errors.push(format!(
        "dirstat {label} was not acknowledged ({})",
        last_transient.unwrap_or_else(|| "no response".to_string())
    ));
}

async fn expect_readable(
    nodes: &[Arc<PersistentMetaDbCluster>],
    name: &str,
    errors: &mut Vec<String>,
) {
    let mut last_transient = None;
    for _ in 0..160 {
        for node in nodes {
            match node.tx_router().lookup(BUCKET_ID, name).await {
                Ok(Some(entry)) => {
                    if entry.attr.size_bytes != FILE_BYTES {
                        errors.push(format!(
                            "lookup {name} size {} on node {}",
                            entry.attr.size_bytes,
                            node.config().node.node_id
                        ));
                    }
                    return;
                }
                Ok(None) => {
                    last_transient =
                        Some(format!("missing on node {}", node.config().node.node_id));
                }
                Err(err) if is_transient(&err) => last_transient = Some(err.to_string()),
                Err(err) => {
                    errors.push(format!(
                        "lookup {name} on node {}: {err}",
                        node.config().node.node_id
                    ));
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    errors.push(format!(
        "lookup {name} was not readable ({})",
        last_transient.unwrap_or_else(|| "no response".to_string())
    ));
}

async fn shutdown_all(nodes: Vec<Arc<PersistentMetaDbCluster>>) {
    for node in &nodes {
        node.shutdown()
            .await
            .unwrap_or_else(|err| panic!("shutdown node {}: {err}", node.config().node.node_id));
    }
    drop(nodes);
}

async fn scale_in_and_reopen(target_voters: usize) {
    assert!(
        (1..=2).contains(&target_voters),
        "this fixture only shrinks three voters to 2 or 1"
    );
    let expected: BTreeSet<u64> = (1..=target_voters as u64).collect();
    let dir = tempdir().unwrap();
    let dir1 = dir.path().join("node1");
    let dir2 = dir.path().join("node2");
    let dir3 = dir.path().join("node3");
    let cfg1 = fast_config(&dir1, 1, "");

    let (node1, addr1) = open_node(cfg1.clone()).await;
    let mut errors = Vec::new();
    match node1
        .tx_router()
        .mkdir(ROOT_INODE_ID, "bucket", BUCKET_ID, 7, 0o755)
        .await
    {
        Ok(_) => {}
        Err(err) => errors.push(format!("mkdir bucket: {err}")),
    }
    create_on_leader(
        &[Arc::clone(&node1)],
        "before_scale_in.dat",
        100,
        500,
        &mut errors,
    )
    .await;

    let cfg2 = fast_config(&dir2, 2, &addr1);
    let cfg3 = fast_config(&dir3, 3, &addr1);
    let (node2, _) = open_node(cfg2.clone()).await;
    let (node3, _) = open_node(cfg3.clone()).await;
    let initial = [Arc::clone(&node1), Arc::clone(&node2), Arc::clone(&node3)];
    assert_eq!(
        raft_voters(&node1),
        BTreeSet::from([1, 2, 3]),
        "three localhost voters before scale-in"
    );

    let leader = current_leader(&initial).await;
    match leader
        .cell_manager()
        .scale_out_replicas(target_voters)
        .await
    {
        Ok(shards) => {
            if shards == 0 {
                errors.push("scale_out_replicas updated no shards".to_string());
            }
        }
        Err(err) => errors.push(format!(
            "scale_out_replicas({target_voters}) on node {}: {err}",
            leader.config().node.node_id
        )),
    }
    if errors.is_empty() {
        assert_eq!(
            raft_voters(&leader),
            expected,
            "leader voter set after shrink to {target_voters}"
        );
        assert_eq!(
            persisted_voters(&leader),
            expected,
            "leader persisted voter set after shrink to {target_voters}"
        );
    }

    let survivors: Vec<_> = initial
        .iter()
        .filter(|node| expected.contains(&node.config().node.node_id))
        .cloned()
        .collect();
    create_on_leader(&survivors, "after_scale_in.dat", 101, 501, &mut errors).await;
    expect_readable(&survivors, "before_scale_in.dat", &mut errors).await;
    expect_readable(&survivors, "after_scale_in.dat", &mut errors).await;
    expect_dirstat(&survivors, "before restart", &mut errors).await;

    // Shutdown, then drop every handle. The allocator lock lives until the last
    // `PersistentMetaDbCluster` is dropped, and reopen needs that lock and the ports.
    drop(survivors);
    drop(leader);
    drop(node1);
    drop(node2);
    drop(node3);
    shutdown_all(Vec::from(initial)).await;

    let (re1, re2, re3) = tokio::join!(open_node(cfg1), open_node(cfg2), open_node(cfg3));
    let reopened = [re1.0, re2.0, re3.0];
    let reopened_survivors: Vec<_> = reopened
        .iter()
        .filter(|node| expected.contains(&node.config().node.node_id))
        .cloned()
        .collect();
    for node in &reopened_survivors {
        let voters = raft_voters(node);
        if voters != expected {
            errors.push(format!(
                "reopened node {} voter set {voters:?}, want {expected:?}",
                node.config().node.node_id
            ));
        }
    }
    expect_readable(&reopened_survivors, "before_scale_in.dat", &mut errors).await;
    expect_readable(&reopened_survivors, "after_scale_in.dat", &mut errors).await;
    expect_dirstat(&reopened_survivors, "after restart", &mut errors).await;

    shutdown_all(reopened.to_vec()).await;
    assert!(
        errors.is_empty(),
        "application errors on the checked scale-in path (target {target_voters}): {errors:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_process_scale_in_three_to_two_survives_restart() {
    // One OS process, three localhost voters. Not a three-VM scale-in.
    scale_in_and_reopen(2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_process_scale_in_three_to_one_survives_restart() {
    // One OS process, three localhost voters. Not a three-VM scale-in.
    scale_in_and_reopen(1).await;
}
