//! A second MetaDB process joins an existing Raft group instead of bootstrapping its own cell.
//!
//! The in-process test runs three `PersistentMetaDbCluster` servers on localhost, each with its
//! own data directory and gRPC listener. The OS-process test launches `metadbd` binaries.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use metadb_cluster::ClusterIdentity;
use metadb_core::codec;
use metadb_core::{HlcTimestamp, MetaDbError, MetaKey, ROOT_INODE_ID};
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{
    CreateFileRequest, GetDirPartitionStatRequest, GetRoutesRequest, LookupRequest, MkdirRequest,
    RequestRoutingHeader, ShardDescriptorProto,
};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use metadb_tx::TxRaftCmd;
use tempfile::tempdir;

const BUCKET_ID: u64 = 10;

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

fn voter_set(cluster: &PersistentMetaDbCluster) -> BTreeSet<u64> {
    let routes = cluster.cell_manager().list_routes();
    assert!(!routes.is_empty(), "cell has no shard routes");
    let mut expected: Option<BTreeSet<u64>> = None;
    for route in routes {
        let voters = route
            .peers
            .iter()
            .filter(|(_, (_, is_voter))| *is_voter)
            .map(|(node_id, _)| *node_id)
            .collect::<BTreeSet<_>>();
        match &expected {
            None => expected = Some(voters),
            Some(prev) => assert_eq!(prev, &voters, "shard voter sets diverged"),
        }
    }
    expected.unwrap()
}

async fn create_named(
    cluster: &PersistentMetaDbCluster,
    name: &str,
    inode_id: u64,
    fid: u64,
) -> bool {
    cluster
        .tx_router()
        .create_file(BUCKET_ID, name, inode_id, fid, 7, 0o644, 32)
        .await
        .is_ok()
}

fn applied_locally(cluster: &PersistentMetaDbCluster, name: &str) -> bool {
    let key = MetaKey::child(BUCKET_ID, name).encode();
    let read_at = HlcTimestamp::new(HlcTimestamp::MAX_PHYSICAL_MS, 0);
    cluster
        .cell_manager()
        .local_shard_handles()
        .iter()
        .any(|(_, group, _)| group.engine().get(&key, read_at).ok().flatten().is_some())
}

async fn wait_applied(cluster: &PersistentMetaDbCluster, name: &str) {
    for _ in 0..80 {
        if applied_locally(cluster, name) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "timed out waiting for {name} to apply on node {}",
        cluster.config().node.node_id
    );
}

async fn wait_visible(cluster: &PersistentMetaDbCluster, name: &str) {
    for _ in 0..160 {
        match cluster.tx_router().lookup(BUCKET_ID, name).await {
            Ok(Some(_)) => return,
            Ok(None) | Err(_) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!(
        "timed out waiting for linearizable lookup of {name} on node {}",
        cluster.config().node.node_id
    );
}

async fn wait_visible_any(nodes: &[Arc<PersistentMetaDbCluster>], name: &str) {
    for _ in 0..160 {
        for node in nodes {
            if node
                .tx_router()
                .lookup(BUCKET_ID, name)
                .await
                .ok()
                .flatten()
                .is_some()
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for linearizable lookup of {name}");
}

async fn write_on_any_leader(
    nodes: &[Arc<PersistentMetaDbCluster>],
    name: &str,
    inode_id: u64,
    fid: u64,
) -> Arc<PersistentMetaDbCluster> {
    for _ in 0..160 {
        for node in nodes {
            if create_named(node, name, inode_id, fid).await {
                return Arc::clone(node);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no survivor accepted a write for {name}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_process_joins_existing_raft_group_fails_over_and_reopens() {
    let dir = tempdir().unwrap();
    let dir1 = dir.path().join("node1");
    let dir2 = dir.path().join("node2");
    let dir3 = dir.path().join("node3");

    let cfg1 = fast_config(&dir1, 1, "");
    let (node1, addr1) = PersistentMetaDbCluster::open_and_serve(cfg1.clone())
        .await
        .unwrap();
    node1
        .tx_router()
        .mkdir(ROOT_INODE_ID, "bucket", BUCKET_ID, 7, 0o755)
        .await
        .unwrap();
    assert!(create_named(&node1, "before_join.dat", 100, 500).await);
    assert_eq!(voter_set(&node1), BTreeSet::from([1]));

    let cfg2 = fast_config(&dir2, 2, &addr1);
    let (node2, addr2) = PersistentMetaDbCluster::open_and_serve(cfg2.clone())
        .await
        .unwrap();
    assert!(node2.cell_manager().joined_existing());
    assert_eq!(voter_set(&node1), BTreeSet::from([1, 2]));
    assert_eq!(voter_set(&node2), BTreeSet::from([1, 2]));
    assert!(create_named(&node1, "on_two_voters.dat", 101, 501).await);
    // A follower with a known leader completes the read and write instead of returning NotLeader.
    wait_visible(&node2, "before_join.dat").await;
    wait_visible(&node2, "on_two_voters.dat").await;
    assert!(create_named(&node2, "from_follower.dat", 110, 510).await);
    wait_visible(&node1, "from_follower.dat").await;
    node2.tx_router().dirstat(BUCKET_ID).await.unwrap();
    let partition = follower_dir_partition(&addr2, &node2).await;
    assert!(
        partition.error.is_none(),
        "follower GetDirPartitionStat returned {:?}",
        partition.error
    );
    assert!(partition.is_anchor);
    assert!(partition.partition_attr.is_some());
    assert_eq!(
        node1.cell_manager().bound_instance(2),
        Some(node2.cell_manager().node_instance_id())
    );
    assert_eq!(
        node2.cell_manager().bound_instance(2),
        Some(node2.cell_manager().node_instance_id())
    );
    let duplicate = codec::serialize(&TxRaftCmd::CreateFile {
        pid: BUCKET_ID,
        name: "before_join.dat".to_string(),
        inode_id: 100,
        fid: 500,
        uid: 7,
        mode: 0o644,
        size_bytes: 32,
    })
    .unwrap();
    let duplicate_error = {
        let (_, follower_group, _) = node2.cell_manager().lookup_local_handle(1).unwrap();
        follower_group
            .propose(
                follower_group.current_epoch(),
                HlcTimestamp::ZERO,
                duplicate,
            )
            .await
            .unwrap_err()
    };
    assert!(
        matches!(duplicate_error, MetaDbError::AlreadyExists { .. }),
        "{duplicate_error}"
    );
    let skewed = {
        let (_, leader_group, _) = node1.cell_manager().lookup_local_handle(1).unwrap();
        leader_group
            .propose_local(
                leader_group.current_epoch(),
                HlcTimestamp::new(10_000_000_000_000, 0),
                vec![0],
            )
            .await
            .unwrap_err()
    };
    assert!(
        matches!(skewed, MetaDbError::ClockSkewExceeded { .. }),
        "{skewed}"
    );

    let cfg3 = fast_config(&dir3, 3, &addr1);
    let (node3, addr3) = PersistentMetaDbCluster::open_and_serve(cfg3.clone())
        .await
        .unwrap();
    assert!(node3.cell_manager().joined_existing());
    assert_eq!(voter_set(&node1), BTreeSet::from([1, 2, 3]));
    assert_eq!(voter_set(&node3), BTreeSet::from([1, 2, 3]));
    assert!(create_named(&node1, "on_three_voters.dat", 102, 502).await);
    wait_applied(&node2, "on_three_voters.dat").await;
    wait_applied(&node3, "before_join.dat").await;
    wait_applied(&node3, "on_three_voters.dat").await;

    node1.shutdown().await.unwrap();
    drop(node1);

    let survivors = [Arc::clone(&node2), Arc::clone(&node3)];
    let leader = write_on_any_leader(&survivors, "after_failover.dat", 103, 503).await;
    let leader_id = leader.config().node.node_id;
    assert_ne!(leader_id, 1);
    let names = [
        "before_join.dat",
        "on_two_voters.dat",
        "on_three_voters.dat",
        "after_failover.dat",
    ];
    for name in names {
        wait_visible(&leader, name).await;
    }

    // Membership is still {1,2,3}. Node 1 is stopped, so the other survivor must stay up
    // while this leader reopens; two live voters are the quorum.
    let (leader_dir_cfg, leader_addr) = if leader_id == 2 {
        (cfg2.clone(), addr2.clone())
    } else {
        (cfg3.clone(), addr3.clone())
    };
    let keep = if leader_id == 2 {
        Arc::clone(&node3)
    } else {
        Arc::clone(&node2)
    };
    leader.shutdown().await.unwrap();
    drop(survivors);
    drop(leader);
    // The exclusive allocator lock lives until this process drops the old server.
    if leader_id == 2 {
        drop(node2);
    } else {
        drop(node3);
    }

    let (reopened, reopened_addr) = PersistentMetaDbCluster::open_and_serve(leader_dir_cfg)
        .await
        .unwrap();
    assert_eq!(
        reopened_addr, leader_addr,
        "restart on an ephemeral port must keep the raft advertise address"
    );
    assert!(reopened.cell_manager().joined_existing());
    let readers = [Arc::clone(&keep), Arc::clone(&reopened)];
    for name in names {
        wait_applied(&reopened, name).await;
        wait_visible_any(&readers, name).await;
    }

    reopened.shutdown().await.unwrap();
    keep.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joined_voter_can_be_removed_while_the_others_stay_up() {
    let dir = tempdir().unwrap();
    let dir1 = dir.path().join("node1");
    let dir2 = dir.path().join("node2");
    let dir3 = dir.path().join("node3");
    let (node1, addr1) = PersistentMetaDbCluster::open_and_serve(fast_config(&dir1, 1, ""))
        .await
        .unwrap();
    node1
        .tx_router()
        .mkdir(ROOT_INODE_ID, "bucket", BUCKET_ID, 7, 0o755)
        .await
        .unwrap();
    assert!(create_named(&node1, "before_join.dat", 100, 500).await);
    let (node2, _) = PersistentMetaDbCluster::open_and_serve(fast_config(&dir2, 2, &addr1))
        .await
        .unwrap();
    let (node3, _) = PersistentMetaDbCluster::open_and_serve(fast_config(&dir3, 3, &addr1))
        .await
        .unwrap();
    wait_applied(&node3, "before_join.dat").await;
    assert_eq!(voter_set(&node1), BTreeSet::from([1, 2, 3]));

    let removed = node1.cell_manager().remove_voter(3).await.unwrap();
    assert!(removed >= 1);
    assert_eq!(voter_set(&node1), BTreeSet::from([1, 2]));
    assert!(create_named(&node1, "after_removal.dat", 104, 504).await);
    wait_applied(&node2, "after_removal.dat").await;

    node3.shutdown().await.unwrap();
    assert!(create_named(&node1, "after_removed_process_stopped.dat", 105, 505).await);
    wait_visible(&node1, "after_removed_process_stopped.dat").await;

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_route_cache_does_not_drop_a_live_voter() {
    let dir = tempdir().unwrap();
    let (node1, addr1) =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n1"), 1, ""))
            .await
            .unwrap();
    node1
        .tx_router()
        .mkdir(ROOT_INODE_ID, "bucket", BUCKET_ID, 7, 0o755)
        .await
        .unwrap();
    assert!(create_named(&node1, "before_join.dat", 100, 500).await);
    let (node2, _) =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n2"), 2, &addr1))
            .await
            .unwrap();
    let (node3, _) =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n3"), 3, &addr1))
            .await
            .unwrap();
    wait_applied(&node2, "before_join.dat").await;
    assert_eq!(voter_set(&node2), BTreeSet::from([1, 2, 3]));
    for route in node2.cell_manager().list_routes() {
        let (_, group, _) = node2
            .cell_manager()
            .lookup_local_handle(route.shard_id)
            .unwrap();
        assert_eq!(
            route.epoch.conf_ver,
            group.current_epoch().conf_ver,
            "shard {} status conf_ver lagged the applied Raft membership",
            route.shard_id
        );
        assert_eq!(
            route.epoch,
            group.current_epoch(),
            "shard {} status epoch lagged the applied Raft epoch",
            route.shard_id
        );
        let reported = route
            .peers
            .iter()
            .filter(|(_, (_, is_voter))| *is_voter)
            .map(|(node_id, _)| *node_id)
            .collect::<BTreeSet<_>>();
        assert_eq!(
            reported,
            node2.cell_manager().raft_voter_ids(route.shard_id).unwrap(),
            "shard {} status voters lagged Raft membership",
            route.shard_id
        );
        assert_eq!(
            reported,
            BTreeSet::from([1, 2, 3]),
            "shard {} reported the previous two-voter set after node 3 joined",
            route.shard_id
        );
    }
    assert_eq!(
        node2.cell_manager().raft_voter_ids(1).unwrap(),
        BTreeSet::from([1, 2, 3])
    );
    assert_eq!(
        node2.cell_manager().voters_after_removal(1, 1).unwrap(),
        BTreeSet::from([2, 3])
    );
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_join_resumes_instead_of_forking() {
    let dir = tempdir().unwrap();
    let (node1, addr1) =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n1"), 1, ""))
            .await
            .unwrap();
    node1
        .tx_router()
        .mkdir(ROOT_INODE_ID, "bucket", BUCKET_ID, 7, 0o755)
        .await
        .unwrap();
    assert!(create_named(&node1, "before_join.dat", 100, 500).await);
    let cfg2 = fast_config(&dir.path().join("n2"), 2, &addr1);
    let (node2, addr2) = PersistentMetaDbCluster::open_and_serve(cfg2.clone())
        .await
        .unwrap();
    wait_applied(&node2, "before_join.dat").await;
    node2
        .cell_manager()
        .testing_mark_join_interrupted()
        .unwrap();
    node2.shutdown().await.unwrap();
    drop(node2);
    let (resumed, resumed_addr) = PersistentMetaDbCluster::open_and_serve(cfg2).await.unwrap();
    assert_eq!(resumed_addr, addr2);
    assert!(resumed.cell_manager().joined_existing());
    wait_applied(&resumed, "before_join.dat").await;
    resumed.shutdown().await.unwrap();
    node1.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_sends_the_shared_secret() {
    let dir = tempdir().unwrap();
    let mut cfg1 = fast_config(&dir.path().join("n1"), 1, "");
    cfg1.security.shared_secret = "join-secret".to_string();
    let (node1, addr1) = PersistentMetaDbCluster::open_and_serve(cfg1).await.unwrap();
    node1
        .tx_router()
        .mkdir(ROOT_INODE_ID, "bucket", BUCKET_ID, 7, 0o755)
        .await
        .unwrap();
    assert!(create_named(&node1, "before_join.dat", 100, 500).await);
    let mut cfg2 = fast_config(&dir.path().join("n2"), 2, &addr1);
    cfg2.security.shared_secret = "join-secret".to_string();
    let (node2, _) = PersistentMetaDbCluster::open_and_serve(cfg2).await.unwrap();
    wait_applied(&node2, "before_join.dat").await;
    assert_eq!(voter_set(&node1), BTreeSet::from([1, 2]));
    node2.shutdown().await.unwrap();
    node1.shutdown().await.unwrap();
}

struct Metadbd {
    child: std::process::Child,
}

impl Metadbd {
    fn spawn(config_path: &Path, log_path: PathBuf) -> Self {
        let log = std::fs::File::create(&log_path).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_metadbd"))
            .arg("--config")
            .arg(config_path)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap();
        Self { child }
    }

    fn stop(&mut self) {
        if self.child.try_wait().ok().flatten().is_some() {
            return;
        }
        let _ = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status();
        let _ = self.child.wait();
    }
}

impl Drop for Metadbd {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-KILL", &self.child.id().to_string()])
                .status();
            let _ = self.child.wait();
        }
    }
}

async fn published_addr(data_dir: &Path, log_path: &Path) -> String {
    let path = data_dir.join("GRPC_ADDR");
    for _ in 0..200 {
        if let Ok(text) = std::fs::read_to_string(&path) {
            let text = text.trim();
            if !text.is_empty() {
                return text.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let log = std::fs::read_to_string(log_path).unwrap_or_default();
    panic!("metadbd did not publish {path:?}\n{log}");
}

fn write_toml(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
}

fn voter_ids_from_routes(shards: &[ShardDescriptorProto]) -> BTreeSet<u64> {
    let mut ids = BTreeSet::new();
    for shard in shards {
        for peer in &shard.peers {
            if peer.is_voter {
                ids.insert(peer.node_id);
            }
        }
    }
    ids
}

fn node_toml(data_dir: &Path, node_id: u64, join: &str) -> String {
    format!(
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
        data_dir.display()
    )
}

async fn wait_voters(addr: &str, expected: BTreeSet<u64>) {
    let mut cp = ControlPlaneServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    for _ in 0..600 {
        if let Ok(routes) = cp.get_routes(GetRoutesRequest { cell_id: 1 }).await {
            let routes = routes.into_inner();
            if voter_ids_from_routes(&routes.shards) == expected {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for voters {expected:?} via {addr}");
}

async fn lookup_found(addr: &str, name: &str) -> bool {
    let Ok(mut meta) = MetadataServiceClient::connect(format!("http://{addr}")).await else {
        return false;
    };
    let Ok(resp) = meta
        .lookup(LookupRequest {
            header: None,
            pid: BUCKET_ID,
            name: name.to_string(),
        })
        .await
    else {
        return false;
    };
    let resp = resp.into_inner();
    resp.error.is_none() && resp.found
}

async fn wait_lookup_on_any(addrs: &[&str], name: &str) -> String {
    for _ in 0..200 {
        for addr in addrs {
            if lookup_found(addr, name).await {
                return (*addr).to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {name} on {addrs:?}");
}

async fn follower_dir_partition(
    addr: &str,
    cluster: &PersistentMetaDbCluster,
) -> metadb_proto::GetDirPartitionStatResponse {
    let route = cluster
        .cell_manager()
        .list_routes()
        .into_iter()
        .next()
        .expect("follower has a shard route");
    let mut meta = MetadataServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    meta.get_dir_partition_stat(GetDirPartitionStatRequest {
        header: Some(RequestRoutingHeader {
            shard_id: route.shard_id,
            expected_epoch: Some(route.epoch.into()),
            caller_hlc: 0,
        }),
        pid: BUCKET_ID,
        use_fixed_barrier_hlc: false,
        barrier_hlc: 0,
    })
    .await
    .unwrap()
    .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_join_returns_and_releases_the_directory() {
    let dir = tempdir().unwrap();
    let mut cfg = fast_config(&dir.path().join("joiner"), 2, "127.0.0.1:1");
    cfg.join_timeout = Some(Duration::from_millis(700));
    let started = std::time::Instant::now();
    let first = match PersistentMetaDbCluster::open_and_serve(cfg.clone()).await {
        Ok(_) => panic!("join to a closed port succeeded"),
        Err(error) => error,
    };
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "a refused join waited without returning"
    );
    assert!(!first.to_string().contains("exclusive lock"), "{first}");
    let second = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok(_) => panic!("second join to a closed port succeeded"),
        Err(error) => error,
    };
    assert!(
        !second.to_string().contains("exclusive lock"),
        "failed join left the data directory locked: {second}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadbd_processes_join_existing_group_and_fail_over() {
    let dir = tempdir().unwrap();
    let dir1 = dir.path().join("node1");
    let dir2 = dir.path().join("node2");
    let dir3 = dir.path().join("node3");
    for data_dir in [&dir1, &dir2, &dir3] {
        std::fs::create_dir_all(data_dir).unwrap();
    }

    let cfg1_path = dir.path().join("node1.toml");
    write_toml(&cfg1_path, &node_toml(&dir1, 1, ""));
    let log1 = dir.path().join("node1.log");
    let mut proc1 = Metadbd::spawn(&cfg1_path, log1.clone());
    let addr1 = published_addr(&dir1, &log1).await;

    let mut meta1 = MetadataServiceClient::connect(format!("http://{addr1}"))
        .await
        .unwrap();
    let mkdir = meta1
        .mkdir(MkdirRequest {
            header: None,
            pid: ROOT_INODE_ID,
            name: "bucket".to_string(),
            dir_id: BUCKET_ID,
            uid: 7,
            mode: 0o755,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        mkdir.error.is_none(),
        "{:?}\n{}",
        mkdir.error,
        std::fs::read_to_string(&log1).unwrap_or_default()
    );
    let created = meta1
        .create_file(CreateFileRequest {
            header: None,
            pid: BUCKET_ID,
            name: "before_join.dat".to_string(),
            inode_id: 100,
            fid: 500,
            uid: 7,
            mode: 0o644,
            size_bytes: 32,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(created.error.is_none(), "{:?}", created.error);

    let cfg2_path = dir.path().join("node2.toml");
    write_toml(&cfg2_path, &node_toml(&dir2, 2, &addr1));
    let log2 = dir.path().join("node2.log");
    let mut proc2 = Metadbd::spawn(&cfg2_path, log2.clone());
    let _addr2 = published_addr(&dir2, &log2).await;
    wait_voters(&addr1, BTreeSet::from([1, 2])).await;

    let cfg3_path = dir.path().join("node3.toml");
    write_toml(&cfg3_path, &node_toml(&dir3, 3, &addr1));
    let log3 = dir.path().join("node3.log");
    let mut proc3 = Metadbd::spawn(&cfg3_path, log3.clone());
    let addr3 = published_addr(&dir3, &log3).await;
    wait_voters(&addr1, BTreeSet::from([1, 2, 3])).await;

    proc1.stop();
    let addr2 = std::fs::read_to_string(dir2.join("GRPC_ADDR"))
        .unwrap()
        .trim()
        .to_string();
    let leader_addr = wait_lookup_on_any(&[&addr2, &addr3], "before_join.dat").await;

    let mut leader = MetadataServiceClient::connect(format!("http://{leader_addr}"))
        .await
        .unwrap();
    let created = leader
        .create_file(CreateFileRequest {
            header: None,
            pid: BUCKET_ID,
            name: "after_failover.dat".to_string(),
            inode_id: 101,
            fid: 501,
            uid: 7,
            mode: 0o644,
            size_bytes: 8,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(created.error.is_none(), "{:?}", created.error);

    let (restart_dir, restart_cfg, restart_proc) = if leader_addr == addr2 {
        (&dir2, &cfg2_path, &mut proc2)
    } else {
        (&dir3, &cfg3_path, &mut proc3)
    };
    restart_proc.stop();
    let _ = std::fs::remove_file(restart_dir.join("GRPC_ADDR"));
    let restart_log = dir.path().join("restart.log");
    let mut restarted = Metadbd::spawn(restart_cfg, restart_log.clone());
    let restart_addr = published_addr(restart_dir, &restart_log).await;
    let survivor = if leader_addr == addr2 { &addr3 } else { &addr2 };
    wait_lookup_on_any(&[survivor, &restart_addr], "before_join.dat").await;
    wait_lookup_on_any(&[survivor, &restart_addr], "after_failover.dat").await;

    restarted.stop();
    proc2.stop();
    proc3.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_joiners_both_become_voters() {
    let dir = tempdir().unwrap();
    let (node1, addr1) =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n1"), 1, ""))
            .await
            .unwrap();
    node1
        .tx_router()
        .mkdir(ROOT_INODE_ID, "bucket", BUCKET_ID, 7, 0o755)
        .await
        .unwrap();
    let join2 =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n2"), 2, &addr1));
    let join3 =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n3"), 3, &addr1));
    let (joined2, joined3) = tokio::join!(join2, join3);
    let (node2, _) = joined2.expect("node 2 join");
    let (node3, _) = joined3.expect("node 3 join");
    let expected = BTreeSet::from([1, 2, 3]);
    assert_eq!(voter_set(&node1), expected);
    assert_eq!(voter_set(&node2), expected);
    assert_eq!(voter_set(&node3), expected);
    assert_ne!(node1.cell_manager().cluster_id(), 0);
    assert_eq!(
        node2.cell_manager().cluster_id(),
        node1.cell_manager().cluster_id()
    );
    assert_eq!(
        node3.cell_manager().cluster_id(),
        node1.cell_manager().cluster_id()
    );
    assert!(create_named(&node2, "concurrent.dat", 130, 530).await);
    wait_visible(&node3, "concurrent.dat").await;
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reused_node_id_and_foreign_cluster_are_rejected() {
    let dir = tempdir().unwrap();
    let (node1, addr1) =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n1"), 1, ""))
            .await
            .unwrap();
    let (node2, addr2) =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n2"), 2, &addr1))
            .await
            .unwrap();
    assert_eq!(
        node2.cell_manager().bound_instance(2),
        Some(node2.cell_manager().node_instance_id())
    );
    let reused = PersistentMetaDbCluster::open_and_serve(fast_config(
        &dir.path().join("n2-reused"),
        2,
        &addr1,
    ))
    .await;
    match reused {
        Ok((cluster, _)) => {
            cluster.shutdown().await.unwrap();
            panic!("a fresh directory reused node id 2");
        }
        Err(error) => assert!(
            matches!(error, MetaDbError::IdentityRejected(_)),
            "unexpected reuse error: {error}"
        ),
    }

    let foreign_dir = dir.path().join("foreign");
    ClusterIdentity {
        cluster_id: 0x1111_2222_3333_4444,
        node_instance_id: 0x5555_6666_7777_8888,
        node_id: 3,
        cell_id: 1,
    }
    .persist(foreign_dir.join("cell_1"))
    .unwrap();
    let foreign =
        PersistentMetaDbCluster::open_and_serve(fast_config(&foreign_dir, 3, &addr1)).await;
    match foreign {
        Ok((cluster, _)) => {
            cluster.shutdown().await.unwrap();
            panic!("joiner adopted a different cluster identity");
        }
        Err(error) => assert!(
            matches!(error, MetaDbError::IdentityRejected(_)),
            "unexpected cluster identity error: {error}"
        ),
    }
    assert_ne!(
        node1.cell_manager().cluster_id(),
        0x1111_2222_3333_4444_u128
    );
    node1.shutdown().await.unwrap();
    drop(node1);
    let reused_after_failover = PersistentMetaDbCluster::open_and_serve(fast_config(
        &dir.path().join("n2-after-failover"),
        2,
        &addr2,
    ))
    .await;
    match reused_after_failover {
        Ok((cluster, _)) => {
            cluster.shutdown().await.unwrap();
            panic!("failover leader accepted a new instance for node 2");
        }
        Err(error) => assert!(
            matches!(error, MetaDbError::IdentityRejected(_)),
            "unexpected failover reuse error: {error}"
        ),
    }
    node2.shutdown().await.unwrap();
    drop(node2);
    std::fs::remove_file(dir.path().join("n2/cell_1/CLUSTER_IDENTITY")).unwrap();
    let legacy =
        PersistentMetaDbCluster::open_and_serve(fast_config(&dir.path().join("n2"), 2, "")).await;
    match legacy {
        Ok((cluster, _)) => {
            cluster.shutdown().await.unwrap();
            panic!("legacy replicated directory invented a private cluster id");
        }
        Err(error) => assert!(
            matches!(error, MetaDbError::IdentityRejected(_)),
            "unexpected legacy identity error: {error}"
        ),
    }
}
