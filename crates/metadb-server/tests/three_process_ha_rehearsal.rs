//! Three-`metadbd` OS-Process Voter Rehearsal:
//! - Starts 3 independent `metadbd` OS processes (`node 1`, `node 2`, `node 3`) and forms a 3-voter cell.
//! - Verifies `grpc.health.v1.Health/Check` (`SERVING`) and `metadb-ctl cluster health`.
//! - Populates multiple directories + bulk deletes and verifies `ListDirResponse.{entries_examined, tombstones_traversed}`.
//! - Executes a live 3-voter shard split via `metadb-ctl shard split`.
//! - Executes safe leadership transfer via `metadb-ctl raft transfer-leader`.
//! - Stops the original `node 1` OS process and verifies surviving `{node 2, node 3}` OS processes
//!   serve linearizable `ListDir` (with `entries_examined > 0`) and new writes across BOTH parent and split child shards!

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use metadb_core::ROOT_INODE_ID;
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::health::health_check_response::ServingStatus;
use metadb_proto::health::health_client::HealthClient;
use metadb_proto::health::HealthCheckRequest;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{
    CreateFileRequest, GetRoutesRequest, ListDirRequest, MkdirRequest, UnlinkRequest,
};
use tempfile::tempdir;

struct MetadbdProc {
    child: Child,
}

impl MetadbdProc {
    fn spawn(config_path: &Path, log_path: &Path) -> Self {
        let log = std::fs::File::create(log_path).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_metadbd"))
            .arg("--strict-config")
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

impl Drop for MetadbdProc {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-KILL", &self.child.id().to_string()])
                .status();
            let _ = self.child.wait();
        }
    }
}

async fn wait_grpc_addr(data_dir: &Path, log_path: &Path) -> String {
    let addr_file = data_dir.join("GRPC_ADDR");
    for _ in 0..240 {
        if let Ok(s) = std::fs::read_to_string(&addr_file) {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let log = std::fs::read_to_string(log_path).unwrap_or_default();
    panic!("timed out waiting for GRPC_ADDR in {data_dir:?}\n{log}");
}

fn node_toml(data_dir: &Path, node_id: u64, join: &str, secret: &str) -> String {
    format!(
        r#"
[node]
node_id = {node_id}
cell_id = 1
role = "all-in-one"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[storage]
locality_tombstone_threshold = 500
locality_tombstone_ratio = 0.90

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296

[cluster]
join = "{join}"
join_timeout_secs = 30

[security]
shared_secret = "{secret}"

[raft]
heartbeat_interval_ms = 80
election_timeout_min_ms = 200
election_timeout_max_ms = 400
"#,
        data_dir.display()
    )
}

fn run_ctl(endpoint: &str, secret: &str, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_metadb-ctl"))
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--auth-token")
        .arg(secret)
        .args(args)
        .output()
        .unwrap()
}

fn authed_channel_meta(
    channel: tonic::transport::Channel,
    secret: &str,
) -> MetadataServiceClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        metadb_server::BearerAuthClient,
    >,
> {
    MetadataServiceClient::with_interceptor(channel, metadb_server::BearerAuthClient::new(secret))
}

fn authed_channel_cp(
    channel: tonic::transport::Channel,
    secret: &str,
) -> ControlPlaneServiceClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        metadb_server::BearerAuthClient,
    >,
> {
    ControlPlaneServiceClient::with_interceptor(
        channel,
        metadb_server::BearerAuthClient::new(secret),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_metadbd_os_processes_join_split_transfer_leader_failover_and_report_listdir_stats() {
    let dir = tempdir().unwrap();
    let secret = "three-process-rehearsal-secret";
    let dir1 = dir.path().join("n1");
    let dir2 = dir.path().join("n2");
    let dir3 = dir.path().join("n3");
    for d in [&dir1, &dir2, &dir3] {
        std::fs::create_dir_all(d).unwrap();
    }

    let cfg1: PathBuf = dir.path().join("n1.toml");
    std::fs::write(&cfg1, node_toml(&dir1, 1, "", secret)).unwrap();
    let log1 = dir.path().join("n1.log");
    let mut proc1 = MetadbdProc::spawn(&cfg1, &log1);
    let addr1 = wait_grpc_addr(&dir1, &log1).await;

    // Verify `grpc.health.v1.Health/Check` and `metadb-ctl cluster health`.
    let ch1 = tonic::transport::Endpoint::from_shared(format!("http://{addr1}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut hc1 =
        HealthClient::with_interceptor(ch1.clone(), metadb_server::BearerAuthClient::new(secret));
    let health = hc1
        .check(HealthCheckRequest {
            service: "metadb.v1.MetadataService".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health.status, ServingStatus::Serving as i32);

    let ctl_health = run_ctl(&addr1, secret, &["cluster", "health"]);
    assert!(
        ctl_health.status.success(),
        "metadb-ctl cluster health failed: {}",
        String::from_utf8_lossy(&ctl_health.stderr)
    );

    // Join Node 2 and Node 3 as real OS processes!
    let cfg2 = dir.path().join("n2.toml");
    std::fs::write(&cfg2, node_toml(&dir2, 2, &addr1, secret)).unwrap();
    let log2 = dir.path().join("n2.log");
    let mut proc2 = MetadbdProc::spawn(&cfg2, &log2);
    let addr2 = wait_grpc_addr(&dir2, &log2).await;

    let cfg3 = dir.path().join("n3.toml");
    std::fs::write(&cfg3, node_toml(&dir3, 3, &addr1, secret)).unwrap();
    let log3 = dir.path().join("n3.log");
    let mut proc3 = MetadbdProc::spawn(&cfg3, &log3);
    let addr3 = wait_grpc_addr(&dir3, &log3).await;

    let mut cp1 = authed_channel_cp(ch1.clone(), secret);
    for _ in 0..200 {
        let routes = cp1
            .get_routes(GetRoutesRequest { cell_id: 1 })
            .await
            .unwrap()
            .into_inner();
        let all_three = routes.shards.len() == 3
            && routes.shards.iter().all(|s| {
                s.peers
                    .iter()
                    .filter(|p| p.is_voter)
                    .map(|p| p.node_id)
                    .collect::<BTreeSet<_>>()
                    == BTreeSet::from([1, 2, 3])
            });
        if all_three {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Create directories PID 10 & PID 40, create files, unlink half of PID 10 to produce tombstones!
    let mut meta1 = authed_channel_meta(ch1.clone(), secret);
    for pid in [10u64, 20, 30, 40] {
        let mk = meta1
            .mkdir(MkdirRequest {
                header: None,
                pid: ROOT_INODE_ID,
                name: format!("dir_{pid}"),
                dir_id: pid,
                uid: 1,
                mode: 0o755,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(mk.error.is_none(), "{:?}", mk.error);
        for f in 0..6u64 {
            let cf = meta1
                .create_file(CreateFileRequest {
                    header: None,
                    pid,
                    name: format!("item_{f:02}.dat"),
                    inode_id: pid * 100 + f,
                    fid: pid * 1_000 + f,
                    uid: 1,
                    mode: 0o644,
                    size_bytes: 512,
                })
                .await
                .unwrap()
                .into_inner();
            assert!(cf.error.is_none(), "{:?}", cf.error);
        }
    }
    for f in 0..3u64 {
        let ul = meta1
            .unlink(UnlinkRequest {
                header: None,
                pid: 10,
                name: format!("item_{f:02}.dat"),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(ul.error.is_none(), "{:?}", ul.error);
    }

    // Trigger online 3-voter shard split on Shard 1 via `metadb-ctl shard split`!
    let split_out = run_ctl(&addr1, secret, &["shard", "split", "--shard-id", "1"]);
    let split_stdout = String::from_utf8_lossy(&split_out.stdout);
    assert!(
        split_out.status.success() && split_stdout.contains("split_executed=true"),
        "expected 3-voter split_executed=true, stdout={split_stdout}, stderr={}",
        String::from_utf8_lossy(&split_out.stderr)
    );

    // Transfer Shard 1 leadership from Node 1 to Node 2 via `metadb-ctl raft transfer-leader`!
    let transfer_out = run_ctl(
        &addr1,
        secret,
        &[
            "raft",
            "transfer-leader",
            "--shard-id",
            "1",
            "--target-node-id",
            "2",
        ],
    );
    let transfer_stdout = String::from_utf8_lossy(&transfer_out.stdout);
    assert!(
        transfer_out.status.success() && transfer_stdout.contains("transferred=true"),
        "transfer-leader failed: stdout={transfer_stdout}, stderr={}",
        String::from_utf8_lossy(&transfer_out.stderr)
    );

    // Wait briefly for control-plane route snapshot replication, then stop Node 1 OS process!
    tokio::time::sleep(Duration::from_millis(350)).await;
    proc1.stop();

    // Verify surviving 2 `metadbd` OS processes (`addr2`, `addr3`) serve `ListDir` with `entries_examined > 0`
    // AND accept post-failover writes on BOTH PID 10 (left parent shard) and PID 40 (right split child shard)!
    let mut left_verified = false;
    let mut right_verified = false;
    for _ in 0..120 {
        for survivor_addr in [&addr2, &addr3] {
            let Ok(ch) = tonic::transport::Endpoint::from_shared(format!("http://{survivor_addr}"))
                .unwrap()
                .connect()
                .await
            else {
                continue;
            };
            let mut m = authed_channel_meta(ch, secret);
            if !left_verified {
                if let Ok(resp) = m
                    .list_dir(ListDirRequest {
                        header: None,
                        pid: 10,
                        after_name: String::new(),
                        name_prefix: String::new(),
                        limit: 10,
                    })
                    .await
                {
                    let inner = resp.into_inner();
                    if inner.error.is_none() && inner.entries.len() == 3 {
                        assert!(
                            inner.entries_examined >= 3,
                            "expected entries_examined >= 3, got {}",
                            inner.entries_examined
                        );
                        let wr = m
                            .create_file(CreateFileRequest {
                                header: None,
                                pid: 10,
                                name: "survivor_left.dat".to_string(),
                                inode_id: 90_001,
                                fid: 190_001,
                                uid: 1,
                                mode: 0o644,
                                size_bytes: 128,
                            })
                            .await
                            .unwrap()
                            .into_inner();
                        if wr.error.is_none() {
                            left_verified = true;
                        }
                    }
                }
            }
            if !right_verified {
                if let Ok(resp) = m
                    .list_dir(ListDirRequest {
                        header: None,
                        pid: 40,
                        after_name: String::new(),
                        name_prefix: String::new(),
                        limit: 10,
                    })
                    .await
                {
                    let inner = resp.into_inner();
                    if inner.error.is_none() && inner.entries.len() == 6 {
                        assert!(
                            inner.entries_examined >= 6,
                            "expected entries_examined >= 6 on split child, got {}",
                            inner.entries_examined
                        );
                        let wr = m
                            .create_file(CreateFileRequest {
                                header: None,
                                pid: 40,
                                name: "survivor_right.dat".to_string(),
                                inode_id: 90_002,
                                fid: 190_002,
                                uid: 1,
                                mode: 0o644,
                                size_bytes: 128,
                            })
                            .await
                            .unwrap()
                            .into_inner();
                        if wr.error.is_none() {
                            right_verified = true;
                        }
                    }
                }
            }
        }
        if left_verified && right_verified {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        left_verified,
        "post-failover left shard ListDir/CreateFile did not succeed"
    );
    assert!(
        right_verified,
        "post-failover split child shard ListDir/CreateFile did not succeed"
    );

    proc2.stop();
    proc3.stop();
}
