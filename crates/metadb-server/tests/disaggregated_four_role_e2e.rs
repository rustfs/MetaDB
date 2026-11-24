//! Disaggregated Multi-Process 4-Role Cluster E2E Test:
//! Starts 4 independent `metadbd` OS processes:
//! 1. `global-manager`: hosts `GLOBAL_CELL_DIRECTORY` and `SHARED_ID_ALLOCATOR`.
//! 2. `cell-manager` (Cell 1): owns `MetaTable` `[0x01, 0x02)` and registers with `global-manager`.
//! 3. `cell-manager` (Cell 2): owns `QuotaTable + SecondaryIndexTable` `[0x02, 0x04)` and registers with `global-manager`.
//! 4. `proxy`: stateless SQL & gRPC gateway routing contained calls across Cell 1 and Cell 2.
//!
//! Verifies:
//! - Shared ID block allocation forwarded by Cell 1 & Cell 2 to `global-manager` produces disjoint blocks.
//! - Parameterized SQL execution via `metadb-ctl sql --param` through `proxy`.
//! - Cross-cell `GrpcRemoteAuxSink` `ApplyAuxBatch` propagates `QuotaDelta` & `IndexUpsert` from Cell 1 (`[0x01, 0x02)`)
//!   to Cell 2 (`[0x02, 0x04)`), making `GetQuota` & `GetSecondaryIndex` converge through `proxy`!

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{AllocateIdBlockRequest, GetQuotaRequest, GetSecondaryIndexRequest};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disaggregated_four_role_processes_route_sql_and_converge_cross_cell_quota_and_index() {
    let dir = tempdir().unwrap();
    let secret = "four-role-disaggregated-secret";

    // 1. Start `global-manager`
    let gm_dir = dir.path().join("gm");
    let gm_cfg = dir.path().join("gm.toml");
    std::fs::write(
        &gm_cfg,
        format!(
            r#"
[node]
node_id = 10
cell_id = 0
role = "global-manager"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[security]
shared_secret = "{secret}"
"#,
            gm_dir.display()
        ),
    )
    .unwrap();
    let gm_log = dir.path().join("gm.log");
    let mut gm_proc = MetadbdProc::spawn(&gm_cfg, &gm_log);
    let gm_addr = wait_grpc_addr(&gm_dir, &gm_log).await;

    // 2. Start Cell 1 (`cell-manager`, MetaTable `[01, 02)`)
    let c1_dir = dir.path().join("cell1");
    let c1_cfg = dir.path().join("cell1.toml");
    std::fs::write(
        &c1_cfg,
        format!(
            r#"
[node]
node_id = 1
cell_id = 1
role = "cell-manager"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[cluster]
cell_start_key = "01"
cell_end_key = "02"
global_manager = "{gm_addr}"

[security]
shared_secret = "{secret}"
"#,
            c1_dir.display()
        ),
    )
    .unwrap();
    let c1_log = dir.path().join("cell1.log");
    let mut c1_proc = MetadbdProc::spawn(&c1_cfg, &c1_log);
    let c1_addr = wait_grpc_addr(&c1_dir, &c1_log).await;

    // 3. Start Cell 2 (`cell-manager`, QuotaTable + SecondaryIndexTable `[02, 04)`)
    let c2_dir = dir.path().join("cell2");
    let c2_cfg = dir.path().join("cell2.toml");
    std::fs::write(
        &c2_cfg,
        format!(
            r#"
[node]
node_id = 2
cell_id = 2
role = "cell-manager"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[cluster]
cell_start_key = "02"
cell_end_key = "04"
global_manager = "{gm_addr}"

[security]
shared_secret = "{secret}"
"#,
            c2_dir.display()
        ),
    )
    .unwrap();
    let c2_log = dir.path().join("cell2.log");
    let mut c2_proc = MetadbdProc::spawn(&c2_cfg, &c2_log);
    let c2_addr = wait_grpc_addr(&c2_dir, &c2_log).await;

    // 4. Start `proxy` pointing upstream at Cell 1 and register Cell 1 & Cell 2 on `proxy`!
    let px_dir = dir.path().join("proxy");
    let px_cfg = dir.path().join("proxy.toml");
    std::fs::write(
        &px_cfg,
        format!(
            r#"
[node]
node_id = 20
cell_id = 0
role = "proxy"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[cluster]
upstream = "{c1_addr}"

[security]
shared_secret = "{secret}"
"#,
            px_dir.display()
        ),
    )
    .unwrap();
    let px_log = dir.path().join("proxy.log");
    let mut px_proc = MetadbdProc::spawn(&px_cfg, &px_log);
    let px_addr = wait_grpc_addr(&px_dir, &px_log).await;

    let reg1 = run_ctl(
        &px_addr,
        secret,
        &[
            "cell",
            "register",
            "--cell-id",
            "1",
            "--endpoints",
            &c1_addr,
            "--start-key",
            "01",
            "--end-key",
            "02",
        ],
    );
    assert!(
        reg1.status.success(),
        "register cell 1 on proxy failed: {}",
        String::from_utf8_lossy(&reg1.stderr)
    );
    let reg2 = run_ctl(
        &px_addr,
        secret,
        &[
            "cell",
            "register",
            "--cell-id",
            "2",
            "--endpoints",
            &c2_addr,
            "--start-key",
            "02",
            "--end-key",
            "04",
        ],
    );
    assert!(
        reg2.status.success(),
        "register cell 2 on proxy failed: {}",
        String::from_utf8_lossy(&reg2.stderr)
    );

    // Verify forwarded `AllocateIdBlock` from Cell 1 and Cell 2 mints disjoint blocks from `global-manager`!
    let ch_c1 = tonic::transport::Endpoint::from_shared(format!("http://{c1_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let ch_c2 = tonic::transport::Endpoint::from_shared(format!("http://{c2_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut cp_c1 = ControlPlaneServiceClient::with_interceptor(
        ch_c1,
        metadb_server::BearerAuthClient::new(secret),
    );
    let mut cp_c2 = ControlPlaneServiceClient::with_interceptor(
        ch_c2,
        metadb_server::BearerAuthClient::new(secret),
    );
    let b1 = cp_c1
        .allocate_id_block(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 1,
            count: 16,
            preferred_shard_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let b2 = cp_c2
        .allocate_id_block(AllocateIdBlockRequest {
            request_uuid_be: Vec::new(),
            id_kind: 1,
            count: 16,
            preferred_shard_id: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(b1.error.is_none() && b2.error.is_none());
    assert!(
        b1.start_id + b1.count <= b2.start_id || b2.start_id + b2.count <= b1.start_id,
        "shared allocator blocks overlapped: {b1:?} vs {b2:?}"
    );

    // Execute parameterized SQL CreateFile through `proxy` using `metadb-ctl sql --param`!
    let sql_out = run_ctl(
        &px_addr,
        secret,
        &[
            "sql",
            "INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)",
            "--param",
            "u64:1",
            "--param",
            "str:cross_cell_doc.parquet",
            "--param",
            &format!("u64:{}", b1.start_id),
            "--param",
            "u64:88001",
            "--param",
            "u64:555",
            "--param",
            "u64:420",
            "--param",
            "i64:16384",
        ],
    );
    assert!(
        sql_out.status.success(),
        "parameterized SQL via proxy failed: {}",
        String::from_utf8_lossy(&sql_out.stderr)
    );

    // Wait for Cell 1's 250ms maintenance loop (`GrpcRemoteAuxSink`) to query `global-manager` and
    // deliver `ApplyAuxBatch` to Cell 2, then verify `GetQuota(555)` and `GetSecondaryIndex(88001)` THROUGH `proxy`!
    let ch_px = tonic::transport::Endpoint::from_shared(format!("http://{px_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut meta_px = MetadataServiceClient::with_interceptor(
        ch_px,
        metadb_server::BearerAuthClient::new(secret),
    );

    let mut quota_converged = false;
    let mut index_converged = false;
    for _ in 0..80 {
        if !quota_converged {
            let q = meta_px
                .get_quota(GetQuotaRequest {
                    header: None,
                    uid: 555,
                })
                .await
                .unwrap()
                .into_inner();
            if q.error.is_none() && q.found {
                let rec = q.record.unwrap();
                if rec.used_inodes == 1 && rec.used_bytes == 16_384 {
                    quota_converged = true;
                }
            }
        }
        if !index_converged {
            let idx = meta_px
                .get_secondary_index(GetSecondaryIndexRequest {
                    header: None,
                    fid: 88_001,
                })
                .await
                .unwrap()
                .into_inner();
            if idx.error.is_none() && idx.found {
                let rec = idx.record.unwrap();
                if rec.pid == 1 && rec.name == "cross_cell_doc.parquet" && !rec.is_deleted {
                    index_converged = true;
                }
            }
        }
        if quota_converged && index_converged {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        quota_converged,
        "cross-cell AuxDispatcher did not converge QuotaTable on Cell 2"
    );
    assert!(
        index_converged,
        "cross-cell AuxDispatcher did not converge SecondaryIndexTable on Cell 2"
    );

    px_proc.stop();
    c1_proc.stop();
    c2_proc.stop();
    gm_proc.stop();
}
