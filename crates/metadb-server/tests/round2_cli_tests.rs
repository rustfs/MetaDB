use std::sync::Arc;

use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};

async fn start(dir: &std::path::Path) -> (Arc<PersistentMetaDbCluster>, String) {
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap()
}

async fn ctl(address: &str, arguments: &[&str]) -> std::process::Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_metadb-ctl"))
        .args(["--endpoint", address])
        .args(arguments)
        .output()
        .await
        .unwrap()
}

fn benchmark_reports(output: &std::process::Output) -> Vec<serde_json::Value> {
    assert!(
        output.status.success(),
        "stdout={:?}; stderr={:?}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn benchmark_rows(
    cluster: &PersistentMetaDbCluster,
    reports: &[serde_json::Value],
) -> Vec<metadb_core::MetaEntry> {
    let pid = reports[0]["bench_pid"].as_u64().unwrap();
    cluster
        .tx_router()
        .listdir_paginated(pid, None, None, 100)
        .await
        .unwrap()
        .0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_application_errors_must_have_a_failure_exit_status() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, address) = start(dir.path()).await;
    let output = ctl(
        &address,
        &[
            "sql",
            "DELETE FROM MetaTable WHERE pid = 1 AND name = 'does-not-exist'",
        ],
    )
    .await;
    cluster.shutdown().await.unwrap();
    let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["operation"], "sql");
    assert!(error["error"]["message"]
        .as_str()
        .unwrap()
        .contains("not found"));
    assert!(
        !output.status.success(),
        "CLI returned success for failed deletion: stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_sql_quota_scan_must_print_returned_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, address) = start(dir.path()).await;
    cluster
        .tx_router()
        .set_quota_limit(42, 123456)
        .await
        .unwrap();
    let output = ctl(
        &address,
        &[
            "sql",
            "SELECT * FROM QuotaTable WHERE uid >= 0 ORDER BY uid ASC LIMIT 10",
        ],
    )
    .await;
    cluster.shutdown().await.unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("42") && stdout.contains("123456"),
        "CLI discarded the nonempty Quotas response: stdout={stdout:?}; stderr={:?}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_cli_benchmark_must_not_report_failed_creates_as_completed() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, address) = start(dir.path()).await;
    let args = [
        "bench",
        "--workload",
        "hotspot-create",
        "--concurrency",
        "1",
        "--ops",
        "1",
    ];
    let first = ctl(&address, &args).await;
    assert!(first.status.success(), "{first:?}");
    let second = ctl(&address, &args).await;
    let first_reports = benchmark_reports(&first);
    let second_reports = benchmark_reports(&second);
    let first_rows = benchmark_rows(&cluster, &first_reports).await;
    let second_rows = benchmark_rows(&cluster, &second_reports).await;
    cluster.shutdown().await.unwrap();
    assert_ne!(
        first_reports[0]["bench_pid"],
        second_reports[0]["bench_pid"]
    );
    assert_eq!(first_rows.len(), 1);
    assert_eq!(second_rows.len(), 1);
    assert_ne!(first_rows[0].attr.inode_id, second_rows[0].attr.inode_id);
    assert_ne!(first_rows[0].attr.fid, second_rows[0].attr.fid);
    for reports in [&first_reports, &second_reports] {
        assert_eq!(reports[1]["attempted"], 1);
        assert_eq!(reports[1]["succeeded"], 1);
        assert_eq!(reports[1]["failed"], 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_benchmark_must_execute_the_requested_number_of_operations() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, address) = start(dir.path()).await;
    for (ops, concurrency) in [(1, 2), (5, 2), (0, 3)] {
        let ops_arg = ops.to_string();
        let concurrency_arg = concurrency.to_string();
        let output = ctl(
            &address,
            &[
                "bench",
                "--workload",
                "hotspot-create",
                "--concurrency",
                &concurrency_arg,
                "--ops",
                &ops_arg,
            ],
        )
        .await;
        let reports = benchmark_reports(&output);
        let rows = benchmark_rows(&cluster, &reports).await;
        assert_eq!(
            rows.len(),
            ops,
            "--ops {ops} with --concurrency {concurrency}: {reports:?}"
        );
        assert_eq!(reports[1]["attempted"], ops);
        assert_eq!(reports[1]["succeeded"], ops);
        assert_eq!(reports[1]["failed"], 0);
    }
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_metrics_connection_must_not_block_other_scrapes() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("server.toml");
    let stdout_path = dir.path().join("daemon.stdout");
    let stderr_path = dir.path().join("daemon.stderr");
    let daemon_output = || {
        format!(
            "stdout:\n{}\nstderr:\n{}",
            std::fs::read_to_string(&stdout_path).unwrap_or_default(),
            std::fs::read_to_string(&stderr_path).unwrap_or_default()
        )
    };
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().join("data");
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.allow_unauthenticated = true;
    cfg.node.metrics_listen_addr = Some("127.0.0.1:0".into());
    std::fs::write(&config_path, toml::to_string(&cfg).unwrap()).unwrap();
    let mut daemon = tokio::process::Command::new(env!("CARGO_BIN_EXE_metadbd"))
        .arg("--config")
        .arg(&config_path)
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .stdout(std::fs::File::create(&stdout_path).unwrap())
        .stderr(std::fs::File::create(&stderr_path).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    // Discover the actual ephemeral listener instead of releasing a reserved
    // port before spawn. Bootstrap replays several Raft groups, so distinguish
    // process exit from bounded readiness under the full parallel CLI suite.
    let readiness = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Some(status) = daemon.try_wait().unwrap() {
                return Err(format!("metadbd exited before metrics readiness: {status}"));
            }
            let output = daemon_output();
            for line in output
                .lines()
                .filter(|line| line.contains("Prometheus metrics endpoint listening"))
            {
                // Locate the numeric address so diagnostics remain usable even
                // if a tracing formatter includes ANSI styling around fields.
                if let Some(start) = line.find("127.0.0.1:") {
                    let address: String = line[start..]
                        .chars()
                        .take_while(|ch| ch.is_ascii_digit() || matches!(ch, '.' | ':'))
                        .collect();
                    if let Ok(address) = address.parse::<std::net::SocketAddr>() {
                        if let Ok(socket) = tokio::net::TcpStream::connect(address).await {
                            return Ok((address, socket));
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let (address, first_idle) = match readiness {
        Ok(Ok(ready)) => ready,
        failed => {
            let _ = daemon.start_kill();
            let _ = daemon.wait().await;
            panic!("metrics readiness failed: {failed:?}\n{}", daemon_output());
        }
    };
    let mut idle_connections = vec![first_idle];
    for _ in 1..8 {
        idle_connections.push(tokio::net::TcpStream::connect(address).await.unwrap());
    }
    // A serial implementation with the two-second connection deadline would
    // spend sixteen seconds on these clients before reaching the actual scrape.
    // Five seconds tolerates scheduler load without accepting that regression.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let scrape = tokio::time::timeout(Duration::from_secs(5), async {
        let mut socket = tokio::net::TcpStream::connect(address).await?;
        socket
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await?;
        let mut response = Vec::new();
        socket.read_to_end(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    })
    .await;
    let idle_expiration = tokio::time::timeout(Duration::from_secs(5), async {
        for mut socket in idle_connections {
            let mut byte = [0];
            if socket.read(&mut byte).await? != 0 {
                return Err(std::io::Error::other(
                    "idle client received unexpected data",
                ));
            }
        }
        Ok::<_, std::io::Error>(())
    })
    .await;
    let mut idle_during_shutdown = tokio::net::TcpStream::connect(address).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    #[cfg(unix)]
    {
        let status = tokio::process::Command::new("kill")
            .args(["-INT", &daemon.id().unwrap().to_string()])
            .status()
            .await
            .unwrap();
        assert!(status.success());
        let shutdown = tokio::time::timeout(Duration::from_secs(10), daemon.wait()).await;
        if shutdown.is_err() {
            let _ = daemon.start_kill();
            let _ = daemon.wait().await;
        }
        assert!(
            shutdown.unwrap().unwrap().success(),
            "daemon failed graceful shutdown with an idle metrics connection:\n{}",
            daemon_output()
        );
    }
    #[cfg(not(unix))]
    {
        daemon.start_kill().unwrap();
        daemon.wait().await.unwrap();
    }
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), idle_during_shutdown.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0,
        "shutdown left an idle metrics connection open"
    );
    let response = scrape
        .expect("idle clients blocked the metrics scrape")
        .unwrap();
    assert!(String::from_utf8_lossy(&response).contains("HTTP/1.1 200 OK"));
    assert!(String::from_utf8_lossy(&response).contains("metadb_shards"));
    idle_expiration
        .expect("idle metrics connections did not expire concurrently")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_benchmark_counts_application_and_transport_failures() {
    use metadb_proto::control_plane_service_server::ControlPlaneServiceServer;
    use metadb_proto::metadata_service_server::MetadataServiceServer;
    use tokio_stream::wrappers::TcpListenerStream;

    let dir = tempfile::tempdir().unwrap();
    let (cluster, _) = start(dir.path()).await;
    let inner = Arc::new(metadb_server::MetaDbGrpcService::new(
        cluster.global_manager().clone(),
        cluster.tx_router().clone(),
        cluster.proxy_router().clone(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(MetadataServiceServer::new(FaultyCreates {
                inner: inner.clone(),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }))
            .add_service(ControlPlaneServiceServer::from_arc(inner))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopped.await;
            }),
    );
    let output = ctl(&address, &["bench", "--concurrency", "2", "--ops", "5"]).await;
    let reports: Vec<serde_json::Value> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let rows = benchmark_rows(&cluster, &reports).await;
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
    cluster.shutdown().await.unwrap();
    assert!(
        !output.status.success(),
        "benchmark hid partial failures: {output:?}"
    );
    assert_eq!(reports[1]["attempted"], 5);
    assert_eq!(reports[1]["succeeded"], 3);
    assert_eq!(reports[1]["failed"], 2);
    assert_eq!(reports[1]["application_failed"], 1);
    assert_eq!(reports[1]["transport_failed"], 1);
    assert_eq!(reports[1]["errors_by_kind"]["InjectedRejection"], 1);
    assert_eq!(reports[1]["errors_by_kind"]["transport:Unavailable"], 1);
    assert_eq!(rows.len(), 3);
}

struct FaultyCreates {
    inner: Arc<metadb_server::MetaDbGrpcService>,
    calls: std::sync::atomic::AtomicUsize,
}

#[tonic::async_trait]
impl metadb_proto::metadata_service_server::MetadataService for FaultyCreates {
    async fn create_file(
        &self,
        request: tonic::Request<metadb_proto::CreateFileRequest>,
    ) -> Result<tonic::Response<metadb_proto::CreateFileResponse>, tonic::Status> {
        match self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            0 => Err(tonic::Status::unavailable("injected transport failure")),
            1 => Ok(tonic::Response::new(metadb_proto::CreateFileResponse {
                error: Some(metadb_proto::ErrorDetailProto {
                    error_kind: "InjectedRejection".into(),
                    message: "injected application rejection".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            _ => self.inner.create_file(request).await,
        }
    }
    async fn mkdir(
        &self,
        request: tonic::Request<metadb_proto::MkdirRequest>,
    ) -> Result<tonic::Response<metadb_proto::MkdirResponse>, tonic::Status> {
        self.inner.mkdir(request).await
    }
    async fn unlink(
        &self,
        request: tonic::Request<metadb_proto::UnlinkRequest>,
    ) -> Result<tonic::Response<metadb_proto::UnlinkResponse>, tonic::Status> {
        self.inner.unlink(request).await
    }
    async fn rmdir(
        &self,
        request: tonic::Request<metadb_proto::RmdirRequest>,
    ) -> Result<tonic::Response<metadb_proto::RmdirResponse>, tonic::Status> {
        self.inner.rmdir(request).await
    }
    async fn set_attr(
        &self,
        request: tonic::Request<metadb_proto::SetAttrRequest>,
    ) -> Result<tonic::Response<metadb_proto::SetAttrResponse>, tonic::Status> {
        self.inner.set_attr(request).await
    }
    async fn lookup(
        &self,
        request: tonic::Request<metadb_proto::LookupRequest>,
    ) -> Result<tonic::Response<metadb_proto::LookupResponse>, tonic::Status> {
        self.inner.lookup(request).await
    }
    async fn dir_stat(
        &self,
        request: tonic::Request<metadb_proto::DirStatRequest>,
    ) -> Result<tonic::Response<metadb_proto::DirStatResponse>, tonic::Status> {
        self.inner.dir_stat(request).await
    }
    async fn get_dir_partition_stat(
        &self,
        request: tonic::Request<metadb_proto::GetDirPartitionStatRequest>,
    ) -> Result<tonic::Response<metadb_proto::GetDirPartitionStatResponse>, tonic::Status> {
        self.inner.get_dir_partition_stat(request).await
    }
    async fn list_dir(
        &self,
        request: tonic::Request<metadb_proto::ListDirRequest>,
    ) -> Result<tonic::Response<metadb_proto::ListDirResponse>, tonic::Status> {
        self.inner.list_dir(request).await
    }
    async fn execute_sql(
        &self,
        request: tonic::Request<metadb_proto::ExecuteSqlRequest>,
    ) -> Result<tonic::Response<metadb_proto::ExecuteSqlResponse>, tonic::Status> {
        self.inner.execute_sql(request).await
    }
    async fn get_quota(
        &self,
        request: tonic::Request<metadb_proto::GetQuotaRequest>,
    ) -> Result<tonic::Response<metadb_proto::GetQuotaResponse>, tonic::Status> {
        self.inner.get_quota(request).await
    }
    async fn set_quota_limit(
        &self,
        request: tonic::Request<metadb_proto::SetQuotaLimitRequest>,
    ) -> Result<tonic::Response<metadb_proto::SetQuotaLimitResponse>, tonic::Status> {
        self.inner.set_quota_limit(request).await
    }
    async fn list_quotas(
        &self,
        request: tonic::Request<metadb_proto::ListQuotasRequest>,
    ) -> Result<tonic::Response<metadb_proto::ListQuotasResponse>, tonic::Status> {
        self.inner.list_quotas(request).await
    }
    async fn get_secondary_index(
        &self,
        request: tonic::Request<metadb_proto::GetSecondaryIndexRequest>,
    ) -> Result<tonic::Response<metadb_proto::GetSecondaryIndexResponse>, tonic::Status> {
        self.inner.get_secondary_index(request).await
    }
    async fn scan_secondary_index(
        &self,
        request: tonic::Request<metadb_proto::ScanSecondaryIndexRequest>,
    ) -> Result<tonic::Response<metadb_proto::ScanSecondaryIndexResponse>, tonic::Status> {
        self.inner.scan_secondary_index(request).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_sql_prints_secondary_indices_and_directory_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, address) = start(dir.path()).await;
    let a = cluster
        .create_file_auto_id(1, "a", 7, 0o644, 10)
        .await
        .unwrap();
    cluster
        .create_file_auto_id(1, "b", 7, 0o644, 20)
        .await
        .unwrap();
    cluster.run_maintenance_tick().await.unwrap();
    let listing = ctl(
        &address,
        &[
            "sql",
            "SELECT * FROM MetaTable WHERE pid = 1 AND name > '' ORDER BY name ASC LIMIT 1",
        ],
    )
    .await;
    assert!(listing.status.success(), "{listing:?}");
    let listing: serde_json::Value = serde_json::from_slice(&listing.stdout).unwrap();
    assert_eq!(listing["entries"].as_array().unwrap().len(), 1);
    assert_eq!(listing["entries"][0]["name"], "a");
    assert_eq!(listing["entries"][0]["attr"]["inode_id"], a.attr.inode_id);
    assert_eq!(listing["next_cursor"], "a");
    assert_eq!(listing["has_more"], true);
    let output = ctl(
        &address,
        &[
            "sql",
            "SELECT * FROM SecondaryIndexTable WHERE fid > 0 ORDER BY fid ASC LIMIT 10",
        ],
    )
    .await;
    assert!(output.status.success(), "{output:?}");
    let indices: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(indices["indices"].as_array().unwrap().len(), 2);
    assert_eq!(indices["indices"][0]["fid"], a.attr.fid.unwrap());
    let single = ctl(
        &address,
        &[
            "sql",
            &format!(
                "SELECT * FROM SecondaryIndexTable WHERE fid = {}",
                a.attr.fid.unwrap()
            ),
        ],
    )
    .await;
    assert!(single.status.success(), "{single:?}");
    let single: serde_json::Value = serde_json::from_slice(&single.stdout).unwrap();
    assert_eq!(single["single_index"]["name"], "a");
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_bulk_benchmark_deletes_exactly_its_successful_creates() {
    let dir = tempfile::tempdir().unwrap();
    let (cluster, address) = start(dir.path()).await;
    let output = ctl(
        &address,
        &[
            "bench",
            "--workload",
            "bulk-ttl-delete",
            "--concurrency",
            "2",
            "--ops",
            "5",
        ],
    )
    .await;
    let reports = benchmark_reports(&output);
    assert_eq!(reports[1]["phase"], "create");
    assert_eq!(reports[2]["phase"], "delete");
    for phase in &reports[1..3] {
        assert_eq!(phase["attempted"], 5);
        assert_eq!(phase["succeeded"], 5);
        assert_eq!(phase["failed"], 0);
    }
    assert_eq!(reports[3]["empty"], true);
    assert!(benchmark_rows(&cluster, &reports).await.is_empty());
    cluster.shutdown().await.unwrap();
}
