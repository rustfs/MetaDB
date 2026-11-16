//! `metadb-ctl` — Operator Administration, SQL Execution & Benchmark CLI for MetaDB.

use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{
    AllocateIdBlockRequest, CreateFileRequest, EnableDualModeSplitRequest, ErrorDetailProto,
    ExecuteSqlRequest, GetRoutesRequest, ListDirRequest, MkdirRequest, RegisterCellRequest,
    ScaleShardReplicasRequest, TriggerShardSplitRequest, UnlinkRequest,
};

#[derive(Debug, Parser)]
#[command(
    name = "metadb-ctl",
    version,
    about = "MetaDB Cluster Administration & Benchmark CLI"
)]
struct Cli {
    /// Target `metadbd` gRPC endpoint (`host:port` or `http://host:port`).
    #[arg(long, default_value = "127.0.0.1:7800")]
    endpoint: String,

    /// Bearer token matching `[security].shared_secret`. Empty skips the header.
    #[arg(long, env = "METADB_AUTH_TOKEN", default_value = "")]
    auth_token: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Cluster-wide status inspection.
    Cluster {
        #[command(subcommand)]
        sub: ClusterCmd,
    },
    /// Shard inspection, splitting, and DualMode management.
    Shard {
        #[command(subcommand)]
        sub: ShardCmd,
    },
    /// Raft replica scaling operations.
    Raft {
        #[command(subcommand)]
        sub: RaftCmd,
    },
    /// Multi-Cell management operations.
    Cell {
        #[command(subcommand)]
        sub: CellCmd,
    },
    /// Execute one of the 20 metadata SQL templates.
    Sql { query: String },
    /// Run a concurrent synthetic metadata workload benchmark.
    ///
    /// `--workload hotspot-create` and `bulk-ttl-delete` call a remote `metadbd`.
    /// `--workload mixed` or `--embedded` starts a temporary all-in-one node and runs
    /// [`metadb_server::stress`] (correctness checks plus latency percentiles).
    Bench {
        #[arg(long, default_value = "hotspot-create")]
        workload: String,
        /// Remote-bench workers. Defaults to 16 when omitted.
        /// When set, also overrides the embedded profile's concurrency.
        #[arg(long)]
        concurrency: Option<usize>,
        #[arg(long, default_value_t = 1000)]
        ops: usize,
        /// Start an in-process persistent node instead of dialing `--endpoint`.
        #[arg(long)]
        embedded: bool,
        /// Embedded profile: `smoke` (5s, ignores env) or `local` (env-driven, default 30s).
        #[arg(long, default_value = "smoke")]
        profile: String,
        /// Overrides the embedded profile duration.
        #[arg(long)]
        duration_secs: Option<u64>,
        /// Overrides the embedded profile directory count.
        #[arg(long)]
        keyspace: Option<usize>,
        /// Overrides the embedded profile seed.
        #[arg(long)]
        seed: Option<u64>,
    },
}

#[derive(Debug, Subcommand)]
enum ClusterCmd {
    Status,
}

#[derive(Debug, Subcommand)]
enum ShardCmd {
    List {
        #[arg(long, default_value_t = 0)]
        cell_id: u64,
    },
    Split {
        #[arg(long)]
        shard_id: u64,
    },
    EnableDualMode {
        #[arg(long)]
        shard_id: u64,
        #[arg(long)]
        pid: u64,
    },
}

#[derive(Debug, Subcommand)]
enum RaftCmd {
    ScaleReplicas {
        #[arg(long)]
        cell_id: u64,
        #[arg(long)]
        target_voters: u32,
    },
}

#[derive(Debug, Subcommand)]
enum CellCmd {
    Register {
        #[arg(long)]
        cell_id: u64,
        #[arg(long)]
        endpoints: String,
    },
}

async fn open_cp(
    url: &str,
    token: &str,
) -> Result<
    ControlPlaneServiceClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            metadb_server::BearerAuthClient,
        >,
    >,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let channel = tonic::transport::Endpoint::from_shared(url.to_string())?
        .connect()
        .await?;
    Ok(ControlPlaneServiceClient::with_interceptor(
        channel,
        metadb_server::BearerAuthClient::new(token),
    ))
}

async fn open_meta(
    url: &str,
    token: &str,
) -> Result<
    MetadataServiceClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            metadb_server::BearerAuthClient,
        >,
    >,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let channel = tonic::transport::Endpoint::from_shared(url.to_string())?
        .connect()
        .await?;
    Ok(MetadataServiceClient::with_interceptor(
        channel,
        metadb_server::BearerAuthClient::new(token),
    ))
}

async fn run_embedded_bench(
    profile_name: &str,
    duration_secs: Option<u64>,
    concurrency: Option<usize>,
    keyspace: Option<usize>,
    seed: Option<u64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut profile = match profile_name {
        "smoke" | "ci" => metadb_server::stress::StressProfile::smoke(),
        "local" => metadb_server::stress::StressProfile::from_env(),
        other => {
            return Err(
                format!("unsupported --profile '{other}': expected 'smoke' or 'local'").into(),
            );
        }
    };
    if let Some(secs) = duration_secs {
        profile.duration = std::time::Duration::from_secs(secs.max(1));
    }
    if let Some(n) = concurrency {
        profile.concurrency = n.max(1);
    }
    if let Some(n) = keyspace {
        profile.keyspace_dirs = n.max(1);
    }
    if let Some(n) = seed {
        profile.seed = n;
    }

    let data_dir = std::env::temp_dir().join(format!(
        "metadb-stress-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&data_dir)?;
    let cfg = metadb_server::stress::persistent_config(&data_dir, 4 * 1024 * 1024 * 1024);
    let (cluster, addr) = metadb_server::PersistentMetaDbCluster::open_and_serve(cfg).await?;
    println!("embedded metadbd listening on {addr}");
    let report = metadb_server::stress::run_mixed(cluster.clone(), &profile).await;
    println!("{report}");
    cluster.shutdown().await?;
    let _ = std::fs::remove_dir_all(&data_dir);
    if report.errors > 0 || report.correctness_errors > 0 {
        return Err(format!("embedded stress failed: {report}").into());
    }
    Ok(())
}

fn normalize_url(ep: &str) -> String {
    if ep.starts_with("http://") || ep.starts_with("https://") {
        ep.to_string()
    } else {
        format!("http://{}", ep)
    }
}

fn reject_app_error(
    action: &str,
    error: Option<metadb_proto::ErrorDetailProto>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(error) = error {
        Err(format!("{action}: {}", error.message).into())
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            if let Some(application) = error.downcast_ref::<ApplicationError>() {
                eprintln!("{}", application);
            } else {
                eprintln!(
                    "{}",
                    serde_json::json!({"error": {
                        "operation": "client", "kind": "ClientError", "message": error.to_string()
                    }})
                );
            }
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    let url = normalize_url(&cli.endpoint);
    let token = cli.auth_token.clone();

    match cli.command {
        Command::Cluster {
            sub: ClusterCmd::Status,
        } => {
            let mut cp = open_cp(&url, &token).await?;
            let resp = cp
                .get_routes(GetRoutesRequest { cell_id: 0 })
                .await?
                .into_inner();
            reject_app_error("cluster status", resp.error)?;
            println!("Active Shards: {}", resp.shards.len());
            for s in resp.shards {
                let ep = s.epoch.unwrap_or_default();
                println!(
                    "  shard={} cell={} leader={} voters={} epoch=(conf_ver={}, ver={}) dual_mode={} jumbo={}",
                    s.shard_id,
                    s.cell_id,
                    s.leader_node_id,
                    s.peers.iter().filter(|p| p.is_voter).count(),
                    ep.conf_ver,
                    ep.version,
                    s.dual_mode_fine_grained,
                    s.jumbo_isolated
                );
            }
        }
        Command::Shard { sub } => {
            let mut cp = open_cp(&url, &token).await?;
            match sub {
                ShardCmd::List { cell_id } => {
                    let resp = cp
                        .get_routes(GetRoutesRequest { cell_id })
                        .await?
                        .into_inner();
                    reject_app_error("shard list", resp.error)?;
                    for s in resp.shards {
                        let ep = s.epoch.unwrap_or_default();
                        println!(
                            "shard={} cell={} range=({:?}..{:?}) epoch=({}, {})",
                            s.shard_id, s.cell_id, s.start_key, s.end_key, ep.conf_ver, ep.version
                        );
                    }
                }
                ShardCmd::Split { shard_id } => {
                    let resp = cp
                        .trigger_shard_split(TriggerShardSplitRequest { shard_id })
                        .await?
                        .into_inner();
                    reject_app_error("shard split", resp.error)?;
                    println!(
                        "split_executed={} child_shard_id={}",
                        resp.split_executed, resp.child_shard_id
                    );
                }
                ShardCmd::EnableDualMode { shard_id, pid } => {
                    let resp = cp
                        .enable_dual_mode_split(EnableDualModeSplitRequest {
                            shard_id,
                            dominant_pid: pid,
                            split_name: String::new(),
                        })
                        .await?
                        .into_inner();
                    reject_app_error("enable-dual-mode", resp.error)?;
                    println!(
                        "DualModeFineGrained enabled on shard {} (PID {})",
                        shard_id, pid
                    );
                }
            }
        }
        Command::Raft {
            sub:
                RaftCmd::ScaleReplicas {
                    cell_id,
                    target_voters,
                },
        } => {
            let mut cp = open_cp(&url, &token).await?;
            let resp = cp
                .scale_shard_replicas(ScaleShardReplicasRequest {
                    cell_id,
                    target_voters,
                })
                .await?
                .into_inner();
            reject_app_error("scale-replicas", resp.error)?;
            println!(
                "Upgraded {} shard(s) to {}-Voter",
                resp.upgraded_shards, target_voters
            );
        }
        Command::Cell {
            sub: CellCmd::Register { cell_id, endpoints },
        } => {
            let mut cp = open_cp(&url, &token).await?;
            let eps: Vec<String> = endpoints.split(',').map(|s| s.trim().to_string()).collect();
            let resp = cp
                .register_cell(RegisterCellRequest {
                    cell_id,
                    start_key: vec![0x01],
                    end_key: vec![0x04],
                    cell_manager_endpoints: eps,
                })
                .await?
                .into_inner();
            reject_app_error("cell register", resp.error)?;
            println!("Registered Cell {}", cell_id);
        }
        Command::Sql { query } => {
            let mut meta = open_meta(&url, &token).await?;
            let resp = meta
                .execute_sql(ExecuteSqlRequest {
                    sql: query,
                    params: vec![],
                    caller_hlc: 0,
                })
                .await?
                .into_inner();
            reject_app_error("sql", resp.error)?;
            if let Some(attr) = resp.single_attr {
                println!(
                    "InodeAttr(id={}, children={}, size_bytes={}, mode=0o{:o})",
                    attr.inode_id, attr.num_children, attr.size_bytes, attr.mode
                );
            } else if !resp.entries.is_empty() {
                for e in resp.entries {
                    println!("Entry(pid={}, name='{}')", e.pid, e.name);
                }
            } else if let Some(q) = resp.single_quota {
                println!(
                    "Quota(uid={}, used_bytes={}, used_inodes={}, limit={})",
                    q.uid, q.used_bytes, q.used_inodes, q.quota_limit_bytes
                );
            } else {
                println!("OK");
            }
        }
        Command::Bench {
            workload,
            concurrency,
            ops,
            embedded,
            profile,
            duration_secs,
            keyspace,
            seed,
        } => {
            if embedded || workload == "mixed" {
                run_embedded_bench(&profile, duration_secs, concurrency, keyspace, seed).await?;
                return Ok(());
            }
            let concurrency = concurrency.unwrap_or(16);
            if workload != "hotspot-create" && workload != "bulk-ttl-delete" {
                return Err(format!(
                    "unsupported --workload '{}': expected 'hotspot-create', 'bulk-ttl-delete', or 'mixed'",
                    workload
                )
                .into());
            }
            let mut setup_client = open_meta(&url, &token).await?;
            let bench_pid = if workload == "hotspot-create" {
                999_000u64
            } else {
                999_001u64
            };
            let _ = setup_client
                .mkdir(MkdirRequest {
                    header: None,
                    pid: 1,
                    name: format!("bench_{}", workload.replace('-', "_")),
                    dir_id: bench_pid,
                    uid: 1,
                    mode: 0o755,
                })
                .await?;

            let start = Instant::now();
            let ops_per_task = (ops / concurrency.max(1)).max(1);
            let mut handles = Vec::new();

            for tid in 0..concurrency.max(1) {
                let u = url.clone();
                let token = token.clone();
                handles.push(tokio::spawn(async move {
                    let mut client = open_meta(&u, &token).await.unwrap();
                    for i in 0..ops_per_task {
                        let seq = (tid * ops_per_task + i) as u64;
                        let _ = client
                            .create_file(CreateFileRequest {
                                header: None,
                                pid: bench_pid,
                                name: format!("f_{:04}_{:06}.dat", tid, i),
                                inode_id: (1u64 << 63) | (100_000 + seq),
                                fid: 500_000 + seq,
                                uid: 1,
                                mode: 0o644,
                                size_bytes: 4096,
                            })
                            .await
                            .unwrap();
                    }
                }));
            }
            for h in handles {
                h.await?;
            }
            let total_done = concurrency.max(1) * ops_per_task;

            if workload == "bulk-ttl-delete" {
                let del_start = Instant::now();
                let mut del_handles = Vec::new();
                for tid in 0..concurrency.max(1) {
                    let u = url.clone();
                    let token = token.clone();
                    del_handles.push(tokio::spawn(async move {
                        let mut client = open_meta(&u, &token).await.unwrap();
                        for i in 0..ops_per_task {
                            let _ = client
                                .unlink(UnlinkRequest {
                                    header: None,
                                    pid: bench_pid,
                                    name: format!("f_{:04}_{:06}.dat", tid, i),
                                })
                                .await
                                .unwrap();
                        }
                    }));
                }
                for h in del_handles {
                    h.await?;
                }
                let scan_start = Instant::now();
                let list_resp = setup_client
                    .list_dir(ListDirRequest {
                        header: None,
                        pid: bench_pid,
                        after_name: String::new(),
                        name_prefix: String::new(),
                        limit: 1000,
                    })
                    .await?
                    .into_inner();
                let scan_ms = scan_start.elapsed().as_secs_f64() * 1000.0;
                let del_elapsed = del_start.elapsed();
                println!(
                    "Completed {} bulk TTL deletes in {:.3}s ({:.0} ops/s); post-delete listdir ({} live) took {:.3} ms",
                    total_done,
                    del_elapsed.as_secs_f64(),
                    total_done as f64 / del_elapsed.as_secs_f64(),
                    list_resp.entries.len(),
                    scan_ms
                );
            } else {
                let elapsed = start.elapsed();
                let qps = total_done as f64 / elapsed.as_secs_f64();
                println!(
                    "Completed {} single-directory hotspot creates in {:.3}s ({:.0} ops/s)",
                    total_done,
                    elapsed.as_secs_f64(),
                    qps
                );
            }
        }
    }

    Ok(())
}

type ClientError = Box<dyn std::error::Error + Send + Sync>;
type MetaClient = MetadataServiceClient<
    tonic::service::interceptor::InterceptedService<
        tonic::transport::Channel,
        metadb_server::BearerAuthClient,
    >,
>;

#[derive(Debug)]
struct ApplicationError(serde_json::Value);

impl std::fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ApplicationError {}

fn application_error(operation: &str, error: &ErrorDetailProto) -> ClientError {
    Box::new(ApplicationError(serde_json::json!({"error": {
        "operation": operation, "kind": error.error_kind, "message": error.message
    }})))
}

fn check_error(operation: &str, error: Option<&ErrorDetailProto>) -> Result<(), ClientError> {
    match error {
        Some(error) => Err(application_error(operation, error)),
        None => Ok(()),
    }
}

#[derive(Default)]
struct BenchStats {
    attempted: usize,
    succeeded: usize,
    transport_failed: usize,
    application_failed: usize,
    errors_by_kind: std::collections::BTreeMap<String, usize>,
    latencies: Vec<Duration>,
}

impl BenchStats {
    fn record(
        &mut self,
        elapsed: Duration,
        outcome: Result<Option<ErrorDetailProto>, tonic::Status>,
    ) -> bool {
        self.attempted += 1;
        self.latencies.push(elapsed);
        match outcome {
            Ok(None) => {
                self.succeeded += 1;
                true
            }
            Ok(Some(error)) => {
                self.application_failed += 1;
                *self.errors_by_kind.entry(error.error_kind).or_default() += 1;
                false
            }
            Err(error) => {
                self.transport_failed += 1;
                *self
                    .errors_by_kind
                    .entry(format!("transport:{:?}", error.code()))
                    .or_default() += 1;
                false
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.attempted += other.attempted;
        self.succeeded += other.succeeded;
        self.transport_failed += other.transport_failed;
        self.application_failed += other.application_failed;
        self.latencies.extend(other.latencies);
        for (kind, count) in other.errors_by_kind {
            *self.errors_by_kind.entry(kind).or_default() += count;
        }
    }

    fn report(&mut self, phase: &str, elapsed: Duration) {
        self.latencies.sort_unstable();
        let percentile = |permille: usize| {
            if self.latencies.is_empty() {
                return 0.0;
            }
            let rank = (self.latencies.len() * permille)
                .div_ceil(1000)
                .saturating_sub(1);
            self.latencies[rank].as_secs_f64() * 1000.0
        };
        println!(
            "{}",
            serde_json::json!({
                "phase": phase, "latency_scope": "all_attempts", "attempted": self.attempted, "succeeded": self.succeeded,
                "failed": self.transport_failed + self.application_failed,
                "transport_failed": self.transport_failed, "application_failed": self.application_failed,
                "errors_by_kind": self.errors_by_kind, "elapsed_seconds": elapsed.as_secs_f64(),
                "successful_ops_per_second": self.succeeded as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE),
                "p50_ms": percentile(500), "p99_ms": percentile(990), "p999_ms": percentile(999)
            })
        );
    }
}

async fn run_benchmark(
    url: &str,
    token: &str,
    workload: &str,
    concurrency: usize,
    ops: usize,
) -> Result<(), ClientError> {
    if workload != "hotspot-create" && workload != "bulk-ttl-delete" {
        return Err(format!(
            "unsupported --workload '{}': expected 'hotspot-create' or 'bulk-ttl-delete'",
            workload
        )
        .into());
    }
    if concurrency == 0 {
        return Err("--concurrency must be greater than zero".into());
    }
    let mut meta = open_meta(url, token).await?;
    let mut cp = open_cp(url, token).await?;
    let mut grants = Vec::new();
    for (kind, count) in [(2, 1), (1, ops as u64), (3, ops as u64)] {
        if count == 0 {
            grants.push(0);
            continue;
        }
        let grant = cp
            .allocate_id_block(AllocateIdBlockRequest {
                request_uuid_be: Vec::new(),
                id_kind: kind,
                count,
                preferred_shard_id: 0,
            })
            .await?
            .into_inner();
        check_error("bench_allocate", grant.error.as_ref())?;
        if grant.count != count || grant.start_id.checked_add(count - 1).is_none() {
            return Err("allocator returned an invalid benchmark ID block".into());
        }
        grants.push(grant.start_id);
    }
    let bench_pid = grants[0];
    let inode_start = grants[1];
    let fid_start = grants[2];
    let setup = meta
        .mkdir(MkdirRequest {
            header: None,
            pid: 1,
            name: format!("bench_{}_{}", workload.replace('-', "_"), bench_pid),
            dir_id: bench_pid,
            uid: 1,
            mode: 0o755,
        })
        .await?
        .into_inner();
    check_error("bench_mkdir", setup.error.as_ref())?;
    println!(
        "{}",
        serde_json::json!({"bench_pid": bench_pid, "workload": workload, "requested_ops": ops})
    );

    let task_count = concurrency.min(ops);
    let mut handles = Vec::new();
    let start = Instant::now();
    for tid in 0..task_count {
        let mut client: MetaClient = meta.clone();
        let base = ops / task_count;
        let remainder = ops % task_count;
        let first = tid * base + tid.min(remainder);
        let count = base + usize::from(tid < remainder);
        handles.push(tokio::spawn(async move {
            let mut stats = BenchStats::default();
            let mut created = Vec::new();
            for seq in first..first + count {
                let name = format!("f_{}.dat", seq);
                let started = Instant::now();
                let result = client
                    .create_file(CreateFileRequest {
                        header: None,
                        pid: bench_pid,
                        name: name.clone(),
                        inode_id: inode_start + seq as u64,
                        fid: fid_start + seq as u64,
                        uid: 1,
                        mode: 0o644,
                        size_bytes: 4096,
                    })
                    .await
                    .map(|r| r.into_inner().error);
                if stats.record(started.elapsed(), result) {
                    created.push(name);
                }
            }
            (stats, created)
        }));
    }
    let mut creates = BenchStats::default();
    let mut created_names = Vec::new();
    for handle in handles {
        let (stats, names) = handle.await?;
        creates.merge(stats);
        created_names.extend(names);
    }
    creates.report("create", start.elapsed());
    let mut failed = creates.succeeded != ops;

    if workload == "bulk-ttl-delete" {
        let mut handles = Vec::new();
        let delete_start = Instant::now();
        let chunk_size = created_names.len().div_ceil(concurrency).max(1);
        for names in created_names.chunks(chunk_size) {
            let names = names.to_vec();
            let mut client: MetaClient = meta.clone();
            handles.push(tokio::spawn(async move {
                let mut stats = BenchStats::default();
                for name in names {
                    let started = Instant::now();
                    let result = client
                        .unlink(UnlinkRequest {
                            header: None,
                            pid: bench_pid,
                            name,
                        })
                        .await
                        .map(|r| r.into_inner().error);
                    stats.record(started.elapsed(), result);
                }
                stats
            }));
        }
        let mut deletes = BenchStats::default();
        for handle in handles {
            deletes.merge(handle.await?);
        }
        deletes.report("delete", delete_start.elapsed());
        failed |= deletes.succeeded != ops;
        let scan_start = Instant::now();
        let listing = meta
            .list_dir(ListDirRequest {
                header: None,
                pid: bench_pid,
                after_name: String::new(),
                name_prefix: String::new(),
                limit: 1,
            })
            .await?
            .into_inner();
        check_error("bench_listdir", listing.error.as_ref())?;
        println!(
            "{}",
            serde_json::json!({"phase": "post_delete_listdir", "empty": listing.entries.is_empty() && !listing.has_more, "elapsed_ms": scan_start.elapsed().as_secs_f64() * 1000.0})
        );
        failed |= !listing.entries.is_empty() || listing.has_more;
    }
    if failed {
        return Err(
            "benchmark did not complete every requested operation successfully; see phase reports"
                .into(),
        );
    }
    Ok(())
}
