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
    /// Take, release, create, or restore a single-node backup.
    Backup {
        #[command(subcommand)]
        sub: BackupCmd,
    },
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
enum BackupCmd {
    /// Prevent log purge and snapshot deletion until release or expiry.
    Hold {
        /// Server data directory (`node.data_dir`).
        #[arg(long)]
        data_dir: std::path::PathBuf,
        /// Wall-clock lifetime in seconds. Minimum 1, maximum 86400.
        #[arg(long, default_value_t = 900)]
        ttl_secs: u64,
    },
    /// Drop the hold file so purge can resume under the normal snapshot rules.
    Release {
        #[arg(long)]
        data_dir: std::path::PathBuf,
    },
    /// Copy a restorable snapshot or flushed LSM plus the unpurged Raft log.
    Create {
        #[arg(long)]
        data_dir: std::path::PathBuf,
        /// New directory for the backup artifact. Must not already exist.
        #[arg(long)]
        output: std::path::PathBuf,
    },
    /// Restore an artifact into a data directory that does not already exist.
    Restore {
        #[arg(long)]
        backup: std::path::PathBuf,
        #[arg(long)]
        data_dir: std::path::PathBuf,
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

async fn run_backup(command: BackupCmd) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command {
        BackupCmd::Hold { data_dir, ttl_secs } => {
            let ttl = std::time::Duration::from_secs(ttl_secs);
            match metadb_raft::backup::begin_hold(&data_dir, ttl)? {
                metadb_raft::backup::BeginHold::AlreadyActive(record) => {
                    print_hold("backup hold already active", &record);
                }
                metadb_raft::backup::BeginHold::Pending => {
                    if metadb_raft::backup::lsm_directory_is_locked(&data_dir) {
                        let deadline =
                            std::time::Instant::now() + std::time::Duration::from_secs(15);
                        loop {
                            if let Some(record) = metadb_raft::backup::read_hold(&data_dir)? {
                                if record.phase == metadb_raft::backup::HOLD_PHASE_ACTIVE {
                                    print_hold("backup hold", &record);
                                    return Ok(());
                                }
                            }
                            if std::time::Instant::now() >= deadline {
                                return Err(
                                    "backup hold: running server did not latch floors before the wait expired"
                                        .into(),
                                );
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    } else {
                        let mut floors = Vec::new();
                        for shard_id in metadb_raft::backup::list_shard_ids(&data_dir)? {
                            let raft_dir = data_dir
                                .join("be")
                                .join("shards")
                                .join(format!("{shard_id:020}"))
                                .join("raft");
                            floors.push(metadb_raft::backup::disk_floor(shard_id, &raft_dir)?);
                        }
                        let record = metadb_raft::backup::finish_hold(&data_dir, floors)?;
                        print_hold("backup hold", &record);
                    }
                }
            }
        }
        BackupCmd::Release { data_dir } => {
            metadb_raft::backup::release_hold(&data_dir)?;
            println!("backup hold released");
        }
        BackupCmd::Create { data_dir, output } => {
            let output = absolute_new_path(&output)?;
            if metadb_raft::backup::lsm_directory_is_locked(&data_dir) {
                metadb_raft::backup::write_backup_job(&data_dir, &output)?;
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
                loop {
                    if let Some(result) = metadb_raft::backup::read_backup_job_result(&data_dir)? {
                        if result.output_dir == output.to_string_lossy() {
                            if result.ok {
                                println!("{}", result.message);
                                return Ok(());
                            }
                            return Err(result.message.into());
                        }
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(
                            "backup rejected: running server did not finish BACKUP_JOB before the wait expired"
                                .into(),
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            } else {
                let summary = metadb_raft::backup::create_offline_backup(
                    &data_dir,
                    &output,
                    &metadb_server::check_backup_tree,
                )?;
                println!(
                    "backup wrote {} files={} shards={} hold_id={}",
                    summary.destination.display(),
                    summary.file_count,
                    summary.shard_count,
                    summary.hold_id
                );
            }
        }
        BackupCmd::Restore { backup, data_dir } => {
            if data_dir.exists() && metadb_raft::backup::lsm_directory_is_locked(&data_dir) {
                return Err(
                    "backup restore rejected: destination is open by a running process".into(),
                );
            }
            let data_dir = absolute_new_path(&data_dir)?;
            let summary = metadb_raft::backup::restore_backup(
                &backup,
                &data_dir,
                &metadb_server::check_backup_tree,
            )?;
            println!(
                "backup restore wrote {} files={} shards={} hold_id={}",
                summary.destination.display(),
                summary.file_count,
                summary.shard_count,
                summary.hold_id
            );
        }
    }
    Ok(())
}

fn absolute_new_path(
    path: &std::path::Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if path.as_os_str().is_empty() {
        return Err("backup path is empty".into());
    }
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => std::path::Path::new("."),
    };
    let parent = std::fs::canonicalize(parent)?;
    let name = path.file_name().ok_or("backup path has no file name")?;
    Ok(parent.join(name))
}

fn print_hold(label: &str, record: &metadb_raft::backup::BackupHoldRecord) {
    let phase = match record.phase {
        metadb_raft::backup::HOLD_PHASE_PENDING => "pending",
        metadb_raft::backup::HOLD_PHASE_ACTIVE => "active",
        metadb_raft::backup::HOLD_PHASE_RELEASED => "released",
        _ => "unknown",
    };
    println!(
        "{label} id={} expires_unix_ms={} phase={phase} shards={}",
        record.hold_id,
        record.expires_unix_ms,
        record.floors.len()
    );
    for floor in &record.floors {
        println!(
            "  shard={} max_purge_index={} retain_snapshots_through={}",
            floor.shard_id, floor.max_purge_index, floor.retain_snapshots_through
        );
    }
}

fn normalize_url(ep: &str) -> String {
    if ep.starts_with("http://") || ep.starts_with("https://") {
        ep.to_string()
    } else {
        format!("http://{}", ep)
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
            check_error("get_routes", resp.error.as_ref())?;
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
                    check_error("get_routes", resp.error.as_ref())?;
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
                    if let Some(err) = resp.error {
                        return Err(application_error("split", &err));
                    }
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
                    if let Some(err) = resp.error {
                        return Err(application_error("enable_dual_mode", &err));
                    }
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
            if let Some(err) = resp.error {
                return Err(application_error("scale_replicas", &err));
            }
            println!(
                "Upgraded {} shard(s) to {}-Voter",
                resp.upgraded_shards, target_voters
            );
        }
        Command::Backup { sub } => run_backup(sub).await?,
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
            if let Some(err) = resp.error {
                return Err(application_error("register_cell", &err));
            }
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
            check_error("sql", resp.error.as_ref())?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
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
            } else {
                run_benchmark(&url, &token, &workload, concurrency.unwrap_or(16), ops).await?;
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
