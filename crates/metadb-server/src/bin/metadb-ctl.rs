//! `metadb-ctl` — Operator Administration, SQL Execution & Benchmark CLI for MetaDB.

use std::time::Instant;

use clap::{Parser, Subcommand};

use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{
    CreateFileRequest, EnableDualModeSplitRequest, ExecuteSqlRequest, GetRoutesRequest,
    ListDirRequest, MkdirRequest, RegisterCellRequest, ScaleShardReplicasRequest,
    TriggerShardSplitRequest, UnlinkRequest,
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
    Bench {
        #[arg(long, default_value = "hotspot-create")]
        workload: String,
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
        #[arg(long, default_value_t = 1000)]
        ops: usize,
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

fn normalize_url(ep: &str) -> String {
    if ep.starts_with("http://") || ep.starts_with("https://") {
        ep.to_string()
    } else {
        format!("http://{}", ep)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let url = normalize_url(&cli.endpoint);

    match cli.command {
        Command::Cluster {
            sub: ClusterCmd::Status,
        } => {
            let mut cp = ControlPlaneServiceClient::connect(url).await?;
            let resp = cp
                .get_routes(GetRoutesRequest { cell_id: 0 })
                .await?
                .into_inner();
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
            let mut cp = ControlPlaneServiceClient::connect(url).await?;
            match sub {
                ShardCmd::List { cell_id } => {
                    let resp = cp
                        .get_routes(GetRoutesRequest { cell_id })
                        .await?
                        .into_inner();
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
                        eprintln!("Split error: {}", err.message);
                    } else {
                        println!(
                            "split_executed={} child_shard_id={}",
                            resp.split_executed, resp.child_shard_id
                        );
                    }
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
                        eprintln!("EnableDualMode error: {}", err.message);
                    } else {
                        println!(
                            "DualModeFineGrained enabled on shard {} (PID {})",
                            shard_id, pid
                        );
                    }
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
            let mut cp = ControlPlaneServiceClient::connect(url).await?;
            let resp = cp
                .scale_shard_replicas(ScaleShardReplicasRequest {
                    cell_id,
                    target_voters,
                })
                .await?
                .into_inner();
            if let Some(err) = resp.error {
                eprintln!("ScaleReplicas error: {}", err.message);
            } else {
                println!(
                    "Upgraded {} shard(s) to {}-Voter",
                    resp.upgraded_shards, target_voters
                );
            }
        }
        Command::Cell {
            sub: CellCmd::Register { cell_id, endpoints },
        } => {
            let mut cp = ControlPlaneServiceClient::connect(url).await?;
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
                eprintln!("RegisterCell error: {}", err.message);
            } else {
                println!("Registered Cell {}", cell_id);
            }
        }
        Command::Sql { query } => {
            let mut meta = MetadataServiceClient::connect(url).await?;
            let resp = meta
                .execute_sql(ExecuteSqlRequest {
                    sql: query,
                    params: vec![],
                    caller_hlc: 0,
                })
                .await?
                .into_inner();
            if let Some(err) = resp.error {
                eprintln!("SQL error: {}", err.message);
            } else if let Some(attr) = resp.single_attr {
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
        } => {
            if workload != "hotspot-create" && workload != "bulk-ttl-delete" {
                return Err(format!(
                    "unsupported --workload '{}': expected 'hotspot-create' or 'bulk-ttl-delete'",
                    workload
                )
                .into());
            }
            let mut setup_client = MetadataServiceClient::connect(url.clone()).await?;
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
                handles.push(tokio::spawn(async move {
                    let mut client = MetadataServiceClient::connect(u).await.unwrap();
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
                    del_handles.push(tokio::spawn(async move {
                        let mut client = MetadataServiceClient::connect(u).await.unwrap();
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
