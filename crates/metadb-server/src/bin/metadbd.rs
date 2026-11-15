//! `metadbd` — Standalone Single-Node & Distributed Multi-Role MetaDB Server Daemon.

use std::path::PathBuf;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::RegisterNodeRequest;
use metadb_server::{
    render_prometheus_metrics, PersistentMetaDbCluster, ServerRole, ServerTomlConfig,
};

#[derive(Debug, Parser)]
#[command(
    name = "metadbd",
    version,
    about = "MetaDB Distributed Metadata Server Daemon"
)]
struct Cli {
    /// Path to TOML configuration file.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Override node ID.
    #[arg(long)]
    node_id: Option<u64>,

    /// Override cell ID.
    #[arg(long)]
    cell_id: Option<u64>,

    /// Override server role (`all-in-one | be | proxy | cell-manager | global-manager`).
    #[arg(long)]
    role: Option<String>,

    /// Override gRPC listen address (`host:port`).
    #[arg(long)]
    listen: Option<String>,

    /// Override data directory.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Optional cell manager endpoint when joining as a BE node.
    #[arg(long)]
    cell_manager: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let mut cfg = match &cli.config {
        Some(p) => ServerTomlConfig::from_toml_file(p)?,
        None => ServerTomlConfig::default(),
    };

    if let Some(nid) = cli.node_id {
        cfg.node.node_id = nid;
    }
    if let Some(cid) = cli.cell_id {
        cfg.node.cell_id = cid;
    }
    if let Some(role_str) = &cli.role {
        cfg.node.role = match role_str.as_str() {
            "all-in-one" => ServerRole::AllInOne,
            "be" => ServerRole::Be,
            "proxy" => ServerRole::Proxy,
            "cell-manager" => ServerRole::CellManager,
            "global-manager" => ServerRole::GlobalManager,
            other => {
                return Err(format!("unsupported --role '{}'", other).into());
            }
        };
    }
    if let Some(listen) = cli.listen {
        cfg.node.grpc_listen_addr = listen;
    }
    if let Some(dd) = cli.data_dir {
        cfg.node.data_dir = dd;
    }
    let cell_manager_ep = cli.cell_manager.clone();
    let node_id = cfg.node.node_id;
    let cell_id = cfg.node.cell_id;

    let metrics_addr_opt = cfg.node.metrics_listen_addr.clone();
    let shared_secret = cfg.security.shared_secret.clone();
    let (cluster, bound_addr) = PersistentMetaDbCluster::open_and_serve(cfg).await?;
    tracing::info!(grpc_addr = %bound_addr, "metadbd online and serving requests");

    if let Some(cm_ep) = cell_manager_ep {
        let cm_url = if cm_ep.starts_with("http://") || cm_ep.starts_with("https://") {
            cm_ep
        } else {
            format!("http://{}", cm_ep)
        };
        let channel = tonic::transport::Endpoint::from_shared(cm_url.clone())?
            .connect()
            .await?;
        let mut cp = ControlPlaneServiceClient::with_interceptor(
            channel,
            metadb_server::BearerAuth::new(shared_secret),
        );
        let reg_resp = cp
            .register_node(RegisterNodeRequest {
                cell_id,
                node_id,
                grpc_addr: bound_addr.clone(),
            })
            .await?
            .into_inner();
        if let Some(err) = reg_resp.error {
            return Err(format!("failed to register node with {}: {}", cm_url, err.message).into());
        }
        tracing::info!(cell_manager = %cm_url, node_id, cell_id, "registered node with cell manager");
    }

    if let Some(m_addr) = metrics_addr_opt {
        let m_cluster = cluster.clone();
        let m_listener = TcpListener::bind(&m_addr).await?;
        tracing::info!(metrics_addr = %m_addr, "Prometheus metrics endpoint listening");
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = m_listener.accept().await {
                let mut req_buf = [0u8; 512];
                let _ = sock.read(&mut req_buf).await;
                let body = render_prometheus_metrics(&m_cluster.metrics_snapshot());
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
    }

    tokio::signal::ctrl_c().await?;
    tracing::info!("received shutdown signal; flushing state and stopping OpenRaft groups");
    cluster.shutdown().await?;
    Ok(())
}
