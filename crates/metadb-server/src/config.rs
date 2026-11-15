//! TOML & programmatic server configuration for `metadbd` and `PersistentMetaDbCluster`.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use metadb_core::{CellId, MetaDbError, NodeId, Result, DEFAULT_MAX_CLOCK_DRIFT_MS};
use metadb_raft::RaftNodeOptions;
use metadb_storage::{LsmEngineConfig, DEFAULT_MVCC_RETENTION_MS};

/// Operational role of a `metadbd` process.
///
/// Only [`ServerRole::AllInOne`] is implemented. The other variants are accepted by
/// config parsing so deployments can name them, but `PersistentMetaDbCluster::open_and_serve`
/// rejects them instead of silently starting every subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ServerRole {
    #[default]
    AllInOne,
    Be,
    Proxy,
    CellManager,
    GlobalManager,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeSection {
    pub node_id: NodeId,
    pub cell_id: CellId,
    pub role: ServerRole,
    pub data_dir: PathBuf,
    pub grpc_listen_addr: String,
    pub metrics_listen_addr: Option<String>,
}

impl Default for NodeSection {
    fn default() -> Self {
        Self {
            node_id: 1,
            cell_id: 1,
            role: ServerRole::AllInOne,
            data_dir: PathBuf::from("./metadb-data"),
            grpc_listen_addr: "127.0.0.1:7800".to_string(),
            metrics_listen_addr: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClockSection {
    pub max_clock_drift_ms: u64,
}

impl Default for ClockSection {
    fn default() -> Self {
        Self {
            max_clock_drift_ms: DEFAULT_MAX_CLOCK_DRIFT_MS,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageSection {
    pub memtable_size_bytes: usize,
    pub block_cache_bytes: usize,
    pub mvcc_retention_ms: u64,
    pub bloom_bits_per_key: usize,
    pub locality_tombstone_threshold: usize,
    pub locality_tombstone_ratio: f64,
}

impl Default for StorageSection {
    fn default() -> Self {
        Self {
            memtable_size_bytes: 32 * 1024 * 1024,
            block_cache_bytes: 64 * 1024 * 1024,
            mvcc_retention_ms: DEFAULT_MVCC_RETENTION_MS,
            bloom_bits_per_key: 10,
            locality_tombstone_threshold: 128,
            locality_tombstone_ratio: 0.50,
        }
    }
}

/// Application-level gRPC auth. TLS is not terminated by this process.
///
/// An empty `shared_secret` leaves metadata and control-plane RPCs unauthenticated.
/// The default bind address is `127.0.0.1`. Do not publish that socket on an
/// untrusted network until a secret (or an external TLS terminator) is configured.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SecuritySection {
    /// When non-empty, every gRPC call must send `authorization: Bearer <shared_secret>`.
    pub shared_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShardingSection {
    pub bootstrap_table_shards: bool,
    pub shard_split_threshold_bytes: usize,
}

impl Default for ShardingSection {
    fn default() -> Self {
        Self {
            bootstrap_table_shards: true,
            shard_split_threshold_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
}

/// Top-level TOML configuration matching `docs/guides/{en,ja,fr,zh,de}/deployment.md`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerTomlConfig {
    pub node: NodeSection,
    pub clock: ClockSection,
    pub storage: StorageSection,
    pub sharding: ShardingSection,
    pub security: SecuritySection,
    #[serde(skip)]
    pub raft_options: RaftNodeOptions,
}

impl ServerTomlConfig {
    pub fn from_toml_str(s: &str) -> Result<Self> {
        toml::from_str(s)
            .map_err(|e| MetaDbError::CodecError(format!("invalid TOML config: {}", e)))
    }

    pub fn from_toml_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Self::from_toml_str(&content)
    }

    pub fn to_lsm_config(&self) -> LsmEngineConfig {
        let ratio_permille =
            ((self.storage.locality_tombstone_ratio * 1000.0) as usize).clamp(1, 1000);
        LsmEngineConfig {
            mvcc_retention_ms: self.storage.mvcc_retention_ms,
            tombstone_density_threshold: self.storage.locality_tombstone_threshold,
            tombstone_min_ratio_permille: ratio_permille,
            bloom_bits_per_key: self.storage.bloom_bits_per_key,
            auto_locality_compaction: true,
        }
    }
}
