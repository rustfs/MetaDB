//! TOML & programmatic server configuration for `metadbd` and `PersistentMetaDbCluster`.

use std::fs;
use std::net::IpAddr;
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
///
/// A second all-in-one process joins an existing Raft group when [`ClusterSection::join`]
/// is set and its data directory does not already own a cell.
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

/// Application-level bearer auth. This process does not terminate TLS.
///
/// An empty `shared_secret` is accepted only for loopback listeners. A wildcard or
/// any other non-loopback bind with an empty secret refuses to start. When the
/// secret is non-empty, gRPC and the metrics listener require the same bearer.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SecuritySection {
    /// When non-empty, gRPC and metrics require `authorization: Bearer <shared_secret>`.
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

/// Seed address for joining an existing cell instead of bootstrapping a new one.
///
/// Leave [`ClusterSection::join`] empty on the first process. A later process with the
/// same `cell_id` and a fresh data directory registers itself and is added to every
/// shard Raft group. The seed must be the current Raft leader. Restarting a directory
/// that already bootstrapped or joined ignores this field and reopens local state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterSection {
    /// `host:port` (or `http://host:port`) of a cell member that can propose membership.
    pub join: String,
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
    pub cluster: ClusterSection,
    #[serde(skip)]
    pub raft_options: RaftNodeOptions,
}

impl ServerTomlConfig {
    pub fn from_toml_str(s: &str) -> Result<Self> {
        let value: toml::Value = toml::from_str(s)
            .map_err(|e| MetaDbError::CodecError(format!("invalid TOML config: {e}")))?;
        reject_unsupported_security_keys(&value)?;
        ServerTomlConfig::deserialize(value)
            .map_err(|e| MetaDbError::CodecError(format!("invalid TOML config: {e}")))
    }

    pub fn from_toml_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Self::from_toml_str(&content)
    }

    pub fn to_lsm_config(&self) -> Result<LsmEngineConfig> {
        let ratio = self.storage.locality_tombstone_ratio;
        if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
            return Err(MetaDbError::CodecError(format!(
                "locality_tombstone_ratio must be a finite value in [0, 1], got {ratio}"
            )));
        }
        let ratio_permille = ((ratio * 1000.0) as usize).clamp(1, 1000);
        Ok(LsmEngineConfig {
            mvcc_retention_ms: self.storage.mvcc_retention_ms,
            tombstone_density_threshold: self.storage.locality_tombstone_threshold,
            tombstone_min_ratio_permille: ratio_permille,
            bloom_bits_per_key: self.storage.bloom_bits_per_key,
            auto_locality_compaction: true,
        })
    }

    /// Refuses to start when a listener is not loopback and no secret is configured.
    ///
    /// Call this on the configured addresses and again on the address that will
    /// actually be bound (a restarted process may reuse a saved advertise address).
    pub fn reject_unauthenticated_bind(&self, addr: &str) -> Result<()> {
        reject_unauthenticated_bind(addr, &self.security.shared_secret)
    }
}

/// `true` only for an explicit loopback bind: `localhost`, `127.0.0.0/8`, or `::1`.
///
/// Wildcard addresses (`0.0.0.0`, `::`) and unparseable hosts are not loopback.
/// `localhost` is accepted as a name and is not resolved, so a hosts-file alias
/// is not detected here.
fn is_loopback_bind(addr: &str) -> bool {
    let host = bind_host(addr.trim());
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match host.parse::<IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => false,
    }
}

fn reject_unauthenticated_bind(addr: &str, secret: &str) -> Result<()> {
    if !secret.trim().is_empty() || is_loopback_bind(addr) {
        return Ok(());
    }
    Err(MetaDbError::CodecError(format!(
        "refusing to serve unauthenticated listener {addr}; a wildcard or non-loopback bind requires a non-empty [security].shared_secret. Empty, missing, and misspelled secrets fail closed. This process does not terminate TLS"
    )))
}

fn bind_host(addr: &str) -> &str {
    if let Some(rest) = addr.strip_prefix('[') {
        let end = rest.find(']').unwrap_or(rest.len());
        return &rest[..end];
    }
    match addr.rsplit_once(':') {
        Some((host, port))
            if !host.is_empty()
                && !host.contains(':')
                && port.bytes().all(|b| b.is_ascii_digit()) =>
        {
            host
        }
        _ => addr,
    }
}

fn normalize_key(key: &str) -> String {
    key.to_ascii_lowercase().replace('-', "_")
}

fn is_tls_key(key: &str) -> bool {
    let key = normalize_key(key);
    [
        "tls", "ssl", "cert", "x509", "mtls", "cipher", "keystore", "https",
    ]
    .iter()
    .any(|needle| key.contains(needle))
}

fn is_secret_key(key: &str) -> bool {
    let key = normalize_key(key);
    [
        "secret",
        "password",
        "passwd",
        "token",
        "credential",
        "auth",
    ]
    .iter()
    .any(|needle| key.contains(needle))
        || key.starts_with("secur")
}

fn security_key_error(key: &str) -> MetaDbError {
    if is_tls_key(key) {
        MetaDbError::CodecError(format!(
            "refusing to start: `{key}` is not a supported TLS setting. This process does not terminate TLS and will not serve plaintext while a TLS option is present. Terminate TLS in an external proxy, then remove `{key}`"
        ))
    } else {
        MetaDbError::CodecError(format!(
            "refusing to start: unrecognized security key `{key}`. A missing or misspelled shared_secret must not leave listeners open. The only accepted security key is [security].shared_secret"
        ))
    }
}

fn reject_unsupported_security_keys(value: &toml::Value) -> Result<()> {
    let Some(root) = value.as_table() else {
        return Err(MetaDbError::CodecError(
            "TOML config must be a table".to_string(),
        ));
    };
    if let Some(security) = root.get("security") {
        let Some(table) = security.as_table() else {
            return Err(MetaDbError::CodecError(
                "refusing to start: [security] must be a table. The only accepted security key is [security].shared_secret".to_string(),
            ));
        };
        for (key, child) in table {
            if key != "shared_secret" {
                return Err(security_key_error(key));
            }
            // The secret value itself must not smuggle a nested TLS or secret table.
            reject_nested_security_keys(child)?;
        }
    }
    for (key, child) in root {
        if key == "security" {
            continue;
        }
        if is_tls_key(key) || is_secret_key(key) {
            return Err(security_key_error(key));
        }
        reject_nested_security_keys(child)?;
    }
    Ok(())
}

/// Rejects secret and TLS keys anywhere except the direct `[security].shared_secret` field.
/// Tables and arrays are walked, including `[node.transport]` and `[[listeners]]`.
fn reject_nested_security_keys(value: &toml::Value) -> Result<()> {
    match value {
        toml::Value::Table(table) => {
            for (key, child) in table {
                if is_tls_key(key) || is_secret_key(key) {
                    return Err(security_key_error(key));
                }
                reject_nested_security_keys(child)?;
            }
        }
        toml::Value::Array(items) => {
            for item in items {
                reject_nested_security_keys(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_ratio_is_validated_before_it_reaches_the_engine() {
        let mut cfg = ServerTomlConfig::default();
        cfg.storage.locality_tombstone_ratio = 0.5;
        assert_eq!(
            cfg.to_lsm_config().unwrap().tombstone_min_ratio_permille,
            500
        );
        cfg.storage.locality_tombstone_ratio = 1.0;
        assert_eq!(
            cfg.to_lsm_config().unwrap().tombstone_min_ratio_permille,
            1000
        );
        cfg.storage.locality_tombstone_ratio = 0.0;
        assert_eq!(cfg.to_lsm_config().unwrap().tombstone_min_ratio_permille, 1);

        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.01, 1.01] {
            cfg.storage.locality_tombstone_ratio = bad;
            assert!(cfg.to_lsm_config().is_err(), "ratio {bad} must be rejected");
        }
    }

    #[test]
    fn toml_parses_storage_byte_sizes() {
        let cfg = ServerTomlConfig::from_toml_str(
            r#"
            [storage]
            memtable_size_bytes = 1024
            block_cache_bytes = 65536
            "#,
        )
        .unwrap();
        assert_eq!(cfg.storage.memtable_size_bytes, 1024);
        assert_eq!(cfg.storage.block_cache_bytes, 65536);
    }

    #[test]
    fn loopback_binds_are_explicit_and_wildcards_are_not() {
        for addr in [
            "127.0.0.1:7800",
            "127.0.0.1:0",
            "localhost:7800",
            "[::1]:7800",
        ] {
            assert!(is_loopback_bind(addr), "{addr}");
            assert!(reject_unauthenticated_bind(addr, "").is_ok(), "{addr}");
        }
        for addr in [
            "0.0.0.0:7800",
            "[::]:7800",
            "10.1.2.3:7800",
            "db.internal:7800",
        ] {
            assert!(!is_loopback_bind(addr), "{addr}");
            let err = reject_unauthenticated_bind(addr, "  ").unwrap_err();
            assert!(err.to_string().contains("refusing to serve"), "{err}");
        }
        assert!(reject_unauthenticated_bind("0.0.0.0:7800", "secret").is_ok());
    }

    #[test]
    fn misspelled_secret_and_tls_keys_fail_closed() {
        let typo = ServerTomlConfig::from_toml_str(
            r#"
            [node]
            grpc_listen_addr = "0.0.0.0:7800"
            [security]
            shared_secrett = "not-applied"
            "#,
        );
        assert!(
            typo.unwrap_err().to_string().contains("shared_secrett"),
            "a misspelled secret key must not parse"
        );

        let tls = ServerTomlConfig::from_toml_str(
            r#"
            [security]
            shared_secret = "secret"
            tls_cert = "/tmp/cert.pem"
            "#,
        );
        let tls_err = tls.unwrap_err().to_string();
        assert!(tls_err.contains("does not terminate TLS"), "{tls_err}");

        let misplaced = ServerTomlConfig::from_toml_str(
            r#"
            [node]
            shared_secret = "not-in-this-section"
            "#,
        );
        assert!(
            misplaced.unwrap_err().to_string().contains("shared_secret"),
            "a secret outside [security] must not be ignored"
        );

        let section = ServerTomlConfig::from_toml_str(
            r#"
            [tls]
            enabled = true
            "#,
        );
        assert!(section
            .unwrap_err()
            .to_string()
            .contains("does not terminate TLS"));

        let nested = ServerTomlConfig::from_toml_str(
            r#"
            [node]
            grpc_listen_addr = "0.0.0.0:7800"
            [node.transport]
            tls_cert_file = "/tmp/cert.pem"
            [security]
            shared_secret = "secret"
            "#,
        );
        let nested_err = nested.unwrap_err().to_string();
        assert!(
            nested_err.contains("does not terminate TLS"),
            "{nested_err}"
        );

        let listeners = ServerTomlConfig::from_toml_str(
            r#"
            [[listeners]]
            tls = true
            [security]
            shared_secret = "secret"
            "#,
        );
        let listeners_err = listeners.unwrap_err().to_string();
        assert!(
            listeners_err.contains("does not terminate TLS"),
            "{listeners_err}"
        );

        let cfg = ServerTomlConfig::from_toml_str(
            r#"
            [node]
            node_id = 2
            extra_note = "ignored"
            [security]
            shared_secret = "secret"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.node.node_id, 2);
        assert_eq!(cfg.security.shared_secret, "secret");
    }
}
