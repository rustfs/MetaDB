//! TOML & programmatic server configuration for `metadbd` and `PersistentMetaDbCluster`.

use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use metadb_core::{CellId, MetaDbError, NodeId, Result, DEFAULT_MAX_CLOCK_DRIFT_MS};
use metadb_raft::RaftNodeOptions;
use metadb_storage::{LsmEngineConfig, DEFAULT_MVCC_RETENTION_MS};
use metadb_transport::{
    check_transport_key, PolicyFields, StartupResolution, TransportPolicy,
    DEFAULT_MAX_INFLIGHT_NEGOTIATIONS, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_QUEUE_PAIRS,
    DEFAULT_MAX_REGISTERED_BYTES, DEFAULT_NEGOTIATION_TIMEOUT_MS,
};

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
    /// Queued immutable memtables allowed before writers wait for a flush.
    /// The storage design documents a backpressure limit of four.
    pub max_imm_memtables: usize,
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
            max_imm_memtables: 4,
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
    /// Optional transport policy. Omitted sections keep TCP/gRPC.
    ///
    /// `mode = "rdma"` fails closed in [`Self::resolve_transport`]. This section
    /// does not open an RDMA device or a second listener.
    pub transport: TransportSection,
    #[serde(skip)]
    pub raft_options: RaftNodeOptions,
    /// Overrides the 45-second join budget. `None` keeps that budget.
    /// Not read from TOML; tests use it so a refused seed returns without waiting.
    #[serde(skip)]
    pub join_timeout: Option<Duration>,
}

impl ServerTomlConfig {
    pub fn from_toml_str(s: &str) -> Result<Self> {
        let value: toml::Value = toml::from_str(s)
            .map_err(|e| MetaDbError::CodecError(format!("invalid TOML config: {e}")))?;
        reject_unsupported_security_keys(&value)?;
        reject_transport_table(&value)?;
        let cfg = ServerTomlConfig::deserialize(value)
            .map_err(|e| MetaDbError::CodecError(format!("invalid TOML config: {e}")))?;
        // Required RDMA fails here, before a listener exists. Prefer-RDMA resolves
        // to an explicit TCP/gRPC fallback and is checked again at startup.
        cfg.resolve_transport()?;
        Ok(cfg)
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
            memtable_size_bytes: self.storage.memtable_size_bytes.max(1),
            max_imm_memtables: self.storage.max_imm_memtables.max(1),
        })
    }

    /// Refuses to start when a listener is not loopback and no secret is configured.
    ///
    /// Call this on the configured addresses and again on the address that will
    /// actually be bound (a restarted process may reuse a saved advertise address).
    pub fn reject_unauthenticated_bind(&self, addr: &str) -> Result<()> {
        reject_unauthenticated_bind(addr, &self.security.shared_secret)
    }

    /// Resolves the carried transport for this process.
    ///
    /// The only successful selection is TCP/gRPC. `mode = "rdma"` returns an
    /// error and must not bind. `mode = "prefer-rdma"` succeeds with an explicit
    /// fallback record. No queue pairs or registered memory are allocated.
    pub fn resolve_transport(&self) -> Result<StartupResolution> {
        let policy = TransportPolicy::try_from_fields(PolicyFields {
            mode: &self.transport.mode,
            negotiation_timeout_ms: self.transport.negotiation_timeout_ms,
            max_inflight_negotiations: self.transport.max_inflight_negotiations,
            max_queue_pairs: self.transport.max_queue_pairs,
            max_registered_bytes: self.transport.max_registered_bytes,
            max_message_bytes: self.transport.max_message_bytes,
        })
        .map_err(transport_config_error)?;
        policy.resolve_startup().map_err(transport_config_error)
    }
}

/// Operator transport policy. This is not an RDMA implementation.
///
/// Unknown keys are rejected. The rest of the file still ignores unknown keys
/// that do not name a secret or TLS setting; an ignored RDMA or bypass key
/// would look like a configured fast path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransportSection {
    /// `tcp-grpc` (default), `prefer-rdma`, or `rdma`.
    ///
    /// `rdma` and the synonyms `roce`, `rocev2`, `infiniband`, `ib`, `ibverbs`,
    /// and `verbs` require an RDMA backend and refuse to start. `tcp` and
    /// `grpc` are not accepted aliases.
    pub mode: String,
    /// Budget for one in-process negotiation, in milliseconds. Zero is rejected.
    pub negotiation_timeout_ms: u64,
    /// Ceiling on concurrent negotiation attempts. This process does not run
    /// a network handshake, and the ceiling is still enforced by the contract.
    pub max_inflight_negotiations: u32,
    /// Future queue-pair ceiling. Nothing is allocated when this is set.
    pub max_queue_pairs: u32,
    /// Future registered-memory ceiling, in bytes. Nothing is registered.
    pub max_registered_bytes: u64,
    /// Future single-message ceiling, in bytes. No RDMA message is posted.
    pub max_message_bytes: u64,
}

impl Default for TransportSection {
    fn default() -> Self {
        Self {
            mode: "tcp-grpc".to_string(),
            negotiation_timeout_ms: DEFAULT_NEGOTIATION_TIMEOUT_MS,
            max_inflight_negotiations: DEFAULT_MAX_INFLIGHT_NEGOTIATIONS,
            max_queue_pairs: DEFAULT_MAX_QUEUE_PAIRS,
            max_registered_bytes: DEFAULT_MAX_REGISTERED_BYTES,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        }
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

fn transport_config_error(error: metadb_transport::TransportError) -> MetaDbError {
    MetaDbError::CodecError(error.to_string())
}

fn reject_transport_table(value: &toml::Value) -> Result<()> {
    let Some(root) = value.as_table() else {
        return Ok(());
    };
    let Some(transport) = root.get("transport") else {
        return Ok(());
    };
    let Some(table) = transport.as_table() else {
        return Err(MetaDbError::CodecError(
            "[transport] must be a table. The only carried transport is tcp-grpc, and RDMA is not implemented".to_string(),
        ));
    };
    for key in table.keys() {
        check_transport_key(key).map_err(transport_config_error)?;
    }
    Ok(())
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
        assert_eq!(cfg.storage.max_imm_memtables, 4);
        let lsm = cfg.to_lsm_config().unwrap();
        assert_eq!(lsm.memtable_size_bytes, 1024);
        assert_eq!(lsm.max_imm_memtables, 4);
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

    #[test]
    fn omitted_transport_section_stays_on_tcp_grpc() {
        let cfg = ServerTomlConfig::from_toml_str(
            r#"
            [node]
            node_id = 3
            "#,
        )
        .unwrap();
        assert_eq!(cfg.transport.mode, "tcp-grpc");
        let resolution = cfg.resolve_transport().unwrap();
        assert_eq!(
            resolution.selected().map(|transport| transport.to_string()),
            Some("tcp-grpc".to_string())
        );
        assert!(!resolution.explicit_rdma_fallback());
        assert!(resolution.outcome.allocation().is_empty());
    }

    #[test]
    fn required_rdma_mode_fails_closed_before_a_listener() {
        let err = ServerTomlConfig::from_toml_str(
            r#"
            [transport]
            mode = "rdma"
            "#,
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("RDMA backend is not implemented"), "{text}");
        assert!(text.contains("does not fall back"), "{text}");

        let mut cfg = ServerTomlConfig::default();
        cfg.transport.mode = "roce".to_string();
        let programmatic = cfg.resolve_transport().unwrap_err().to_string();
        assert!(
            programmatic.contains("backend-not-implemented"),
            "{programmatic}"
        );
    }

    #[test]
    fn prefer_rdma_records_tcp_fallback_and_does_not_allocate() {
        let cfg = ServerTomlConfig::from_toml_str(
            r#"
            [transport]
            mode = "prefer-rdma"
            negotiation_timeout_ms = 250
            max_inflight_negotiations = 4
            max_queue_pairs = 8
            max_registered_bytes = 1048576
            max_message_bytes = 65536
            "#,
        )
        .unwrap();
        assert_eq!(cfg.transport.max_queue_pairs, 8);
        let resolution = cfg.resolve_transport().unwrap();
        assert!(resolution.explicit_rdma_fallback());
        assert_eq!(
            resolution
                .selected()
                .map(|transport| transport.to_string())
                .as_deref(),
            Some("tcp-grpc")
        );
        assert!(resolution.outcome.allocation().is_empty());
        assert!(!metadb_transport::RDMA_BACKEND_IMPLEMENTED);
    }

    #[test]
    fn transport_bypass_device_and_unbounded_keys_fail_closed() {
        let insecure = ServerTomlConfig::from_toml_str(
            r#"
            [transport]
            mode = "tcp-grpc"
            insecure = true
            "#,
        )
        .unwrap_err()
        .to_string();
        assert!(
            insecure.contains("not an authorization bypass"),
            "{insecure}"
        );

        let device = ServerTomlConfig::from_toml_str(
            r#"
            [transport]
            device = "mlx5_0"
            "#,
        )
        .unwrap_err()
        .to_string();
        assert!(
            device.contains("no RDMA backend is implemented"),
            "{device}"
        );

        let unlimited = ServerTomlConfig::from_toml_str(
            r#"
            [transport]
            max_registered_bytes = 0
            "#,
        )
        .unwrap_err()
        .to_string();
        assert!(unlimited.contains("max_registered_bytes"), "{unlimited}");
        assert!(unlimited.contains("Zero is not unlimited"), "{unlimited}");

        let not_a_table = ServerTomlConfig::from_toml_str(
            r#"
            transport = "rdma"
            "#,
        )
        .unwrap_err()
        .to_string();
        assert!(
            not_a_table.contains("[transport] must be a table"),
            "{not_a_table}"
        );
    }

    #[tokio::test]
    async fn required_rdma_open_does_not_create_a_data_directory() {
        let parent = tempfile::tempdir().unwrap();
        let data = parent.path().join("not-created");
        let mut cfg = ServerTomlConfig::default();
        cfg.node.data_dir = data.clone();
        cfg.node.grpc_listen_addr = "127.0.0.1:0".to_string();
        cfg.transport.mode = "rdma".to_string();
        let text = match crate::PersistentMetaDbCluster::open_and_serve(cfg).await {
            Ok(_) => panic!("required RDMA must not start"),
            Err(err) => err.to_string(),
        };
        assert!(text.contains("backend-not-implemented"), "{text}");
        assert!(text.contains("does not fall back"), "{text}");
        assert!(!data.exists(), "required RDMA must not create {data:?}");
    }
}
