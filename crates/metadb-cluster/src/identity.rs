//! Durable cluster and node-instance identities.
//!
//! A cluster id is a random 128-bit value created once when a data directory
//! bootstraps. A joiner adopts that id and refuses any other. A node instance id
//! is random per data directory, so a wiped directory cannot reuse a node id that
//! is already bound in the cluster.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use metadb_core::{append_crc32c_le, verify_crc32c_le, CellId, MetaDbError, NodeId, Result};
use metadb_storage::sstable::sync_dir;

pub const CLUSTER_IDENTITY_FILE: &str = "CLUSTER_IDENTITY";
const MAGIC: &[u8; 8] = b"MCID0001";
const BODY_LEN: usize = 56;
/// Node ids at or above this value cannot own a disjoint inode or shard namespace.
pub const MAX_HA_NODE_ID: NodeId = 1 << 15;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterIdentity {
    pub cluster_id: u128,
    pub node_instance_id: u128,
    pub node_id: NodeId,
    pub cell_id: CellId,
}

impl ClusterIdentity {
    pub fn load(dir: impl AsRef<Path>) -> Result<Option<Self>> {
        let path = dir.as_ref().join(CLUSTER_IDENTITY_FILE);
        if !path.exists() {
            return Ok(None);
        }
        let mut raw = Vec::new();
        File::open(&path)?.read_to_end(&mut raw)?;
        let body = verify_crc32c_le(&raw, CLUSTER_IDENTITY_FILE)?;
        Ok(Some(Self::decode(body)?))
    }

    /// Loads the directory identity, or creates one.
    ///
    /// `adopt_cluster_id = None` bootstraps a new random cluster id.
    /// `Some(id)` joins that cluster. A stored id must match.
    pub fn load_or_create(
        dir: impl AsRef<Path>,
        node_id: NodeId,
        cell_id: CellId,
        adopt_cluster_id: Option<u128>,
    ) -> Result<Self> {
        node_id_in_ha_range(node_id)?;
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        if let Some(existing) = Self::load(dir)? {
            if existing.node_id != node_id || existing.cell_id != cell_id {
                return Err(MetaDbError::IdentityRejected(format!(
                    "data directory identity is node {} cell {}, refusing node {} cell {}",
                    existing.node_id, existing.cell_id, node_id, cell_id
                )));
            }
            if let Some(adopt) = adopt_cluster_id {
                if adopt == 0 || adopt != existing.cluster_id {
                    return Err(MetaDbError::IdentityRejected(format!(
                        "refusing to join cluster {adopt:#x}; this directory belongs to cluster {:#x}",
                        existing.cluster_id
                    )));
                }
            }
            return Ok(existing);
        }
        let cluster_id = if let Some(adopt) = adopt_cluster_id {
            if adopt == 0 {
                return Err(MetaDbError::IdentityRejected(
                    "refusing to join a cluster that has no identity".to_string(),
                ));
            }
            adopt
        } else {
            random_id()?
        };
        let identity = Self {
            cluster_id,
            node_instance_id: random_id()?,
            node_id,
            cell_id,
        };
        identity.persist(dir)?;
        Ok(identity)
    }

    pub fn persist(&self, dir: impl AsRef<Path>) -> Result<()> {
        if self.cluster_id == 0 || self.node_instance_id == 0 || self.node_id == 0 {
            return Err(MetaDbError::IdentityRejected(
                "refusing to persist a zero cluster, node instance, or node id".to_string(),
            ));
        }
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let mut raw = Vec::with_capacity(BODY_LEN + 4);
        raw.extend_from_slice(MAGIC);
        raw.extend_from_slice(&self.cluster_id.to_le_bytes());
        raw.extend_from_slice(&self.node_instance_id.to_le_bytes());
        raw.extend_from_slice(&self.node_id.to_le_bytes());
        raw.extend_from_slice(&self.cell_id.to_le_bytes());
        append_crc32c_le(&mut raw);
        let tmp = dir.join(format!("{CLUSTER_IDENTITY_FILE}.tmp"));
        let dst = dir.join(CLUSTER_IDENTITY_FILE);
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            file.write_all(&raw)?;
            file.sync_data()?;
        }
        fs::rename(&tmp, &dst)?;
        sync_dir(dir)?;
        Ok(())
    }

    fn decode(body: &[u8]) -> Result<Self> {
        if body.len() != BODY_LEN || &body[..8] != MAGIC {
            return Err(MetaDbError::CodecError(
                "invalid CLUSTER_IDENTITY header".to_string(),
            ));
        }
        let cluster_id = u128::from_le_bytes(body[8..24].try_into().unwrap());
        let node_instance_id = u128::from_le_bytes(body[24..40].try_into().unwrap());
        let node_id = u64::from_le_bytes(body[40..48].try_into().unwrap());
        let cell_id = u64::from_le_bytes(body[48..56].try_into().unwrap());
        if cluster_id == 0 || node_instance_id == 0 || node_id == 0 {
            return Err(MetaDbError::IdentityRejected(
                "CLUSTER_IDENTITY contains a zero id".to_string(),
            ));
        }
        Ok(Self {
            cluster_id,
            node_instance_id,
            node_id,
            cell_id,
        })
    }
}

pub fn random_id() -> Result<u128> {
    // `getrandom` 0.4 is already in this workspace. It uses the platform CSPRNG
    // (including Windows) and does not open `/dev/urandom`.
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|error| MetaDbError::StorageIoError(format!("secure random failed: {error}")))?;
    let id = u128::from_le_bytes(bytes);
    if id == 0 {
        return Err(MetaDbError::StorageIoError(
            "secure random returned a zero identity".to_string(),
        ));
    }
    Ok(id)
}

pub fn node_id_in_ha_range(node_id: NodeId) -> Result<()> {
    if node_id == 0 || node_id >= MAX_HA_NODE_ID {
        return Err(MetaDbError::IdentityRejected(format!(
            "node_id {node_id} cannot be allocated a disjoint inode and shard namespace; use 1..=32767"
        )));
    }
    Ok(())
}
