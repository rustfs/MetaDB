//! Durable per-process cell directory.
//!
//! This file records the local cell and any remote cell whose routes were
//! probed and covered the requested range exactly. It is not a replicated
//! global manager, and it is not a heartbeat scheduler.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use metadb_core::codec;
use metadb_core::{
    append_crc32c_le, verify_crc32c_le, CellId, CompositeKeyRange, MetaDbError, NodeId, Result,
};
use metadb_storage::sstable::sync_dir;

use crate::meta_cache::CompositeShardRoute;

pub const CELL_DIRECTORY_FILE: &str = "GLOBAL_CELL_DIRECTORY";
const CELL_DIRECTORY_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryCell {
    pub cell_id: CellId,
    pub range: CompositeKeyRange,
    pub endpoints: Vec<String>,
    pub local: bool,
    pub cluster_id: u128,
    pub routes: Vec<CompositeShardRoute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePublication {
    pub cell_id: CellId,
    pub range: CompositeKeyRange,
    pub endpoints: Vec<String>,
    pub cluster_id: u128,
    pub routes: Vec<CompositeShardRoute>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemotePrecheck {
    /// No stored remote row has this id, range, and endpoint list.
    New,
    /// The stored row already has this id, range, and endpoint list.
    SameIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]
struct PersistedDirectory {
    version: u32,
    cells: Vec<PersistedDirectoryCell>,
}

#[derive(Debug, Clone, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead)]
struct PersistedDirectoryCell {
    cell_id: CellId,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    endpoints: Vec<String>,
    local: bool,
    cluster_id: u128,
    routes: Vec<CompositeShardRoute>,
}

/// Shard routes to bootstrap for `owned`.
///
/// `None` means the whole user key space. A configured range must be a
/// contiguous union of the meta, quota, and secondary-index table roots.
/// `per_table` false with the whole space keeps the historical single shard.
pub fn shards_covering(
    owned: Option<&CompositeKeyRange>,
    per_table: bool,
) -> Result<Vec<(u64, CompositeKeyRange)>> {
    let owned = match owned {
        Some(range) => range.clone(),
        None => CompositeKeyRange::all_user_tables(),
    };
    let roots = table_roots();
    let mut included = Vec::new();
    for (index, root) in roots.iter().enumerate() {
        let inside = root.start_key.as_slice() >= owned.start_key.as_slice()
            && root.end_key.as_slice() <= owned.end_key.as_slice();
        if inside {
            included.push(((index as u64) + 1, root.clone()));
            continue;
        }
        if root.overlaps(&owned.start_key, &owned.end_key) {
            return Err(MetaDbError::ShardRoutingError(format!(
                "cell range {}..{} cuts through a table; a cell owns whole tables",
                hex_key(&owned.start_key),
                hex_key(&owned.end_key)
            )));
        }
    }
    if included.is_empty()
        || included[0].1.start_key != owned.start_key
        || included.last().unwrap().1.end_key != owned.end_key
    {
        return Err(MetaDbError::ShardRoutingError(format!(
            "cell range {}..{} is not a contiguous union of table roots",
            hex_key(&owned.start_key),
            hex_key(&owned.end_key)
        )));
    }
    if !per_table && included.len() == roots.len() {
        return Ok(vec![(1, CompositeKeyRange::all_user_tables())]);
    }
    if !per_table {
        return Ok(vec![(1, owned)]);
    }
    Ok(included)
}

pub fn union_route_range<'a>(
    ranges: impl IntoIterator<Item = &'a CompositeKeyRange>,
) -> Result<CompositeKeyRange> {
    let mut ranges: Vec<&CompositeKeyRange> = ranges.into_iter().collect();
    if ranges.is_empty() {
        return Err(MetaDbError::ShardRoutingError(
            "cell has no routes to publish".to_string(),
        ));
    }
    ranges.sort_by(|left, right| left.start_key.cmp(&right.start_key));
    routes_cover_exactly(
        &ranges,
        &ranges[0].start_key,
        &ranges.last().unwrap().end_key,
    )?;
    CompositeKeyRange::new(
        ranges[0].start_key.clone(),
        ranges.last().unwrap().end_key.clone(),
    )
}

/// `routes` must tile `[start, end)` with no gap, overlap, or extra byte.
pub fn routes_cover_exactly(routes: &[&CompositeKeyRange], start: &[u8], end: &[u8]) -> Result<()> {
    if start.is_empty() || end.is_empty() || start >= end {
        return Err(MetaDbError::ShardRoutingError(
            "cell range requires a non-empty start before end".to_string(),
        ));
    }
    if routes.is_empty() {
        return Err(MetaDbError::ShardRoutingError(format!(
            "cell routes do not cover {}..{}",
            hex_key(start),
            hex_key(end)
        )));
    }
    let mut ordered = routes.to_vec();
    ordered.sort_by(|left, right| left.start_key.cmp(&right.start_key));
    if ordered[0].start_key.as_slice() != start {
        return Err(MetaDbError::ShardRoutingError(format!(
            "cell routes start at {} but the requested range starts at {}",
            hex_key(&ordered[0].start_key),
            hex_key(start)
        )));
    }
    let mut cursor = start.to_vec();
    for route in &ordered {
        if route.start_key.as_slice() != cursor.as_slice() || route.end_key <= route.start_key {
            return Err(MetaDbError::ShardRoutingError(format!(
                "cell routes do not cover {}..{} without a gap; refusing a partial publication",
                hex_key(start),
                hex_key(end)
            )));
        }
        cursor = route.end_key.clone();
    }
    if cursor.as_slice() != end {
        return Err(MetaDbError::ShardRoutingError(format!(
            "cell routes end at {} but the requested range ends at {}; refusing a partial publication",
            hex_key(&cursor),
            hex_key(end)
        )));
    }
    Ok(())
}

pub fn precheck_remote(
    records: &BTreeMap<CellId, DirectoryCell>,
    cell_id: CellId,
    range: &CompositeKeyRange,
    endpoints: &[String],
) -> Result<RemotePrecheck> {
    if cell_id == 0 {
        return Err(MetaDbError::ShardRoutingError(
            "cell id must be non-zero".to_string(),
        ));
    }
    let endpoints = normalize_endpoints(endpoints)?;
    shards_covering(Some(range), true)?;
    if let Some(existing) = records.get(&cell_id) {
        if existing.local {
            return Err(MetaDbError::ShardRoutingError(format!(
                "cell {cell_id} is the local cell; RegisterCell does not replace it"
            )));
        }
        if existing.range == *range && existing.endpoints == endpoints {
            return Ok(RemotePrecheck::SameIdentity);
        }
    }
    for record in records.values() {
        if record.cell_id == cell_id {
            continue;
        }
        if record.range.overlaps(&range.start_key, &range.end_key) {
            return Err(MetaDbError::ShardRoutingError(format!(
                "cell {cell_id} range {}..{} overlaps cell {} range {}..{}",
                hex_key(&range.start_key),
                hex_key(&range.end_key),
                record.cell_id,
                hex_key(&record.range.start_key),
                hex_key(&record.range.end_key)
            )));
        }
    }
    Ok(RemotePrecheck::New)
}

pub fn publication_from_probe(
    records: &BTreeMap<CellId, DirectoryCell>,
    publication: RemotePublication,
    node_ids_in_use: &BTreeSet<NodeId>,
) -> Result<Option<DirectoryCell>> {
    let endpoints = normalize_endpoints(&publication.endpoints)?;
    let precheck = precheck_remote(records, publication.cell_id, &publication.range, &endpoints)?;
    if publication.cluster_id == 0 {
        return Err(MetaDbError::ShardRoutingError(
            "remote cell has no cluster identity; refusing to publish it".to_string(),
        ));
    }
    if publication.routes.is_empty() {
        return Err(MetaDbError::ShardRoutingError(
            "remote cell returned no routes; refusing a partial publication".to_string(),
        ));
    }
    for route in &publication.routes {
        if route.cell_id != publication.cell_id {
            return Err(MetaDbError::ShardRoutingError(format!(
                "remote route cell {} does not match requested cell {}",
                route.cell_id, publication.cell_id
            )));
        }
    }
    let ranges: Vec<&CompositeKeyRange> = publication
        .routes
        .iter()
        .map(|route| &route.range)
        .collect();
    routes_cover_exactly(
        &ranges,
        &publication.range.start_key,
        &publication.range.end_key,
    )?;
    for route in &publication.routes {
        for node_id in route.peers.keys() {
            if node_ids_in_use.contains(node_id) {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "remote cell {} uses node {node_id}, which already owns an id namespace in this directory; a second cell must use a disjoint node id",
                    publication.cell_id
                )));
            }
        }
    }
    let stored = DirectoryCell {
        cell_id: publication.cell_id,
        range: publication.range,
        endpoints,
        local: false,
        cluster_id: publication.cluster_id,
        routes: publication.routes,
    };
    if precheck == RemotePrecheck::SameIdentity {
        if let Some(existing) = records.get(&stored.cell_id) {
            if same_remote(existing, &stored) {
                return Ok(None);
            }
        }
    }
    Ok(Some(stored))
}

pub fn load_directory(path: &Path) -> Result<BTreeMap<CellId, DirectoryCell>> {
    let mut raw = Vec::new();
    File::open(path)?.read_to_end(&mut raw)?;
    let body = verify_crc32c_le(&raw, CELL_DIRECTORY_FILE)?;
    let parsed: PersistedDirectory = decode_exact(body)?;
    if parsed.version != CELL_DIRECTORY_VERSION {
        return Err(MetaDbError::CodecError(format!(
            "{CELL_DIRECTORY_FILE} version {} is not supported",
            parsed.version
        )));
    }
    let mut records = BTreeMap::new();
    for cell in parsed.cells {
        let range = CompositeKeyRange::new(cell.start_key, cell.end_key)?;
        if records
            .insert(
                cell.cell_id,
                DirectoryCell {
                    cell_id: cell.cell_id,
                    range,
                    endpoints: cell.endpoints,
                    local: cell.local,
                    cluster_id: cell.cluster_id,
                    routes: cell.routes,
                },
            )
            .is_some()
        {
            return Err(MetaDbError::CodecError(format!(
                "{CELL_DIRECTORY_FILE} lists cell {} twice",
                cell.cell_id
            )));
        }
    }
    Ok(records)
}

pub fn store_directory(path: &Path, records: &BTreeMap<CellId, DirectoryCell>) -> Result<()> {
    let persisted = PersistedDirectory {
        version: CELL_DIRECTORY_VERSION,
        cells: records
            .values()
            .map(|cell| PersistedDirectoryCell {
                cell_id: cell.cell_id,
                start_key: cell.range.start_key.clone(),
                end_key: cell.range.end_key.clone(),
                endpoints: cell.endpoints.clone(),
                local: cell.local,
                cluster_id: cell.cluster_id,
                routes: cell.routes.clone(),
            })
            .collect(),
    };
    let mut raw = codec::serialize(&persisted)
        .map_err(|error| MetaDbError::CodecError(format!("{CELL_DIRECTORY_FILE}: {error}")))?;
    append_crc32c_le(&mut raw);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = temp_path(path);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(&raw)?;
        file.sync_data()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

pub fn hex_key(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn table_roots() -> [CompositeKeyRange; 3] {
    [
        CompositeKeyRange::meta_table_root(),
        CompositeKeyRange::quota_table_root(),
        CompositeKeyRange::secondary_index_table_root(),
    ]
}

fn normalize_endpoints(endpoints: &[String]) -> Result<Vec<String>> {
    if endpoints.is_empty() {
        return Err(MetaDbError::ShardRoutingError(
            "RegisterCell requires a cell manager endpoint".to_string(),
        ));
    }
    let mut out = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        let endpoint = endpoint.trim();
        if endpoint.is_empty() {
            return Err(MetaDbError::ShardRoutingError(
                "RegisterCell endpoint is empty".to_string(),
            ));
        }
        if let Some((scheme, _)) = endpoint.split_once("://") {
            if scheme != "http" && scheme != "https" {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "RegisterCell endpoint scheme {scheme} is not http or https"
                )));
            }
        }
        if out.iter().any(|existing: &String| existing == endpoint) {
            return Err(MetaDbError::ShardRoutingError(format!(
                "RegisterCell endpoint {endpoint} is repeated"
            )));
        }
        out.push(endpoint.to_string());
    }
    Ok(out)
}

fn same_remote(existing: &DirectoryCell, incoming: &DirectoryCell) -> bool {
    existing.cell_id == incoming.cell_id
        && existing.range == incoming.range
        && existing.endpoints == incoming.endpoints
        && existing.cluster_id == incoming.cluster_id
        && existing.routes == incoming.routes
}

fn decode_exact(body: &[u8]) -> Result<PersistedDirectory> {
    wincode::config::deserialize_exact(body, wincode::config::DefaultConfig::default())
        .map_err(|error| MetaDbError::CodecError(format!("{CELL_DIRECTORY_FILE}: {error}")))
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    name.push(".tmp");
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::{ShardHealthState, ShardSplitMode};
    use metadb_core::{ShardEpoch, TABLE_PREFIX_QUOTA, TABLE_PREFIX_SECONDARY_INDEX};
    use std::collections::BTreeMap as Map;

    fn route(cell: u64, shard: u64, start: u8, end: u8, node: u64) -> CompositeShardRoute {
        let mut peers = Map::new();
        peers.insert(node, (format!("127.0.0.1:{node}"), true));
        CompositeShardRoute {
            cell_id: cell,
            shard_id: shard,
            range: CompositeKeyRange::new(vec![start], vec![end]).unwrap(),
            epoch: ShardEpoch::initial(),
            leader_node: node,
            peers,
            split_mode: ShardSplitMode::DirectoryColocated,
            health_state: ShardHealthState::Normal,
            dual_mode_pid: None,
        }
    }

    #[test]
    fn table_ranges_and_partial_cover_are_exact() {
        let all = shards_covering(None, true).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].1, CompositeKeyRange::meta_table_root());
        let one = shards_covering(None, false).unwrap();
        assert_eq!(one, vec![(1, CompositeKeyRange::all_user_tables())]);
        let meta = CompositeKeyRange::meta_table_root();
        assert_eq!(
            shards_covering(Some(&meta), true).unwrap(),
            vec![(1, meta.clone())]
        );
        let slice = CompositeKeyRange::new(vec![0x01, 0x10], vec![0x01, 0x20]).unwrap();
        assert!(shards_covering(Some(&slice), true).is_err());
        let quota = CompositeKeyRange::quota_table_root();
        let secondary = CompositeKeyRange::secondary_index_table_root();
        let ranges = [&quota, &secondary];
        let requested = CompositeKeyRange::new(vec![TABLE_PREFIX_QUOTA], vec![0x04]).unwrap();
        assert!(routes_cover_exactly(&ranges, &requested.start_key, &requested.end_key).is_ok());
        assert!(routes_cover_exactly(&[&quota], &requested.start_key, &requested.end_key).is_err());
    }

    #[test]
    fn overlap_node_alias_and_identical_publication() {
        let local = DirectoryCell {
            cell_id: 1,
            range: CompositeKeyRange::meta_table_root(),
            endpoints: vec!["127.0.0.1:1".into()],
            local: true,
            cluster_id: 9,
            routes: Vec::new(),
        };
        let mut records = BTreeMap::new();
        records.insert(1, local);
        let quota = CompositeKeyRange::quota_table_root();
        assert!(precheck_remote(&records, 0, &quota, &["127.0.0.1:2".into()]).is_err());
        assert!(precheck_remote(
            &records,
            2,
            &CompositeKeyRange::meta_table_root(),
            &["127.0.0.1:2".into()]
        )
        .unwrap_err()
        .to_string()
        .contains("overlaps"));
        assert_eq!(
            precheck_remote(&records, 2, &quota, &["127.0.0.1:2".into()]).unwrap(),
            RemotePrecheck::New
        );
        let publication = RemotePublication {
            cell_id: 2,
            range: quota.clone(),
            endpoints: vec!["127.0.0.1:2".into()],
            cluster_id: 8,
            routes: vec![route(
                2,
                2,
                TABLE_PREFIX_QUOTA,
                TABLE_PREFIX_SECONDARY_INDEX,
                1,
            )],
        };
        let mut used = BTreeSet::new();
        used.insert(1);
        assert!(publication_from_probe(&records, publication, &used)
            .unwrap_err()
            .to_string()
            .contains("node 1"));
        let publication = RemotePublication {
            cell_id: 2,
            range: quota.clone(),
            endpoints: vec!["127.0.0.1:2".into()],
            cluster_id: 8,
            routes: vec![route(
                2,
                2,
                TABLE_PREFIX_QUOTA,
                TABLE_PREFIX_SECONDARY_INDEX,
                2,
            )],
        };
        let stored = publication_from_probe(&records, publication.clone(), &used)
            .unwrap()
            .unwrap();
        records.insert(stored.cell_id, stored);
        assert!(publication_from_probe(&records, publication, &used)
            .unwrap()
            .is_none());
        let gapped = RemotePublication {
            cell_id: 2,
            range: CompositeKeyRange::new(vec![TABLE_PREFIX_QUOTA], vec![0x04]).unwrap(),
            endpoints: vec!["127.0.0.1:2".into()],
            cluster_id: 8,
            routes: vec![route(
                2,
                2,
                TABLE_PREFIX_QUOTA,
                TABLE_PREFIX_SECONDARY_INDEX,
                2,
            )],
        };
        assert!(publication_from_probe(&records, gapped, &BTreeSet::new())
            .unwrap_err()
            .to_string()
            .contains("partial"));
    }

    #[test]
    fn directory_roundtrip_rejects_trailing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CELL_DIRECTORY_FILE);
        let mut records = BTreeMap::new();
        records.insert(
            1,
            DirectoryCell {
                cell_id: 1,
                range: CompositeKeyRange::meta_table_root(),
                endpoints: vec!["127.0.0.1:9".into()],
                local: true,
                cluster_id: 4,
                routes: Vec::new(),
            },
        );
        store_directory(&path, &records).unwrap();
        assert_eq!(load_directory(&path).unwrap(), records);
        let mut raw = std::fs::read(&path).unwrap();
        raw.push(0xAA);
        std::fs::write(&path, raw).unwrap();
        assert!(load_directory(&path).is_err());
    }
}
