//! Validation for the cell-directory and cell-manager snapshot stored in the
//! existing shard Raft log.
//!
//! The snapshot is not a second consensus group. Apply on every replica of the
//! shard that committed it checks these bytes before keeping them. A body that
//! does not decode, or that does not list the published cell ids, is rejected.

use metadb_core::{MetaDbError, Result};
use metadb_tx::ReplicatedControlRecord;

use crate::cell_directory::decode_directory_body;
use crate::cell_manager::decode_manager_body_exact;

pub fn validate_replicated_record(record: &ReplicatedControlRecord) -> Result<()> {
    let cells = decode_directory_body(&record.directory_body).map_err(|error| {
        MetaDbError::CodecError(format!(
            "GLOBAL_CELL_DIRECTORY publication is corrupt or inconsistent: {error}"
        ))
    })?;
    let ids: Vec<u64> = cells.keys().copied().collect();
    if ids != record.directory_cell_ids {
        return Err(MetaDbError::ShardRoutingError(
            "GLOBAL_CELL_DIRECTORY body does not list the published cell ids; refusing to drop or invent cells"
                .to_string(),
        ));
    }
    let manager = decode_manager_body_exact(&record.manager_body)?;
    if manager.nodes.is_empty()
        || manager.voters.is_empty()
        || manager.voters.values().any(|voters| voters.is_empty())
    {
        return Err(MetaDbError::ShardRoutingError(
            "CELL_MANAGER_STATE publication has no membership; refusing to drop it".to_string(),
        ));
    }
    for (node_id, address) in &manager.nodes {
        if address.is_empty() {
            return Err(MetaDbError::ShardRoutingError(format!(
                "CELL_MANAGER_STATE publication has an empty address for node {node_id}"
            )));
        }
    }
    Ok(())
}
