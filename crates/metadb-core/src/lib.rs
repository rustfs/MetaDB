//! Core types, table schemas, binary key encodings, and Hybrid Logical Clock (HLC)
//! for MetaDB.
//!
//! Inspired by Baidu's trillion-scale metadata database design:
//! *"MetaDB: Putting File-System Structure Back into Distributed Databases"*
//! (arXiv:2610.32247v1).

pub mod error;
pub mod hlc;
pub mod schema;

pub use error::{MetaDbError, Result};
pub use hlc::{HlcTimestamp, HybridLogicalClock};
pub use schema::{
    EntryType, FileId, InodeAttr, InodeId, MetaEntry, MetaKey, NodeId, QuotaKey, QuotaRecord,
    SecondaryIndexKey, SecondaryIndexRecord, ShardId, UserId, ATTR_ENTRY_NAME, ROOT_INODE_ID,
};
