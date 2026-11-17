//! Core types, table schemas, binary key encodings, and Hybrid Logical Clock (HLC)
//! for MetaDB.
//!
//! Inspired by Baidu's trillion-scale metadata database design:
//! *"MetaDB: Putting File-System Structure Back into Distributed Databases"*
//! (arXiv:2610.32247v1).

pub mod checksum;
pub mod codec;
pub mod epoch;
pub mod error;
pub mod hlc;
pub mod schema;
pub mod system_keys;

pub use checksum::{
    append_crc32c_be, append_crc32c_le, checksum_crc32c, verify_crc32c_be, verify_crc32c_le,
};
pub use epoch::{CellId, CompositeKeyRange, ShardEpoch, TxnId};
pub use error::{MetaDbError, Result};
pub use hlc::{HlcTimestamp, HybridLogicalClock, DEFAULT_MAX_CLOCK_DRIFT_MS};
pub use schema::{
    is_dir_inode_id, is_file_inode_id, make_file_inode_id, negate_accounted_size,
    saturating_add_i64, validate_dir_inode_id, validate_file_size, validate_user_entry_name,
    AuxOrderingTuple, EntryType, FileId, InodeAttr, InodeId, MetaEntry, MetaKey, NodeId, QuotaKey,
    QuotaRecord, SecondaryIndexKey, SecondaryIndexRecord, ShardId, UserId, ATTR_ENTRY_NAME,
    FILE_INODE_TAG_BIT, INODE_ATTR_ENCODED_LEN, MAX_USER_NAME_BYTES, ROOT_INODE_ID,
    SYSTEM_PREFIX_DIR_SHARD_DELTA, SYSTEM_PREFIX_QUOTA_ORIGIN_CURSOR,
    SYSTEM_PREFIX_SPILLED_AUX_OUTBOX, SYSTEM_PREFIX_TXN_RECORD, SYSTEM_PREFIX_WRITE_INTENT,
    TABLE_PREFIX_META, TABLE_PREFIX_QUOTA, TABLE_PREFIX_SECONDARY_INDEX,
};
pub use system_keys::{
    affinity_key_for_raw_entry, decode_origin_quota_cursor, encode_origin_quota_cursor,
    AttrDeltaSpec, AuxIntentSpec, AuxOutboxEntry, AuxTaskPayload, DirShardDeltaKey,
    DirShardDeltaRecord, IntentOp, OriginQuotaCursorKey, SpilledAuxOutboxKey, TxnRecordKey,
    TxnState, TxnStatusRecord, WriteIntentKey, WriteIntentRecord,
};
