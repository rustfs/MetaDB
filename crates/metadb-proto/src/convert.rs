//! Lossless conversions between `metadb-core` domain structs and `metadb.v1` Protobuf messages.

use std::collections::BTreeMap;

use metadb_core::codec;
use metadb_core::{
    EntryType, HlcTimestamp, InodeAttr, MetaDbError, MetaEntry, MetaKey, QuotaRecord, Result,
    SecondaryIndexRecord, ShardEpoch,
};

use crate::v1::{
    ErrorDetailProto, HlcTimestampProto, InodeAttrProto, MetaEntryProto, QuotaRecordProto,
    SecondaryIndexRecordProto, ShardCursorEntryProto, ShardEpochProto,
};

/// Encodes a 128-bit `TxnId` or `request_uuid` into a fixed 16-byte big-endian buffer.
#[inline]
pub fn encode_u128_be(val: u128) -> [u8; 16] {
    val.to_be_bytes()
}

/// Strictly decodes a 16-byte big-endian `u128` (`TxnId` / `request_uuid`).
pub fn decode_u128_be(bytes: &[u8]) -> Result<u128> {
    let arr: [u8; 16] = bytes.try_into().map_err(|_| {
        MetaDbError::CodecError(format!(
            "expected 16-byte big-endian u128, got {} bytes",
            bytes.len()
        ))
    })?;
    Ok(u128::from_be_bytes(arr))
}

/// Decodes an optional 16-byte big-endian `u128` (`Ok(0)` when empty; `Err(CodecError)` if non-empty & `!= 16`).
pub fn decode_optional_u128_be(bytes: &[u8]) -> Result<u128> {
    if bytes.is_empty() {
        return Ok(0);
    }
    decode_u128_be(bytes)
}

impl From<HlcTimestamp> for HlcTimestampProto {
    fn from(ts: HlcTimestamp) -> Self {
        Self { raw: ts.0 }
    }
}

impl From<HlcTimestampProto> for HlcTimestamp {
    fn from(p: HlcTimestampProto) -> Self {
        HlcTimestamp(p.raw)
    }
}

impl From<ShardEpoch> for ShardEpochProto {
    fn from(epoch: ShardEpoch) -> Self {
        Self {
            conf_ver: epoch.conf_ver,
            version: epoch.version,
        }
    }
}

impl From<ShardEpochProto> for ShardEpoch {
    fn from(p: ShardEpochProto) -> Self {
        Self {
            conf_ver: p.conf_ver,
            version: p.version,
        }
    }
}

impl From<&InodeAttr> for InodeAttrProto {
    fn from(attr: &InodeAttr) -> Self {
        Self {
            inode_id: attr.inode_id,
            parent_pid: attr.parent_pid,
            entry_type: attr.entry_type as u32,
            uid: attr.uid,
            mode: attr.mode,
            link_count: attr.link_count,
            num_children: attr.num_children,
            size_bytes: attr.size_bytes,
            create_time_hlc: attr.create_time.0,
            modify_time_hlc: attr.modify_time.0,
            has_fid: attr.fid.is_some(),
            fid: attr.fid.unwrap_or(0),
            applied_raft_index: attr.applied_lsn,
        }
    }
}

impl TryFrom<InodeAttrProto> for InodeAttr {
    type Error = MetaDbError;

    fn try_from(p: InodeAttrProto) -> Result<Self> {
        let raw_type = u8::try_from(p.entry_type).map_err(|_| {
            MetaDbError::CodecError(format!("entry_type out of u8 range: {}", p.entry_type))
        })?;
        let entry_type = EntryType::from_u8(raw_type)?;
        Ok(Self {
            inode_id: p.inode_id,
            parent_pid: p.parent_pid,
            entry_type,
            uid: p.uid,
            mode: p.mode,
            link_count: p.link_count,
            num_children: p.num_children,
            size_bytes: p.size_bytes,
            create_time: HlcTimestamp(p.create_time_hlc),
            modify_time: HlcTimestamp(p.modify_time_hlc),
            fid: if p.has_fid { Some(p.fid) } else { None },
            applied_lsn: p.applied_raft_index,
        })
    }
}

impl From<&MetaEntry> for MetaEntryProto {
    fn from(entry: &MetaEntry) -> Self {
        Self {
            pid: entry.key.pid,
            name: entry.key.name.clone(),
            attr: Some(InodeAttrProto::from(&entry.attr)),
        }
    }
}

impl TryFrom<MetaEntryProto> for MetaEntry {
    type Error = MetaDbError;

    fn try_from(p: MetaEntryProto) -> Result<Self> {
        let attr_proto = p
            .attr
            .ok_or_else(|| MetaDbError::CodecError("missing InodeAttrProto".to_string()))?;
        Ok(Self {
            key: MetaKey {
                pid: p.pid,
                name: p.name,
            },
            attr: InodeAttr::try_from(attr_proto)?,
        })
    }
}

impl From<&QuotaRecord> for QuotaRecordProto {
    fn from(rec: &QuotaRecord) -> Self {
        let applied_by_shard = rec
            .applied_by_shard
            .iter()
            .map(
                |(&origin_shard_id, &(raft_index, task_ordinal))| ShardCursorEntryProto {
                    origin_shard_id,
                    raft_index,
                    task_ordinal: u32::from(task_ordinal),
                },
            )
            .collect();
        Self {
            uid: rec.uid,
            used_bytes: rec.used_bytes,
            used_inodes: rec.used_inodes,
            quota_limit_bytes: rec.quota_limit_bytes,
            last_applied_hlc: rec.last_applied_hlc.0,
            applied_by_shard,
        }
    }
}

impl TryFrom<QuotaRecordProto> for QuotaRecord {
    type Error = MetaDbError;

    fn try_from(p: QuotaRecordProto) -> Result<Self> {
        let mut applied_by_shard = BTreeMap::new();
        for c in p.applied_by_shard {
            let ord = u16::try_from(c.task_ordinal).map_err(|_| {
                MetaDbError::CodecError(format!(
                    "QuotaRecord task_ordinal out of u16 range: {}",
                    c.task_ordinal
                ))
            })?;
            applied_by_shard.insert(c.origin_shard_id, (c.raft_index, ord));
        }
        Ok(Self {
            uid: p.uid,
            used_bytes: p.used_bytes,
            used_inodes: p.used_inodes,
            quota_limit_bytes: p.quota_limit_bytes,
            last_applied_hlc: HlcTimestamp(p.last_applied_hlc),
            applied_by_shard,
        })
    }
}

impl From<&SecondaryIndexRecord> for SecondaryIndexRecordProto {
    fn from(rec: &SecondaryIndexRecord) -> Self {
        Self {
            fid: rec.fid,
            pid: rec.pid,
            name: rec.name.clone(),
            create_time_hlc: rec.create_time.0,
            last_applied_hlc: rec.last_ver.0 .0,
            last_origin_shard_id: rec.last_ver.1,
            last_origin_raft_index: rec.last_ver.2,
            last_task_ordinal: u32::from(rec.last_ver.3),
            tombstone_hlc: rec.tombstone_ver.0 .0,
            is_deleted: rec.is_deleted,
            tombstone_origin_shard_id: rec.tombstone_ver.1,
            tombstone_origin_raft_index: rec.tombstone_ver.2,
            tombstone_task_ordinal: u32::from(rec.tombstone_ver.3),
        }
    }
}

impl TryFrom<SecondaryIndexRecordProto> for SecondaryIndexRecord {
    type Error = MetaDbError;

    fn try_from(p: SecondaryIndexRecordProto) -> Result<Self> {
        let last_ord = u16::try_from(p.last_task_ordinal).map_err(|_| {
            MetaDbError::CodecError(format!(
                "SecondaryIndexRecord last_task_ordinal out of u16 range: {}",
                p.last_task_ordinal
            ))
        })?;
        let tomb_ord = u16::try_from(p.tombstone_task_ordinal).map_err(|_| {
            MetaDbError::CodecError(format!(
                "SecondaryIndexRecord tombstone_task_ordinal out of u16 range: {}",
                p.tombstone_task_ordinal
            ))
        })?;
        Ok(Self {
            fid: p.fid,
            pid: p.pid,
            name: p.name,
            create_time: HlcTimestamp(p.create_time_hlc),
            last_ver: (
                HlcTimestamp(p.last_applied_hlc),
                p.last_origin_shard_id,
                p.last_origin_raft_index,
                last_ord,
            ),
            tombstone_ver: (
                HlcTimestamp(p.tombstone_hlc),
                p.tombstone_origin_shard_id,
                p.tombstone_origin_raft_index,
                tomb_ord,
            ),
            is_deleted: p.is_deleted,
        })
    }
}

impl From<&MetaDbError> for ErrorDetailProto {
    fn from(err: &MetaDbError) -> Self {
        let serialized_core_error = codec::serialize(err).unwrap_or_default();
        match err {
            MetaDbError::EpochMismatch {
                shard_id,
                expected,
                actual,
            } => Self {
                error_kind: "EpochMismatch".to_string(),
                message: err.to_string(),
                shard_id: *shard_id,
                expected_epoch: Some((*expected).into()),
                actual_epoch: Some((*actual).into()),
                updated_descriptor: None,
                split_child_hint: None,
                has_leader_hint: false,
                leader_hint: 0,
                serialized_core_error,
            },
            MetaDbError::NotLeader {
                shard_id,
                leader_hint,
            } => Self {
                error_kind: "NotLeader".to_string(),
                message: err.to_string(),
                shard_id: *shard_id,
                expected_epoch: None,
                actual_epoch: None,
                updated_descriptor: None,
                split_child_hint: None,
                has_leader_hint: leader_hint.is_some(),
                leader_hint: leader_hint.unwrap_or(0),
                serialized_core_error,
            },
            _ => Self {
                error_kind: "MetaDbError".to_string(),
                message: err.to_string(),
                shard_id: 0,
                expected_epoch: None,
                actual_epoch: None,
                updated_descriptor: None,
                split_child_hint: None,
                has_leader_hint: false,
                leader_hint: 0,
                serialized_core_error,
            },
        }
    }
}

impl From<ErrorDetailProto> for MetaDbError {
    fn from(p: ErrorDetailProto) -> Self {
        if !p.serialized_core_error.is_empty() {
            if let Ok(decoded) = codec::deserialize::<MetaDbError>(&p.serialized_core_error) {
                return decoded;
            }
        }
        match p.error_kind.as_str() {
            "EpochMismatch" => MetaDbError::EpochMismatch {
                shard_id: p.shard_id,
                expected: p.expected_epoch.unwrap_or_default().into(),
                actual: p.actual_epoch.unwrap_or_default().into(),
            },
            "NotLeader" => MetaDbError::NotLeader {
                shard_id: p.shard_id,
                leader_hint: p.has_leader_hint.then_some(p.leader_hint),
            },
            _ => MetaDbError::RpcError(p.message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metadb_core::ROOT_INODE_ID;

    #[test]
    fn transaction_conflict_error_preserves_full_u128_id() {
        let error = MetaDbError::WriteIntentConflict {
            conflicting_txn_id: u128::MAX,
            primary_key: vec![1, 2, 3],
            expire_hlc: 42,
        };
        let proto = ErrorDetailProto::from(&error);
        assert!(!proto.serialized_core_error.is_empty());
        assert_eq!(MetaDbError::from(proto), error);
    }

    #[test]
    fn test_proto_domain_roundtrips() {
        let entry = MetaEntry {
            key: MetaKey::child(ROOT_INODE_ID, "train.bin"),
            attr: InodeAttr {
                inode_id: 42,
                parent_pid: ROOT_INODE_ID,
                entry_type: EntryType::File,
                uid: 7,
                mode: 0o644,
                link_count: 1,
                num_children: 0,
                size_bytes: 8192,
                create_time: HlcTimestamp::new(100, 1),
                modify_time: HlcTimestamp::new(101, 2),
                fid: Some(9001),
                applied_lsn: 12,
            },
        };
        let proto = MetaEntryProto::from(&entry);
        assert_eq!(MetaEntry::try_from(proto).unwrap(), entry);

        let mut quota = QuotaRecord::new(7, 1_000_000);
        quota.used_bytes = 8192;
        quota.used_inodes = 1;
        quota.applied_by_shard.insert(2, (55, 1));
        let q_proto = QuotaRecordProto::from(&quota);
        assert_eq!(QuotaRecord::try_from(q_proto).unwrap(), quota);

        let idx = SecondaryIndexRecord {
            fid: 9001,
            pid: ROOT_INODE_ID,
            name: "train.bin".to_string(),
            create_time: HlcTimestamp::new(100, 1),
            last_ver: (HlcTimestamp::new(100, 1), 1, 10, 0),
            tombstone_ver: (HlcTimestamp::new(200, 3), 4, 77, 2),
            is_deleted: true,
        };
        let idx_proto = SecondaryIndexRecordProto::from(&idx);
        assert_eq!(SecondaryIndexRecord::try_from(idx_proto).unwrap(), idx);

        // Checked narrowing rejections (`u8::try_from`, `u16::try_from`, `decode_u128_be`).
        let mut bad_attr = InodeAttrProto::from(&entry.attr);
        bad_attr.entry_type = 300;
        assert!(InodeAttr::try_from(bad_attr).is_err());

        let mut bad_idx = SecondaryIndexRecordProto::from(&idx);
        bad_idx.tombstone_task_ordinal = 70_000;
        assert!(SecondaryIndexRecord::try_from(bad_idx).is_err());

        let id128 = 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10_u128;
        assert_eq!(decode_u128_be(&encode_u128_be(id128)).unwrap(), id128);
        assert!(decode_u128_be(&[1, 2, 3]).is_err());
        assert_eq!(decode_optional_u128_be(&[]).unwrap(), 0);
        assert!(decode_optional_u128_be(&[1, 2]).is_err());

        let err = MetaDbError::EpochMismatch {
            shard_id: 5,
            expected: ShardEpoch {
                conf_ver: 1,
                version: 2,
            },
            actual: ShardEpoch {
                conf_ver: 1,
                version: 3,
            },
        };
        let mut err_proto = ErrorDetailProto::from(&err);
        assert_eq!(MetaDbError::from(err_proto.clone()), err);
        err_proto.serialized_core_error.clear();
        assert_eq!(MetaDbError::from(err_proto), err);

        let nl = MetaDbError::NotLeader {
            shard_id: 9,
            leader_hint: Some(2),
        };
        let mut nl_proto = ErrorDetailProto::from(&nl);
        nl_proto.serialized_core_error.clear();
        assert_eq!(MetaDbError::from(nl_proto), nl);
    }
}
