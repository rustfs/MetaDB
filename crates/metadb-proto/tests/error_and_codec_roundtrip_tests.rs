//! Comprehensive roundtrip tests for `MetaDbError` <-> `ErrorDetailProto`, streaming `Crc32cHasher`,
//! `OriginQuotaCursorKey`, `CompositeKeyRange` user-table bounds, and `split_lease_preimage_key`.

use metadb_core::{
    checksum_crc32c, decode_lease_preimage, decode_origin_quota_cursor, encode_lease_preimage,
    encode_origin_quota_cursor, lease_preimage_key, CompositeKeyRange, Crc32cHasher, HlcTimestamp,
    LeasePreimageValue, LeaseRenewReject, MetaDbError, OriginQuotaCursorKey,
    RetryableConsensusKind, ShardEpoch,
};
use metadb_proto::ErrorDetailProto;

#[test]
fn all_metadb_error_variants_roundtrip_through_error_detail_proto() {
    let samples = vec![
        MetaDbError::NotFound {
            pid: 42,
            name: "missing.txt".to_string(),
        },
        MetaDbError::AlreadyExists {
            pid: 42,
            name: "dup.txt".to_string(),
        },
        MetaDbError::DirectoryNotFound(99),
        MetaDbError::DirectoryNotEmpty {
            dir_id: 100,
            child_count: 3,
        },
        MetaDbError::InvalidEntryType {
            expected: "Directory".to_string(),
            actual: "File".to_string(),
        },
        MetaDbError::ReservedEntryName("_attr_".to_string()),
        MetaDbError::InvalidRenameCycle {
            dir_id: 10,
            dst_pid: 20,
        },
        MetaDbError::EpochMismatch {
            shard_id: 7,
            expected: ShardEpoch {
                conf_ver: 1,
                version: 2,
            },
            actual: ShardEpoch {
                conf_ver: 1,
                version: 3,
            },
        },
        MetaDbError::NotLeader {
            shard_id: 7,
            leader_hint: Some(2),
        },
        MetaDbError::ShardRoutingError("no route".to_string()),
        MetaDbError::UnsplittableJumboShard {
            shard_id: 3,
            pid: 500,
        },
        MetaDbError::WriteIntentConflict {
            conflicting_txn_id: 0x1234_5678_9abc_def0,
            primary_key: vec![0x01, 0, 0, 0, 0, 0, 0, 0, 1, b'a'],
            expire_hlc: 9_999,
        },
        MetaDbError::TransactionTimeout {
            snapshot_hlc: 100,
            oldest_retained_hlc: 200,
        },
        MetaDbError::TransactionAborted("lease expired".to_string()),
        MetaDbError::ClockSkewExceeded {
            remote_ms: 9_000,
            local_ms: 1_000,
            max_drift_ms: 500,
        },
        MetaDbError::ChecksumMismatch {
            context: "sstable block".to_string(),
            expected: 0xdead_beef,
            actual: 0xcafe_babe,
        },
        MetaDbError::ManifestCorrupted("bad manifest edit".to_string()),
        MetaDbError::CodecError("bad frame".to_string()),
        MetaDbError::AuxOutOfOrderGap {
            origin_shard_id: 1,
            uid: 7,
            expected_after: (10, 0),
            got_prev: (12, 0),
        },
        MetaDbError::StorageIoError("disk full".to_string()),
        MetaDbError::RpcError("unavailable".to_string()),
        MetaDbError::ConsensusError("quorum failed".to_string()),
        MetaDbError::SqlParseError("unexpected token".to_string()),
        MetaDbError::InvalidFileSize(-5),
        MetaDbError::HlcSaturated,
        MetaDbError::SplitDeferred("pending recovery".to_string()),
        MetaDbError::IdentityRejected("node 2 bound elsewhere".to_string()),
        MetaDbError::RetryableConsensus {
            shard_id: 1,
            leader_hint: Some(3),
            kind: RetryableConsensusKind::QuorumUnavailable,
        },
        MetaDbError::LeaseRenewRejected(LeaseRenewReject::LifetimeCap),
    ];

    for original in samples {
        let proto = ErrorDetailProto::from(&original);
        assert!(!proto.error_kind.is_empty());
        assert!(!proto.message.is_empty());
        let decoded = MetaDbError::from(proto);
        assert_eq!(decoded, original, "roundtrip mismatch for {original:?}");
    }
}

#[test]
fn streaming_crc32c_hasher_matches_oneshot_across_chunk_boundaries() {
    let mut payload = Vec::with_capacity(131_072);
    for i in 0..131_072usize {
        payload.push(((i.wrapping_mul(31)).wrapping_add(17) & 0xff) as u8);
    }
    let expected = checksum_crc32c(&payload);

    for chunk_size in [1usize, 7, 64, 511, 4096, 65536] {
        let mut hasher = Crc32cHasher::new();
        for chunk in payload.chunks(chunk_size) {
            hasher.update(chunk);
        }
        assert_eq!(
            hasher.finalize(),
            expected,
            "streaming CRC32C diverged at chunk_size={chunk_size}"
        );
    }
}

#[test]
fn origin_quota_cursor_key_and_composite_key_range_validation() {
    let cursor = OriginQuotaCursorKey { uid: 9001 };
    let encoded = cursor.encode();
    let decoded = OriginQuotaCursorKey::decode(&encoded).expect("valid cursor key");
    assert_eq!(decoded, cursor);
    assert!(OriginQuotaCursorKey::decode(&encoded[..encoded.len() - 1]).is_err());

    let cursor_val = encode_origin_quota_cursor(42, 3);
    assert_eq!(decode_origin_quota_cursor(&cursor_val).unwrap(), (42, 3));

    // Valid user-table ranges in [0x01, 0x04] succeed.
    assert!(CompositeKeyRange::new(vec![0x01], vec![0x02]).is_ok());
    assert!(CompositeKeyRange::new(vec![0x01], vec![0x04]).is_ok());
    assert!(CompositeKeyRange::new(vec![0x02, 0x00], vec![0x03, 0xff]).is_ok());

    // System prefixes (>= 0x04 start or > [0x04] end) and prefix 0x00 are rejected.
    assert!(CompositeKeyRange::new(vec![0x00], vec![0x02]).is_err());
    assert!(CompositeKeyRange::new(vec![0x01], vec![0x05]).is_err());
    assert!(CompositeKeyRange::new(vec![0x01], vec![0x04, 0x01]).is_err());
    assert!(CompositeKeyRange::new(vec![0x04], vec![0x05]).is_err());

    // `decode_lease_preimage` requires a non-empty user key after the 17-byte `[0x0D | txn_id]` header.
    let valid_preimage = lease_preimage_key(42, &[0x01, 0, 0, 0, 0, 0, 0, 0, 1, b'a']);
    let pre_val = encode_lease_preimage(
        HlcTimestamp::new(1_000, 0),
        &LeasePreimageValue::Value(b"v".to_vec()),
    );
    let (txn_id, user_key, _, _) =
        decode_lease_preimage(&valid_preimage, &pre_val).expect("valid preimage");
    assert_eq!(txn_id, 42);
    assert_eq!(user_key, vec![0x01, 0, 0, 0, 0, 0, 0, 0, 1, b'a']);
    assert!(decode_lease_preimage(&valid_preimage[..17], &pre_val).is_none());
    assert!(decode_lease_preimage(&[0x05; 20], &pre_val).is_none());
    let _ = HlcTimestamp::ZERO;
}
