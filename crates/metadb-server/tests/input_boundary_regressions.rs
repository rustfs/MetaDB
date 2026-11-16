use metadb_core::{
    affinity_key_for_raw_entry, validate_user_entry_name, HlcTimestamp, HybridLogicalClock,
    MetaKey, ShardEpoch,
};
use metadb_proto::convert::{decode_optional_u128_be, decode_u128_be};
use metadb_proxy::SqlTemplateParser;

#[test]
fn wire_identifier_lengths_and_key_prefixes_are_checked() {
    for n in 0..=64 {
        let bytes = vec![0; n];
        assert_eq!(decode_u128_be(&bytes).is_ok(), n == 16);
        assert_eq!(decode_optional_u128_be(&bytes).is_ok(), n == 0 || n == 16);
        assert_eq!(ShardEpoch::decode(&bytes).is_ok(), n == 16);
    }
    for prefix in [0x00, 0x08, 0xff] {
        let mut raw = vec![0; 9];
        raw[0] = prefix;
        assert!(MetaKey::decode(&raw).is_err());
        assert!(affinity_key_for_raw_entry(&raw, &[]).is_err());
    }
}

#[test]
fn names_pid_max_and_future_hlc_have_defined_boundaries() {
    for name in ["", "_attr_", "contains\0nul"] {
        assert!(validate_user_entry_name(name).is_err());
    }
    // Names must fit the largest internal SST key at every restart boundary.
    assert!(validate_user_entry_name(&"a".repeat(metadb_core::MAX_USER_NAME_BYTES)).is_ok());
    assert!(validate_user_entry_name(&"a".repeat(metadb_core::MAX_USER_NAME_BYTES + 1)).is_err());
    assert_eq!(MetaKey::encode_pid_prefix_end(u64::MAX), vec![0x02]);
    let clock = HybridLogicalClock::new();
    assert!(clock
        .update_checked_with_wall_ms(HlcTimestamp::new(11_000, 0), 1000)
        .is_err());
    assert_eq!(clock.now_with_wall_ms(1000).physical_ms(), 1000);
}

#[test]
fn malformed_sql_and_truncated_templates_return_errors() {
    for sql in [
        "",
        "'",
        "SELECT",
        "SELECT * FROM",
        "DELETE FROM MetaTable WHERE pid = -1 AND name = 'a'",
        "SELECT * FROM MetaTable WHERE pid = 18446744073709551616 AND name = 'a'",
        "SELECT * FROM MetaTable WHERE pid = 1 AND name = 'a'; DROP TABLE MetaTable",
        "SELECT * FROM MetaTable WHERE pid = 1 AND name = 'unterminated",
    ] {
        assert!(SqlTemplateParser::parse(sql).is_err(), "accepted {sql:?}");
    }
    let valid = "SELECT * FROM MetaTable WHERE pid = 1 AND name = 'a'";
    for end in 0..valid.len() {
        assert!(SqlTemplateParser::parse(&valid[..end]).is_err());
    }
    assert!(SqlTemplateParser::parse(valid).is_ok());
}
