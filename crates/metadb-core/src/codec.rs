//! Bincode 1.x compatible encoding backed by wincode.
//!
//! Field order, fixed-width little-endian integers, u32 enum tags and u64
//! sequence lengths are part of the persisted and Raft transport formats.

use serde::{Deserialize, Serialize};
use wincode::config::{self, Configuration, DefaultConfig};
use wincode::{SchemaRead, SchemaWrite};

pub use serde_wincode::SerdeCompat;

// Writers accept existing payload sizes. Readers use bounded native
// preallocation and fall back to incremental Serde decoding for large sequences.
type Config = Configuration<true, { usize::MAX }>;

pub fn serialize<T: SchemaWrite<Config, Src = T>>(value: &T) -> wincode::WriteResult<Vec<u8>> {
    config::serialize(value, Config::new())
}

pub fn deserialize<'de, T>(bytes: &'de [u8]) -> wincode::ReadResult<T>
where
    T: SchemaRead<'de, DefaultConfig, Dst = T> + Deserialize<'de>,
{
    match config::deserialize(bytes, DefaultConfig::default()) {
        Err(wincode::ReadError::PreallocationSizeLimit { .. }) => deserialize_serde(bytes),
        result => result,
    }
}

/// Serde bridge for foreign OpenRaft types whose schemas we cannot derive.
/// Local commands with u128 IDs use [`serialize`], since serde-wincode 0.1.2
/// lacks 128-bit integer serialization.
pub fn serialize_serde<T: Serialize>(value: &T) -> wincode::WriteResult<Vec<u8>> {
    <SerdeCompat<T> as config::Serialize<Config>>::serialize(value, Config::new())
}

pub fn deserialize_serde<'de, T: Deserialize<'de>>(bytes: &'de [u8]) -> wincode::ReadResult<T> {
    <SerdeCompat<T> as config::Deserialize<Config>>::deserialize(bytes, Config::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};

    #[derive(
        Debug, PartialEq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
    )]
    enum Record {
        Empty,
        Tx {
            id: u128,
            delta: i64,
            name: String,
            data: Option<Vec<u8>>,
        },
    }

    #[test]
    fn legacy_fixed_integer_enum_fixture() {
        let value = Record::Tx {
            id: u128::MAX,
            delta: -2,
            name: "db".into(),
            data: Some(vec![0, 255]),
        };
        // Frozen bincode 1.x default layout, including a 16-byte transaction ID.
        let bytes = [
            &1u32.to_le_bytes()[..],
            &u128::MAX.to_le_bytes(),
            &(-2i64).to_le_bytes(),
            &2u64.to_le_bytes(),
            b"db",
            &[1],
            &2u64.to_le_bytes(),
            &[0, 255],
        ]
        .concat();
        assert_eq!(serialize(&value).unwrap(), bytes);
        assert_eq!(deserialize::<Record>(&bytes).unwrap(), value);
        assert_eq!(deserialize_serde::<Record>(&bytes).unwrap(), value);
        for len in 0..bytes.len() {
            assert!(deserialize::<Record>(&bytes[..len]).is_err());
        }
        assert!(deserialize::<Record>(&2u32.to_le_bytes()).is_err());
    }

    #[test]
    fn project_enum_ordinals_match_legacy_format() {
        use crate::{EntryType, MetaDbError};
        assert_eq!(serialize(&EntryType::File).unwrap(), 0u32.to_le_bytes());
        assert_eq!(
            serialize(&EntryType::Directory).unwrap(),
            1u32.to_le_bytes()
        );
        let bytes = [&23u32.to_le_bytes()[..], &(-1i64).to_le_bytes()].concat();
        assert_eq!(serialize(&MetaDbError::InvalidFileSize(-1)).unwrap(), bytes);
        assert_eq!(
            deserialize::<MetaDbError>(&bytes).unwrap(),
            MetaDbError::InvalidFileSize(-1)
        );
    }

    #[test]
    fn legacy_allocator_container_fixture() {
        let value = (
            BTreeMap::from([(u128::MAX, (1u64, 8u64))]),
            VecDeque::from([u128::MAX]),
        );
        let bytes = [
            &1u64.to_le_bytes()[..],
            &u128::MAX.to_le_bytes(),
            &1u64.to_le_bytes(),
            &8u64.to_le_bytes(),
            &1u64.to_le_bytes(),
            &u128::MAX.to_le_bytes(),
        ]
        .concat();
        assert_eq!(serialize(&value).unwrap(), bytes);
        assert_eq!(
            deserialize::<(BTreeMap<u128, (u64, u64)>, VecDeque<u128>)>(&bytes).unwrap(),
            value
        );
    }

    #[test]
    fn snapshot_payload_exceeding_default_preallocation_limit() {
        let value = vec![42u8; (4 << 20) + 1];
        let bytes = serialize(&value).unwrap();
        assert_eq!(deserialize::<Vec<u8>>(&bytes).unwrap(), value);
        assert_eq!(deserialize_serde::<Vec<u8>>(&bytes).unwrap(), value);
        assert_eq!(serialize_serde(&value).unwrap(), bytes);
        assert!(deserialize::<Vec<u8>>(&u64::MAX.to_le_bytes()).is_err());
        assert!(deserialize::<Vec<String>>(&u64::MAX.to_le_bytes()).is_err());
        assert!(deserialize::<bool>(&[2]).is_err());
    }
}
