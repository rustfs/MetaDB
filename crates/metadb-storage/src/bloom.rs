//! Kirsch-Mitzenmacher double-hashing Bloom filter (`10 bits/key`, `k = 7` probes) with CRC32C.
//!
//! Crucially, both live values AND `PhysicalTombstone` keys are inserted into every SSTable's
//! `FilterBlock` so point lookups never bypass a newer deletion tombstone in `L0`.

use metadb_core::{append_crc32c_le, checksum_crc32c, verify_crc32c_le, MetaDbError, Result};

pub const DEFAULT_BLOOM_BITS_PER_KEY: usize = 10;
pub const DEFAULT_BLOOM_PROBES: u8 = 7;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    k_probes: u8,
}

impl BloomFilter {
    /// Builds a serialized Bloom filter block `[bitset][k_probes: u8][crc32c: u32 LE]`.
    pub fn build_encoded(keys: &[&[u8]], bits_per_key: usize) -> Vec<u8> {
        let bpk = bits_per_key.max(4);
        let total_bits = (keys.len().saturating_mul(bpk)).max(512);
        let byte_len = total_bits.div_ceil(8);
        let bit_len = (byte_len as u64) * 8;
        let k_probes = DEFAULT_BLOOM_PROBES;

        let mut buf = vec![0u8; byte_len];
        for key in keys {
            let h1 = checksum_crc32c(key);
            let delta = h1.rotate_right(17);
            let mut h = h1;
            for _ in 0..k_probes {
                let bit_pos = ((h as u64) % bit_len) as usize;
                buf[bit_pos / 8] |= 1u8 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }
        buf.push(k_probes);
        append_crc32c_le(&mut buf);
        buf
    }

    /// Decodes and validates a `FilterBlock` from disk.
    pub fn from_raw_checked(raw: &[u8]) -> Result<Self> {
        let body = verify_crc32c_le(raw, "SSTable FilterBlock")?;
        if body.len() < 2 {
            return Err(MetaDbError::CodecError(
                "FilterBlock body too short".to_string(),
            ));
        }
        let k_probes = body[body.len() - 1];
        if k_probes == 0 || k_probes > 30 {
            return Err(MetaDbError::CodecError(format!(
                "invalid Bloom k_probes {}",
                k_probes
            )));
        }
        Ok(Self {
            bits: body[..body.len() - 1].to_vec(),
            k_probes,
        })
    }

    /// Returns `true` if `key` may exist in the SSTable (`false` guarantees absence).
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let bit_len = (self.bits.len() as u64) * 8;
        if bit_len == 0 {
            return true;
        }
        let h1 = checksum_crc32c(key);
        let delta = h1.rotate_right(17);
        let mut h = h1;
        for _ in 0..self.k_probes {
            let bit_pos = ((h as u64) % bit_len) as usize;
            if (self.bits[bit_pos / 8] & (1u8 << (bit_pos % 8))) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
}
