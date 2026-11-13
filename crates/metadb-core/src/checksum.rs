//! Pure-Rust CRC32C (Castagnoli) framing helpers for on-disk and wire codecs.

use crate::error::{MetaDbError, Result};

/// Computes the 32-bit Castagnoli CRC32C checksum over `data`.
#[inline]
pub fn checksum_crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Appends a 4-byte big-endian CRC32C trailer computed over the current contents of `buf`.
#[inline]
pub fn append_crc32c_be(buf: &mut Vec<u8>) {
    let crc = checksum_crc32c(buf);
    buf.extend_from_slice(&crc.to_be_bytes());
}

/// Verifies a trailing 4-byte big-endian CRC32C checksum and returns the payload slice without trailer.
pub fn verify_crc32c_be<'a>(bytes: &'a [u8], context: &'static str) -> Result<&'a [u8]> {
    if bytes.len() < 4 {
        return Err(MetaDbError::CodecError(format!(
            "{}: buffer too short ({} bytes) for CRC32C trailer",
            context,
            bytes.len()
        )));
    }
    let split = bytes.len() - 4;
    let payload = &bytes[..split];
    let expected = u32::from_be_bytes(bytes[split..].try_into().unwrap());
    let actual = checksum_crc32c(payload);
    if expected != actual {
        return Err(MetaDbError::ChecksumMismatch {
            context: context.to_string(),
            expected,
            actual,
        });
    }
    Ok(payload)
}

/// Appends a 4-byte little-endian CRC32C trailer computed over the current contents of `buf`.
#[inline]
pub fn append_crc32c_le(buf: &mut Vec<u8>) {
    let crc = checksum_crc32c(buf);
    buf.extend_from_slice(&crc.to_le_bytes());
}

/// Verifies a trailing 4-byte little-endian CRC32C checksum and returns the payload slice without trailer.
pub fn verify_crc32c_le<'a>(bytes: &'a [u8], context: &'static str) -> Result<&'a [u8]> {
    if bytes.len() < 4 {
        return Err(MetaDbError::CodecError(format!(
            "{}: buffer too short ({} bytes) for CRC32C trailer",
            context,
            bytes.len()
        )));
    }
    let split = bytes.len() - 4;
    let payload = &bytes[..split];
    let expected = u32::from_le_bytes(bytes[split..].try_into().unwrap());
    let actual = checksum_crc32c(payload);
    if expected != actual {
        return Err(MetaDbError::ChecksumMismatch {
            context: context.to_string(),
            expected,
            actual,
        });
    }
    Ok(payload)
}
