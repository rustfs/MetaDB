//! Pure-Rust CRC32C (Castagnoli) framing helpers for on-disk and wire codecs.

use crate::error::{MetaDbError, Result};

/// Computes the 32-bit Castagnoli CRC32C checksum over `data`.
#[inline]
pub fn checksum_crc32c(data: &[u8]) -> u32 {
    crc_fast::crc32_iscsi(data)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_crc32c(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for byte in bytes {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
            }
        }
        !crc
    }

    #[test]
    fn castagnoli_vectors_and_dispatch_boundaries() {
        assert_eq!(checksum_crc32c(b""), 0);
        assert_eq!(checksum_crc32c(b"123456789"), 0xe306_9283);
        let data: Vec<u8> = (0..65538).map(|i| (i * 37) as u8).collect();
        for len in [
            0, 1, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 511, 512, 1023, 1024, 4095,
            4096, 65536,
        ] {
            for offset in 0..2 {
                let bytes = &data[offset..offset + len];
                assert_eq!(checksum_crc32c(bytes), reference_crc32c(bytes));
            }
        }
    }

    #[test]
    fn legacy_trailers_and_corruption() {
        let mut be = b"123456789".to_vec();
        append_crc32c_be(&mut be);
        assert_eq!(&be[9..], &[0xe3, 0x06, 0x92, 0x83]);
        assert_eq!(verify_crc32c_be(&be, "test").unwrap(), b"123456789");
        let mut le = b"123456789".to_vec();
        append_crc32c_le(&mut le);
        assert_eq!(&le[9..], &[0x83, 0x92, 0x06, 0xe3]);
        assert_eq!(verify_crc32c_le(&le, "test").unwrap(), b"123456789");
        be[0] ^= 1;
        le[9] ^= 1;
        assert!(verify_crc32c_be(&be, "test").is_err());
        assert!(verify_crc32c_le(&le, "test").is_err());
        for len in 0..4 {
            assert!(verify_crc32c_be(&be[..len], "test").is_err());
            assert!(verify_crc32c_le(&le[..len], "test").is_err());
        }
    }
}
