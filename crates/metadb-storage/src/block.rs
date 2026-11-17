//! 4 KiB prefix-compressed SSTable Data Blocks with 16-entry restart intervals and CRC32C trailers.

use metadb_core::{append_crc32c_le, verify_crc32c_le, MetaDbError, Result};

pub const DEFAULT_BLOCK_SIZE_BYTES: usize = 4096;
pub const RESTART_INTERVAL: usize = 16;

pub const ENTRY_TAG_VALUE: u8 = 0x01;
pub const ENTRY_TAG_TOMBSTONE: u8 = 0x02;

/// Decoded entry yielded from an SSTable `DataBlock`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockEntryValue {
    Value(Vec<u8>),
    PhysicalTombstone,
}

/// Builds a prefix-compressed SSTable `DataBlock`.
pub struct DataBlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    last_key: Vec<u8>,
    entries_count: usize,
}

impl Default for DataBlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DataBlockBuilder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(DEFAULT_BLOCK_SIZE_BYTES + 256),
            restarts: vec![0],
            counter: 0,
            last_key: Vec::new(),
            entries_count: 0,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries_count == 0
    }

    #[inline]
    pub fn estimated_size(&self) -> usize {
        self.buf.len() + (self.restarts.len() * 4) + 8
    }

    pub fn last_key(&self) -> &[u8] {
        &self.last_key
    }

    /// Appends a sorted `(key, value)` entry to the block, rejecting keys whose unshared suffix exceeds `u16::MAX`.
    pub fn add(&mut self, key: &[u8], value: &BlockEntryValue) -> Result<()> {
        debug_assert!(
            self.entries_count == 0 || key > self.last_key.as_slice(),
            "DataBlock keys must be strictly ascending"
        );

        let mut shared = 0usize;
        if self.counter < RESTART_INTERVAL {
            let limit = self.last_key.len().min(key.len()).min(u16::MAX as usize);
            while shared < limit && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
        }

        let unshared = key.len() - shared;
        let unshared_u16 = u16::try_from(unshared).map_err(|_| {
            MetaDbError::CodecError(format!(
                "DataBlock unshared key suffix length {} exceeds u16::MAX",
                unshared
            ))
        })?;
        self.buf.extend_from_slice(&(shared as u16).to_le_bytes());
        self.buf.extend_from_slice(&unshared_u16.to_le_bytes());
        match value {
            BlockEntryValue::Value(v) => {
                self.buf.push(ENTRY_TAG_VALUE);
                self.buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                self.buf.extend_from_slice(&key[shared..]);
                self.buf.extend_from_slice(v);
            }
            BlockEntryValue::PhysicalTombstone => {
                self.buf.push(ENTRY_TAG_TOMBSTONE);
                self.buf.extend_from_slice(&0u32.to_le_bytes());
                self.buf.extend_from_slice(&key[shared..]);
            }
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
        self.entries_count += 1;
        Ok(())
    }

    /// Finalizes the block with `[restart_offsets: u32 LE * R][num_restarts: u32 LE][crc32c: u32 LE]`.
    pub fn finish(mut self) -> Vec<u8> {
        for &offset in &self.restarts {
            self.buf.extend_from_slice(&offset.to_le_bytes());
        }
        self.buf
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        append_crc32c_le(&mut self.buf);
        self.buf
    }
}

/// Verified immutable SSTable `DataBlock` supporting $O(\log R)$ restart-point binary search.
#[derive(Debug, Clone)]
pub struct DataBlock {
    data: Vec<u8>,
    entries_end: usize,
    restarts: Vec<u32>,
}

impl DataBlock {
    /// Parses a raw on-disk block and validates its trailing CRC32C checksum.
    pub fn from_raw_checked(raw: &[u8]) -> Result<Self> {
        let body = verify_crc32c_le(raw, "SSTable DataBlock")?;
        if body.len() < 4 {
            return Err(MetaDbError::CodecError(
                "DataBlock body shorter than num_restarts".to_string(),
            ));
        }
        let num_restarts = u32::from_le_bytes(body[body.len() - 4..].try_into().unwrap()) as usize;
        let restarts_bytes = num_restarts
            .checked_mul(4)
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| MetaDbError::CodecError("DataBlock restarts overflow".to_string()))?;
        if num_restarts == 0 || body.len() < restarts_bytes {
            return Err(MetaDbError::CodecError(
                "invalid DataBlock restart table bounds".to_string(),
            ));
        }
        let entries_end = body.len() - restarts_bytes;
        let mut restarts = Vec::with_capacity(num_restarts);
        for i in 0..num_restarts {
            let off = entries_end + i * 4;
            let r = u32::from_le_bytes(body[off..off + 4].try_into().unwrap());
            if (r as usize) > entries_end {
                return Err(MetaDbError::CodecError(
                    "DataBlock restart offset out of range".to_string(),
                ));
            }
            restarts.push(r);
        }
        Ok(Self {
            data: body[..entries_end].to_vec(),
            entries_end,
            restarts,
        })
    }

    #[inline]
    pub fn byte_size(&self) -> usize {
        self.data.len() + self.restarts.len() * 4
    }

    /// Decodes all `(key, BlockEntryValue)` pairs in ascending order or starting from a restart point.
    pub fn get(&self, target_key: &[u8]) -> Result<Option<BlockEntryValue>> {
        // Binary search restart points (each restart point has shared_prefix_len == 0).
        let mut left = 0usize;
        let mut right = self.restarts.len();
        while left + 1 < right {
            let mid = left + (right - left) / 2;
            let restart_key = self.decode_restart_key(self.restarts[mid] as usize)?;
            if restart_key.as_slice() <= target_key {
                left = mid;
            } else {
                right = mid;
            }
        }

        let mut offset = self.restarts[left] as usize;
        let mut current_key = Vec::new();
        while offset < self.entries_end {
            let val = self.decode_next_entry(&mut offset, &mut current_key)?;
            if current_key.as_slice() == target_key {
                return Ok(Some(val));
            }
            if current_key.as_slice() > target_key {
                break;
            }
        }
        Ok(None)
    }

    /// Collects all entries in `[start_key, end_key)` within this block.
    pub fn scan_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        out: &mut Vec<(Vec<u8>, BlockEntryValue)>,
    ) -> Result<()> {
        let mut left = 0usize;
        let mut right = self.restarts.len();
        while left + 1 < right {
            let mid = left + (right - left) / 2;
            let restart_key = self.decode_restart_key(self.restarts[mid] as usize)?;
            if restart_key.as_slice() <= start_key {
                left = mid;
            } else {
                right = mid;
            }
        }

        let mut offset = self.restarts[left] as usize;
        let mut current_key = Vec::new();
        while offset < self.entries_end {
            let val = self.decode_next_entry(&mut offset, &mut current_key)?;
            if current_key.as_slice() < start_key {
                continue;
            }
            if current_key.as_slice() >= end_key {
                break;
            }
            out.push((current_key.clone(), val));
        }
        Ok(())
    }

    /// Byte offset of the restart point that may contain `start_key`.
    /// An empty `start_key` selects the first restart.
    pub(crate) fn restart_offset_for(&self, start_key: &[u8]) -> Result<usize> {
        if start_key.is_empty() || self.restarts.is_empty() {
            return Ok(self.restarts.first().copied().unwrap_or(0) as usize);
        }
        let mut left = 0usize;
        let mut right = self.restarts.len();
        while left + 1 < right {
            let mid = left + (right - left) / 2;
            let restart_key = self.decode_restart_key(self.restarts[mid] as usize)?;
            if restart_key.as_slice() <= start_key {
                left = mid;
            } else {
                right = mid;
            }
        }
        Ok(self.restarts[left] as usize)
    }

    /// Decodes the entry at `offset`, advancing `offset` and `key`.
    /// Returns `None` at the end of the block.
    pub(crate) fn read_entry(
        &self,
        offset: &mut usize,
        key: &mut Vec<u8>,
    ) -> Result<Option<BlockEntryValue>> {
        if *offset >= self.entries_end {
            return Ok(None);
        }
        let value = self.decode_next_entry(offset, key)?;
        Ok(Some(value))
    }

    /// Decodes every entry in the block (used by compaction iterators).
    pub fn iter_all(&self) -> Result<Vec<(Vec<u8>, BlockEntryValue)>> {
        let mut offset = 0usize;
        let mut current_key = Vec::new();
        let mut out = Vec::new();
        while offset < self.entries_end {
            let val = self.decode_next_entry(&mut offset, &mut current_key)?;
            out.push((current_key.clone(), val));
        }
        Ok(out)
    }

    fn decode_restart_key(&self, offset: usize) -> Result<Vec<u8>> {
        if offset + 9 > self.entries_end {
            return Err(MetaDbError::CodecError(
                "truncated DataBlock restart header".to_string(),
            ));
        }
        let shared = u16::from_le_bytes(self.data[offset..offset + 2].try_into().unwrap()) as usize;
        let unshared =
            u16::from_le_bytes(self.data[offset + 2..offset + 4].try_into().unwrap()) as usize;
        if shared != 0 || offset + 9 + unshared > self.entries_end {
            return Err(MetaDbError::CodecError(
                "corrupted DataBlock restart key".to_string(),
            ));
        }
        Ok(self.data[offset + 9..offset + 9 + unshared].to_vec())
    }

    fn decode_next_entry(
        &self,
        offset: &mut usize,
        current_key: &mut Vec<u8>,
    ) -> Result<BlockEntryValue> {
        let pos = *offset;
        if pos + 9 > self.entries_end {
            return Err(MetaDbError::CodecError(
                "truncated DataBlock entry header".to_string(),
            ));
        }
        let shared = u16::from_le_bytes(self.data[pos..pos + 2].try_into().unwrap()) as usize;
        let unshared = u16::from_le_bytes(self.data[pos + 2..pos + 4].try_into().unwrap()) as usize;
        let tag = self.data[pos + 4];
        let val_len = u32::from_le_bytes(self.data[pos + 5..pos + 9].try_into().unwrap()) as usize;

        let end = pos
            .checked_add(9)
            .and_then(|p| p.checked_add(unshared))
            .and_then(|p| p.checked_add(val_len))
            .ok_or_else(|| MetaDbError::CodecError("DataBlock entry overflow".to_string()))?;
        if shared > current_key.len() || end > self.entries_end {
            return Err(MetaDbError::CodecError(
                "corrupted DataBlock entry bounds".to_string(),
            ));
        }

        current_key.truncate(shared);
        current_key.extend_from_slice(&self.data[pos + 9..pos + 9 + unshared]);
        let val_slice = &self.data[pos + 9 + unshared..end];
        *offset = end;

        match tag {
            ENTRY_TAG_VALUE => Ok(BlockEntryValue::Value(val_slice.to_vec())),
            ENTRY_TAG_TOMBSTONE => Ok(BlockEntryValue::PhysicalTombstone),
            other => Err(MetaDbError::CodecError(format!(
                "unknown DataBlock entry tag 0x{:02x}",
                other
            ))),
        }
    }
}
