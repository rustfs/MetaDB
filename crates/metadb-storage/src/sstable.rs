//! Immutable SSTable builder and reader (`<file_id:020>.sst`) with 64-byte magic/CRC32C footer.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use metadb_core::{
    append_crc32c_le, checksum_crc32c, verify_crc32c_le, HlcTimestamp, MetaDbError, Result, ShardId,
};

use crate::block::{BlockEntryValue, DataBlock, DataBlockBuilder, DEFAULT_BLOCK_SIZE_BYTES};
use crate::bloom::{BloomFilter, DEFAULT_BLOOM_BITS_PER_KEY};
use crate::cache::{BlockCacheKey, ShardedBlockCache};

pub const SSTABLE_MAGIC: u64 = 0x4D45_5441_5353_5431; // "METASST1"
pub const SSTABLE_FORMAT_VERSION: u32 = 1;
pub const SSTABLE_FOOTER_LEN: usize = 64;

/// Durable metadata describing an SSTable file in `MetaBlock` and `MANIFEST`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SstFileMeta {
    pub level: u8,
    pub file_id: u64,
    pub flush_generation: u64,
    pub file_size: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub num_entries: u64,
    pub num_tombstones: u64,
    pub max_raft_index: u64,
    pub max_hlc: HlcTimestamp,
}

#[derive(Debug, Clone)]
pub struct IndexBlockEntry {
    pub last_key: Vec<u8>,
    pub block_offset: u64,
    pub block_len: u32,
}

pub fn sst_file_name(file_id: u64) -> String {
    format!("{:020}.sst", file_id)
}

pub fn sst_tmp_file_name(file_id: u64) -> String {
    format!("{:020}.sst.tmp", file_id)
}

pub fn sync_dir(dir: &Path) -> Result<()> {
    let f = File::open(dir)?;
    f.sync_all()?;
    Ok(())
}

/// Writes sorted `(key, BlockEntryValue)` entries into a new `.sst` file atomically via `.sst.tmp`.
pub struct SstableBuilder {
    level: u8,
    file_id: u64,
    flush_generation: u64,
    max_raft_index: u64,
    max_hlc: HlcTimestamp,
    bloom_bits_per_key: usize,
}

impl SstableBuilder {
    pub fn new(
        level: u8,
        file_id: u64,
        flush_generation: u64,
        max_raft_index: u64,
        max_hlc: HlcTimestamp,
    ) -> Self {
        Self {
            level,
            file_id,
            flush_generation,
            max_raft_index,
            max_hlc,
            bloom_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
        }
    }

    pub fn with_bloom_bits(mut self, bits_per_key: usize) -> Self {
        self.bloom_bits_per_key = bits_per_key.max(4);
        self
    }

    /// Writes a sorted non-empty slice of `(key, BlockEntryValue)` into `lsm_dir/<file_id:020>.sst`.
    pub fn write_to_dir(
        &self,
        lsm_dir: &Path,
        entries: &[(Vec<u8>, BlockEntryValue)],
    ) -> Result<SstFileMeta> {
        if entries.is_empty() {
            return Err(MetaDbError::CodecError(
                "cannot write empty SSTable".to_string(),
            ));
        }

        let tmp_path = lsm_dir.join(sst_tmp_file_name(self.file_id));
        let final_path = lsm_dir.join(sst_file_name(self.file_id));

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        let mut offset: u64 = 0;
        let mut block_builder = DataBlockBuilder::new();
        let mut index_entries: Vec<IndexBlockEntry> = Vec::new();
        let mut all_key_refs: Vec<&[u8]> = Vec::with_capacity(entries.len());
        let mut num_tombstones: u64 = 0;

        for (k, v) in entries {
            // Both live values AND PhysicalTombstone keys are indexed in the Bloom filter!
            all_key_refs.push(k.as_slice());
            if matches!(v, BlockEntryValue::PhysicalTombstone) {
                num_tombstones += 1;
            }
            block_builder.add(k, v);
            if block_builder.estimated_size() >= DEFAULT_BLOCK_SIZE_BYTES {
                let last_key = block_builder.last_key().to_vec();
                let raw_block = std::mem::take(&mut block_builder).finish();
                let len = raw_block.len() as u32;
                file.write_all(&raw_block)?;
                index_entries.push(IndexBlockEntry {
                    last_key,
                    block_offset: offset,
                    block_len: len,
                });
                offset += u64::from(len);
            }
        }

        if !block_builder.is_empty() {
            let last_key = block_builder.last_key().to_vec();
            let raw_block = block_builder.finish();
            let len = raw_block.len() as u32;
            file.write_all(&raw_block)?;
            index_entries.push(IndexBlockEntry {
                last_key,
                block_offset: offset,
                block_len: len,
            });
            offset += u64::from(len);
        }

        // 1. Filter Block
        let filter_offset = offset;
        let filter_raw = BloomFilter::build_encoded(&all_key_refs, self.bloom_bits_per_key);
        let filter_len = filter_raw.len() as u64;
        file.write_all(&filter_raw)?;
        offset += filter_len;

        // 2. Index Block: [num_blocks: u32 LE]([last_key_len: u32 LE][last_key][offset: u64 LE][len: u32 LE])*[crc32c: u32 LE]
        let index_offset = offset;
        let mut index_raw = Vec::with_capacity(index_entries.len() * 32 + 8);
        index_raw.extend_from_slice(&(index_entries.len() as u32).to_le_bytes());
        for ie in &index_entries {
            index_raw.extend_from_slice(&(ie.last_key.len() as u32).to_le_bytes());
            index_raw.extend_from_slice(&ie.last_key);
            index_raw.extend_from_slice(&ie.block_offset.to_le_bytes());
            index_raw.extend_from_slice(&ie.block_len.to_le_bytes());
        }
        append_crc32c_le(&mut index_raw);
        let index_len = index_raw.len() as u64;
        file.write_all(&index_raw)?;
        offset += index_len;

        // 3. Meta Block
        let min_key = entries.first().unwrap().0.clone();
        let max_key = entries.last().unwrap().0.clone();
        let num_entries = entries.len() as u64;

        let meta_offset = offset;
        let mut meta_raw = Vec::with_capacity(min_key.len() + max_key.len() + 64);
        meta_raw.extend_from_slice(&(min_key.len() as u32).to_le_bytes());
        meta_raw.extend_from_slice(&min_key);
        meta_raw.extend_from_slice(&(max_key.len() as u32).to_le_bytes());
        meta_raw.extend_from_slice(&max_key);
        meta_raw.extend_from_slice(&num_entries.to_le_bytes());
        meta_raw.extend_from_slice(&num_tombstones.to_le_bytes());
        meta_raw.push(self.level);
        meta_raw.extend_from_slice(&self.flush_generation.to_le_bytes());
        meta_raw.extend_from_slice(&self.max_raft_index.to_le_bytes());
        meta_raw.extend_from_slice(&self.max_hlc.0.to_le_bytes());
        append_crc32c_le(&mut meta_raw);
        let meta_len = meta_raw.len() as u64;
        file.write_all(&meta_raw)?;
        offset += meta_len;

        // 4. Fixed 64-Byte Footer
        let mut footer = [0u8; SSTABLE_FOOTER_LEN];
        footer[0..8].copy_from_slice(&filter_offset.to_le_bytes());
        footer[8..16].copy_from_slice(&filter_len.to_le_bytes());
        footer[16..24].copy_from_slice(&index_offset.to_le_bytes());
        footer[24..32].copy_from_slice(&index_len.to_le_bytes());
        footer[32..40].copy_from_slice(&meta_offset.to_le_bytes());
        footer[40..48].copy_from_slice(&meta_len.to_le_bytes());
        footer[48..52].copy_from_slice(&SSTABLE_FORMAT_VERSION.to_le_bytes());
        footer[56..64].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());

        let mut crc_input = Vec::with_capacity(60);
        crc_input.extend_from_slice(&footer[0..52]);
        crc_input.extend_from_slice(&footer[56..64]);
        let footer_crc = checksum_crc32c(&crc_input);
        footer[52..56].copy_from_slice(&footer_crc.to_le_bytes());

        file.write_all(&footer)?;
        file.sync_data()?;
        drop(file);

        let file_size = offset + SSTABLE_FOOTER_LEN as u64;
        fs::rename(&tmp_path, &final_path)?;
        sync_dir(lsm_dir)?;

        Ok(SstFileMeta {
            level: self.level,
            file_id: self.file_id,
            flush_generation: self.flush_generation,
            file_size,
            min_key,
            max_key,
            num_entries,
            num_tombstones,
            max_raft_index: self.max_raft_index,
            max_hlc: self.max_hlc,
        })
    }
}

/// Thread-safe reader over an immutable `.sst` file using `pread` (`read_exact_at`) and `ShardedBlockCache`.
pub struct SstableReader {
    shard_id: ShardId,
    path: PathBuf,
    file: File,
    meta: SstFileMeta,
    bloom: BloomFilter,
    index: Vec<IndexBlockEntry>,
    block_cache: Arc<ShardedBlockCache>,
}

impl SstableReader {
    pub fn open(
        shard_id: ShardId,
        path: PathBuf,
        block_cache: Arc<ShardedBlockCache>,
    ) -> Result<Self> {
        let file = File::open(&path)?;
        let file_size = file.metadata()?.len();
        if file_size < SSTABLE_FOOTER_LEN as u64 {
            return Err(MetaDbError::ManifestCorrupted(format!(
                "SSTable {:?} smaller than 64-byte footer ({} bytes)",
                path, file_size
            )));
        }

        let mut footer = [0u8; SSTABLE_FOOTER_LEN];
        file.read_exact_at(&mut footer, file_size - SSTABLE_FOOTER_LEN as u64)?;

        let magic = u64::from_le_bytes(footer[56..64].try_into().unwrap());
        if magic != SSTABLE_MAGIC {
            return Err(MetaDbError::ManifestCorrupted(format!(
                "invalid SSTable magic 0x{:016x} in {:?}",
                magic, path
            )));
        }
        let version = u32::from_le_bytes(footer[48..52].try_into().unwrap());
        if version != SSTABLE_FORMAT_VERSION {
            return Err(MetaDbError::CodecError(format!(
                "unsupported SSTable format version {}",
                version
            )));
        }

        let expected_crc = u32::from_le_bytes(footer[52..56].try_into().unwrap());
        let mut crc_input = Vec::with_capacity(60);
        crc_input.extend_from_slice(&footer[0..52]);
        crc_input.extend_from_slice(&footer[56..64]);
        let actual_crc = checksum_crc32c(&crc_input);
        if expected_crc != actual_crc {
            return Err(MetaDbError::ChecksumMismatch {
                context: "SSTable Footer".to_string(),
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        let filter_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let filter_len = u64::from_le_bytes(footer[8..16].try_into().unwrap()) as usize;
        let index_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;
        let meta_offset = u64::from_le_bytes(footer[32..40].try_into().unwrap());
        let meta_len = u64::from_le_bytes(footer[40..48].try_into().unwrap()) as usize;

        let mut filter_buf = vec![0u8; filter_len];
        file.read_exact_at(&mut filter_buf, filter_offset)?;
        let bloom = BloomFilter::from_raw_checked(&filter_buf)?;

        let mut index_buf = vec![0u8; index_len];
        file.read_exact_at(&mut index_buf, index_offset)?;
        let index = Self::decode_index_block(&index_buf)?;

        let mut meta_buf = vec![0u8; meta_len];
        file.read_exact_at(&mut meta_buf, meta_offset)?;
        let file_id = Self::parse_file_id_from_path(&path)?;
        let meta = Self::decode_meta_block(&meta_buf, file_id, file_size)?;

        Ok(Self {
            shard_id,
            path,
            file,
            meta,
            bloom,
            index,
            block_cache,
        })
    }

    #[inline]
    pub fn meta(&self) -> &SstFileMeta {
        &self.meta
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline]
    pub fn index_entries(&self) -> &[IndexBlockEntry] {
        &self.index
    }

    /// Point lookup consulting key bounds -> Bloom filter -> sparse `IndexBlock` -> `ShardedBlockCache`.
    pub fn get(&self, key: &[u8]) -> Result<Option<BlockEntryValue>> {
        if key < self.meta.min_key.as_slice() || key > self.meta.max_key.as_slice() {
            return Ok(None);
        }
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // Binary search first IndexBlockEntry with `last_key >= key`.
        let idx = self
            .index
            .partition_point(|ie| ie.last_key.as_slice() < key);
        if idx >= self.index.len() {
            return Ok(None);
        }
        let block = self.load_data_block(&self.index[idx])?;
        block.get(key)
    }

    /// Scans all `(key, BlockEntryValue)` entries in `[start_key, end_key)`.
    pub fn scan_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<(Vec<u8>, BlockEntryValue)>> {
        if end_key <= self.meta.min_key.as_slice() || start_key > self.meta.max_key.as_slice() {
            return Ok(Vec::new());
        }
        let first_idx = self
            .index
            .partition_point(|ie| ie.last_key.as_slice() < start_key);
        let mut out = Vec::new();
        for ie in &self.index[first_idx..] {
            let block = self.load_data_block(ie)?;
            let prev_len = out.len();
            block.scan_range(start_key, end_key, &mut out)?;
            if ie.last_key.as_slice() >= end_key && out.len() == prev_len {
                break;
            }
            if ie.last_key.as_slice() >= end_key {
                break;
            }
        }
        Ok(out)
    }

    /// Reads every entry across all data blocks in this SSTable (used during compaction).
    pub fn read_all_entries(&self) -> Result<Vec<(Vec<u8>, BlockEntryValue)>> {
        let mut out = Vec::with_capacity(self.meta.num_entries as usize);
        for ie in &self.index {
            let block = self.load_data_block(ie)?;
            out.extend(block.iter_all()?);
        }
        Ok(out)
    }

    fn load_data_block(&self, ie: &IndexBlockEntry) -> Result<Arc<DataBlock>> {
        let cache_key = BlockCacheKey {
            shard_id: self.shard_id,
            sst_file_id: self.meta.file_id,
            block_offset: ie.block_offset,
        };
        if let Some(cached) = self.block_cache.get(&cache_key) {
            return Ok(cached);
        }
        let mut raw = vec![0u8; ie.block_len as usize];
        self.file.read_exact_at(&mut raw, ie.block_offset)?;
        let parsed = Arc::new(DataBlock::from_raw_checked(&raw)?);
        self.block_cache.insert(cache_key, parsed.clone());
        Ok(parsed)
    }

    fn decode_index_block(raw: &[u8]) -> Result<Vec<IndexBlockEntry>> {
        let body = verify_crc32c_le(raw, "SSTable IndexBlock")?;
        if body.len() < 4 {
            return Err(MetaDbError::CodecError(
                "IndexBlock shorter than header".to_string(),
            ));
        }
        let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
        let mut pos = 4usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 4 > body.len() {
                return Err(MetaDbError::CodecError("truncated IndexBlock".to_string()));
            }
            let klen = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + klen + 12 > body.len() {
                return Err(MetaDbError::CodecError(
                    "truncated IndexBlock entry".to_string(),
                ));
            }
            let last_key = body[pos..pos + klen].to_vec();
            pos += klen;
            let block_offset = u64::from_le_bytes(body[pos..pos + 8].try_into().unwrap());
            let block_len = u32::from_le_bytes(body[pos + 8..pos + 12].try_into().unwrap());
            pos += 12;
            out.push(IndexBlockEntry {
                last_key,
                block_offset,
                block_len,
            });
        }
        if pos != body.len() {
            return Err(MetaDbError::CodecError(
                "trailing bytes in IndexBlock".to_string(),
            ));
        }
        Ok(out)
    }

    fn decode_meta_block(raw: &[u8], file_id: u64, file_size: u64) -> Result<SstFileMeta> {
        let body = verify_crc32c_le(raw, "SSTable MetaBlock")?;
        if body.len() < 4 {
            return Err(MetaDbError::CodecError("MetaBlock too short".to_string()));
        }
        let mut pos = 0usize;
        let min_len = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + min_len + 4 > body.len() {
            return Err(MetaDbError::CodecError(
                "truncated MetaBlock min_key".to_string(),
            ));
        }
        let min_key = body[pos..pos + min_len].to_vec();
        pos += min_len;

        let max_len = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + max_len + 41 != body.len() {
            return Err(MetaDbError::CodecError(
                "invalid MetaBlock length".to_string(),
            ));
        }
        let max_key = body[pos..pos + max_len].to_vec();
        pos += max_len;

        let num_entries = u64::from_le_bytes(body[pos..pos + 8].try_into().unwrap());
        let num_tombstones = u64::from_le_bytes(body[pos + 8..pos + 16].try_into().unwrap());
        let level = body[pos + 16];
        let flush_generation = u64::from_le_bytes(body[pos + 17..pos + 25].try_into().unwrap());
        let max_raft_index = u64::from_le_bytes(body[pos + 25..pos + 33].try_into().unwrap());
        let max_hlc = HlcTimestamp(u64::from_le_bytes(
            body[pos + 33..pos + 41].try_into().unwrap(),
        ));

        Ok(SstFileMeta {
            level,
            file_id,
            flush_generation,
            file_size,
            min_key,
            max_key,
            num_entries,
            num_tombstones,
            max_raft_index,
            max_hlc,
        })
    }

    fn parse_file_id_from_path(path: &Path) -> Result<u64> {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| MetaDbError::CodecError("invalid SSTable filename".to_string()))?;
        stem.parse::<u64>().map_err(|e| {
            MetaDbError::CodecError(format!("invalid SSTable file_id '{}': {}", stem, e))
        })
    }
}
