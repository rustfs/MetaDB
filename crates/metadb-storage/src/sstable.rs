//! Immutable SSTable builder and reader (`<file_id:020>.sst`) with 64-byte magic/CRC32C footer.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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

// File IDs are local to a database history and may be reused after snapshot
// installation. Give each opened immutable file its own cache namespace, also
// isolating old readers that remain pinned during a replacement.
static NEXT_READER_ID: AtomicU64 = AtomicU64::new(1);

/// Durable metadata describing an SSTable file in `MetaBlock` and `MANIFEST`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
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
            block_builder.add(k, v)?;
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
    cache_reader_id: u64,
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
        let filter_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let index_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let meta_offset = u64::from_le_bytes(footer[32..40].try_into().unwrap());
        let meta_len = u64::from_le_bytes(footer[40..48].try_into().unwrap());

        // Validate all footer extents before converting lengths or allocating.
        let data_end = file_size - SSTABLE_FOOTER_LEN as u64;
        if filter_offset == 0
            || filter_len < 6
            || index_len < 8
            || meta_len < 53
            || filter_offset.checked_add(filter_len) != Some(index_offset)
            || index_offset.checked_add(index_len) != Some(meta_offset)
            || meta_offset.checked_add(meta_len) != Some(data_end)
        {
            return Err(MetaDbError::ManifestCorrupted(
                "invalid SSTable section bounds".to_string(),
            ));
        }

        let filter_buf = Self::read_section(&file, filter_offset, filter_len)?;
        let bloom = BloomFilter::from_raw_checked(&filter_buf)?;

        let index_buf = Self::read_section(&file, index_offset, index_len)?;
        let index = Self::decode_index_block(&index_buf)?;
        for entry in &index {
            if entry.block_len < 8
                || entry
                    .block_offset
                    .checked_add(u64::from(entry.block_len))
                    .is_none_or(|end| end > filter_offset)
            {
                return Err(MetaDbError::ManifestCorrupted(
                    "SSTable data block exceeds data section".to_string(),
                ));
            }
        }

        let meta_buf = Self::read_section(&file, meta_offset, meta_len)?;
        let file_id = Self::parse_file_id_from_path(&path)?;
        let meta = Self::decode_meta_block(&meta_buf, file_id, file_size)?;
        if meta.level as usize >= crate::version_set::NUM_LSM_LEVELS
            || meta.min_key > meta.max_key
            || meta.num_entries == 0
            || meta.num_tombstones > meta.num_entries
            || meta.num_entries > filter_offset / 9
            || index.is_empty()
        {
            return Err(MetaDbError::ManifestCorrupted(
                "invalid SSTable metadata".to_string(),
            ));
        }
        let mut cache_reader_id = NEXT_READER_ID.load(Ordering::Relaxed);
        loop {
            let next_id = cache_reader_id.checked_add(1).ok_or_else(|| {
                MetaDbError::StorageIoError("SSTable cache reader IDs exhausted".to_string())
            })?;
            match NEXT_READER_ID.compare_exchange_weak(
                cache_reader_id,
                next_id,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cache_reader_id = observed,
            }
        }

        Ok(Self {
            shard_id,
            path,
            file,
            meta,
            bloom,
            index,
            block_cache,
            cache_reader_id,
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

    /// Scans up to `limit` `(key, BlockEntryValue)` entries in `[start_key, end_key)`.
    pub fn scan_range_limit(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, BlockEntryValue)>> {
        if limit == 0
            || end_key <= self.meta.min_key.as_slice()
            || start_key > self.meta.max_key.as_slice()
        {
            return Ok(Vec::new());
        }
        let first_idx = self
            .index
            .partition_point(|ie| ie.last_key.as_slice() < start_key);
        let mut out = Vec::new();
        for ie in &self.index[first_idx..] {
            let block = self.load_data_block(ie)?;
            block.scan_range(start_key, end_key, &mut out)?;
            if out.len() >= limit {
                out.truncate(limit);
                break;
            }
            if ie.last_key.as_slice() >= end_key {
                break;
            }
        }
        Ok(out)
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
        let mut out = Vec::new();
        for ie in &self.index {
            let block = self.load_data_block(ie)?;
            out.extend(block.iter_all()?);
        }
        Ok(out)
    }

    /// Verifies every data block before accepting a transferred checkpoint.
    /// Work is bounded to one block at a time rather than materializing the entire SST.
    pub fn verify_all_data_blocks(&self) -> Result<()> {
        let mut entries = 0u64;
        let mut tombstones = 0u64;
        let mut first_key = None;
        let mut last_key = None;
        let mut next_offset = 0;
        for index_entry in &self.index {
            if index_entry.block_offset != next_offset {
                return Err(MetaDbError::ManifestCorrupted(
                    "noncontiguous SST data blocks".to_string(),
                ));
            }
            let block = self.load_data_block(index_entry)?;
            for (key, value) in block.iter_all()? {
                if last_key.as_ref().is_some_and(|previous| previous >= &key) {
                    return Err(MetaDbError::ManifestCorrupted(
                        "unordered SST data keys".to_string(),
                    ));
                }
                entries += 1;
                tombstones += u64::from(matches!(value, BlockEntryValue::PhysicalTombstone));
                first_key.get_or_insert_with(|| key.clone());
                last_key = Some(key);
            }
            if last_key.as_deref() != Some(index_entry.last_key.as_slice()) {
                return Err(MetaDbError::ManifestCorrupted(
                    "SST index key differs from data block".to_string(),
                ));
            }
            next_offset += u64::from(index_entry.block_len);
        }
        if entries != self.meta.num_entries
            || tombstones != self.meta.num_tombstones
            || first_key.as_deref() != Some(self.meta.min_key.as_slice())
            || last_key.as_deref() != Some(self.meta.max_key.as_slice())
        {
            return Err(MetaDbError::ManifestCorrupted(
                "SST data differs from file metadata".to_string(),
            ));
        }
        Ok(())
    }

    fn load_data_block(&self, ie: &IndexBlockEntry) -> Result<Arc<DataBlock>> {
        let cache_key = BlockCacheKey {
            shard_id: self.shard_id,
            sst_file_id: self.meta.file_id,
            block_offset: ie.block_offset,
        };
        if let Some(cached) = self
            .block_cache
            .get_for_reader(self.cache_reader_id, &cache_key)
        {
            return Ok(cached);
        }
        let raw = Self::read_section(&self.file, ie.block_offset, u64::from(ie.block_len))?;
        let parsed = Arc::new(DataBlock::from_raw_checked(&raw)?);
        self.block_cache
            .insert_for_reader(self.cache_reader_id, cache_key, parsed.clone());
        Ok(parsed)
    }

    fn read_section(file: &File, offset: u64, len: u64) -> Result<Vec<u8>> {
        let len = usize::try_from(len).map_err(|_| {
            MetaDbError::CodecError("SSTable section length exceeds address space".to_string())
        })?;
        let mut raw = Vec::new();
        raw.try_reserve_exact(len).map_err(|_| {
            MetaDbError::CodecError("SSTable section allocation failed".to_string())
        })?;
        raw.resize(len, 0);
        file.read_exact_at(&mut raw, offset)?;
        Ok(raw)
    }

    fn decode_index_block(raw: &[u8]) -> Result<Vec<IndexBlockEntry>> {
        let body = verify_crc32c_le(raw, "SSTable IndexBlock")?;
        if body.len() < 4 {
            return Err(MetaDbError::CodecError(
                "IndexBlock shorter than header".to_string(),
            ));
        }
        let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
        if count > (body.len() - 4) / 16 {
            return Err(MetaDbError::CodecError(
                "IndexBlock count exceeds available bytes".to_string(),
            ));
        }
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
