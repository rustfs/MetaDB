//! 16-shard bounded LRU `BlockCache` for parsed SSTable `DataBlock`s.

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use metadb_core::ShardId;

use crate::block::DataBlock;

const NUM_CACHE_SHARDS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockCacheKey {
    pub shard_id: ShardId,
    pub sst_file_id: u64,
    pub block_offset: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ScopedCacheKey {
    reader_id: u64,
    block: BlockCacheKey,
}

struct LruShard {
    capacity_bytes: usize,
    used_bytes: usize,
    order: VecDeque<ScopedCacheKey>,
    entries: HashMap<ScopedCacheKey, Arc<DataBlock>>,
}

impl LruShard {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes: capacity_bytes.max(4096),
            used_bytes: 0,
            order: VecDeque::new(),
            entries: HashMap::new(),
        }
    }

    fn get(&mut self, key: &ScopedCacheKey) -> Option<Arc<DataBlock>> {
        let block = self.entries.get(key)?.clone();
        if self.order.back() != Some(key) {
            self.order.retain(|k| k != key);
            self.order.push_back(*key);
        }
        Some(block)
    }

    fn insert(&mut self, key: ScopedCacheKey, block: Arc<DataBlock>) {
        let block_bytes = block.byte_size().max(64);
        if block_bytes > self.capacity_bytes {
            return;
        }
        if let Some(old) = self.entries.remove(&key) {
            self.used_bytes = self.used_bytes.saturating_sub(old.byte_size().max(64));
            self.order.retain(|k| k != &key);
        }
        while self.used_bytes + block_bytes > self.capacity_bytes {
            let Some(evict_key) = self.order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&evict_key) {
                self.used_bytes = self.used_bytes.saturating_sub(evicted.byte_size().max(64));
            }
        }
        self.used_bytes += block_bytes;
        self.order.push_back(key);
        self.entries.insert(key, block);
    }
}

/// Concurrent 16-shard LRU cache sharing a bounded memory budget across shard SSTables.
pub struct ShardedBlockCache {
    shards: [Mutex<LruShard>; NUM_CACHE_SHARDS],
}

impl ShardedBlockCache {
    pub fn new(total_capacity_bytes: usize) -> Self {
        let per_shard = (total_capacity_bytes / NUM_CACHE_SHARDS).max(16 * 1024);
        Self {
            shards: std::array::from_fn(|_| Mutex::new(LruShard::new(per_shard))),
        }
    }

    pub fn get(&self, key: &BlockCacheKey) -> Option<Arc<DataBlock>> {
        self.get_for_reader(0, key)
    }

    pub fn insert(&self, key: BlockCacheKey, block: Arc<DataBlock>) {
        self.insert_for_reader(0, key, block);
    }

    pub(crate) fn get_for_reader(
        &self,
        reader_id: u64,
        key: &BlockCacheKey,
    ) -> Option<Arc<DataBlock>> {
        let key = ScopedCacheKey {
            reader_id,
            block: *key,
        };
        let idx = Self::shard_index(&key);
        self.shards[idx].lock().unwrap().get(&key)
    }

    pub(crate) fn insert_for_reader(
        &self,
        reader_id: u64,
        key: BlockCacheKey,
        block: Arc<DataBlock>,
    ) {
        let key = ScopedCacheKey {
            reader_id,
            block: key,
        };
        let idx = Self::shard_index(&key);
        self.shards[idx].lock().unwrap().insert(key, block);
    }

    fn shard_index(key: &ScopedCacheKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & (NUM_CACHE_SHARDS - 1)
    }
}
