//! 16-shard $O(1)$ bounded LRU `BlockCache` for parsed SSTable `DataBlock`s.

use std::collections::HashMap;
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

struct LruNode {
    key: ScopedCacheKey,
    block: Option<Arc<DataBlock>>,
    bytes: usize,
    prev: Option<usize>,
    next: Option<usize>,
}

/// $O(1)$ slab-backed doubly-linked list LRU shard.
struct LruShard {
    capacity_bytes: usize,
    used_bytes: usize,
    nodes: Vec<LruNode>,
    free_list: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    index: HashMap<ScopedCacheKey, usize>,
}

impl LruShard {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes: capacity_bytes.max(4096),
            used_bytes: 0,
            nodes: Vec::new(),
            free_list: Vec::new(),
            head: None,
            tail: None,
            index: HashMap::new(),
        }
    }

    fn detach(&mut self, idx: usize) {
        let prev = self.nodes[idx].prev.take();
        let next = self.nodes[idx].next.take();
        if let Some(p) = prev {
            self.nodes[p].next = next;
        } else {
            self.head = next;
        }
        if let Some(n) = next {
            self.nodes[n].prev = prev;
        } else {
            self.tail = prev;
        }
    }

    fn push_back(&mut self, idx: usize) {
        self.nodes[idx].prev = self.tail;
        self.nodes[idx].next = None;
        if let Some(t) = self.tail {
            self.nodes[t].next = Some(idx);
        } else {
            self.head = Some(idx);
        }
        self.tail = Some(idx);
    }

    fn remove_idx(&mut self, idx: usize) {
        self.detach(idx);
        let key = self.nodes[idx].key;
        self.index.remove(&key);
        self.used_bytes = self.used_bytes.saturating_sub(self.nodes[idx].bytes);
        self.nodes[idx].block = None;
        self.free_list.push(idx);
    }

    fn get(&mut self, key: &ScopedCacheKey) -> Option<Arc<DataBlock>> {
        let &idx = self.index.get(key)?;
        let block = self.nodes[idx].block.as_ref()?.clone();
        if self.tail != Some(idx) {
            self.detach(idx);
            self.push_back(idx);
        }
        Some(block)
    }

    fn insert(&mut self, key: ScopedCacheKey, block: Arc<DataBlock>) {
        let block_bytes = block.byte_size().max(64);
        if block_bytes > self.capacity_bytes {
            return;
        }
        if let Some(&existing_idx) = self.index.get(&key) {
            self.remove_idx(existing_idx);
        }
        while self.used_bytes + block_bytes > self.capacity_bytes {
            let Some(evict_idx) = self.head else {
                break;
            };
            self.remove_idx(evict_idx);
        }
        let idx = if let Some(reused) = self.free_list.pop() {
            self.nodes[reused] = LruNode {
                key,
                block: Some(block),
                bytes: block_bytes,
                prev: None,
                next: None,
            };
            reused
        } else {
            let idx = self.nodes.len();
            self.nodes.push(LruNode {
                key,
                block: Some(block),
                bytes: block_bytes,
                prev: None,
                next: None,
            });
            idx
        };
        self.used_bytes += block_bytes;
        self.push_back(idx);
        self.index.insert(key, idx);
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
