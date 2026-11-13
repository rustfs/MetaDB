//! Lifecycle-Aware Storage Engine for MetaDB (§6 of `arXiv:2610.32247v1`).
//!
//! Includes:
//! 1. [`raft_log`]: Single-source-of-truth Raft durability log (eliminating double-logging
//!    by disabling the underlying KV store's WAL) with three-watermark truncation safety.
//! 2. [`mvcc`]: In-Memory MVCC (`MemMvccStore`) retaining transient multi-version states only
//!    for the short metadata transaction timeout window (`5s`) and pruning MVCC tombstones
//!    on flush to eliminate the double-tombstone penalty.
//! 3. [`compaction`]: Locality-Aware Compaction (`LocalityCompactor`) tracking range deletion
//!    density and purging localized LSM tombstones to maintain flat `listdir` latency.
//! 4. [`engine`]: Unified `LifecycleStorageEngine` combining RaftLog, In-Memory MVCC,
//!    persistent sorted KV storage, and Locality-Aware Compaction.

pub mod backend;
pub mod block;
pub mod bloom;
pub mod cache;
pub mod compaction;
pub mod engine;
pub mod manifest;
pub mod mvcc;
pub mod raft_log;
pub mod sstable;
pub mod version_set;

pub use backend::{InMemoryBackend, LsmStorageBackend, PersistentEntry, StorageBackend};
pub use cache::ShardedBlockCache;
pub use compaction::{CompactionStats, LocalityCompactor};
pub use engine::{LifecycleStorageEngine, LsmEngineConfig, StorageMetrics};
pub use manifest::{DurableShardCheckpoint, ManifestLog, VersionEdit};
pub use mvcc::{
    ActiveMemTable, ActiveSnapshotRegistry, ImmMemTable, MemMvccStore, MvccValue, MvccVersion,
    PinnedSnapshotData, PreFlushHook, ReadCut, SnapshotGuard, ViewAtRaftIndex,
    DEFAULT_MVCC_RETENTION_MS,
};
pub use raft_log::{
    LogSequenceNumber, RaftLog, RaftLogEntry, RaftLogPayload, TruncationWatermarks,
};
pub use sstable::{SstFileMeta, SstableBuilder, SstableReader, SSTABLE_FOOTER_LEN};
pub use version_set::Version;
