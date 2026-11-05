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

pub mod compaction;
pub mod engine;
pub mod mvcc;
pub mod raft_log;

pub use compaction::{CompactionStats, LocalityCompactor};
pub use engine::{LifecycleStorageEngine, PersistentEntry, StorageMetrics};
pub use mvcc::{MemMvccStore, MvccValue, MvccVersion};
pub use raft_log::{LogSequenceNumber, RaftLog, RaftLogEntry, RaftLogPayload, TruncationWatermarks};
