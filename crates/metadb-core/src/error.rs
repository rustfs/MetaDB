use std::fmt;

/// Result alias for MetaDB operations.
pub type Result<T> = std::result::Result<T, MetaDbError>;

/// Canonical error variants across all MetaDB subsystems.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaDbError {
    /// Attempted to use the reserved `"_attr_"` name for a user entry.
    ReservedEntryName(String),
    /// Target file or directory entry already exists.
    AlreadyExists { pid: u64, name: String },
    /// Target file or directory entry was not found.
    NotFound { pid: u64, name: String },
    /// Target directory inode was not found.
    DirectoryNotFound(u64),
    /// Attempted to remove a non-empty directory via `rmdir`.
    DirectoryNotEmpty { dir_id: u64, child_count: usize },
    /// Operation type mismatch (e.g., `rmdir` on a regular file or `unlink` on a directory).
    InvalidEntryType { expected: &'static str, actual: &'static str },
    /// Snapshot read timestamp is older than the In-Memory MVCC retention window (`>5s`).
    TransactionTimeout {
        snapshot_hlc: u64,
        oldest_retained_hlc: u64,
    },
    /// Key or value binary decoding failed.
    CodecError(String),
    /// Shard routing or boundary error.
    ShardRoutingError(String),
    /// Shard cannot be split along `group_key = PID` because all keys share one `PID` (Jumbo Shard).
    UnsplittableJumboShard { shard_id: u64, pid: u64 },
    /// Consensus or replication state error.
    ConsensusError(String),
    /// Lightweight SQL template parse error.
    SqlParseError(String),
}

impl fmt::Display for MetaDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetaDbError::ReservedEntryName(name) => {
                write!(f, "entry name '{}' is reserved for directory attributes", name)
            }
            MetaDbError::AlreadyExists { pid, name } => {
                write!(f, "entry ({}, '{}') already exists", pid, name)
            }
            MetaDbError::NotFound { pid, name } => {
                write!(f, "entry ({}, '{}') not found", pid, name)
            }
            MetaDbError::DirectoryNotFound(id) => {
                write!(f, "directory inode {} not found", id)
            }
            MetaDbError::DirectoryNotEmpty { dir_id, child_count } => {
                write!(
                    f,
                    "directory {} is not empty ({} live children remain)",
                    dir_id, child_count
                )
            }
            MetaDbError::InvalidEntryType { expected, actual } => {
                write!(f, "invalid entry type: expected {}, got {}", expected, actual)
            }
            MetaDbError::TransactionTimeout {
                snapshot_hlc,
                oldest_retained_hlc,
            } => write!(
                f,
                "transaction snapshot HLC {} expired (oldest retained in-memory MVCC HLC is {})",
                snapshot_hlc, oldest_retained_hlc
            ),
            MetaDbError::CodecError(msg) => write!(f, "codec error: {}", msg),
            MetaDbError::ShardRoutingError(msg) => write!(f, "shard routing error: {}", msg),
            MetaDbError::UnsplittableJumboShard { shard_id, pid } => write!(
                f,
                "shard {} is an unsplittable jumbo shard dominated by single PID {}",
                shard_id, pid
            ),
            MetaDbError::ConsensusError(msg) => write!(f, "consensus error: {}", msg),
            MetaDbError::SqlParseError(msg) => write!(f, "SQL template parse error: {}", msg),
        }
    }
}

impl std::error::Error for MetaDbError {}
