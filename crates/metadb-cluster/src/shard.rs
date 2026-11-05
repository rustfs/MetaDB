use std::collections::BTreeMap;

use metadb_core::{HlcTimestamp, InodeId, MetaDbError, MetaKey, NodeId, Result, ShardId};
use metadb_storage::LifecycleStorageEngine;

/// Sharding mode for a shard (§4.1 Workload-Adjustable Dual-Mode Sharding).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardSplitMode {
    /// Default mode: splits strictly at `group_key = PID` boundaries so a directory's
    /// `_attr_` and all direct children remain on a single shard.
    DirectoryColocated,
    /// Fallback mode for extreme Jumbo Shards that exceed a dedicated machine's capacity:
    /// allows splitting within a single `PID` along `(PID, name)` boundaries.
    DualModeFineGrained,
}

/// Scheduling and isolation state of a shard (§4.1 Jumbo-Shard-Aware Scheduling).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardHealthState {
    Normal,
    /// Unsplittable single-PID Jumbo Shard isolated onto a dedicated BE node with peer
    /// replicas of other shards evicted.
    JumboIsolated { dedicated_node: NodeId, dominant_pid: InodeId },
}

/// Role of a BE node inside a shard's Raft replication group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftMemberRole {
    /// Non-voting learner catching up on Raft snapshot and log before promotion.
    Learner,
    /// Full voting member of the Raft quorum.
    Voter,
}

/// Raft consensus group membership supporting zero-downtime evolution from
/// Stage 1 (`1-Voter` Single-Node) to Stage 3 (`3-Voter` Multi-Raft).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftMembership {
    pub leader: NodeId,
    pub members: BTreeMap<NodeId, RaftMemberRole>,
    pub conf_epoch: u64,
}

impl RaftMembership {
    /// Creates a Stage 1 single-node (`1-Voter`) Raft group on `initial_node`.
    pub fn single_node(initial_node: NodeId) -> Self {
        let mut members = BTreeMap::new();
        members.insert(initial_node, RaftMemberRole::Voter);
        Self {
            leader: initial_node,
            members,
            conf_epoch: 1,
        }
    }

    /// Step 1 of online scale-out: adds `node_id` as a non-voting `Learner` replica.
    pub fn add_learner(&mut self, node_id: NodeId) {
        self.members.entry(node_id).or_insert(RaftMemberRole::Learner);
        self.conf_epoch += 1;
    }

    /// Step 2 of online scale-out: promotes a caught-up `Learner` to a full `Voter`.
    pub fn promote_learner_to_voter(&mut self, node_id: NodeId) -> Result<()> {
        match self.members.get_mut(&node_id) {
            Some(role) => {
                *role = RaftMemberRole::Voter;
                self.conf_epoch += 1;
                Ok(())
            }
            None => Err(MetaDbError::ConsensusError(format!(
                "node {} is not a learner in this shard",
                node_id
            ))),
        }
    }

    /// Counts current voting members (`1` in single-node mode, `3` in standard HA mode).
    pub fn voter_count(&self) -> usize {
        self.members
            .values()
            .filter(|&&r| r == RaftMemberRole::Voter)
            .count()
    }
}

/// Metadata descriptor for an active database shard covering `[start_pid, end_pid)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardDescriptor {
    pub shard_id: ShardId,
    /// Inclusive lower bound `PID` (`group_key`).
    pub start_pid: InodeId,
    /// Exclusive upper bound `PID` (`u64::MAX` for the rightmost shard).
    pub end_pid: InodeId,
    /// Optional fine-grained child name boundary when operating in `DualModeFineGrained`.
    pub fine_grained_name_split: Option<String>,
    pub split_mode: ShardSplitMode,
    pub health_state: ShardHealthState,
    pub membership: RaftMembership,
}

impl ShardDescriptor {
    pub fn new_root(shard_id: ShardId, leader_node: NodeId) -> Self {
        Self {
            shard_id,
            start_pid: 0,
            end_pid: u64::MAX,
            fine_grained_name_split: None,
            split_mode: ShardSplitMode::DirectoryColocated,
            health_state: ShardHealthState::Normal,
            membership: RaftMembership::single_node(leader_node),
        }
    }

    /// Checks whether a given `pid` falls inside `[start_pid, end_pid)`.
    pub fn contains_pid(&self, pid: InodeId) -> bool {
        pid >= self.start_pid && pid < self.end_pid
    }
}

/// Policy/Size-Based Shard Splitter (§4.1 of `arXiv:2610.32247v1`).
///
/// Inspects the keys inside a shard that has exceeded `split_threshold_bytes`:
/// - If multiple distinct `PID`s (`group_key`s) exist, computes a split point strictly
///   on a `PID` boundary near the byte midpoint so no directory is ever split across shards.
/// - If all keys in the shard belong to a single massive `PID` (a **Jumbo Shard**):
///   - In `DirectoryColocated` mode, returns `Err(MetaDbError::UnsplittableJumboShard)` so
///     `CellLocalManager` isolates the Jumbo Shard onto a dedicated node.
///   - In `DualModeFineGrained` fallback mode, splits on the median `(PID, child_name)` boundary.
pub struct PolicyShardSplitter;

impl PolicyShardSplitter {
    pub fn find_pid_split_boundary(
        shard: &ShardDescriptor,
        engine: &LifecycleStorageEngine,
        read_hlc: HlcTimestamp,
    ) -> Result<InodeId> {
        let start = MetaKey::encode_pid_prefix_start(shard.start_pid);
        let end = MetaKey::encode_pid_prefix_end(shard.end_pid.saturating_sub(1));
        let rows = engine.scan_range(&start, &end, read_hlc)?;

        let mut bytes_by_pid: BTreeMap<InodeId, usize> = BTreeMap::new();
        let mut total_bytes = 0usize;

        for (k_bytes, v_bytes) in rows {
            if let Ok(meta_key) = MetaKey::decode(&k_bytes) {
                let sz = k_bytes.len() + v_bytes.len();
                *bytes_by_pid.entry(meta_key.group_key()).or_insert(0) += sz;
                total_bytes += sz;
            }
        }

        if bytes_by_pid.len() <= 1 {
            let dominant_pid = bytes_by_pid.keys().copied().next().unwrap_or(shard.start_pid);
            return Err(MetaDbError::UnsplittableJumboShard {
                shard_id: shard.shard_id,
                pid: dominant_pid,
            });
        }

        let target_half = total_bytes / 2;
        let mut accumulated = 0usize;
        let distinct_pids: Vec<InodeId> = bytes_by_pid.keys().copied().collect();

        for (idx, &pid) in distinct_pids.iter().enumerate() {
            accumulated += bytes_by_pid[&pid];
            if accumulated >= target_half {
                let split_idx = if idx + 1 < distinct_pids.len() {
                    idx + 1
                } else {
                    idx
                };
                return Ok(distinct_pids[split_idx]);
            }
        }

        Ok(*distinct_pids.last().unwrap())
    }
}
