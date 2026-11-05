//! Tree-Topology-Induced Sharding and Two-Tier Control Plane (§4.1 & §8 of `arXiv:2610.32247v1`).
//!
//! Includes:
//! - [`shard`]: Policy/size-based sharding on `group_key = PID`, Jumbo Shard isolation,
//!   Dual-Mode sharding fallback, and Raft membership evolution (`1-Voter` -> `3-Voter`).
//! - [`control_plane`]: Two-Tier Control Plane (`GlobalManager` + `CellLocalManager`) and
//!   `MetaCache` shard routing cache for scaling to 100B–10T+ records across 1,000+ nodes.

pub mod control_plane;
pub mod shard;

pub use control_plane::{CellId, CellLocalManager, GlobalManager, MetaCache, ShardRoute};
pub use shard::{
    PolicyShardSplitter, RaftMemberRole, RaftMembership, ShardDescriptor, ShardHealthState,
    ShardSplitMode,
};
