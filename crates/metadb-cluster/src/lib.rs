//! Tree-Topology-Induced Sharding and Two-Tier Control Plane (§4.1 & §8 of `arXiv:2610.32247v1`).
//!
//! Includes:
//! - [`shard`]: Policy/size-based sharding on `group_key = PID`, Jumbo Shard isolation,
//!   Dual-Mode sharding fallback, and Raft membership evolution (`1-Voter` -> `3-Voter`).
//! - [`control_plane`]: Two-Tier Control Plane (`GlobalManager` + `CellLocalManager`) and
//!   `MetaCache` shard routing cache for scaling to 100B–10T+ records across 1,000+ nodes.

pub mod cell_manager;
pub mod control_plane;
pub mod control_plane_state;
pub mod global_manager;
pub mod id_allocator;
pub mod identity;
pub mod jumbo;
pub mod jumbo_placement;
pub mod meta_cache;
pub mod shard;
pub mod split_worker;

pub use cell_manager::{
    join_scale_target, plan_membership_voters, CellDirectoryRole, DurableCellManager,
    SplitPlanRecord, SplitPlanState,
};
pub use control_plane::{CellId, CellLocalManager, GlobalManager, MetaCache, ShardRoute};
pub use control_plane_state::{StoredHeartbeat, StoredPlacement, HEARTBEAT_INTERVAL_MS};
pub use global_manager::DurableGlobalManager;
pub use id_allocator::{
    AllocIdKind, DurableIdAllocator, LocalIdBuffer, CELL_DIR_AFFINITY_BLOCK_SIZE,
    CELL_FILE_ID_BLOCK_SIZE, GLOBAL_ID_BLOCK_SIZE, GLOBAL_SHARD_ID_BLOCK_SIZE,
};
pub use identity::{node_id_in_ha_range, ClusterIdentity, CLUSTER_IDENTITY_FILE};
pub use jumbo::JumboIsolationPlanner;
pub use jumbo_placement::{
    fault_tolerance, jumbo_membership_change_allowed, plan_jumbo_placement, CommittedMove,
    MembershipRefusal, PlacementAction, PlacementDecision, PlacementObservation, PlacementPhase,
    ShardVoters,
};
pub use meta_cache::{CompositeMetaCache, CompositeShardRoute};
pub use shard::{
    PolicyShardSplitter, RaftMemberRole, RaftMembership, ShardDescriptor, ShardHealthState,
    ShardSplitMode,
};
pub use split_worker::{RaftShardSplitWorker, SplitCandidate, SplitCrashPoint};
