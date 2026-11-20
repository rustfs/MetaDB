//! Tree-Topology-Induced Sharding and Two-Tier Control Plane (§4.1 & §8 of `arXiv:2610.32247v1`).
//!
//! Includes:
//! - [`shard`]: Policy/size-based sharding on `group_key = PID`, Jumbo Shard isolation,
//!   Dual-Mode sharding fallback, and Raft membership evolution (`1-Voter` -> `3-Voter`).
//! - [`control_plane`]: Two-Tier Control Plane (`GlobalManager` + `CellLocalManager`) and
//!   `MetaCache` shard routing cache for scaling to 100B–10T+ records across 1,000+ nodes.

pub mod cell_directory;
pub mod cell_manager;
pub mod control_plane;
pub mod control_plane_state;
pub mod global_manager;
pub mod heartbeat_scheduler;
pub mod id_allocator;
pub mod identity;
pub mod jumbo;
pub mod jumbo_files;
pub mod jumbo_placement;
pub mod jumbo_remote;
pub mod meta_cache;
pub mod replicated_control;
pub mod shard;
pub mod shared_id_allocator;
pub mod split_worker;

pub use cell_directory::{
    shards_covering, DirectoryCell, RemotePrecheck, RemotePublication, CELL_DIRECTORY_FILE,
};
pub use cell_manager::{
    join_scale_target, manager_nodes_from_body, manager_voters_from_body, plan_membership_voters,
    CellDirectoryRole, DurableCellManager, SplitPlanRecord, SplitPlanState,
};
pub use control_plane::{CellId, CellLocalManager, GlobalManager, MetaCache, ShardRoute};
pub use control_plane_state::{
    heartbeat_is_scheduling_truth, StoredHeartbeat, StoredPlacement, HEARTBEAT_INTERVAL_MS,
    MAX_HEARTBEAT_SEQ_ADVANCE,
};
pub use global_manager::{CellPlacement, DurableGlobalManager};
pub use heartbeat_scheduler::{
    capture_mono_ns, decide_liveness, liveness_effects, publish_interval_elapsed,
    trusted_wall_unix_ms, wall_unix_ms_is_trusted, HeartbeatScheduler, LivenessDecision,
    LivenessEffects, LivenessRefusal, MeasuredSample, MeasurementReject, ScheduleNow,
    HEARTBEAT_FRESHNESS_MS, WALL_CLOCK_FALLBACK_MS,
};
pub use id_allocator::{
    AllocIdKind, DurableIdAllocator, LocalIdBuffer, CELL_DIR_AFFINITY_BLOCK_SIZE,
    CELL_FILE_ID_BLOCK_SIZE, GLOBAL_ID_BLOCK_SIZE, GLOBAL_SHARD_ID_BLOCK_SIZE,
};
pub use identity::{node_id_in_ha_range, ClusterIdentity, CLUSTER_IDENTITY_FILE};
pub use jumbo::JumboIsolationPlanner;
pub use jumbo_placement::{
    automatic_leadership_transfer_allowed, executable_placement_actions, fault_tolerance,
    jumbo_membership_change_allowed, leadership_move_recorded, plan_jumbo_placement,
    plan_spare_voter_drain, transfer_poll_covers_election, CommittedMove, MembershipRefusal,
    PlacementAction, PlacementDecision, PlacementObservation, PlacementPhase, ShardVoters,
    SpareDrainPlan, SpareDrainRequest,
};
pub use jumbo_remote::{RemoteHandoffRecord, RemoteJumboDisk, RemoteMoveOutcome, SpareDrainRecord};
pub use meta_cache::{CompositeMetaCache, CompositeShardRoute};
pub use replicated_control::validate_replicated_record;
pub use shard::{
    PolicyShardSplitter, RaftMemberRole, RaftMembership, ShardDescriptor, ShardHealthState,
    ShardSplitMode,
};
pub use shared_id_allocator::SharedIdAllocator;
pub use split_worker::{RaftShardSplitWorker, SplitCandidate, SplitCrashPoint};
