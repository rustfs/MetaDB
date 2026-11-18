//! OpenRaft Multi-Raft consensus integration for MetaDB (`consensus_and_transactions.md` §1–§5).

pub mod backup;
pub mod conf_change;
mod error_map;
pub mod group_host;
pub mod log_store;
pub mod network;
pub mod read_index;
pub mod state_machine;
pub mod types;

pub use backup::{
    restore_backup, BackupHoldRecord, BackupSummary, ShardHoldFloor, MAX_BACKUP_HOLD,
};
pub use conf_change::{ConfChangeOrchestrator, DEFAULT_LEARNER_CATCHUP_THRESHOLD_ENTRIES};
pub use group_host::{
    ProposalShutdownGate, RaftNodeOptions, RegistryShardState, ShardGroupHost, ShardRaftGroup,
};
pub use log_store::SegmentedRaftLogStore;
pub use network::{
    ChannelPool, MetaDbRaft, MultiRaftRouter, MultiRaftTransportServer, TonicRaftNetworkFactory,
};
pub use read_index::{ReadIndexBatcher, ReadToken};
pub use state_machine::{BasicKvApplyHandler, ShardApplyHandler, ShardStateMachine};
pub use types::{
    AppliedFrontier, BasicKvCommand, MetaDbRaftRequest, MetaDbRaftResponse, MetaDbTypeConfig,
    ShardWatermarkTracker, SnapshotArchive, SnapshotFileEntry,
};
