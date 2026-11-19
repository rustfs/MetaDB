//! Transaction and Semantic-Aware Off-Critical-Path Execution Layer (§4.2 & §5 of `arXiv:2610.32247v1`).
//!
//! Includes:
//! - [`attr_aggregator`]: Conflict-Free Asynchronous Attribute Aggregation with Read-Path Reconciliation.
//! - [`aux_sink`]: Decoupled Asynchronous Auxiliary Updates (`QuotaTable` and `SecondaryIndexTable`).
//! - [`pushdown`]: Single-Shard Transaction Logic Pushdown (`1-RTT` RMW) and Cross-Shard 2PC `Rename`.

pub mod attr_aggregator;
pub mod aux_outbox;
pub mod aux_sink;
mod leader_forward;
pub mod pushdown;
pub mod recovery_worker;
pub mod rename_cycle;
pub mod state_machine_impl;
pub mod two_pc;
mod validation;

pub use attr_aggregator::{AsyncAttrAggregator, AttrDeltaItem};
pub use aux_outbox::{ReplicatedAttrAggregator, ShardAuxOutbox, DEFAULT_OUTBOX_SPILL_THRESHOLD};
pub use aux_sink::{AsyncAuxiliarySink, AuxUpdateTask};
pub use pushdown::{PushdownExecutor, TwoPhaseCommitCoordinator};
pub use recovery_worker::{AuxDispatcher, TxnRecoveryWorker};
pub use state_machine_impl::{
    command_payload, encode_proposed_command, fresh_proposal_id, node_instance_key,
    proposal_receipt_key, NodeInstanceRecord, SetAttrPatch, TxRaftCmd, TxRaftOutput,
    TxStateMachineHandler, PROPOSAL_RECEIPT_RETENTION_MS,
};
pub use two_pc::{
    DirPartitionStat, DirRenameLease, DistributedTxRouter, RoutedShardHandle, ShardDirectory,
    TwoPcCrashStage,
};
