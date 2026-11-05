//! Transaction and Semantic-Aware Off-Critical-Path Execution Layer (§4.2 & §5 of `arXiv:2610.32247v1`).
//!
//! Includes:
//! - [`attr_aggregator`]: Conflict-Free Asynchronous Attribute Aggregation with Read-Path Reconciliation.
//! - [`aux_sink`]: Decoupled Asynchronous Auxiliary Updates (`QuotaTable` and `SecondaryIndexTable`).
//! - [`pushdown`]: Single-Shard Transaction Logic Pushdown (`1-RTT` RMW) and Cross-Shard 2PC `Rename`.

pub mod attr_aggregator;
pub mod aux_sink;
pub mod pushdown;

pub use attr_aggregator::{AttrDeltaItem, AsyncAttrAggregator};
pub use aux_sink::{AsyncAuxiliarySink, AuxUpdateTask};
pub use pushdown::{PushdownExecutor, TwoPhaseCommitCoordinator};
