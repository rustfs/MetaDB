//! Transport capability policy for MetaDB.
//!
//! The only carried transport is tonic gRPC over TCP, implemented by
//! `metadb-raft` and `metadb-server`. This crate does not open sockets, does
//! not speak InfiniBand or RoCE, and does not link a C or C++ library.
//!
//! [`RDMA_BACKEND_IMPLEMENTED`] is a compile-time fact. It is not a feature
//! flag, a device probe, or a simulation switch. Configuration may name RDMA
//! so the request can fail closed. Naming it does not select it.
//!
//! There is intentionally no `rdma` Cargo feature and no transport trait whose
//! implementor could be treated as an RDMA backend.

#![deny(unsafe_code)]

mod backend;
mod negotiate;
mod policy;

pub use backend::{missing_rdma_backend_work, BackendWorkItem, RDMA_BACKEND_IMPLEMENTED};
pub use negotiate::{
    negotiate, negotiate_policies, NegotiationAttempt, NegotiationFailure, NegotiationOutcome,
    PeerOffer, Selection,
};
pub use policy::{
    check_transport_key, parse_mode, CarriedTransport, LocalAdvertisement, PolicyFields,
    RdmaDesire, RdmaRejection, RdmaRejectionCode, ResourceAllocation, ResourceBounds, RetryClass,
    StartupResolution, TransportError, TransportMode, TransportName, TransportPolicy,
    DEFAULT_MAX_INFLIGHT_NEGOTIATIONS, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_QUEUE_PAIRS,
    DEFAULT_MAX_REGISTERED_BYTES, DEFAULT_NEGOTIATION_TIMEOUT_MS, TRANSPORT_CONFIG_KEYS,
};

/// `RDMA_BACKEND_IMPLEMENTED` stays false until a reviewed backend exists.
const _: () = assert!(!RDMA_BACKEND_IMPLEMENTED);
