//! Why this repository does not carry RDMA, and the work still required.

/// Compile-time implementation status. False means this binary has no RDMA backend.
///
/// Do not read this as "no device was found at runtime". This crate does not
/// open `/dev/infiniband` and does not load Soft-RoCE, `siw`, or a provider plugin.
pub const RDMA_BACKEND_IMPLEMENTED: bool = false;

/// One piece of backend work that this increment does not do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendWorkItem {
    /// Stable identifier. Tests and the design note use this id.
    pub id: &'static str,
    /// What remains before that item can be called done.
    pub summary: &'static str,
}

/// Tracked work that would have to land before this crate could select RDMA.
///
/// The list is the tracking record for this increment. It is not a claim that
/// the work is scheduled or that a stub already covers it.
pub fn missing_rdma_backend_work() -> &'static [BackendWorkItem] {
    &[
        BackendWorkItem {
            id: "ffi-policy",
            summary: "Architecture.md forbids C/C++ FFI. libibverbs, librdmacm, UCX, and libfabric are FFI. No exception is granted in this crate.",
        },
        BackendWorkItem {
            id: "license-review",
            summary: "cargo-deny allows Apache-2.0, MIT, BSD-3-Clause, Unicode-3.0, and Zlib. async-rdma is GPL-3.0, sideway is MPL-2.0, and rdma-core is GPL-2.0 OR BSD-2-Clause. Those licenses are not allow-listed, and this crate adds no exception.",
        },
        BackendWorkItem {
            id: "hardware-validation",
            summary: "No InfiniBand, RoCE, iWARP, or Soft-RoCE device is opened or provisioned. Soft-RoCE is not a validated backend and is not TCP renamed to RDMA.",
        },
        BackendWorkItem {
            id: "queue-pair-lifecycle",
            summary: "No protection domain, queue pair, completion queue, or memory region is created. Configured ceilings are not allocations. Allocation stays zero.",
        },
        BackendWorkItem {
            id: "authorization-binding",
            summary: "The process bearer remains the authorization boundary. A future RDMA session must enforce that same decision before posting work and must not be a second auth domain.",
        },
        BackendWorkItem {
            id: "cancellation-and-deadlines",
            summary: "A future backend must drop every queue pair and memory registration when an attempt is cancelled or its deadline passes. This crate allocates nothing to leak.",
        },
        BackendWorkItem {
            id: "wire-negotiation",
            summary: "No protobuf or gRPC field advertises RDMA. Startup does not run a network handshake. A future handshake must call this contract and must not advertise RDMA while the backend constant is false.",
        },
        BackendWorkItem {
            id: "openraft-integration",
            summary: "Raft replication stays on TonicRaftNetworkFactory. This crate is not an openraft network implementation.",
        },
        BackendWorkItem {
            id: "idempotency-review",
            summary: "2PC, Raft proposal identity, and on-disk formats are unchanged. Negotiation retry classes must not be wired into command replay.",
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_constant_is_false_and_work_ids_are_stable() {
        const { assert!(!RDMA_BACKEND_IMPLEMENTED) };
        let ids: Vec<&str> = missing_rdma_backend_work()
            .iter()
            .map(|item| item.id)
            .collect();
        assert_eq!(
            ids,
            vec![
                "ffi-policy",
                "license-review",
                "hardware-validation",
                "queue-pair-lifecycle",
                "authorization-binding",
                "cancellation-and-deadlines",
                "wire-negotiation",
                "openraft-integration",
                "idempotency-review",
            ]
        );
        for item in missing_rdma_backend_work() {
            assert!(!item.summary.is_empty(), "{}", item.id);
        }
    }
}
