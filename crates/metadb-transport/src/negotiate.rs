//! In-process transport negotiation.
//!
//! This is the contract a future handshake would call. It does not send bytes,
//! open a device, or register memory. Startup uses it locally. No protobuf
//! field carries the result.

use std::fmt;

use crate::policy::{
    CarriedTransport, RdmaRejection, ResourceAllocation, RetryClass, TransportMode, TransportName,
    TransportPolicy,
};

/// Authorization boundary for a negotiation attempt.
///
/// RDMA is not an authentication domain. The process bearer, owned by the
/// server security configuration, remains the decision for every byte this
/// process accepts. Production startup passes [`AuthBoundary::enforced`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthBoundary {
    rdma_is_separate_auth_domain: bool,
    unauthenticated_bypass: bool,
}

impl AuthBoundary {
    pub const fn enforced() -> Self {
        Self {
            rdma_is_separate_auth_domain: false,
            unauthenticated_bypass: false,
        }
    }

    /// Contract input that must fail closed. Configuration cannot build this.
    pub const fn separate_rdma_domain() -> Self {
        Self {
            rdma_is_separate_auth_domain: true,
            unauthenticated_bypass: false,
        }
    }

    /// Contract input that must fail closed. Configuration cannot build this.
    pub const fn unauthenticated_bypass() -> Self {
        Self {
            rdma_is_separate_auth_domain: false,
            unauthenticated_bypass: true,
        }
    }

    const fn violation(self) -> Option<&'static str> {
        if self.unauthenticated_bypass {
            Some(
                "authentication boundary violated: an unauthenticated bypass is refused. RDMA is not a way around the process bearer",
            )
        } else if self.rdma_is_separate_auth_domain {
            Some(
                "authentication boundary violated: RDMA is not a separate authorization domain. The process bearer still applies",
            )
        } else {
            None
        }
    }
}

/// One negotiation attempt. Times are caller-supplied milliseconds on one clock.
///
/// The library does not spawn a timer and does not read the system clock.
/// `now_ms == deadline_ms` is still inside the deadline. A later millisecond
/// selects nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NegotiationAttempt {
    pub now_ms: u64,
    pub deadline_ms: u64,
    pub cancelled: bool,
    pub inflight_negotiations: u32,
    pub auth: AuthBoundary,
}

impl NegotiationAttempt {
    /// A local startup attempt that has not spent its budget and is not cancelled.
    pub const fn startup(timeout_ms: u64) -> Self {
        Self {
            now_ms: 0,
            deadline_ms: timeout_ms,
            cancelled: false,
            inflight_negotiations: 1,
            auth: AuthBoundary::enforced(),
        }
    }
}

/// Names a peer included in its offer.
///
/// Only [`TransportName::TcpGrpc`] can be selected. A peer that lists RDMA is
/// naming a transport this binary will not carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerOffer {
    names: Vec<TransportName>,
}

impl PeerOffer {
    pub fn from_names(names: impl IntoIterator<Item = TransportName>) -> Self {
        Self {
            names: names.into_iter().collect(),
        }
    }

    pub fn tcp_grpc() -> Self {
        Self::from_names([TransportName::TcpGrpc])
    }

    pub fn rdma_name_only() -> Self {
        Self::from_names([TransportName::Rdma])
    }

    pub fn names(&self) -> &[TransportName] {
        &self.names
    }

    pub fn offers_tcp_grpc(&self) -> bool {
        self.names
            .iter()
            .any(|name| name.selectable() == Some(CarriedTransport::TcpGrpc))
    }

    pub fn names_rdma(&self) -> bool {
        self.names
            .iter()
            .any(|name| matches!(name, TransportName::Rdma))
    }
}

/// The transport selected for a successful attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Selection {
    pub transport: CarriedTransport,
    /// The peer listed the RDMA name. It was not selected.
    pub peer_named_rdma: bool,
    pub allocation: ResourceAllocation,
}

impl Selection {
    fn tcp(peer_named_rdma: bool) -> Self {
        Self {
            transport: CarriedTransport::TcpGrpc,
            peer_named_rdma,
            allocation: ResourceAllocation::NONE,
        }
    }
}

/// Why negotiation produced no carried transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationFailure {
    AuthBoundaryViolated(&'static str),
    Cancelled,
    DeadlineExceeded {
        now_ms: u64,
        deadline_ms: u64,
    },
    ResourceBoundExceeded {
        inflight: u32,
        max_inflight: u32,
    },
    /// The operator required RDMA. TCP is available in the binary and is not used.
    RdmaRequiredUnavailable(RdmaRejection),
    /// The peer did not offer a transport this process can both carry and accept.
    NoCommonCarriedTransport,
}

impl fmt::Display for NegotiationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthBoundaryViolated(reason) => f.write_str(reason),
            Self::Cancelled => f.write_str(
                "transport negotiation cancelled before a transport was selected. No RDMA resources were allocated",
            ),
            Self::DeadlineExceeded { now_ms, deadline_ms } => write!(
                f,
                "transport negotiation deadline exceeded at {now_ms} ms (deadline {deadline_ms} ms). No transport was selected and no RDMA resources were allocated"
            ),
            Self::ResourceBoundExceeded {
                inflight,
                max_inflight,
            } => write!(
                f,
                "transport negotiation refused: {inflight} inflight attempts exceeds max_inflight_negotiations {max_inflight}. No RDMA resources were allocated"
            ),
            Self::RdmaRequiredUnavailable(rejection) => write!(
                f,
                "{rejection}. Required RDMA does not select tcp-grpc"
            ),
            Self::NoCommonCarriedTransport => f.write_str(
                "no common carried transport. RDMA was not selected, and tcp-grpc was not mutually accepted",
            ),
        }
    }
}

/// Result of one in-process negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NegotiationOutcome {
    /// TCP/gRPC was selected because it is the carried transport.
    Selected(Selection),
    /// The operator preferred RDMA. RDMA was rejected and TCP/gRPC was selected.
    ExplicitFallback {
        selection: Selection,
        rejected: RdmaRejection,
    },
    Failed {
        failure: NegotiationFailure,
        allocation: ResourceAllocation,
    },
}

impl NegotiationOutcome {
    pub fn selected_transport(&self) -> Option<CarriedTransport> {
        match self {
            Self::Selected(selection) | Self::ExplicitFallback { selection, .. } => {
                Some(selection.transport)
            }
            Self::Failed { .. } => None,
        }
    }

    pub fn allocation(&self) -> ResourceAllocation {
        match self {
            Self::Selected(selection) | Self::ExplicitFallback { selection, .. } => {
                selection.allocation
            }
            Self::Failed { allocation, .. } => *allocation,
        }
    }

    pub fn retry_class(&self) -> RetryClass {
        match self {
            Self::Selected(_) | Self::ExplicitFallback { .. } => {
                RetryClass::UnchangedTcpGrpcIdempotency
            }
            Self::Failed { failure, .. } => match failure {
                NegotiationFailure::Cancelled
                | NegotiationFailure::DeadlineExceeded { .. }
                | NegotiationFailure::ResourceBoundExceeded { .. } => {
                    RetryClass::RetryNegotiationOnly
                }
                NegotiationFailure::AuthBoundaryViolated(_)
                | NegotiationFailure::RdmaRequiredUnavailable(_)
                | NegotiationFailure::NoCommonCarriedTransport => {
                    RetryClass::NotRetriableConfiguration
                }
            },
        }
    }

    fn failed(failure: NegotiationFailure) -> Self {
        Self::Failed {
            failure,
            allocation: ResourceAllocation::NONE,
        }
    }
}

/// Negotiates `local` against `peer` for one attempt.
///
/// Check order:
/// 1. Authentication boundary. A bypass never becomes a selected transport.
/// 2. Required RDMA. The operator refused TCP, so an expired deadline must not
///    hide that configuration error, and TCP must not be selected.
/// 3. Cancellation.
/// 4. Deadline (`now_ms > deadline_ms`).
/// 5. In-flight ceiling.
/// 6. Intersection of carried transports the peer offered and this policy accepts.
/// 7. Prefer-RDMA records an explicit TCP/gRPC fallback. Any other mutual TCP
///    selection is plain TCP/gRPC.
///
/// Every arm sets allocation to [`ResourceAllocation::NONE`].
#[must_use]
pub fn negotiate(
    local: &TransportPolicy,
    peer: &PeerOffer,
    attempt: &NegotiationAttempt,
) -> NegotiationOutcome {
    if let Some(reason) = attempt.auth.violation() {
        return NegotiationOutcome::failed(NegotiationFailure::AuthBoundaryViolated(reason));
    }
    if matches!(local.mode(), TransportMode::RdmaRequired) {
        return NegotiationOutcome::failed(NegotiationFailure::RdmaRequiredUnavailable(
            RdmaRejection::BACKEND_NOT_IMPLEMENTED,
        ));
    }
    if attempt.cancelled {
        return NegotiationOutcome::failed(NegotiationFailure::Cancelled);
    }
    if attempt.now_ms > attempt.deadline_ms {
        return NegotiationOutcome::failed(NegotiationFailure::DeadlineExceeded {
            now_ms: attempt.now_ms,
            deadline_ms: attempt.deadline_ms,
        });
    }
    let max_inflight = local.bounds().max_inflight_negotiations;
    if attempt.inflight_negotiations > max_inflight {
        return NegotiationOutcome::failed(NegotiationFailure::ResourceBoundExceeded {
            inflight: attempt.inflight_negotiations,
            max_inflight,
        });
    }
    if !peer.offers_tcp_grpc() || !local.advertisement().accepts_tcp_grpc {
        return NegotiationOutcome::failed(NegotiationFailure::NoCommonCarriedTransport);
    }
    let selection = Selection::tcp(peer.names_rdma());
    if matches!(local.mode(), TransportMode::PreferRdma) {
        NegotiationOutcome::ExplicitFallback {
            selection,
            rejected: RdmaRejection::BACKEND_NOT_IMPLEMENTED,
        }
    } else {
        NegotiationOutcome::Selected(selection)
    }
}

/// Negotiates two local policies.
///
/// The remote offer comes from [`TransportPolicy::as_peer_offer`], so a
/// prefer-RDMA peer does not advertise an RDMA capability.
#[must_use]
pub fn negotiate_policies(
    local: &TransportPolicy,
    remote: &TransportPolicy,
    attempt: &NegotiationAttempt,
) -> NegotiationOutcome {
    local.negotiate(&remote.as_peer_offer(), attempt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{PolicyFields, RetryClass, TransportMode, TransportPolicy};

    fn policy(mode: &str) -> TransportPolicy {
        TransportPolicy::try_from_fields(PolicyFields::from_mode(mode)).unwrap()
    }

    fn attempt() -> NegotiationAttempt {
        NegotiationAttempt::startup(1_000)
    }

    #[test]
    fn default_peers_select_tcp_and_allocate_nothing() {
        let outcome = negotiate_policies(&policy("tcp-grpc"), &policy("tcp-grpc"), &attempt());
        match outcome {
            NegotiationOutcome::Selected(selection) => {
                assert_eq!(selection.transport, CarriedTransport::TcpGrpc);
                assert!(!selection.peer_named_rdma);
                assert!(selection.allocation.is_empty());
            }
            other => panic!("expected tcp selection, got {other:?}"),
        }
        assert_eq!(
            outcome.retry_class(),
            RetryClass::UnchangedTcpGrpcIdempotency
        );
    }

    #[test]
    fn required_rdma_never_selects_tcp() {
        let local = policy("rdma");
        let outcome = negotiate(
            &local,
            &PeerOffer::from_names([TransportName::Rdma, TransportName::TcpGrpc]),
            &NegotiationAttempt {
                now_ms: 50,
                deadline_ms: 1,
                cancelled: true,
                inflight_negotiations: u32::MAX,
                auth: AuthBoundary::enforced(),
            },
        );
        assert!(matches!(
            outcome,
            NegotiationOutcome::Failed {
                failure: NegotiationFailure::RdmaRequiredUnavailable(
                    RdmaRejection::BACKEND_NOT_IMPLEMENTED
                ),
                allocation: ResourceAllocation::NONE,
            }
        ));
        assert_eq!(outcome.selected_transport(), None);
        assert_eq!(outcome.retry_class(), RetryClass::NotRetriableConfiguration);
        assert_eq!(local.mode(), TransportMode::RdmaRequired);
    }

    #[test]
    fn prefer_rdma_records_tcp_fallback_without_claiming_rdma() {
        let outcome = negotiate(
            &policy("prefer-rdma"),
            &PeerOffer::from_names([TransportName::Rdma, TransportName::TcpGrpc]),
            &attempt(),
        );
        match outcome {
            NegotiationOutcome::ExplicitFallback {
                selection,
                rejected,
            } => {
                assert_eq!(selection.transport, CarriedTransport::TcpGrpc);
                assert!(selection.peer_named_rdma);
                assert!(selection.allocation.is_empty());
                assert_eq!(rejected, RdmaRejection::BACKEND_NOT_IMPLEMENTED);
            }
            other => panic!("expected explicit fallback, got {other:?}"),
        }
        assert_eq!(
            outcome.retry_class(),
            RetryClass::UnchangedTcpGrpcIdempotency
        );
    }

    #[test]
    fn two_prefer_rdma_policies_fall_back_and_do_not_offer_rdma() {
        let left = policy("prefer-rdma");
        let right = policy("prefer-rdma");
        assert!(!left.as_peer_offer().names_rdma());
        let outcome = negotiate_policies(&left, &right, &attempt());
        match outcome {
            NegotiationOutcome::ExplicitFallback { selection, .. } => {
                assert_eq!(selection.transport, CarriedTransport::TcpGrpc);
                assert!(!selection.peer_named_rdma);
                assert!(selection.allocation.is_empty());
            }
            other => panic!("expected fallback between two prefer-rdma policies, got {other:?}"),
        }
    }

    #[test]
    fn rdma_only_peer_has_no_common_transport() {
        for mode in ["tcp-grpc", "prefer-rdma"] {
            let outcome = negotiate(&policy(mode), &PeerOffer::rdma_name_only(), &attempt());
            assert!(
                matches!(
                    outcome,
                    NegotiationOutcome::Failed {
                        failure: NegotiationFailure::NoCommonCarriedTransport,
                        allocation: ResourceAllocation::NONE,
                    }
                ),
                "{mode}: {outcome:?}"
            );
            assert_eq!(outcome.selected_transport(), None);
            assert_eq!(outcome.retry_class(), RetryClass::NotRetriableConfiguration);
        }
        let remote_required = policy("rdma");
        let outcome = negotiate_policies(&policy("tcp-grpc"), &remote_required, &attempt());
        assert!(matches!(
            outcome,
            NegotiationOutcome::Failed {
                failure: NegotiationFailure::NoCommonCarriedTransport,
                ..
            }
        ));
    }

    #[test]
    fn tcp_policy_ignores_a_peer_rdma_name() {
        let outcome = negotiate(
            &policy("tcp-grpc"),
            &PeerOffer::from_names([TransportName::TcpGrpc, TransportName::Rdma]),
            &attempt(),
        );
        match outcome {
            NegotiationOutcome::Selected(selection) => {
                assert_eq!(selection.transport, CarriedTransport::TcpGrpc);
                assert!(selection.peer_named_rdma);
                assert!(selection.allocation.is_empty());
            }
            other => panic!("expected plain tcp selection, got {other:?}"),
        }
    }

    #[test]
    fn auth_cancel_deadline_and_inflight_select_nothing() {
        let local = policy("prefer-rdma");
        let peer = PeerOffer::tcp_grpc();

        let required_bypass = negotiate(
            &policy("rdma"),
            &peer,
            &NegotiationAttempt {
                auth: AuthBoundary::unauthenticated_bypass(),
                ..attempt()
            },
        );
        assert!(matches!(
            required_bypass,
            NegotiationOutcome::Failed {
                failure: NegotiationFailure::AuthBoundaryViolated(_),
                allocation: ResourceAllocation::NONE,
            }
        ));
        assert_eq!(required_bypass.selected_transport(), None);

        let bypass = negotiate(
            &local,
            &peer,
            &NegotiationAttempt {
                auth: AuthBoundary::unauthenticated_bypass(),
                ..attempt()
            },
        );
        assert!(matches!(
            bypass,
            NegotiationOutcome::Failed {
                failure: NegotiationFailure::AuthBoundaryViolated(_),
                allocation: ResourceAllocation::NONE,
            }
        ));
        assert_eq!(bypass.retry_class(), RetryClass::NotRetriableConfiguration);

        let separate = negotiate(
            &local,
            &peer,
            &NegotiationAttempt {
                auth: AuthBoundary::separate_rdma_domain(),
                ..attempt()
            },
        );
        assert!(matches!(
            separate,
            NegotiationOutcome::Failed {
                failure: NegotiationFailure::AuthBoundaryViolated(_),
                ..
            }
        ));

        let cancelled = negotiate(
            &local,
            &peer,
            &NegotiationAttempt {
                cancelled: true,
                ..attempt()
            },
        );
        assert!(matches!(
            cancelled,
            NegotiationOutcome::Failed {
                failure: NegotiationFailure::Cancelled,
                allocation: ResourceAllocation::NONE,
            }
        ));
        assert_eq!(cancelled.retry_class(), RetryClass::RetryNegotiationOnly);
        assert_eq!(cancelled.selected_transport(), None);

        let on_time = negotiate(
            &local,
            &peer,
            &NegotiationAttempt {
                now_ms: 1_000,
                deadline_ms: 1_000,
                ..attempt()
            },
        );
        assert!(matches!(
            on_time,
            NegotiationOutcome::ExplicitFallback { .. }
        ));
        assert!(on_time.allocation().is_empty());

        let late = negotiate(
            &local,
            &peer,
            &NegotiationAttempt {
                now_ms: 1_001,
                deadline_ms: 1_000,
                ..attempt()
            },
        );
        assert!(matches!(
            late,
            NegotiationOutcome::Failed {
                failure: NegotiationFailure::DeadlineExceeded {
                    now_ms: 1_001,
                    deadline_ms: 1_000,
                },
                allocation: ResourceAllocation::NONE,
            }
        ));
        assert_eq!(late.retry_class(), RetryClass::RetryNegotiationOnly);

        let crowded = negotiate(
            &local,
            &peer,
            &NegotiationAttempt {
                inflight_negotiations: local.bounds().max_inflight_negotiations + 1,
                ..attempt()
            },
        );
        assert!(matches!(
            crowded,
            NegotiationOutcome::Failed {
                failure: NegotiationFailure::ResourceBoundExceeded { .. },
                allocation: ResourceAllocation::NONE,
            }
        ));
        assert_eq!(crowded.retry_class(), RetryClass::RetryNegotiationOnly);

        let at_cap = negotiate(
            &local,
            &peer,
            &NegotiationAttempt {
                inflight_negotiations: local.bounds().max_inflight_negotiations,
                ..attempt()
            },
        );
        assert!(matches!(
            at_cap,
            NegotiationOutcome::ExplicitFallback { .. }
        ));
        assert!(at_cap.allocation().is_empty());
    }

    #[test]
    fn empty_peer_offer_selects_nothing() {
        let outcome = negotiate(&policy("tcp-grpc"), &PeerOffer::from_names([]), &attempt());
        assert!(matches!(
            outcome,
            NegotiationOutcome::Failed {
                failure: NegotiationFailure::NoCommonCarriedTransport,
                allocation: ResourceAllocation::NONE,
            }
        ));
    }
}
