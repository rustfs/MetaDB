//! Operator policy: which transport may be selected, and which requests are refused.

use std::fmt;

use crate::backend::RDMA_BACKEND_IMPLEMENTED;
use crate::negotiate::{negotiate, NegotiationAttempt, NegotiationFailure, NegotiationOutcome};

/// Keys accepted under the `[transport]` table.
///
/// `metadb-server` rejects every other key. Device, library, provider, and
/// authentication-bypass keys are not in this list.
pub const TRANSPORT_CONFIG_KEYS: &[&str] = &[
    "mode",
    "negotiation_timeout_ms",
    "max_inflight_negotiations",
    "max_queue_pairs",
    "max_registered_bytes",
    "max_message_bytes",
];

pub const DEFAULT_NEGOTIATION_TIMEOUT_MS: u64 = 1_000;
pub const MAX_NEGOTIATION_TIMEOUT_MS: u64 = 60_000;
pub const DEFAULT_MAX_INFLIGHT_NEGOTIATIONS: u32 = 64;
pub const MAX_INFLIGHT_NEGOTIATIONS: u32 = 4_096;
pub const DEFAULT_MAX_QUEUE_PAIRS: u32 = 16;
pub const MAX_QUEUE_PAIRS_CEILING: u32 = 1_024;
pub const DEFAULT_MAX_REGISTERED_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_REGISTERED_BYTES_CEILING: u64 = 64 * 1024 * 1024;
pub const DEFAULT_MAX_MESSAGE_BYTES: u64 = 1024 * 1024;
pub const MAX_MESSAGE_BYTES_CEILING: u64 = 8 * 1024 * 1024;

/// A transport this binary can carry bytes on.
///
/// The only variant is TCP/gRPC. RDMA is not a variant: a missing arm cannot be
/// selected by a feature flag or a stub impl.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarriedTransport {
    /// tonic gRPC over TCP. This is the default and the required fallback.
    TcpGrpc,
}

impl fmt::Display for CarriedTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TcpGrpc => f.write_str("tcp-grpc"),
        }
    }
}

/// A name an operator or a peer may use. [`TransportName::Rdma`] is recognized
/// so it can be rejected. Recognition is not an implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportName {
    TcpGrpc,
    Rdma,
}

impl TransportName {
    /// Maps a name onto a transport this binary can carry.
    ///
    /// RDMA maps to `None` while [`RDMA_BACKEND_IMPLEMENTED`] is false. The
    /// match is exhaustive so a new name cannot pass through unnoticed.
    pub const fn selectable(self) -> Option<CarriedTransport> {
        match self {
            Self::TcpGrpc => Some(CarriedTransport::TcpGrpc),
            Self::Rdma => None,
        }
    }
}

/// How the operator asked this process to treat RDMA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    /// Do not ask for RDMA. Carry TCP/gRPC.
    TcpGrpc,
    /// Ask for RDMA, and keep TCP/gRPC when RDMA is unavailable.
    ///
    /// The fallback is recorded. It is not reported as an RDMA session.
    PreferRdma,
    /// Ask for RDMA and refuse to start on TCP/gRPC.
    RdmaRequired,
}

/// Whether this process wants RDMA. Wanting it is not the same as offering it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaDesire {
    Absent,
    Preferred,
    Required,
}

/// Why a request to carry RDMA was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RdmaRejection {
    pub code: RdmaRejectionCode,
}

impl RdmaRejection {
    pub const BACKEND_NOT_IMPLEMENTED: Self = Self {
        code: RdmaRejectionCode::BackendNotImplemented,
    };
}

impl fmt::Display for RdmaRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RDMA backend is not implemented ({})",
            self.code.as_str()
        )
    }
}

/// Stable rejection codes. Device-not-found is intentionally absent: this
/// crate does not probe hardware, so it must not report a probe result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaRejectionCode {
    BackendNotImplemented,
}

impl RdmaRejectionCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BackendNotImplemented => "backend-not-implemented",
        }
    }
}

/// Ceilings a future backend would have to honor.
///
/// Values are policy. [`ResourceAllocation::NONE`] is what this process does
/// with them today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceBounds {
    pub negotiation_timeout_ms: u64,
    pub max_inflight_negotiations: u32,
    pub max_queue_pairs: u32,
    pub max_registered_bytes: u64,
    pub max_message_bytes: u64,
}

impl Default for ResourceBounds {
    fn default() -> Self {
        Self {
            negotiation_timeout_ms: DEFAULT_NEGOTIATION_TIMEOUT_MS,
            max_inflight_negotiations: DEFAULT_MAX_INFLIGHT_NEGOTIATIONS,
            max_queue_pairs: DEFAULT_MAX_QUEUE_PAIRS,
            max_registered_bytes: DEFAULT_MAX_REGISTERED_BYTES,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        }
    }
}

impl ResourceBounds {
    pub fn try_new(
        negotiation_timeout_ms: u64,
        max_inflight_negotiations: u32,
        max_queue_pairs: u32,
        max_registered_bytes: u64,
        max_message_bytes: u64,
    ) -> Result<Self, TransportError> {
        check_u64(
            "negotiation_timeout_ms",
            negotiation_timeout_ms,
            1,
            MAX_NEGOTIATION_TIMEOUT_MS,
        )?;
        check_u64(
            "max_inflight_negotiations",
            u64::from(max_inflight_negotiations),
            1,
            u64::from(MAX_INFLIGHT_NEGOTIATIONS),
        )?;
        check_u64(
            "max_queue_pairs",
            u64::from(max_queue_pairs),
            1,
            u64::from(MAX_QUEUE_PAIRS_CEILING),
        )?;
        check_u64(
            "max_registered_bytes",
            max_registered_bytes,
            1,
            MAX_REGISTERED_BYTES_CEILING,
        )?;
        check_u64(
            "max_message_bytes",
            max_message_bytes,
            1,
            MAX_MESSAGE_BYTES_CEILING,
        )?;
        Ok(Self {
            negotiation_timeout_ms,
            max_inflight_negotiations,
            max_queue_pairs,
            max_registered_bytes,
            max_message_bytes,
        })
    }
}

/// Device resources this process actually created.
///
/// The only constructor result this crate produces is [`NONE`](Self::NONE).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceAllocation {
    pub queue_pairs: u32,
    pub registered_bytes: u64,
    pub messages_posted: u64,
}

impl ResourceAllocation {
    pub const NONE: Self = Self {
        queue_pairs: 0,
        registered_bytes: 0,
        messages_posted: 0,
    };

    pub const fn is_empty(self) -> bool {
        self.queue_pairs == 0 && self.registered_bytes == 0 && self.messages_posted == 0
    }
}

/// What a caller may retry after a negotiation outcome.
///
/// These classes describe the negotiation attempt. They do not change Raft
/// proposal identity or 2PC replay. No RDMA send exists to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    /// The same configuration fails again. Do not replay a command.
    NotRetriableConfiguration,
    /// The attempt ended before a transport was selected and before any
    /// command bytes were submitted. Repeating the negotiation is not a
    /// command replay.
    RetryNegotiationOnly,
    /// TCP/gRPC was selected. Command retry stays on the existing gRPC path
    /// and its existing idempotency rules.
    UnchangedTcpGrpcIdempotency,
}

/// What this process tells a peer it can do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalAdvertisement {
    /// Always TCP/gRPC. Never an RDMA transport.
    pub implemented: &'static [CarriedTransport],
    /// Compile-time fact copied from [`RDMA_BACKEND_IMPLEMENTED`].
    pub rdma_implemented: bool,
    pub rdma_desire: RdmaDesire,
    /// False when the operator required RDMA. That process must not start.
    pub accepts_tcp_grpc: bool,
}

/// Parsed operator policy after bounds checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportPolicy {
    mode: TransportMode,
    bounds: ResourceBounds,
}

impl TransportPolicy {
    pub fn try_from_fields(fields: PolicyFields<'_>) -> Result<Self, TransportError> {
        let mode = parse_mode(fields.mode)?;
        let bounds = ResourceBounds::try_new(
            fields.negotiation_timeout_ms,
            fields.max_inflight_negotiations,
            fields.max_queue_pairs,
            fields.max_registered_bytes,
            fields.max_message_bytes,
        )?;
        Ok(Self { mode, bounds })
    }

    pub fn tcp_grpc() -> Self {
        Self {
            mode: TransportMode::TcpGrpc,
            bounds: ResourceBounds::default(),
        }
    }

    pub fn mode(&self) -> TransportMode {
        self.mode
    }

    pub fn bounds(&self) -> ResourceBounds {
        self.bounds
    }

    pub fn desire(&self) -> RdmaDesire {
        match self.mode {
            TransportMode::TcpGrpc => RdmaDesire::Absent,
            TransportMode::PreferRdma => RdmaDesire::Preferred,
            TransportMode::RdmaRequired => RdmaDesire::Required,
        }
    }

    /// Advertisement this process is allowed to make.
    ///
    /// `rdma_implemented` is the compile-time constant. Desire is separate so
    /// a preference cannot be read as an implementation.
    pub fn advertisement(&self) -> LocalAdvertisement {
        LocalAdvertisement {
            implemented: &[CarriedTransport::TcpGrpc],
            rdma_implemented: RDMA_BACKEND_IMPLEMENTED,
            rdma_desire: self.desire(),
            accepts_tcp_grpc: !matches!(self.mode, TransportMode::RdmaRequired),
        }
    }

    /// Offer this process would hand to a peer.
    ///
    /// Prefer-RDMA offers TCP/gRPC only. Putting RDMA in that offer would let
    /// a peer treat the name as a capability. Required-RDMA offers the RDMA
    /// name and does not offer TCP, so a peer must fail the intersection
    /// instead of selecting a fallback the operator refused.
    pub fn as_peer_offer(&self) -> crate::negotiate::PeerOffer {
        match self.mode {
            TransportMode::TcpGrpc | TransportMode::PreferRdma => {
                crate::negotiate::PeerOffer::tcp_grpc()
            }
            TransportMode::RdmaRequired => crate::negotiate::PeerOffer::rdma_name_only(),
        }
    }

    pub fn negotiate(
        &self,
        peer: &crate::negotiate::PeerOffer,
        attempt: &NegotiationAttempt,
    ) -> NegotiationOutcome {
        negotiate(self, peer, attempt)
    }

    /// Startup decision for this process.
    ///
    /// There is no network handshake. The assumed peer is another process that
    /// was allowed to start, which means it accepts TCP/gRPC. A required-RDMA
    /// configuration returns an error and must not bind a listener. Prefer-RDMA
    /// returns an explicit TCP fallback.
    pub fn resolve_startup(&self) -> Result<StartupResolution, TransportError> {
        let outcome = self.negotiate(
            &Self::tcp_grpc().as_peer_offer(),
            &NegotiationAttempt::startup(self.bounds.negotiation_timeout_ms),
        );
        match outcome {
            NegotiationOutcome::Failed { failure, .. } => match failure {
                NegotiationFailure::RdmaRequiredUnavailable(rejection) => {
                    Err(TransportError::RdmaRequired(rejection))
                }
                other => Err(TransportError::Negotiation(other)),
            },
            selected => Ok(StartupResolution {
                mode: self.mode,
                outcome: selected,
            }),
        }
    }
}

/// Startup result. `outcome` is either TCP/gRPC or an explicit TCP fallback.
/// It is never an RDMA selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupResolution {
    pub mode: TransportMode,
    pub outcome: NegotiationOutcome,
}

impl StartupResolution {
    pub fn explicit_rdma_fallback(&self) -> bool {
        matches!(self.outcome, NegotiationOutcome::ExplicitFallback { .. })
    }

    pub fn selected(&self) -> Option<CarriedTransport> {
        self.outcome.selected_transport()
    }
}

/// Fields read from configuration. Strings stay owned by the caller.
#[derive(Debug, Clone, Copy)]
pub struct PolicyFields<'a> {
    pub mode: &'a str,
    pub negotiation_timeout_ms: u64,
    pub max_inflight_negotiations: u32,
    pub max_queue_pairs: u32,
    pub max_registered_bytes: u64,
    pub max_message_bytes: u64,
}

impl<'a> PolicyFields<'a> {
    pub fn from_mode(mode: &'a str) -> Self {
        let bounds = ResourceBounds::default();
        Self {
            mode,
            negotiation_timeout_ms: bounds.negotiation_timeout_ms,
            max_inflight_negotiations: bounds.max_inflight_negotiations,
            max_queue_pairs: bounds.max_queue_pairs,
            max_registered_bytes: bounds.max_registered_bytes,
            max_message_bytes: bounds.max_message_bytes,
        }
    }
}

/// Configuration and negotiation errors for this crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    UnrecognizedMode(String),
    InvalidBound {
        field: &'static str,
        min: u64,
        max: u64,
        got: u64,
    },
    RejectedKey(String),
    RdmaRequired(RdmaRejection),
    Negotiation(NegotiationFailure),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnrecognizedMode(mode) => write!(
                f,
                "unrecognized transport mode `{mode}`. Accepted values are `tcp-grpc`, `prefer-rdma`, and `rdma`. `rdma` is recognized and rejected because no RDMA backend is implemented. TCP is not accepted as an alias and is not RDMA"
            ),
            Self::InvalidBound {
                field,
                min,
                max,
                got,
            } => write!(
                f,
                "{field} must be in {min}..={max}, got {got}. Zero is not unlimited, and this process allocates no RDMA resources"
            ),
            Self::RejectedKey(message) => f.write_str(message),
            Self::RdmaRequired(rejection) => write!(
                f,
                "{rejection}. Refusing to start: mode `rdma` does not fall back to tcp-grpc, and no listener is opened. The carried transport remains tcp-grpc for configurations that do not require RDMA. Missing backend work is tracked by missing_rdma_backend_work()"
            ),
            Self::Negotiation(failure) => write!(f, "{failure}"),
        }
    }
}

impl std::error::Error for TransportError {}

/// Parses a transport mode.
///
/// RDMA synonyms are the required-RDMA mode. They are not separate backends
/// and they are not TCP.
pub fn parse_mode(raw: &str) -> Result<TransportMode, TransportError> {
    match raw.trim() {
        "tcp-grpc" => Ok(TransportMode::TcpGrpc),
        "prefer-rdma" => Ok(TransportMode::PreferRdma),
        "rdma" | "roce" | "rocev2" | "infiniband" | "ib" | "ibverbs" | "verbs" => {
            Ok(TransportMode::RdmaRequired)
        }
        other => Err(TransportError::UnrecognizedMode(other.to_string())),
    }
}

/// Rejects a `[transport]` key this increment does not implement.
pub fn check_transport_key(key: &str) -> Result<(), TransportError> {
    if TRANSPORT_CONFIG_KEYS.contains(&key) {
        return Ok(());
    }
    let norm = key.to_ascii_lowercase().replace('-', "_");
    if is_auth_bypass_key(&norm) {
        return Err(TransportError::RejectedKey(format!(
            "refusing transport key `{key}`: it would create an unauthenticated or unverified transport. RDMA is not an authorization bypass, and this process will not open a second listener"
        )));
    }
    Err(TransportError::RejectedKey(format!(
        "refusing transport key `{key}`: [transport] only accepts {}. Device, library, and provider keys are rejected because no RDMA backend is implemented",
        TRANSPORT_CONFIG_KEYS.join(", ")
    )))
}

fn is_auth_bypass_key(norm: &str) -> bool {
    const NEEDLES: &[&str] = &[
        "insecure",
        "no_auth",
        "disable_auth",
        "anonymous",
        "plaintext",
        "skip_auth",
        "unauthenticated",
        "bypass",
        "no_verify",
        "trust_rdma",
        "allow_insecure",
        "disable_authorization",
        "without_auth",
    ];
    NEEDLES.iter().any(|needle| norm.contains(needle))
}

fn check_u64(field: &'static str, got: u64, min: u64, max: u64) -> Result<(), TransportError> {
    if got < min || got > max {
        Err(TransportError::InvalidBound {
            field,
            min,
            max,
            got,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_parser_keeps_tcp_and_rdma_apart() {
        assert_eq!(parse_mode("tcp-grpc").unwrap(), TransportMode::TcpGrpc);
        assert_eq!(
            parse_mode(" prefer-rdma ").unwrap(),
            TransportMode::PreferRdma
        );
        for synonym in [
            "rdma",
            "roce",
            "rocev2",
            "infiniband",
            "ib",
            "ibverbs",
            "verbs",
        ] {
            assert_eq!(parse_mode(synonym).unwrap(), TransportMode::RdmaRequired);
        }
        for rejected in ["tcp", "grpc", "RDMA", "prefer_rdma", "soft-roce", ""] {
            let err = parse_mode(rejected).unwrap_err();
            assert!(
                err.to_string().contains("unrecognized transport mode"),
                "{rejected}: {err}"
            );
        }
    }

    #[test]
    fn bounds_reject_zero_and_values_above_the_ceiling() {
        assert!(ResourceBounds::try_new(0, 1, 1, 1, 1).is_err());
        assert!(ResourceBounds::try_new(1, 0, 1, 1, 1).is_err());
        assert!(ResourceBounds::try_new(1, 1, 0, 1, 1).is_err());
        assert!(ResourceBounds::try_new(1, 1, 1, 0, 1).is_err());
        assert!(ResourceBounds::try_new(1, 1, 1, 1, 0).is_err());
        let over = ResourceBounds::try_new(
            MAX_NEGOTIATION_TIMEOUT_MS + 1,
            MAX_INFLIGHT_NEGOTIATIONS,
            MAX_QUEUE_PAIRS_CEILING,
            MAX_REGISTERED_BYTES_CEILING,
            MAX_MESSAGE_BYTES_CEILING,
        );
        assert!(over
            .unwrap_err()
            .to_string()
            .contains("negotiation_timeout_ms"));
        let ok = ResourceBounds::try_new(
            MAX_NEGOTIATION_TIMEOUT_MS,
            MAX_INFLIGHT_NEGOTIATIONS,
            MAX_QUEUE_PAIRS_CEILING,
            MAX_REGISTERED_BYTES_CEILING,
            MAX_MESSAGE_BYTES_CEILING,
        );
        assert!(ok.is_ok());
    }

    #[test]
    fn rdma_name_is_not_a_selectable_transport() {
        assert_eq!(
            TransportName::TcpGrpc.selectable(),
            Some(CarriedTransport::TcpGrpc)
        );
        assert_eq!(TransportName::Rdma.selectable(), None);
        const { assert!(!crate::RDMA_BACKEND_IMPLEMENTED) };
    }

    #[test]
    fn advertisement_never_claims_an_rdma_implementation() {
        for mode in ["tcp-grpc", "prefer-rdma", "rdma"] {
            let policy = TransportPolicy::try_from_fields(PolicyFields::from_mode(mode)).unwrap();
            let ad = policy.advertisement();
            assert_eq!(ad.implemented, &[CarriedTransport::TcpGrpc]);
            assert!(!ad.rdma_implemented);
            assert!(ad
                .implemented
                .iter()
                .all(|item| *item == CarriedTransport::TcpGrpc));
        }
        let required = TransportPolicy::try_from_fields(PolicyFields::from_mode("rdma")).unwrap();
        assert!(!required.advertisement().accepts_tcp_grpc);
        assert!(required.as_peer_offer().names_rdma());
        assert!(!required.as_peer_offer().offers_tcp_grpc());
        let preferred =
            TransportPolicy::try_from_fields(PolicyFields::from_mode("prefer-rdma")).unwrap();
        assert!(preferred.advertisement().accepts_tcp_grpc);
        assert!(!preferred.as_peer_offer().names_rdma());
        assert!(preferred.as_peer_offer().offers_tcp_grpc());
    }

    #[test]
    fn startup_rejects_required_rdma_and_records_prefer_fallback() {
        let required = TransportPolicy::try_from_fields(PolicyFields::from_mode("ib")).unwrap();
        let err = required.resolve_startup().unwrap_err();
        let text = err.to_string();
        assert!(text.contains("RDMA backend is not implemented"), "{text}");
        assert!(text.contains("backend-not-implemented"), "{text}");
        assert!(text.contains("does not fall back"), "{text}");
        assert!(!crate::missing_rdma_backend_work().is_empty());

        let preferred =
            TransportPolicy::try_from_fields(PolicyFields::from_mode("prefer-rdma")).unwrap();
        let started = preferred.resolve_startup().unwrap();
        assert!(started.explicit_rdma_fallback());
        assert_eq!(started.selected(), Some(CarriedTransport::TcpGrpc));
        assert!(started.outcome.allocation().is_empty());
        assert_eq!(
            started.outcome.retry_class(),
            RetryClass::UnchangedTcpGrpcIdempotency
        );

        let tcp = TransportPolicy::tcp_grpc().resolve_startup().unwrap();
        assert!(!tcp.explicit_rdma_fallback());
        assert_eq!(tcp.selected(), Some(CarriedTransport::TcpGrpc));
        assert!(tcp.outcome.allocation().is_empty());
    }

    #[test]
    fn invalid_bounds_are_reported_before_the_rdma_rejection() {
        let mut fields = PolicyFields::from_mode("rdma");
        fields.max_registered_bytes = 0;
        let err = TransportPolicy::try_from_fields(fields).unwrap_err();
        assert!(err.to_string().contains("max_registered_bytes"), "{err}");
        assert!(!err.to_string().contains("backend-not-implemented"));
    }

    #[test]
    fn rejected_keys_do_not_select_a_backend_or_a_bypass() {
        assert!(check_transport_key("mode").is_ok());
        assert!(check_transport_key("max_queue_pairs").is_ok());
        for key in [
            "insecure",
            "anonymous",
            "no-verify",
            "trust-rdma",
            "bypass_auth",
        ] {
            let err = check_transport_key(key).unwrap_err();
            assert!(
                err.to_string().contains("not an authorization bypass"),
                "{key}: {err}"
            );
        }
        for key in ["device", "ib_dev", "provider", "libibverbs", "rdma_cm"] {
            let err = check_transport_key(key).unwrap_err();
            assert!(
                err.to_string().contains("no RDMA backend is implemented"),
                "{key}: {err}"
            );
        }
    }
}
