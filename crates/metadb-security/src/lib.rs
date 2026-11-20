//! Transport policy for MetaDB listeners and outbound clients.
//!
//! Successful startup is plaintext. `tls` and `mtls` are recognized and refused
//! before a listener exists. This crate does not terminate TLS, does not link a
//! TLS provider, and does not install method RBAC.
//!
//! The scale stream owns process role separation. A later authorizer must not
//! treat `[security].shared_secret` as a role. See
//! `docs/designs/transport_security.md`.

mod session;
mod spec;

pub use session::TransportSecurity;
pub use spec::{SecurityError, TlsMode, TlsSpec};

/// Certificate identity for a future authorizer.
///
/// Nothing in this tree constructs a principal from a peer certificate, because
/// TLS is not terminated. The string is not a role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    identity: String,
}

impl AuthenticatedPrincipal {
    pub fn as_str(&self) -> &str {
        &self.identity
    }

    #[cfg(test)]
    pub(crate) fn from_identity_unchecked(identity: impl Into<String>) -> Self {
        Self {
            identity: identity.into(),
        }
    }
}

impl std::fmt::Display for AuthenticatedPrincipal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.identity)
    }
}

/// Method authorization is not installed.
///
/// The server does not call this function. A shared bearer is not a role, and
/// neither is a certificate name.
pub fn authorize_method(
    _principal: &AuthenticatedPrincipal,
    _service: &str,
    _method: &str,
) -> Result<(), SecurityError> {
    Err(SecurityError::rbac_not_installed())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorize_method_refuses_until_an_authorizer_is_installed() {
        let principal =
            AuthenticatedPrincipal::from_identity_unchecked("uri:spiffe://metadb/node/1");
        let error = authorize_method(&principal, "metadb.v1.ControlPlaneService", "GetRoutes")
            .expect_err("RBAC must not be treated as installed");
        let text = error.to_string();
        assert!(text.contains("RBAC is not installed"), "{text}");
        assert!(!text.contains("shared_secret"), "{text}");
        assert!(!text.contains("Bearer"), "{text}");
    }

    #[test]
    fn off_mode_rejects_partial_material_and_https() {
        let error = TlsSpec::from_fields("off", "/tmp/cert.pem", "", "", "").unwrap_err();
        assert!(error.to_string().contains("mode is off"), "{error}");

        let spec = TlsSpec::from_fields("", "", "", "", "").unwrap();
        assert_eq!(spec.mode(), TlsMode::Off);
        let downgrade = spec.prepare_url("https://127.0.0.1:7800").unwrap_err();
        assert!(downgrade.to_string().contains("https"), "{downgrade}");
        assert_eq!(
            spec.prepare_url("127.0.0.1:7800").unwrap(),
            "http://127.0.0.1:7800"
        );
        let loaded = TransportSecurity::load(&spec).unwrap();
        assert!(loaded.is_plaintext());
        assert!(!loaded.verifies_peer_certificates());
    }

    #[test]
    fn tls_and_mtls_are_refused_without_reading_key_material() {
        let spec =
            TlsSpec::from_fields("mtls", "cert.pem", "key.pem", "ca.pem", "127.0.0.1").unwrap();
        let error = TransportSecurity::load(&spec).unwrap_err();
        let text = error.to_string();
        assert!(text.contains("does not terminate TLS"), "{text}");
        assert!(text.contains("ISC"), "{text}");
        assert!(text.contains("rustls-pure"), "{text}");
        assert!(text.contains("revocation"), "{text}");
        assert!(!text.contains("BEGIN"));
        assert!(!text.contains("PRIVATE"));

        let missing =
            TlsSpec::from_fields("tls", "cert.pem", "key.pem", "", "127.0.0.1").unwrap_err();
        assert!(missing.to_string().contains("requires"), "{missing}");
        let http = spec.prepare_url("http://127.0.0.1:7800").unwrap_err();
        assert!(http.to_string().contains("plaintext"), "{http}");
        let userinfo = spec
            .prepare_url("https://user:secret@127.0.0.1:7800")
            .unwrap_err();
        assert!(!userinfo.to_string().contains("secret"), "{userinfo}");
        let wildcard = spec.reject_listen_host("0.0.0.0:7800").unwrap_err();
        assert!(wildcard.to_string().contains("wildcard"), "{wildcard}");
    }

    #[test]
    fn unknown_mode_and_bad_names_fail_closed() {
        let mode = TlsSpec::from_fields("starttls", "a", "b", "c", "127.0.0.1").unwrap_err();
        assert!(mode.to_string().contains("mode"), "{mode}");
        let wildcard = TlsSpec::from_fields("tls", "a", "b", "c", "*.internal").unwrap_err();
        assert!(wildcard.to_string().contains("server_name"), "{wildcard}");
        let unspecified = TlsSpec::from_fields("tls", "a", "b", "c", "0.0.0.0").unwrap_err();
        assert!(
            unspecified.to_string().contains("server_name"),
            "{unspecified}"
        );
    }
}
