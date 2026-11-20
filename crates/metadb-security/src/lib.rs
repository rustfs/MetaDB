//! TLS 1.3 transport policy for MetaDB listeners and outbound clients.
//!
//! The cryptography is rustls 0.23 with the maintained `ring` provider.
//! `aws-lc-rs` is not enabled. Certificate chain, hostname, and extended key
//! usage are checked with rustls WebPKI verifiers and the configured CA.
//! There is no accept-all verifier and no skip-verify switch.
//!
//! Method RBAC is not installed. A verified certificate name is not a role,
//! and TLS does not replace the bearer secret. See
//! `docs/designs/transport_security.md`.

mod identity;
mod session;
mod spec;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use identity::AuthenticatedPrincipal;
pub use session::{TlsServerIo, TransportSecurity};
pub use spec::{SecurityError, TlsMode, TlsSpec};

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
    fn missing_material_fails_before_any_listener_and_does_not_echo_a_key() {
        let spec =
            TlsSpec::from_fields("mtls", "cert.pem", "key.pem", "ca.pem", "127.0.0.1").unwrap();
        let error = TransportSecurity::load(&spec).unwrap_err();
        let text = error.to_string();
        assert!(text.contains("cannot read"), "{text}");
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

    fn scratch() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "metadb-security-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch");
        dir
    }

    #[test]
    fn mtls_material_verifies_and_bad_material_fails_closed() {
        let dir = scratch();
        let mut ca = crate::testing::TestCa::new("metadb-test-ca");
        let issued = ca.issue(crate::testing::IssueRequest::server_and_client(
            "127.0.0.1".parse().unwrap(),
            "spiffe://metadb/node/test",
        ));
        let (cert, key, ca_pem) =
            crate::testing::write_bundle(&dir, ca.pem(), &issued.cert_pem, &issued.key_pem);
        let spec = TlsSpec::from_fields("mtls", &cert, &key, &ca_pem, "127.0.0.1").unwrap();
        let loaded = TransportSecurity::load(&spec).expect("valid mtls material");
        assert!(!loaded.is_plaintext());
        assert!(loaded.verifies_peer_certificates());
        assert_eq!(loaded.server_name(), Some("127.0.0.1"));
        let http = loaded.prepare_url("http://127.0.0.1:7800").unwrap_err();
        assert!(http.to_string().contains("plaintext"), "{http}");

        let expired = ca.issue(crate::testing::IssueRequest {
            uri: Some("spiffe://metadb/node/expired"),
            dns: None,
            ip: Some("127.0.0.1".parse().unwrap()),
            server_auth: true,
            client_auth: true,
            validity: crate::testing::expired_validity(),
        });
        let (cert, key, ca_pem) =
            crate::testing::write_bundle(&dir, ca.pem(), &expired.cert_pem, &expired.key_pem);
        let error = TransportSecurity::load(
            &TlsSpec::from_fields("mtls", &cert, &key, &ca_pem, "127.0.0.1").unwrap(),
        )
        .unwrap_err();
        let text = error.to_string();
        assert!(
            text.contains("not verified") || text.contains("extended key usage"),
            "{text}"
        );
        assert!(!text.contains("BEGIN PRIVATE"));

        let wrong_name = ca.issue(crate::testing::IssueRequest::server_and_client(
            "127.0.0.2".parse().unwrap(),
            "spiffe://metadb/node/other",
        ));
        let (cert, key, ca_pem) =
            crate::testing::write_bundle(&dir, ca.pem(), &wrong_name.cert_pem, &wrong_name.key_pem);
        let error = TransportSecurity::load(
            &TlsSpec::from_fields("mtls", &cert, &key, &ca_pem, "127.0.0.1").unwrap(),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("subject alternative name")
                || error.to_string().contains("not verified"),
            "{error}"
        );

        let mut missing_eku = crate::testing::IssueRequest::server_and_client(
            "127.0.0.1".parse().unwrap(),
            "spiffe://metadb/node/no-eku",
        );
        missing_eku.server_auth = false;
        missing_eku.client_auth = false;
        let issued = ca.issue(missing_eku);
        let (cert, key, ca_pem) =
            crate::testing::write_bundle(&dir, ca.pem(), &issued.cert_pem, &issued.key_pem);
        let error = TransportSecurity::load(
            &TlsSpec::from_fields("mtls", &cert, &key, &ca_pem, "127.0.0.1").unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("extended key usage"), "{error}");

        let wildcard = ca.issue(crate::testing::IssueRequest {
            uri: None,
            dns: Some("*.example.test"),
            ip: None,
            server_auth: true,
            client_auth: true,
            validity: crate::testing::validity_from_now(std::time::Duration::from_secs(3600)),
        });
        let (cert, key, ca_pem) =
            crate::testing::write_bundle(&dir, ca.pem(), &wildcard.cert_pem, &wildcard.key_pem);
        let error = TransportSecurity::load(
            &TlsSpec::from_fields("tls", &cert, &key, &ca_pem, "node.example.test").unwrap(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("wildcard"), "{error}");

        let marker = "SUPER-SECRET-KEY-MATERIAL";
        std::fs::write(dir.join("key.pem"), marker).unwrap();
        let error = TransportSecurity::load(&spec).unwrap_err();
        assert!(!error.to_string().contains(marker), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
