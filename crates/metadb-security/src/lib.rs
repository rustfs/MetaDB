//! Transport policy and installed method grants.
//!
//! Successful startup is plaintext. `tls` and `mtls` are recognized and refused
//! before a listener exists. This crate does not terminate TLS and does not
//! link a TLS provider. `deny.toml` has no ISC exceptions.
//!
//! Method RBAC is installed only when the caller builds [`InstalledRoles`].
//! The shared bearer is not a role. See `docs/designs/transport_security.md`.

mod rbac;
mod session;
mod spec;

pub use rbac::{
    authorize_method, InstalledRoles, RoleGrantSpec, RolePresentation, CREDENTIAL_HEADER,
    PRINCIPAL_HEADER,
};
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

    /// Builds a principal name after the credential check has already succeeded.
    /// This does not authenticate and it is not a role by itself.
    pub fn from_name(identity: impl Into<String>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_roles() -> InstalledRoles {
        InstalledRoles::install(
            "node",
            &[
                RoleGrantSpec {
                    principal: "node".into(),
                    credential: "node-credential".into(),
                    allow: vec![
                        "metadb.v1.RaftTransportService/AppendEntries".into(),
                        "metadb.v1.ControlPlaneService/GetRoutes".into(),
                    ],
                },
                RoleGrantSpec {
                    principal: "reader".into(),
                    credential: "reader-credential".into(),
                    allow: vec!["metadb.v1.MetadataService/Lookup".into()],
                },
            ],
            "shared-bearer",
        )
        .expect("roles")
    }

    #[test]
    fn installed_roles_allow_a_listed_method_and_deny_the_rest() {
        let roles = sample_roles();
        let reader = roles
            .authenticate_and_authorize(
                "reader",
                "reader-credential",
                "metadb.v1.MetadataService",
                "Lookup",
            )
            .expect("lookup is granted");
        assert_eq!(reader.as_str(), "reader");
        authorize_method(&roles, &reader, "metadb.v1.MetadataService", "Lookup")
            .expect("same grant");
        let denied = roles
            .authenticate_and_authorize(
                "reader",
                "reader-credential",
                "metadb.v1.MetadataService",
                "CreateFile",
            )
            .expect_err("create is not granted");
        assert_eq!(denied.to_string(), "authorization denied");
        assert!(!denied.to_string().contains("reader-credential"));
        assert!(!denied.to_string().contains("shared-bearer"));

        let unknown = roles
            .authenticate_and_authorize(
                "missing",
                "reader-credential",
                "metadb.v1.MetadataService",
                "Lookup",
            )
            .expect_err("unknown principal");
        assert_eq!(unknown.to_string(), "authorization denied");
        let wrong = roles
            .authenticate_and_authorize(
                "reader",
                "shared-bearer",
                "metadb.v1.MetadataService",
                "Lookup",
            )
            .expect_err("shared bearer is not the role credential");
        assert_eq!(wrong.to_string(), "authorization denied");
    }

    #[test]
    fn role_installation_rejects_empty_wildcard_and_shared_secret() {
        let empty = InstalledRoles::install("node", &[], "shared").unwrap_err();
        assert!(empty.to_string().contains("no grants"), "{empty}");
        let star = InstalledRoles::install(
            "node",
            &[RoleGrantSpec {
                principal: "node".into(),
                credential: "node-credential".into(),
                allow: vec!["*".into()],
            }],
            "shared",
        )
        .unwrap_err();
        assert!(
            star.to_string().contains("not a known method")
                || star.to_string().contains("service/Method"),
            "{star}"
        );
        let same = InstalledRoles::install(
            "node",
            &[RoleGrantSpec {
                principal: "node".into(),
                credential: "s3cret-value".into(),
                allow: vec!["metadb.v1.ControlPlaneService/GetRoutes".into()],
            }],
            "s3cret-value",
        )
        .unwrap_err();
        assert!(same.to_string().contains("not a role"), "{same}");
        assert!(!same.to_string().contains("s3cret-value"));
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
