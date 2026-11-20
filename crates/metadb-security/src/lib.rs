//! Transport security for MetaDB listeners and outbound clients.
//!
//! This crate terminates TLS 1.3 and, when `mode = "mtls"`, mutual TLS. It checks
//! the peer certificate against the configured CA and the dialed host name. It does
//! not install method RBAC. [`authorize_method`] always refuses so a shared bearer
//! cannot be relabeled as a role.
//!
//! The scale stream owns process role separation. It should read
//! [`AuthenticatedPrincipal`] from [`TlsConnectInfo`] and supply its own method
//! policy. It should not open a second TLS stack or treat `[security].shared_secret`
//! as a principal.

mod identity;
mod session;
mod spec;

pub use identity::AuthenticatedPrincipal;
pub use session::{ClientHttp, MetricsIo, TlsConnectInfo, TlsServerIo, TransportSecurity};
pub use spec::{SecurityError, TlsMode, TlsSpec};

/// Method authorization is not installed.
///
/// `principal` is the peer certificate identity. It is not a role. The server does
/// not call this function, so a verified mTLS peer is not granted or denied per
/// method here. A later authorizer must replace this refusal with an explicit
/// allow entry for each service method.
pub fn authorize_method(
    _principal: &AuthenticatedPrincipal,
    _service: &str,
    _method: &str,
) -> Result<(), SecurityError> {
    Err(SecurityError::rbac_not_installed())
}

#[cfg(feature = "testing")]
pub mod testing;

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
    }

    #[test]
    fn tls_mode_requires_every_field_and_refuses_plaintext_and_wildcards() {
        let missing =
            TlsSpec::from_fields("tls", "cert.pem", "key.pem", "", "127.0.0.1").unwrap_err();
        assert!(missing.to_string().contains("requires"), "{missing}");

        let spec =
            TlsSpec::from_fields("mtls", "cert.pem", "key.pem", "ca.pem", "127.0.0.1").unwrap();
        assert_eq!(spec.mode(), TlsMode::Mtls);
        let http = spec.prepare_url("http://127.0.0.1:7800").unwrap_err();
        assert!(http.to_string().contains("plaintext"), "{http}");
        assert_eq!(
            spec.prepare_url("127.0.0.1:7800").unwrap(),
            "https://127.0.0.1:7800"
        );
        let userinfo = spec
            .prepare_url("https://user:secret@127.0.0.1:7800")
            .unwrap_err();
        assert!(!userinfo.to_string().contains("secret"), "{userinfo}");
        let wildcard = spec.reject_listen_host("0.0.0.0:7800").unwrap_err();
        assert!(wildcard.to_string().contains("wildcard"), "{wildcard}");
        spec.reject_listen_host("127.0.0.1:0").unwrap();
        let other = spec.reject_listen_host("10.1.2.3:7800").unwrap_err();
        assert!(other.to_string().contains("server_name"), "{other}");
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

#[cfg(all(test, feature = "testing"))]
mod handshake_tests {
    use std::io::Write;
    use std::net::IpAddr;
    use std::time::{Duration, SystemTime};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::testing::{IssueRequest, TestCa};

    fn scratch(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "metadb-tls-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_pem(dir: &std::path::Path, name: &str, pem: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(pem.as_bytes()).unwrap();
        path
    }

    #[tokio::test]
    async fn mtls_checks_ca_hostname_and_principal() {
        let dir = scratch("ok");
        let mut ca = TestCa::generate().unwrap();
        let server = ca
            .issue(IssueRequest::valid_for(
                Vec::new(),
                vec!["127.0.0.1".parse::<IpAddr>().unwrap()],
                vec!["spiffe://metadb/node/server".into()],
            ))
            .unwrap();
        let client = ca
            .issue(IssueRequest::valid_for(
                Vec::new(),
                Vec::new(),
                vec!["spiffe://metadb/node/client".into()],
            ))
            .unwrap();
        let ca_path = write_pem(&dir, "ca.pem", ca.cert_pem());
        let server_cert = write_pem(&dir, "server.pem", &server.cert_pem);
        let server_key = write_pem(&dir, "server.key", &server.key_pem);
        let client_cert = write_pem(&dir, "client.pem", &client.cert_pem);
        let client_key = write_pem(&dir, "client.key", &client.key_pem);
        let server_sec = TransportSecurity::load(
            &TlsSpec::from_fields(
                "mtls",
                server_cert.to_str().unwrap(),
                server_key.to_str().unwrap(),
                ca_path.to_str().unwrap(),
                "127.0.0.1",
            )
            .unwrap(),
        )
        .unwrap();
        assert!(server_sec.verifies_peer_certificates());
        let client_sec = TransportSecurity::load(
            &TlsSpec::from_client_fields(
                "mtls",
                client_cert.to_str().unwrap(),
                client_key.to_str().unwrap(),
                ca_path.to_str().unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accept = server_sec.clone();
        let served = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut io = accept.accept_metrics(socket).await.unwrap();
            let principal = io
                .principal()
                .map(|principal| principal.as_str().to_string())
                .unwrap();
            let mut buf = [0u8; 256];
            let _ = tokio::time::timeout(Duration::from_secs(2), io.read(&mut buf)).await;
            io.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
            principal
        });
        let mut http = client_sec.connect_http(&addr).await.unwrap();
        http.write_all(b"GET /readyz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(2), http.read_to_end(&mut response)).await;
        let text = String::from_utf8_lossy(&response);
        assert!(text.contains("200"), "{text}");
        let principal = served.await.unwrap();
        assert_eq!(principal, "uri:spiffe://metadb/node/client");
        assert!(!principal.contains("Bearer"));

        let mut other = TestCa::generate().unwrap();
        let foreign = other
            .issue(IssueRequest::valid_for(
                Vec::new(),
                Vec::new(),
                vec!["spiffe://metadb/node/foreign".into()],
            ))
            .unwrap();
        let foreign_cert = write_pem(&dir, "foreign.pem", &foreign.cert_pem);
        let foreign_key = write_pem(&dir, "foreign.key", &foreign.key_pem);
        let foreign_ca = write_pem(&dir, "foreign-ca.pem", other.cert_pem());
        let foreign_sec = TransportSecurity::load(
            &TlsSpec::from_client_fields(
                "mtls",
                foreign_cert.to_str().unwrap(),
                foreign_key.to_str().unwrap(),
                foreign_ca.to_str().unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accept = server_sec.clone();
        let rejected = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            accept.accept_metrics(socket).await
        });
        let client_error = match foreign_sec.connect_http(&addr).await {
            Ok(_) => panic!("a certificate from a different CA must not connect"),
            Err(error) => error,
        };
        let server_error = match rejected.await.unwrap() {
            Ok(_) => panic!("mtls server accepted a certificate from a different CA"),
            Err(error) => error,
        };
        let client_text = client_error.to_string();
        let server_text = server_error.to_string();
        assert!(
            !client_text.contains("BEGIN") && !server_text.contains("BEGIN"),
            "{client_text} {server_text}"
        );
        assert!(!client_text.contains(&foreign.key_pem));
        assert!(!server_text.contains(&foreign.key_pem));
    }

    #[test]
    fn incomplete_and_mismatched_material_fails_before_a_listener() {
        let dir = scratch("bad");
        let mut ca = TestCa::generate().unwrap();
        let wrong = ca
            .issue(IssueRequest::valid_for(
                Vec::new(),
                vec!["10.9.8.7".parse().unwrap()],
                Vec::new(),
            ))
            .unwrap();
        let expired = ca
            .issue(IssueRequest {
                dns: Vec::new(),
                ips: vec!["127.0.0.1".parse().unwrap()],
                uris: Vec::new(),
                not_before: SystemTime::now() - Duration::from_secs(48 * 3600),
                not_after: SystemTime::now() - Duration::from_secs(60),
            })
            .unwrap();
        let no_san = ca
            .issue(IssueRequest::valid_for(Vec::new(), Vec::new(), Vec::new()))
            .unwrap();
        let ca_path = write_pem(&dir, "ca.pem", ca.cert_pem());
        let wrong_cert = write_pem(&dir, "wrong.pem", &wrong.cert_pem);
        let wrong_key = write_pem(&dir, "wrong.key", &wrong.key_pem);
        let name_error = TransportSecurity::load(
            &TlsSpec::from_fields(
                "tls",
                wrong_cert.to_str().unwrap(),
                wrong_key.to_str().unwrap(),
                ca_path.to_str().unwrap(),
                "127.0.0.1",
            )
            .unwrap(),
        )
        .unwrap_err();
        assert!(name_error.to_string().contains("127.0.0.1"), "{name_error}");

        let expired_cert = write_pem(&dir, "expired.pem", &expired.cert_pem);
        let expired_key = write_pem(&dir, "expired.key", &expired.key_pem);
        let expired_error = TransportSecurity::load(
            &TlsSpec::from_fields(
                "tls",
                expired_cert.to_str().unwrap(),
                expired_key.to_str().unwrap(),
                ca_path.to_str().unwrap(),
                "127.0.0.1",
            )
            .unwrap(),
        )
        .unwrap_err();
        assert!(
            expired_error.to_string().contains("certificate"),
            "{expired_error}"
        );
        assert!(!expired_error.to_string().contains("BEGIN"));

        let bare_cert = write_pem(&dir, "bare.pem", &no_san.cert_pem);
        let bare_key = write_pem(&dir, "bare.key", &no_san.key_pem);
        let bare_error = TransportSecurity::load(
            &TlsSpec::from_fields(
                "mtls",
                bare_cert.to_str().unwrap(),
                bare_key.to_str().unwrap(),
                ca_path.to_str().unwrap(),
                "127.0.0.1",
            )
            .unwrap(),
        )
        .unwrap_err();
        assert!(
            bare_error.to_string().contains("subject alternative name")
                || bare_error.to_string().contains("127.0.0.1"),
            "{bare_error}"
        );

        let marker = "SUPER-SECRET-KEY-MATERIAL";
        let junk = write_pem(&dir, "junk.key", marker);
        let good = ca
            .issue(IssueRequest::valid_for(
                Vec::new(),
                vec!["127.0.0.1".parse().unwrap()],
                Vec::new(),
            ))
            .unwrap();
        let good_cert = write_pem(&dir, "good.pem", &good.cert_pem);
        let junk_error = TransportSecurity::load(
            &TlsSpec::from_fields(
                "tls",
                good_cert.to_str().unwrap(),
                junk.to_str().unwrap(),
                ca_path.to_str().unwrap(),
                "127.0.0.1",
            )
            .unwrap(),
        )
        .unwrap_err();
        assert!(!junk_error.to_string().contains(marker), "{junk_error}");
        assert!(!junk_error.to_string().contains("PRIVATE"));
    }

    #[tokio::test]
    async fn plaintext_client_is_rejected_by_mtls_and_tls_client_cannot_skip_the_name() {
        let dir = scratch("plain");
        let mut ca = TestCa::generate().unwrap();
        let server = ca
            .issue(IssueRequest::valid_for(
                Vec::new(),
                vec!["127.0.0.1".parse().unwrap()],
                vec!["spiffe://metadb/node/server".into()],
            ))
            .unwrap();
        let ca_path = write_pem(&dir, "ca.pem", ca.cert_pem());
        let cert = write_pem(&dir, "server.pem", &server.cert_pem);
        let key = write_pem(&dir, "server.key", &server.key_pem);
        let security = TransportSecurity::load(
            &TlsSpec::from_fields(
                "mtls",
                cert.to_str().unwrap(),
                key.to_str().unwrap(),
                ca_path.to_str().unwrap(),
                "127.0.0.1",
            )
            .unwrap(),
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accept = security.clone();
        let rejected = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            accept.accept_metrics(socket).await
        });
        let _ = TcpStream::connect(&addr).await.unwrap();
        let error = match rejected.await.unwrap() {
            Ok(_) => panic!("plaintext must not complete a TLS handshake"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("handshake"), "{error}");
        assert!(!error.to_string().contains(&server.key_pem));
    }
}
