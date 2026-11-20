//! TLS 1.3 sessions through rustls and its maintained ring provider.
//!
//! `aws-lc-rs` is not a feature of this crate. rustls is built with
//! `default-features = false` and `features = ["std", "ring"]`. The provider
//! is passed to every builder. This module does not install a process-global
//! crypto provider and does not call `allow_unauthenticated` or any
//! certificate-verification bypass.
//!
//! The PEM buffer is wiped after the key is parsed. rustls then keeps its own
//! copy inside `ServerConfig` and, for mTLS, `ClientConfig` for the life of
//! the process. Dropping the PEM buffer does not wipe those copies. There is
//! no certificate reload. Replacing a certificate means restarting the
//! process. No CRL or OCSP check is performed.

use std::io::{self, Read as _};
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::client::danger::ServerCertVerifier;
use rustls::client::Resumption;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::{NoServerSessionStorage, WebPkiClientVerifier};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tonic::transport::server::Connected;
use tonic::transport::{Channel, Endpoint};
use zeroize::Zeroize;

use crate::identity::{leaf_covers_exact_name, principal_from_leaf, require_eku};
use crate::spec::{SecurityError, TlsMode, TlsSpec};

const ALPN_H2: &[u8] = b"h2";
const ALPN_HTTP11: &[u8] = b"http/1.1";

/// Process-wide transport policy. Debug output is the mode and server name.
pub struct TransportSecurity {
    inner: Inner,
}

enum Inner {
    Plaintext,
    Enabled(Box<Enabled>),
}

#[derive(Clone)]
struct Enabled {
    mode: TlsMode,
    server_name: String,
    server: Option<Arc<ServerConfig>>,
    client: ClientConfig,
}

impl std::fmt::Debug for TransportSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportSecurity")
            .field("mode", &self.mode())
            .field("server_name", &self.server_name())
            .finish()
    }
}

impl Clone for TransportSecurity {
    fn clone(&self) -> Self {
        match &self.inner {
            Inner::Plaintext => Self {
                inner: Inner::Plaintext,
            },
            Inner::Enabled(enabled) => Self {
                inner: Inner::Enabled(enabled.clone()),
            },
        }
    }
}

impl Default for TransportSecurity {
    fn default() -> Self {
        Self::plaintext()
    }
}

impl TransportSecurity {
    pub fn plaintext() -> Self {
        Self {
            inner: Inner::Plaintext,
        }
    }

    /// Load certificate material and build rustls configs, or return plaintext.
    ///
    /// `tls` and `mtls` fail before the caller binds or creates a data
    /// directory when the files are missing, unreadable, expired, or do not
    /// match the configured CA, name, or extended key usage.
    pub fn load(spec: &TlsSpec) -> Result<Self, SecurityError> {
        if spec.mode() == TlsMode::Off {
            return Ok(Self::plaintext());
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let roots = Arc::new(load_roots(spec.ca_file())?);
        let identity = load_identity(spec)?;
        if let Some(material) = &identity {
            check_identity(spec, material, &roots, &provider)?;
        }
        let client = build_client(spec.mode(), &roots, &provider, identity.as_ref())?;
        let server = if spec.client_only() {
            None
        } else {
            Some(Arc::new(build_server(
                spec.mode(),
                &roots,
                &provider,
                identity.as_ref().ok_or_else(|| {
                    SecurityError::new(
                        "refusing to start: a listener requires a certificate and private key"
                            .to_string(),
                    )
                })?,
            )?))
        };
        Ok(Self {
            inner: Inner::Enabled(Box::new(Enabled {
                mode: spec.mode(),
                server_name: spec.server_name().to_string(),
                server,
                client,
            })),
        })
    }

    pub fn mode(&self) -> TlsMode {
        match &self.inner {
            Inner::Plaintext => TlsMode::Off,
            Inner::Enabled(enabled) => enabled.mode,
        }
    }

    pub fn is_plaintext(&self) -> bool {
        matches!(self.inner, Inner::Plaintext)
    }

    pub fn server_name(&self) -> Option<&str> {
        match &self.inner {
            Inner::Plaintext => None,
            Inner::Enabled(enabled) if enabled.server_name.is_empty() => None,
            Inner::Enabled(enabled) => Some(enabled.server_name.as_str()),
        }
    }

    /// Outbound TLS always verifies the peer with the configured CA.
    /// Plaintext does not.
    pub fn verifies_peer_certificates(&self) -> bool {
        !self.is_plaintext()
    }

    pub fn prepare_url(&self, addr: &str) -> Result<String, SecurityError> {
        match self.mode() {
            TlsMode::Off => TlsSpec::from_fields("off", "", "", "", "")?.prepare_url(addr),
            mode => {
                let name = self.server_name().unwrap_or("127.0.0.1");
                TlsSpec::from_fields(mode.as_str(), "cert.pem", "key.pem", "ca.pem", name)?
                    .prepare_url(addr)
            }
        }
    }

    /// Connect a tonic channel. `endpoint` must already use [`Self::prepare_url`].
    pub async fn connect(&self, endpoint: Endpoint) -> Result<Channel, SecurityError> {
        match &self.inner {
            Inner::Plaintext => {
                let scheme = endpoint.uri().scheme_str().unwrap_or("");
                if scheme == "https" {
                    return Err(SecurityError::new(
                        "refusing to dial: https is refused while [security.tls] mode is off"
                            .to_string(),
                    ));
                }
                endpoint
                    .connect()
                    .await
                    .map_err(|error| SecurityError::new(format!("plaintext connect: {error}")))
            }
            Inner::Enabled(enabled) => {
                let enabled = enabled.as_ref().clone();
                let connector = tower::service_fn(move |uri: http::Uri| {
                    let enabled = enabled.clone();
                    async move {
                        let io = dial_tls(&enabled, &uri, ALPN_H2).await?;
                        Ok::<_, SecurityError>(hyper_util::rt::TokioIo::new(io))
                    }
                });
                endpoint
                    .connect_with_connector(connector)
                    .await
                    .map_err(|error| SecurityError::new(format!("tls connect: {error}")))
            }
        }
    }

    /// Accept one inbound gRPC connection. The negotiated protocol must be h2.
    pub async fn accept_grpc(&self, tcp: TcpStream) -> Result<TlsServerIo, SecurityError> {
        let stream = self.accept_alpn(tcp, ALPN_H2).await?;
        Ok(TlsServerIo { inner: stream })
    }

    /// Accept one inbound metrics connection. The negotiated protocol must be HTTP/1.1.
    pub async fn accept_metrics(&self, tcp: TcpStream) -> Result<TlsServerIo, SecurityError> {
        let stream = self.accept_alpn(tcp, ALPN_HTTP11).await?;
        Ok(TlsServerIo { inner: stream })
    }

    /// One HTTP/1.1 exchange on this same policy. Used by probes and tests.
    /// The response is capped at 64 KiB.
    pub async fn exchange_http1(
        &self,
        addr: &str,
        request: &[u8],
    ) -> Result<Vec<u8>, SecurityError> {
        let url = self.prepare_url(addr)?;
        let uri: http::Uri = url.parse().map_err(|_| {
            SecurityError::new("refusing to dial: the probe URL is not valid".to_string())
        })?;
        let mut io = match &self.inner {
            Inner::Plaintext => {
                if uri.scheme_str() == Some("https") {
                    return Err(SecurityError::new(
                        "refusing to dial: https is refused while [security.tls] mode is off"
                            .to_string(),
                    ));
                }
                ProbeIo::Plain(dial_tcp(&uri).await?)
            }
            Inner::Enabled(enabled) => {
                ProbeIo::Tls(Box::new(dial_tls(enabled, &uri, ALPN_HTTP11).await?))
            }
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::io::AsyncWriteExt::write_all(&mut io, request).await?;
            let mut response = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut io, &mut response).await?;
            if response.len() > 64 * 1024 {
                response.truncate(64 * 1024);
            }
            Ok(response)
        })
        .await
        .map_err(|_| SecurityError::new("http exchange timed out".to_string()))?
        .map_err(|error: io::Error| SecurityError::new(format!("http exchange: {error}")))
    }

    async fn accept_alpn(
        &self,
        tcp: TcpStream,
        expected: &[u8],
    ) -> Result<tokio_rustls::server::TlsStream<TcpStream>, SecurityError> {
        let Inner::Enabled(enabled) = &self.inner else {
            return Err(SecurityError::new(
                "refusing connection: plaintext mode has no TLS acceptor".to_string(),
            ));
        };
        let server = enabled.server.clone().ok_or_else(|| {
            SecurityError::new("refusing connection: this client does not listen".to_string())
        })?;
        let _ = tcp.set_nodelay(true);
        let acceptor = tokio_rustls::TlsAcceptor::from(server);
        let stream = acceptor.accept(tcp).await.map_err(|error| {
            SecurityError::new(format!(
                "refusing connection: TLS handshake failed: {error}"
            ))
        })?;
        let (_, connection) = stream.get_ref();
        match connection.alpn_protocol() {
            Some(protocol) if protocol == expected => {}
            _ => {
                return Err(SecurityError::new(
                    "refusing connection: the negotiated protocol is not the one required by this listener"
                        .to_string(),
                ));
            }
        }
        if enabled.mode == TlsMode::Mtls {
            let leaf = connection
                .peer_certificates()
                .and_then(|chain| chain.first())
                .ok_or_else(|| {
                    SecurityError::new(
                        "refusing connection: mTLS requires a client certificate".to_string(),
                    )
                })?;
            require_eku(leaf.as_ref(), false, true)?;
            principal_from_leaf(leaf.as_ref())?;
        } else if connection.peer_certificates().is_some() {
            return Err(SecurityError::new(
                "refusing connection: tls mode does not accept a client certificate".to_string(),
            ));
        }
        Ok(stream)
    }
}

/// Inbound gRPC stream after a completed TLS 1.3 handshake.
pub struct TlsServerIo {
    inner: tokio_rustls::server::TlsStream<TcpStream>,
}

impl AsyncRead for TlsServerIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsServerIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl Connected for TlsServerIo {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

struct IdentityMaterial {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

enum ProbeIo {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for ProbeIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ProbeIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

fn load_identity(spec: &TlsSpec) -> Result<Option<IdentityMaterial>, SecurityError> {
    let needs_identity = !spec.client_only() || spec.mode() == TlsMode::Mtls;
    if !needs_identity {
        return Ok(None);
    }
    let mut cert_pem = read_file(spec.cert_file())?;
    let mut key_pem = read_file(spec.key_file())?;
    let parsed = parse_identity(&cert_pem, &key_pem);
    cert_pem.zeroize();
    key_pem.zeroize();
    parsed.map(Some)
}

fn parse_identity(cert_pem: &[u8], key_pem: &[u8]) -> Result<IdentityMaterial, SecurityError> {
    let chain = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            SecurityError::new("refusing to start: certificate PEM could not be parsed".to_string())
        })?;
    if chain.is_empty() {
        return Err(SecurityError::new(
            "refusing to start: certificate PEM contained no certificate".to_string(),
        ));
    }
    let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|_| {
        SecurityError::new("refusing to start: private key PEM could not be parsed".to_string())
    })?;
    Ok(IdentityMaterial { chain, key })
}

fn load_roots(path: &std::path::Path) -> Result<RootCertStore, SecurityError> {
    let mut pem = read_file(path)?;
    let certs = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            SecurityError::new("refusing to start: CA PEM could not be parsed".to_string())
        });
    pem.zeroize();
    let certs = certs?;
    if certs.is_empty() {
        return Err(SecurityError::new(
            "refusing to start: CA PEM contained no certificate".to_string(),
        ));
    }
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|_| {
            SecurityError::new(
                "refusing to start: a CA certificate was not a trust anchor".to_string(),
            )
        })?;
    }
    Ok(roots)
}

fn read_file(path: &std::path::Path) -> Result<Vec<u8>, SecurityError> {
    let mut file = std::fs::File::open(path).map_err(|error| {
        SecurityError::new(format!(
            "refusing to start: cannot read {}: {error}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|error| {
        SecurityError::new(format!(
            "refusing to start: cannot read {}: {error}",
            path.display()
        ))
    })?;
    if bytes.is_empty() {
        bytes.zeroize();
        return Err(SecurityError::new(format!(
            "refusing to start: {} is empty",
            path.display()
        )));
    }
    Ok(bytes)
}

fn check_identity(
    spec: &TlsSpec,
    material: &IdentityMaterial,
    roots: &RootCertStore,
    provider: &Arc<rustls::crypto::CryptoProvider>,
) -> Result<(), SecurityError> {
    let leaf = &material.chain[0];
    let intermediates = &material.chain[1..];
    let now = UnixTime::now();
    if !spec.client_only() {
        require_eku(leaf.as_ref(), true, spec.mode() == TlsMode::Mtls)?;
        leaf_covers_exact_name(leaf.as_ref(), spec.server_name())?;
        let name = ServerName::try_from(spec.server_name().to_string()).map_err(|_| {
            SecurityError::new(
                "refusing to start: server_name is not a DNS name or IP address".to_string(),
            )
        })?;
        let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            Arc::clone(provider),
        )
        .build()
        .map_err(|_| {
            SecurityError::new("refusing to start: the CA trust anchors were rejected".to_string())
        })?;
        verifier
            .verify_server_cert(leaf, intermediates, &name, &[], now)
            .map_err(|error| {
                SecurityError::new(format!(
                    "refusing to start: the server certificate was not verified for its name, chain, expiry, or extended key usage: {error}"
                ))
            })?;
    }
    if spec.mode() == TlsMode::Mtls {
        require_eku(leaf.as_ref(), false, true)?;
        principal_from_leaf(leaf.as_ref())?;
        let verifier = WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots.clone()),
            Arc::clone(provider),
        )
        .build()
        .map_err(|_| {
            SecurityError::new("refusing to start: the CA trust anchors were rejected".to_string())
        })?;
        verifier
            .verify_client_cert(leaf, intermediates, now)
            .map_err(|error| {
                SecurityError::new(format!(
                    "refusing to start: the client certificate was not verified for its chain, expiry, or extended key usage: {error}"
                ))
            })?;
    }
    Ok(())
}

fn build_server(
    mode: TlsMode,
    roots: &Arc<RootCertStore>,
    provider: &Arc<rustls::crypto::CryptoProvider>,
    material: &IdentityMaterial,
) -> Result<ServerConfig, SecurityError> {
    let builder = ServerConfig::builder_with_provider(Arc::clone(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| {
            SecurityError::new(format!(
                "refusing to start: the ring provider cannot negotiate TLS 1.3: {error}"
            ))
        })?;
    let mut config = if mode == TlsMode::Mtls {
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::clone(roots), Arc::clone(provider))
                .build()
                .map_err(|_| {
                    SecurityError::new(
                        "refusing to start: the CA trust anchors were rejected".to_string(),
                    )
                })?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(material.chain.clone(), material.key.clone_key())
            .map_err(|error| {
                SecurityError::new(format!(
                    "refusing to start: the certificate and private key do not match: {error}"
                ))
            })?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(material.chain.clone(), material.key.clone_key())
            .map_err(|error| {
                SecurityError::new(format!(
                    "refusing to start: the certificate and private key do not match: {error}"
                ))
            })?
    };
    config.alpn_protocols = vec![ALPN_H2.to_vec(), ALPN_HTTP11.to_vec()];
    config.session_storage = Arc::new(NoServerSessionStorage {});
    config.send_tls13_tickets = 0;
    Ok(config)
}

fn build_client(
    mode: TlsMode,
    roots: &Arc<RootCertStore>,
    provider: &Arc<rustls::crypto::CryptoProvider>,
    material: Option<&IdentityMaterial>,
) -> Result<ClientConfig, SecurityError> {
    let builder = ClientConfig::builder_with_provider(Arc::clone(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| {
            SecurityError::new(format!(
                "refusing to start: the ring provider cannot negotiate TLS 1.3: {error}"
            ))
        })?
        .with_root_certificates(Arc::clone(roots));
    let mut config = if mode == TlsMode::Mtls {
        let material = material.ok_or_else(|| {
            SecurityError::new(
                "refusing to start: mtls requires a certificate and private key".to_string(),
            )
        })?;
        builder
            .with_client_auth_cert(material.chain.clone(), material.key.clone_key())
            .map_err(|error| {
                SecurityError::new(format!(
                    "refusing to start: the client certificate and private key do not match: {error}"
                ))
            })?
    } else {
        builder.with_no_client_auth()
    };
    config.enable_sni = true;
    config.resumption = Resumption::disabled();
    Ok(config)
}

async fn dial_tcp(uri: &http::Uri) -> Result<TcpStream, SecurityError> {
    let host = uri
        .host()
        .ok_or_else(|| SecurityError::new("refusing to dial: the URL has no host".to_string()))?;
    let port = uri
        .port_u16()
        .unwrap_or(if uri.scheme_str() == Some("https") {
            443
        } else {
            80
        });
    let target = (host.to_string(), port);
    let mut addresses = target.to_socket_addrs().map_err(|error| {
        SecurityError::new(format!(
            "refusing to dial: {host} could not be resolved: {error}"
        ))
    })?;
    let address = addresses
        .next()
        .ok_or_else(|| SecurityError::new(format!("refusing to dial: {host} did not resolve")))?;
    let tcp = TcpStream::connect(address).await.map_err(|error| {
        SecurityError::new(format!(
            "refusing to dial: connect to {address} failed: {error}"
        ))
    })?;
    let _ = tcp.set_nodelay(true);
    Ok(tcp)
}

async fn dial_tls(
    enabled: &Enabled,
    uri: &http::Uri,
    alpn: &[u8],
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, SecurityError> {
    if uri.scheme_str() != Some("https") {
        return Err(SecurityError::new(
            "refusing to dial: plaintext http is refused while TLS is enabled".to_string(),
        ));
    }
    let host = uri
        .host()
        .ok_or_else(|| SecurityError::new("refusing to dial: the URL has no host".to_string()))?;
    let name = ServerName::try_from(host.to_string()).map_err(|_| {
        SecurityError::new("refusing to dial: the host is not a DNS name or IP address".to_string())
    })?;
    let tcp = dial_tcp(uri).await?;
    let mut config = enabled.client.clone();
    config.alpn_protocols = vec![alpn.to_vec()];
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let stream = connector.connect(name, tcp).await.map_err(|error| {
        SecurityError::new(format!(
            "refusing to dial: TLS certificate, name, or protocol verification failed: {error}"
        ))
    })?;
    let (_, connection) = stream.get_ref();
    match connection.alpn_protocol() {
        Some(protocol) if protocol == alpn => {}
        _ => {
            return Err(SecurityError::new(
                "refusing to dial: the peer did not negotiate the required protocol".to_string(),
            ));
        }
    }
    let leaf = connection
        .peer_certificates()
        .and_then(|chain| chain.first())
        .ok_or_else(|| {
            SecurityError::new("refusing to dial: the peer presented no certificate".to_string())
        })?;
    require_eku(leaf.as_ref(), true, false)?;
    Ok(stream)
}
