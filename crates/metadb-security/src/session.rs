//! Load certificates and run TLS 1.3 handshakes.
//!
//! Verifiers are rustls WebPKI verifiers over the configured CA only. This file
//! does not construct an accept-all verifier and does not disable server-name
//! checks. The `danger` traits below are rustls's verifier interfaces; the only
//! implementations used are `WebPkiServerVerifier` and `WebPkiClientVerifier`.

use std::fs;
use std::io::{self, IoSlice};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use http::Uri;
use hyper_util::rt::TokioIo;
use rustls::client::danger::ServerCertVerifier;
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
use zeroize::Zeroize;

use crate::identity::{leaf_covers_exact_name, principal_from_leaf};
use crate::spec::{SecurityError, TlsMode, TlsSpec};
use crate::AuthenticatedPrincipal;

const MAX_CERT_BYTES: u64 = 1024 * 1024;
const MAX_KEY_BYTES: u64 = 64 * 1024;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Process-wide transport policy. Plaintext is the default.
///
/// Cloning is cheap. Debug output is the mode and server name, never key bytes.
#[derive(Clone)]
pub struct TransportSecurity {
    inner: Inner,
}

#[derive(Clone)]
enum Inner {
    Plaintext,
    Enabled(Arc<Enabled>),
}

struct Enabled {
    mode: TlsMode,
    server_name: String,
    mtls: bool,
    grpc_acceptor: Option<TlsAcceptor>,
    metrics_acceptor: Option<TlsAcceptor>,
    grpc_connector: TlsConnector,
    metrics_connector: TlsConnector,
    /// Kept so tests can see that SNI stays on. Not a verifier bypass.
    grpc_client_sni: bool,
}

/// Peer identity observed on one accepted TLS connection.
///
/// tonic stores this as `ConnectInfo<TlsConnectInfo>` on each request once the
/// connection is served. Method RBAC is not consulted.
#[derive(Debug, Clone)]
pub struct TlsConnectInfo {
    pub principal: Option<AuthenticatedPrincipal>,
    pub server_name: String,
}

/// gRPC connection after a verified TLS handshake.
pub struct TlsServerIo {
    inner: ServerTlsStream<TcpStream>,
    info: TlsConnectInfo,
}

impl std::fmt::Debug for TransportSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Inner::Plaintext => f.write_str("TransportSecurity { mode: off }"),
            Inner::Enabled(enabled) => write!(
                f,
                "TransportSecurity {{ mode: {}, server_name: {} }}",
                enabled.mode, enabled.server_name
            ),
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

    /// Reads and checks material before any listener is bound.
    ///
    /// `mode = off` does not open the certificate paths. Any other mode fails
    /// before returning when a file, chain, name, or key does not check out.
    pub fn load(spec: &TlsSpec) -> Result<Self, SecurityError> {
        if spec.mode() == TlsMode::Off {
            return Ok(Self::plaintext());
        }
        let provider = Arc::new(rustls_pure::provider());
        let roots = Arc::new(load_roots(spec.ca_file())?);
        let identity = load_identity(spec)?;
        if let Some(identity) = &identity {
            if !spec.client_only() {
                verify_server_certificate(&provider, &roots, identity, spec.server_name())?;
                leaf_covers_exact_name(&identity.certs[0], spec.server_name())?;
            }
            if spec.mode() == TlsMode::Mtls || !spec.client_only() {
                let _ = principal_from_leaf(&identity.certs[0])?;
            }
            if spec.mode() == TlsMode::Mtls {
                verify_client_certificate(&provider, &roots, identity)?;
            }
        } else if spec.mode() == TlsMode::Mtls || !spec.client_only() {
            return Err(SecurityError::new(
                "refusing to start: TLS identity certificate and key are required".to_string(),
            ));
        }

        let grpc_client = client_config(
            &provider,
            &roots,
            identity.as_ref(),
            spec.mode() == TlsMode::Mtls,
            b"h2",
        )?;
        let metrics_client = client_config(
            &provider,
            &roots,
            identity.as_ref(),
            spec.mode() == TlsMode::Mtls,
            b"http/1.1",
        )?;
        let (grpc_acceptor, metrics_acceptor) = if spec.client_only() {
            (None, None)
        } else {
            let identity = identity.as_ref().ok_or_else(|| {
                SecurityError::new(
                    "refusing to start: TLS identity certificate and key are required".to_string(),
                )
            })?;
            (
                Some(TlsAcceptor::from(Arc::new(server_config(
                    &provider,
                    &roots,
                    identity,
                    spec.mode() == TlsMode::Mtls,
                    b"h2",
                )?))),
                Some(TlsAcceptor::from(Arc::new(server_config(
                    &provider,
                    &roots,
                    identity,
                    spec.mode() == TlsMode::Mtls,
                    b"http/1.1",
                )?))),
            )
        };
        if !grpc_client.enable_sni {
            return Err(SecurityError::new(
                "refusing to start: TLS client name verification is off".to_string(),
            ));
        }
        Ok(Self {
            inner: Inner::Enabled(Arc::new(Enabled {
                mode: spec.mode(),
                server_name: spec.server_name().to_string(),
                mtls: spec.mode() == TlsMode::Mtls,
                grpc_acceptor,
                metrics_acceptor,
                grpc_connector: TlsConnector::from(Arc::new(grpc_client)),
                metrics_connector: TlsConnector::from(Arc::new(metrics_client)),
                grpc_client_sni: true,
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

    /// True when outbound handshakes use the WebPKI verifier and SNI.
    pub fn verifies_peer_certificates(&self) -> bool {
        match &self.inner {
            Inner::Plaintext => false,
            Inner::Enabled(enabled) => enabled.grpc_client_sni,
        }
    }

    pub fn prepare_url(&self, addr: &str) -> Result<String, SecurityError> {
        // The loaded mode is what dials. A plaintext security object refuses https.
        let mode = self.mode();
        let stub = if mode == TlsMode::Off {
            TlsSpec::from_fields("off", "", "", "", "")?
        } else {
            // prepare_url only reads the mode. The paths are not opened.
            TlsSpec::from_fields(mode.as_str(), "cert.pem", "key.pem", "ca.pem", "127.0.0.1")?
        };
        stub.prepare_url(addr)
    }

    /// Connect a tonic channel. `endpoint` must already use [`Self::prepare_url`].
    pub async fn connect(&self, endpoint: Endpoint) -> Result<Channel, SecurityError> {
        match &self.inner {
            Inner::Plaintext => endpoint
                .connect()
                .await
                .map_err(|error| public_error("plaintext connect", error)),
            Inner::Enabled(enabled) => {
                let enabled = Arc::clone(enabled);
                let connector = service_fn(move |uri: Uri| {
                    let enabled = Arc::clone(&enabled);
                    async move { enabled.dial(&uri, false).await.map(TokioIo::new) }
                });
                endpoint
                    .connect_with_connector(connector)
                    .await
                    .map_err(|error| public_error("tls connect", error))
            }
        }
    }

    /// HTTP/1.1 TLS connection, used by the metrics listener's clients.
    pub async fn connect_http(&self, addr: &str) -> Result<ClientHttp, SecurityError> {
        let url = self.prepare_url(addr)?;
        let enabled = self.enabled()?;
        let uri: Uri = url.parse().map_err(|_| {
            SecurityError::new("refusing to dial: the address is not a URI".to_string())
        })?;
        let stream = enabled
            .dial(&uri, true)
            .await
            .map_err(|error| SecurityError::new(public_error("tls connect", error).to_string()))?;
        Ok(ClientHttp { inner: stream })
    }

    pub async fn accept_grpc(&self, stream: TcpStream) -> Result<TlsServerIo, SecurityError> {
        let enabled = self.enabled()?;
        let acceptor = enabled.grpc_acceptor.clone().ok_or_else(|| {
            SecurityError::new(
                "refusing TLS accept: this configuration does not listen".to_string(),
            )
        })?;
        let tls = accept_timeout(acceptor.accept(stream)).await?;
        finish_server_handshake(tls, enabled, b"h2")
    }

    pub async fn accept_metrics(&self, stream: TcpStream) -> Result<MetricsIo, SecurityError> {
        let enabled = self.enabled()?;
        let acceptor = enabled.metrics_acceptor.clone().ok_or_else(|| {
            SecurityError::new(
                "refusing TLS accept: this configuration does not listen".to_string(),
            )
        })?;
        let tls = accept_timeout(acceptor.accept(stream)).await?;
        let io = finish_server_handshake(tls, enabled, b"http/1.1")?;
        Ok(MetricsIo {
            inner: io.inner,
            principal: io.info.principal,
        })
    }
}

/// Metrics connection after a verified TLS handshake.
pub struct MetricsIo {
    inner: ServerTlsStream<TcpStream>,
    principal: Option<AuthenticatedPrincipal>,
}

impl MetricsIo {
    pub fn principal(&self) -> Option<&AuthenticatedPrincipal> {
        self.principal.as_ref()
    }
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

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl tonic::transport::server::Connected for TlsServerIo {
    type ConnectInfo = TlsConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for MetricsIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for MetricsIo {
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

impl TransportSecurity {
    fn enabled(&self) -> Result<&Arc<Enabled>, SecurityError> {
        match &self.inner {
            Inner::Enabled(enabled) => Ok(enabled),
            Inner::Plaintext => Err(SecurityError::new(
                "refusing TLS accept: [security.tls] mode is off".to_string(),
            )),
        }
    }
}

impl Enabled {
    async fn dial(
        &self,
        uri: &Uri,
        http1: bool,
    ) -> Result<tokio_rustls::client::TlsStream<TcpStream>, io::Error> {
        if uri.scheme_str() != Some("https") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "refusing non-https URI for a TLS client",
            ));
        }
        if uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "refusing URI userinfo",
            ));
        }
        let host = uri
            .host()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TLS URI has no host"))?;
        let port = uri
            .port_u16()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TLS URI has no port"))?;
        let server_name = ServerName::try_from(host.to_string()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS URI host is not a certificate name",
            )
        })?;
        let tcp = tokio::time::timeout(HANDSHAKE_TIMEOUT, TcpStream::connect((host, port)))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "tls connect timed out"))?
            .map_err(|error| io::Error::new(io::ErrorKind::ConnectionRefused, error.to_string()))?;
        let _ = tcp.set_nodelay(true);
        let connector = if http1 {
            self.metrics_connector.clone()
        } else {
            self.grpc_connector.clone()
        };
        let tls = tokio::time::timeout(HANDSHAKE_TIMEOUT, connector.connect(server_name, tcp))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "tls handshake timed out"))?
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    public_error("tls handshake", error).to_string(),
                )
            })?;
        let expected: &[u8] = if http1 { b"http/1.1" } else { b"h2" };
        let (_, connection) = tls.get_ref();
        if connection.alpn_protocol() != Some(expected) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer did not negotiate the required application protocol",
            ));
        }
        Ok(tls)
    }
}

/// Metrics client connection after the peer certificate and hostname are verified.
pub struct ClientHttp {
    inner: tokio_rustls::client::TlsStream<TcpStream>,
}

impl AsyncRead for ClientHttp {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for ClientHttp {
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

struct IdentityMaterial {
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

fn load_identity(spec: &TlsSpec) -> Result<Option<IdentityMaterial>, SecurityError> {
    if spec.cert_file().as_os_str().is_empty() {
        return Ok(None);
    }
    let certs = load_certs(spec.cert_file())?;
    let key = load_key(spec.key_file())?;
    Ok(Some(IdentityMaterial { certs, key }))
}

fn load_roots(path: &Path) -> Result<RootCertStore, SecurityError> {
    let bytes = read_limited(path, MAX_CERT_BYTES)?;
    let mut roots = RootCertStore::empty();
    let mut count = 0usize;
    for cert in CertificateDer::pem_slice_iter(&bytes) {
        let cert = cert.map_err(|error| {
            public_error(&format!("refusing to start: {}", path.display()), error)
        })?;
        roots
            .add(cert)
            .map_err(|error| public_error("refusing to start: CA certificate", error))?;
        count += 1;
    }
    if count == 0 {
        return Err(SecurityError::new(format!(
            "refusing to start: {} contains no CA certificate",
            path.display()
        )));
    }
    Ok(roots)
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, SecurityError> {
    let bytes = read_limited(path, MAX_CERT_BYTES)?;
    let certs = CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| public_error(&format!("refusing to start: {}", path.display()), error))?;
    if certs.is_empty() {
        return Err(SecurityError::new(format!(
            "refusing to start: {} contains no certificate",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, SecurityError> {
    let mut bytes = read_limited(path, MAX_KEY_BYTES)?;
    let key = PrivateKeyDer::from_pem_slice(&bytes)
        .map_err(|error| public_error(&format!("refusing to start: {}", path.display()), error));
    bytes.zeroize();
    key
}

fn read_limited(path: &Path, max: u64) -> Result<Vec<u8>, SecurityError> {
    let metadata = fs::metadata(path).map_err(|error| {
        SecurityError::new(format!(
            "refusing to start: cannot read {}: {error}",
            path.display()
        ))
    })?;
    if !metadata.is_file() {
        return Err(SecurityError::new(format!(
            "refusing to start: {} is not a regular file",
            path.display()
        )));
    }
    if metadata.len() == 0 || metadata.len() > max {
        return Err(SecurityError::new(format!(
            "refusing to start: {} has an invalid length",
            path.display()
        )));
    }
    fs::read(path).map_err(|error| {
        SecurityError::new(format!(
            "refusing to start: cannot read {}: {error}",
            path.display()
        ))
    })
}

fn verify_server_certificate(
    provider: &Arc<rustls::crypto::CryptoProvider>,
    roots: &Arc<RootCertStore>,
    identity: &IdentityMaterial,
    server_name: &str,
) -> Result<(), SecurityError> {
    let verifier =
        WebPkiServerVerifier::builder_with_provider(Arc::clone(roots), Arc::clone(provider))
            .build()
            .map_err(|error| public_error("refusing to start: server verifier", error))?;
    let name = ServerName::try_from(server_name.to_string()).map_err(|_| {
        SecurityError::new("refusing to start: server_name is not a DNS name or IP".to_string())
    })?;
    verifier
        .verify_server_cert(
            &identity.certs[0],
            &identity.certs[1..],
            &name,
            &[],
            UnixTime::now(),
        )
        .map(|_| ())
        .map_err(|error| public_error("refusing to start: server certificate", error))
}

fn verify_client_certificate(
    provider: &Arc<rustls::crypto::CryptoProvider>,
    roots: &Arc<RootCertStore>,
    identity: &IdentityMaterial,
) -> Result<(), SecurityError> {
    let verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::clone(roots), Arc::clone(provider))
            .build()
            .map_err(|error| public_error("refusing to start: client verifier", error))?;
    verifier
        .verify_client_cert(&identity.certs[0], &identity.certs[1..], UnixTime::now())
        .map(|_| ())
        .map_err(|error| public_error("refusing to start: client certificate", error))
}

fn server_config(
    provider: &Arc<rustls::crypto::CryptoProvider>,
    roots: &Arc<RootCertStore>,
    identity: &IdentityMaterial,
    mtls: bool,
    alpn: &[u8],
) -> Result<ServerConfig, SecurityError> {
    let builder = ServerConfig::builder_with_provider(Arc::clone(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| public_error("refusing to start: TLS 1.3", error))?;
    let mut config = if mtls {
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::clone(roots), Arc::clone(provider))
                .build()
                .map_err(|error| public_error("refusing to start: client verifier", error))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(identity.certs.clone(), clone_key(&identity.key)?)
            .map_err(|error| public_error("refusing to start: server certificate", error))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(identity.certs.clone(), clone_key(&identity.key)?)
            .map_err(|error| public_error("refusing to start: server certificate", error))?
    };
    config.alpn_protocols = vec![alpn.to_vec()];
    Ok(config)
}

fn client_config(
    provider: &Arc<rustls::crypto::CryptoProvider>,
    roots: &Arc<RootCertStore>,
    identity: Option<&IdentityMaterial>,
    mtls: bool,
    alpn: &[u8],
) -> Result<ClientConfig, SecurityError> {
    let builder = ClientConfig::builder_with_provider(Arc::clone(provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|error| public_error("refusing to start: TLS 1.3", error))?
        .with_root_certificates(Arc::clone(roots));
    let mut config = if mtls {
        let identity = identity.ok_or_else(|| {
            SecurityError::new(
                "refusing to start: mtls requires a client certificate and key".to_string(),
            )
        })?;
        builder
            .with_client_auth_cert(identity.certs.clone(), clone_key(&identity.key)?)
            .map_err(|error| public_error("refusing to start: client certificate", error))?
    } else {
        builder.with_no_client_auth()
    };
    config.alpn_protocols = vec![alpn.to_vec()];
    config.enable_sni = true;
    Ok(config)
}

fn clone_key(key: &PrivateKeyDer<'static>) -> Result<PrivateKeyDer<'static>, SecurityError> {
    Ok(key.clone_key())
}

fn finish_server_handshake(
    tls: ServerTlsStream<TcpStream>,
    enabled: &Enabled,
    alpn: &[u8],
) -> Result<TlsServerIo, SecurityError> {
    let principal = {
        let (_, connection) = tls.get_ref();
        if connection.alpn_protocol() != Some(alpn) {
            return Err(SecurityError::new(
                "tls handshake rejected: peer did not negotiate the required application protocol"
                    .to_string(),
            ));
        }
        let presented = connection
            .peer_certificates()
            .map(|certs| !certs.is_empty())
            .unwrap_or(false);
        if enabled.mtls {
            let leaf = connection
                .peer_certificates()
                .and_then(|certs| certs.first())
                .ok_or_else(|| {
                    SecurityError::new(
                        "tls handshake rejected: mtls peer presented no certificate".to_string(),
                    )
                })?;
            Some(principal_from_leaf(leaf.as_ref())?)
        } else if presented {
            return Err(SecurityError::new(
                "tls handshake rejected: a client certificate was presented but mode is tls, not mtls"
                    .to_string(),
            ));
        } else {
            None
        }
    };
    Ok(TlsServerIo {
        inner: tls,
        info: TlsConnectInfo {
            principal,
            server_name: enabled.server_name.clone(),
        },
    })
}

async fn accept_timeout(
    accept: impl std::future::Future<Output = io::Result<ServerTlsStream<TcpStream>>>,
) -> Result<ServerTlsStream<TcpStream>, SecurityError> {
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, accept).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(error)) => Err(public_error("tls handshake rejected", error)),
        Err(_) => Err(SecurityError::new(
            "tls handshake rejected: timed out".to_string(),
        )),
    }
}

fn public_error(context: &str, error: impl std::fmt::Display) -> SecurityError {
    let detail = error.to_string();
    if detail.contains("BEGIN")
        || detail.contains("PRIVATE")
        || detail.contains("SECRET")
        || detail.len() > 300
    {
        SecurityError::new(format!("{context}: certificate material was rejected"))
    } else {
        SecurityError::new(format!("{context}: {detail}"))
    }
}
