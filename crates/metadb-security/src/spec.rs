//! Structural TLS configuration. This module does not read certificate files.

use std::net::IpAddr;
use std::path::PathBuf;

/// How this process protects gRPC, metrics, Raft, control, and admin traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsMode {
    /// Plaintext. This is the default and keeps the existing bearer rules.
    Off,
    /// Server-authenticated TLS 1.3. Clients verify the server certificate.
    Tls,
    /// Mutual TLS 1.3. Each side presents a certificate and verifies the peer.
    Mtls,
}

impl TlsMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Tls => "tls",
            Self::Mtls => "mtls",
        }
    }
}

impl std::fmt::Display for TlsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Failure that is safe to return to an operator. It does not contain key bytes.
#[derive(Debug, Clone)]
pub struct SecurityError {
    message: String,
}

impl SecurityError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn rbac_not_installed() -> Self {
        Self::new(
            "RBAC is not installed. An authenticated certificate principal is not a role, and method authorization is not enforced. The scale stream's role separation must supply a method authorizer before least-privilege RBAC is active",
        )
    }
}

impl std::fmt::Display for SecurityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SecurityError {}

/// Validated TLS field shape. [`crate::TransportSecurity::load`] opens the files only after this shape check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsSpec {
    mode: TlsMode,
    cert_file: PathBuf,
    key_file: PathBuf,
    ca_file: PathBuf,
    /// Exact DNS name or IP in this process's certificate, and the listen host.
    /// Empty for a client that does not listen.
    server_name: String,
    client_only: bool,
}

impl TlsSpec {
    pub fn mode(&self) -> TlsMode {
        self.mode
    }

    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// Server or all-in-one process. `mode` is `off`, `tls`, or `mtls`.
    ///
    /// `off` rejects any certificate field. `tls` and `mtls` require all four
    /// fields. `server_name` is a DNS name or IP, never a wildcard.
    pub fn from_fields(
        mode: &str,
        cert_file: &str,
        key_file: &str,
        ca_file: &str,
        server_name: &str,
    ) -> Result<Self, SecurityError> {
        Self::build(mode, cert_file, key_file, ca_file, server_name, false)
    }

    /// Admin or other client. There is no separate name override. `tls` and
    /// `mtls` still fail in `TransportSecurity::load` without opening files.
    pub fn from_client_fields(
        mode: &str,
        cert_file: &str,
        key_file: &str,
        ca_file: &str,
    ) -> Result<Self, SecurityError> {
        Self::build(mode, cert_file, key_file, ca_file, "", true)
    }

    fn build(
        mode: &str,
        cert_file: &str,
        key_file: &str,
        ca_file: &str,
        server_name: &str,
        client_only: bool,
    ) -> Result<Self, SecurityError> {
        let mode_raw = mode.trim();
        let mode_raw = if mode_raw.is_empty() { "off" } else { mode_raw };
        let mode = match mode_raw.to_ascii_lowercase().as_str() {
            "off" => TlsMode::Off,
            "tls" => TlsMode::Tls,
            "mtls" => TlsMode::Mtls,
            other => {
                return Err(SecurityError::new(format!(
                    "refusing to start: [security.tls].mode `{other}` is not off, tls, or mtls"
                )));
            }
        };
        let cert_file = cert_file.trim();
        let key_file = key_file.trim();
        let ca_file = ca_file.trim();
        let server_name = server_name.trim();
        if mode == TlsMode::Off {
            if !cert_file.is_empty()
                || !key_file.is_empty()
                || !ca_file.is_empty()
                || !server_name.is_empty()
            {
                return Err(SecurityError::new(
                    "refusing to start: [security.tls] material is set while mode is off. Set mode to tls or mtls, or remove the material".to_string(),
                ));
            }
            return Ok(Self {
                mode,
                cert_file: PathBuf::new(),
                key_file: PathBuf::new(),
                ca_file: PathBuf::new(),
                server_name: String::new(),
                client_only,
            });
        }
        if ca_file.is_empty()
            || cert_file.contains('\0')
            || key_file.contains('\0')
            || ca_file.contains('\0')
        {
            return Err(SecurityError::new(
                "refusing to start: [security.tls] mode tls or mtls requires cert_file, key_file, and ca_file"
                    .to_string(),
            ));
        }
        let needs_identity = !client_only || mode == TlsMode::Mtls;
        if needs_identity && (cert_file.is_empty() || key_file.is_empty()) {
            return Err(SecurityError::new(
                "refusing to start: [security.tls] mode tls or mtls requires cert_file, key_file, and ca_file"
                    .to_string(),
            ));
        }
        if client_only && mode == TlsMode::Tls && (!cert_file.is_empty() || !key_file.is_empty()) {
            return Err(SecurityError::new(
                "refusing to start: a tls client certificate is unused unless mode is mtls. Remove cert_file and key_file, or set mode to mtls"
                    .to_string(),
            ));
        }
        if !client_only && (cert_file.is_empty() || key_file.is_empty() || server_name.is_empty()) {
            return Err(SecurityError::new(
                "refusing to start: [security.tls] mode tls or mtls requires cert_file, key_file, ca_file, and server_name"
                    .to_string(),
            ));
        }
        let server_name = if client_only {
            String::new()
        } else {
            canonicalize_server_name(server_name)?
        };
        Ok(Self {
            mode,
            cert_file: PathBuf::from(cert_file),
            key_file: PathBuf::from(key_file),
            ca_file: PathBuf::from(ca_file),
            server_name,
            client_only,
        })
    }

    pub(crate) fn cert_file(&self) -> &std::path::Path {
        &self.cert_file
    }

    pub(crate) fn key_file(&self) -> &std::path::Path {
        &self.key_file
    }

    pub(crate) fn ca_file(&self) -> &std::path::Path {
        &self.ca_file
    }

    pub(crate) fn client_only(&self) -> bool {
        self.client_only
    }

    /// Scheme and host that match this mode. `http` is refused while TLS is on.
    /// `https` is refused while TLS is off. Userinfo is refused so a secret in
    /// the URL cannot be dialed or logged.
    pub fn prepare_url(&self, addr: &str) -> Result<String, SecurityError> {
        let addr = addr.trim();
        if addr.is_empty() || addr.contains('@') || addr.contains(' ') || addr.contains('\\') {
            return Err(SecurityError::new(
                "refusing to dial: the address is empty or contains userinfo or whitespace"
                    .to_string(),
            ));
        }
        let (scheme, rest) = if let Some(rest) = addr.strip_prefix("https://") {
            ("https", rest)
        } else if let Some(rest) = addr.strip_prefix("http://") {
            ("http", rest)
        } else if addr.contains("://") {
            return Err(SecurityError::new(
                "refusing to dial: only http and https schemes are recognized".to_string(),
            ));
        } else {
            ("", addr)
        };
        if rest.contains('/') || rest.contains('?') || rest.contains('#') {
            return Err(SecurityError::new(
                "refusing to dial: the address must be host:port with no path".to_string(),
            ));
        }
        let (host, port) = split_host_port(rest)?;
        let host = canonicalize_host(&host)?;
        if port.is_empty() || !port.bytes().all(|byte| byte.is_ascii_digit()) || port.len() > 5 {
            return Err(SecurityError::new(
                "refusing to dial: the address needs a numeric port".to_string(),
            ));
        }
        let rendered = render_host_port(&host, &port);
        match self.mode {
            TlsMode::Off => {
                if scheme == "https" {
                    return Err(SecurityError::new(
                        "refusing to dial https while [security.tls] mode is off. Set mode to tls or mtls, or use an http address"
                            .to_string(),
                    ));
                }
                Ok(format!("http://{rendered}"))
            }
            TlsMode::Tls | TlsMode::Mtls => {
                if scheme == "http" {
                    return Err(SecurityError::new(
                        "refusing to dial plaintext http while TLS is enabled".to_string(),
                    ));
                }
                Ok(format!("https://{rendered}"))
            }
        }
    }

    /// The listen host must be the certificate name. A wildcard is not a name.
    pub fn reject_listen_host(&self, addr: &str) -> Result<(), SecurityError> {
        if self.mode == TlsMode::Off {
            return Ok(());
        }
        let addr = addr.trim();
        let addr = addr
            .strip_prefix("https://")
            .or_else(|| addr.strip_prefix("http://"))
            .unwrap_or(addr);
        let (host, _) = split_host_port(addr)?;
        if host_is_wildcard(&host) {
            return Err(SecurityError::new(format!(
                "refusing to serve: TLS listen address {addr} is a wildcard. Bind the host named by [security.tls].server_name"
            )));
        }
        let host = canonicalize_host(&host)?;
        if host != self.server_name {
            return Err(SecurityError::new(format!(
                "refusing to serve: TLS listen host {host} must equal [security.tls].server_name {}",
                self.server_name
            )));
        }
        Ok(())
    }
}

pub(crate) fn split_host_port(addr: &str) -> Result<(String, String), SecurityError> {
    if let Some(rest) = addr.strip_prefix('[') {
        let Some((host, port)) = rest.split_once("]:") else {
            return Err(SecurityError::new(
                "refusing to use a bracketed address without a port".to_string(),
            ));
        };
        if host.is_empty() || port.is_empty() {
            return Err(SecurityError::new(
                "refusing to use an empty host or port".to_string(),
            ));
        }
        return Ok((host.to_string(), port.to_string()));
    }
    let Some((host, port)) = addr.rsplit_once(':') else {
        return Err(SecurityError::new(
            "refusing to use an address without a port".to_string(),
        ));
    };
    if host.is_empty() || host.contains(':') || port.is_empty() {
        return Err(SecurityError::new(
            "refusing to use an address that is not host:port".to_string(),
        ));
    }
    Ok((host.to_string(), port.to_string()))
}

fn host_is_wildcard(host: &str) -> bool {
    host.is_empty()
        || host == "*"
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_unspecified())
            .unwrap_or(false)
}

pub(crate) fn canonicalize_host(host: &str) -> Result<String, SecurityError> {
    if host_is_wildcard(host) {
        return Err(SecurityError::new(
            "refusing to use a wildcard or unspecified host as a certificate name".to_string(),
        ));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if ip.is_multicast() {
            return Err(SecurityError::new(
                "refusing to use a multicast address as a certificate name".to_string(),
            ));
        }
        return Ok(ip.to_string());
    }
    validate_dns_name(host)?;
    Ok(host.to_ascii_lowercase())
}

fn canonicalize_server_name(name: &str) -> Result<String, SecurityError> {
    if name.contains('*') {
        return Err(SecurityError::new(
            "refusing to start: [security.tls].server_name must be an exact DNS name or IP, not a wildcard"
                .to_string(),
        ));
    }
    canonicalize_host(name).map_err(|error| {
        SecurityError::new(format!(
            "refusing to start: [security.tls].server_name is not a DNS name or IP ({error})"
        ))
    })
}

fn validate_dns_name(name: &str) -> Result<(), SecurityError> {
    if name.len() > 253
        || name.starts_with('.')
        || name.ends_with('.')
        || name.contains("..")
        || name.contains('*')
        || name.contains(' ')
        || name.contains(':')
        || name.contains('/')
    {
        return Err(SecurityError::new(
            "DNS name is empty, wildcard, or not a single host".to_string(),
        ));
    }
    for label in name.split('.') {
        let bytes = label.as_bytes();
        if label.is_empty()
            || bytes.len() > 63
            || bytes[0] == b'-'
            || bytes[bytes.len() - 1] == b'-'
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
        {
            return Err(SecurityError::new(
                "DNS name has an empty or illegal label".to_string(),
            ));
        }
    }
    Ok(())
}

fn render_host_port(host: &str, port: &str) -> String {
    if host
        .parse::<IpAddr>()
        .ok()
        .filter(|ip| ip.is_ipv6())
        .is_some()
    {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}
