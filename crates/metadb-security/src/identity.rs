//! Certificate identity. The principal is a subject alternative name, not a role
//! and not the shared bearer.

use std::net::IpAddr;

use x509_parser::extensions::{GeneralName, ParsedExtension};
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::spec::SecurityError;

/// Identity taken from a verified peer certificate.
///
/// The string is stable for a future method authorizer. It does not grant any
/// method by itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    identity: String,
}

impl AuthenticatedPrincipal {
    pub fn as_str(&self) -> &str {
        &self.identity
    }

    /// Test helper. Production principals come from [`principal_from_leaf`].
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

pub(crate) fn principal_from_leaf(der: &[u8]) -> Result<AuthenticatedPrincipal, SecurityError> {
    let names = san_names(der)?;
    if names.identity.is_empty() {
        return Err(SecurityError::new(
            "refusing certificate: it has no URI, DNS, or IP subject alternative name".to_string(),
        ));
    }
    if names.identity.len() > 512 {
        return Err(SecurityError::new(
            "refusing certificate: its identity is longer than 512 bytes".to_string(),
        ));
    }
    Ok(AuthenticatedPrincipal {
        identity: names.identity,
    })
}

pub(crate) fn leaf_covers_exact_name(der: &[u8], server_name: &str) -> Result<(), SecurityError> {
    let names = san_names(der)?;
    let covered = if let Ok(ip) = server_name.parse::<IpAddr>() {
        names.ips.iter().any(|candidate| candidate == &ip)
    } else {
        let want = server_name.to_ascii_lowercase();
        names.dns.iter().any(|candidate| candidate == &want)
    };
    if covered {
        Ok(())
    } else {
        Err(SecurityError::new(format!(
            "refusing to start: certificate has no exact subject alternative name for {server_name}"
        )))
    }
}

struct CollectedNames {
    identity: String,
    dns: Vec<String>,
    ips: Vec<IpAddr>,
}

fn san_names(der: &[u8]) -> Result<CollectedNames, SecurityError> {
    let (_, cert) = X509Certificate::from_der(der).map_err(|_| {
        SecurityError::new(
            "refusing certificate: the leaf certificate is not valid DER".to_string(),
        )
    })?;
    let mut uris = Vec::new();
    let mut dns = Vec::new();
    let mut ips = Vec::new();
    for extension in cert.extensions() {
        let ParsedExtension::SubjectAlternativeName(san) = extension.parsed_extension() else {
            continue;
        };
        for name in &san.general_names {
            match name {
                GeneralName::URI(value) => push_text(&mut uris, value, "URI")?,
                GeneralName::DNSName(value) => {
                    if value.contains('*') {
                        return Err(SecurityError::new(
                            "refusing certificate: a wildcard DNS name is not an identity"
                                .to_string(),
                        ));
                    }
                    push_text(&mut dns, value, "DNS")?;
                    if let Some(item) = dns.last_mut() {
                        *item = item.to_ascii_lowercase();
                    }
                }
                GeneralName::IPAddress(bytes) => {
                    let ip = match bytes.len() {
                        4 => {
                            let mut octets = [0u8; 4];
                            octets.copy_from_slice(bytes);
                            IpAddr::from(octets)
                        }
                        16 => {
                            let mut octets = [0u8; 16];
                            octets.copy_from_slice(bytes);
                            IpAddr::from(octets)
                        }
                        _ => {
                            return Err(SecurityError::new(
                                "refusing certificate: an IP subject alternative name has a bad length"
                                    .to_string(),
                            ));
                        }
                    };
                    if ip.is_unspecified() || ip.is_multicast() {
                        return Err(SecurityError::new(
                            "refusing certificate: an unspecified or multicast IP is not an identity"
                                .to_string(),
                        ));
                    }
                    ips.push(ip);
                }
                _ => {}
            }
        }
    }
    uris.sort();
    uris.dedup();
    dns.sort();
    dns.dedup();
    ips.sort();
    ips.dedup();
    let identity = if !uris.is_empty() {
        join_prefixed("uri", &uris)
    } else if !dns.is_empty() {
        join_prefixed("dns", &dns)
    } else if !ips.is_empty() {
        join_prefixed(
            "ip",
            &ips.iter().map(|ip| ip.to_string()).collect::<Vec<_>>(),
        )
    } else {
        String::new()
    };
    Ok(CollectedNames { identity, dns, ips })
}

fn push_text(out: &mut Vec<String>, value: &str, kind: &str) -> Result<(), SecurityError> {
    if value.is_empty() || value.contains(',') || value.contains('\n') || value.contains('\r') {
        return Err(SecurityError::new(format!(
            "refusing certificate: a {kind} subject alternative name is empty or contains a separator"
        )));
    }
    out.push(value.to_string());
    Ok(())
}

fn join_prefixed(prefix: &str, values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("{prefix}:{value}"))
        .collect::<Vec<_>>()
        .join(",")
}
