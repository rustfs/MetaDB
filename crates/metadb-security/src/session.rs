//! Plaintext tonic channels. TLS modes never reach this type.

use tonic::transport::{Channel, Endpoint};

use crate::spec::{SecurityError, TlsMode, TlsSpec};

/// Process-wide transport policy after startup checks.
///
/// The only value `load` returns is plaintext. Debug output is the mode.
#[derive(Clone, Debug)]
pub struct TransportSecurity {
    mode: TlsMode,
}

impl Default for TransportSecurity {
    fn default() -> Self {
        Self::plaintext()
    }
}

impl TransportSecurity {
    pub fn plaintext() -> Self {
        Self { mode: TlsMode::Off }
    }

    /// Refuses `tls` and `mtls` before any certificate file is opened.
    ///
    /// This function does not parse PEM, does not copy a private key, and does
    /// not zeroize one. There is no key material in memory to wipe.
    pub fn load(spec: &TlsSpec) -> Result<Self, SecurityError> {
        match spec.mode() {
            TlsMode::Off => Ok(Self::plaintext()),
            TlsMode::Tls | TlsMode::Mtls => Err(SecurityError::unavailable()),
        }
    }

    pub fn mode(&self) -> TlsMode {
        self.mode
    }

    pub fn is_plaintext(&self) -> bool {
        self.mode == TlsMode::Off
    }

    pub fn server_name(&self) -> Option<&str> {
        None
    }

    /// Plaintext does not verify peer certificates. TLS is not linked.
    pub fn verifies_peer_certificates(&self) -> bool {
        false
    }

    pub fn prepare_url(&self, addr: &str) -> Result<String, SecurityError> {
        TlsSpec::from_fields("off", "", "", "", "")?.prepare_url(addr)
    }

    /// Connect a tonic channel. `endpoint` must already use [`Self::prepare_url`].
    pub async fn connect(&self, endpoint: Endpoint) -> Result<Channel, SecurityError> {
        endpoint
            .connect()
            .await
            .map_err(|error| SecurityError::new(format!("plaintext connect: {error}")))
    }
}
