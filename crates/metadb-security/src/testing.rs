//! Ephemeral Ed25519 certificates for tests.
//!
//! These keys are generated in memory for a unit test and are not production
//! credentials. Callers must not log the PEM strings.

use std::net::IpAddr;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use der::asn1::{BitString, Ia5String};
use der::pem::LineEnding;
use der::{DecodePem, Encode, EncodePem};
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use x509_cert::builder::{Builder, CertificateBuilder, Profile};
use x509_cert::ext::pkix::name::GeneralName;
use x509_cert::ext::pkix::SubjectAltName;
use x509_cert::name::Name;
use x509_cert::serial_number::SerialNumber;
use x509_cert::spki::SubjectPublicKeyInfoOwned;
use x509_cert::time::Validity;
use x509_cert::Certificate;

use crate::AuthenticatedPrincipal;

/// A test CA. Debug output does not include the private key.
pub struct TestCa {
    key: SigningKey,
    name: Name,
    cert_pem: String,
    next_serial: u32,
}

/// Names and lifetime for one issued certificate.
pub struct IssueRequest {
    pub dns: Vec<String>,
    pub ips: Vec<IpAddr>,
    pub uris: Vec<String>,
    pub not_before: SystemTime,
    pub not_after: SystemTime,
}

impl IssueRequest {
    pub fn valid_for(dns: Vec<String>, ips: Vec<IpAddr>, uris: Vec<String>) -> Self {
        let now = SystemTime::now();
        Self {
            dns,
            ips,
            uris,
            not_before: now
                .checked_sub(Duration::from_secs(3600))
                .unwrap_or(UNIX_EPOCH),
            not_after: now + Duration::from_secs(24 * 3600),
        }
    }
}

/// Certificate and key PEM. Neither field is printed by Debug.
pub struct IssuedCert {
    pub cert_pem: String,
    pub key_pem: String,
}

impl std::fmt::Debug for TestCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TestCa")
    }
}

impl std::fmt::Debug for IssuedCert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("IssuedCert")
    }
}

impl TestCa {
    pub fn generate() -> Result<Self, String> {
        let key = SigningKey::generate(&mut OsRng);
        let name = Name::from_str("CN=metadb-test-ca").map_err(|error| error.to_string())?;
        let cert = issue_with(
            &key,
            &key.verifying_key(),
            Profile::Root,
            &name,
            &[],
            validity_now(Duration::from_secs(24 * 3600))?,
            1,
        )?;
        Ok(Self {
            key,
            name,
            cert_pem: cert,
            next_serial: 2,
        })
    }

    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    pub fn issue(&mut self, request: IssueRequest) -> Result<IssuedCert, String> {
        let key = SigningKey::generate(&mut OsRng);
        let subject = Name::from_str("CN=metadb-test").map_err(|error| error.to_string())?;
        let names = general_names(&request)?;
        let serial = self.next_serial;
        self.next_serial = self.next_serial.saturating_add(1);
        let profile = Profile::Leaf {
            issuer: self.name.clone(),
            enable_key_agreement: false,
            enable_key_encipherment: false,
        };
        let cert = issue_with(
            &self.key,
            &key.verifying_key(),
            profile,
            &subject,
            &names,
            validity_between(request.not_before, request.not_after)?,
            serial,
        )?;
        let key_pem = key
            .to_pkcs8_pem(LineEnding::LF)
            .map_err(|error| error.to_string())?
            .to_string();
        Ok(IssuedCert {
            cert_pem: cert,
            key_pem,
        })
    }
}

fn issue_with(
    signer: &SigningKey,
    subject_key: &VerifyingKey,
    profile: Profile,
    subject: &Name,
    names: &[GeneralName],
    validity: Validity,
    serial: u32,
) -> Result<String, String> {
    let public_key =
        SubjectPublicKeyInfoOwned::from_key(*subject_key).map_err(|error| error.to_string())?;
    let mut builder = CertificateBuilder::new(
        profile,
        SerialNumber::from(serial),
        validity,
        subject.clone(),
        public_key,
        signer,
    )
    .map_err(|error| error.to_string())?;
    if !names.is_empty() {
        builder
            .add_extension(&SubjectAltName(names.to_vec()))
            .map_err(|error| error.to_string())?;
    }
    let tbs = builder.finalize().map_err(|error| error.to_string())?;
    let signature = signer.sign(&tbs);
    let signature =
        BitString::from_bytes(&signature.to_bytes()).map_err(|error| error.to_string())?;
    let certificate: Certificate = builder
        .assemble(signature)
        .map_err(|error| error.to_string())?;
    certificate
        .to_pem(LineEnding::LF)
        .map_err(|error| error.to_string())
}

fn general_names(request: &IssueRequest) -> Result<Vec<GeneralName>, String> {
    let mut names = Vec::new();
    for dns in &request.dns {
        names.push(GeneralName::DnsName(
            Ia5String::new(dns).map_err(|error| error.to_string())?,
        ));
    }
    for uri in &request.uris {
        names.push(GeneralName::UniformResourceIdentifier(
            Ia5String::new(uri).map_err(|error| error.to_string())?,
        ));
    }
    for ip in &request.ips {
        names.push(GeneralName::from(*ip));
    }
    Ok(names)
}

fn validity_now(duration: Duration) -> Result<Validity, String> {
    Validity::from_now(duration).map_err(|error| error.to_string())
}

fn validity_between(not_before: SystemTime, not_after: SystemTime) -> Result<Validity, String> {
    Ok(Validity {
        not_before: x509_cert::time::Time::try_from(not_before)
            .map_err(|error| error.to_string())?,
        not_after: x509_cert::time::Time::try_from(not_after).map_err(|error| error.to_string())?,
    })
}

/// Reads a principal back from a certificate PEM. Tests use this to lock the
/// identity string without going through a listener.
pub fn principal_from_cert_pem(pem: &str) -> Result<AuthenticatedPrincipal, String> {
    let certificate = Certificate::from_pem(pem).map_err(|error| error.to_string())?;
    let der = certificate.to_der().map_err(|error| error.to_string())?;
    crate::identity::principal_from_leaf(&der).map_err(|error| error.to_string())
}
