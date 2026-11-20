//! Test-only certificate minting. This module is not linked into `metadbd`.
//!
//! The keys are Ed25519 from `ed25519-dalek`. They are not the ring provider
//! and they are not a second TLS stack.

use std::net::IpAddr;
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use const_oid::db::rfc5280::{ID_KP_CLIENT_AUTH, ID_KP_SERVER_AUTH};
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};
use x509_cert::builder::{Builder, CertificateBuilder, Profile};
use x509_cert::der::asn1::{BitString, Ia5String};
use x509_cert::der::{pem::LineEnding, EncodePem};
use x509_cert::ext::pkix::name::GeneralName;
use x509_cert::ext::pkix::{ExtendedKeyUsage, SubjectAltName};
use x509_cert::name::Name;
use x509_cert::serial_number::SerialNumber;
use x509_cert::spki::SubjectPublicKeyInfoOwned;
use x509_cert::time::{Time, Validity};

pub struct TestCa {
    name: Name,
    signer: SigningKey,
    pem: String,
    next_serial: u32,
}

pub struct IssuedCert {
    pub cert_pem: String,
    pub key_pem: String,
}

impl TestCa {
    pub fn new(common_name: &str) -> Self {
        let signer = key_from_seed(1);
        let name = Name::from_str(&format!("CN={common_name}")).expect("name");
        let public = SubjectPublicKeyInfoOwned::from_key(signer.verifying_key()).expect("spki");
        let builder = CertificateBuilder::new(
            Profile::Root,
            SerialNumber::from(1u32),
            validity_from_now(Duration::from_secs(24 * 60 * 60)),
            name.clone(),
            public,
            &signer,
        )
        .expect("ca builder");
        let cert = finish(builder);
        Self {
            name,
            signer,
            pem: cert.to_pem(LineEnding::LF).expect("ca pem"),
            next_serial: 2,
        }
    }

    pub fn pem(&self) -> &str {
        &self.pem
    }

    pub fn issue(&mut self, request: IssueRequest<'_>) -> IssuedCert {
        let signer = key_from_seed(self.next_serial);
        let public = SubjectPublicKeyInfoOwned::from_key(signer.verifying_key()).expect("spki");
        let serial = self.next_serial;
        self.next_serial += 1;
        let mut builder = CertificateBuilder::new(
            Profile::Leaf {
                issuer: self.name.clone(),
                enable_key_agreement: false,
                enable_key_encipherment: false,
            },
            SerialNumber::from(serial),
            request.validity,
            Name::from_str("CN=metadb-test").expect("leaf name"),
            public,
            &self.signer,
        )
        .expect("leaf builder");
        let mut names = Vec::new();
        if let Some(uri) = request.uri {
            names.push(GeneralName::UniformResourceIdentifier(
                Ia5String::new(uri).expect("uri"),
            ));
        }
        if let Some(dns) = request.dns {
            names.push(GeneralName::DnsName(Ia5String::new(dns).expect("dns")));
        }
        if let Some(ip) = request.ip {
            names.push(GeneralName::from(ip));
        }
        if !names.is_empty() {
            builder.add_extension(&SubjectAltName(names)).expect("san");
        }
        let mut purposes = Vec::new();
        if request.server_auth {
            purposes.push(ID_KP_SERVER_AUTH);
        }
        if request.client_auth {
            purposes.push(ID_KP_CLIENT_AUTH);
        }
        if !purposes.is_empty() {
            builder
                .add_extension(&ExtendedKeyUsage(purposes))
                .expect("eku");
        }
        let cert = finish(builder);
        IssuedCert {
            cert_pem: cert.to_pem(LineEnding::LF).expect("cert pem"),
            key_pem: signer
                .to_pkcs8_pem(LineEnding::LF)
                .expect("key pem")
                .to_string(),
        }
    }
}

pub struct IssueRequest<'a> {
    pub uri: Option<&'a str>,
    pub dns: Option<&'a str>,
    pub ip: Option<IpAddr>,
    pub server_auth: bool,
    pub client_auth: bool,
    pub validity: Validity,
}

impl IssueRequest<'static> {
    pub fn server_and_client(ip: IpAddr, uri: &'static str) -> Self {
        Self {
            uri: Some(uri),
            dns: None,
            ip: Some(ip),
            server_auth: true,
            client_auth: true,
            validity: validity_from_now(Duration::from_secs(60 * 60)),
        }
    }
}

pub fn validity_from_now(duration: Duration) -> Validity {
    Validity::from_now(duration).expect("validity")
}

pub fn expired_validity() -> Validity {
    let now = SystemTime::now();
    Validity {
        not_before: Time::try_from(now - Duration::from_secs(48 * 60 * 60)).expect("start"),
        not_after: Time::try_from(now - Duration::from_secs(24 * 60 * 60)).expect("end"),
    }
}

pub fn write_bundle(dir: &Path, ca: &str, cert: &str, key: &str) -> (String, String, String) {
    let ca_path = dir.join("ca.pem");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&ca_path, ca).expect("ca");
    std::fs::write(&cert_path, cert).expect("cert");
    std::fs::write(&key_path, key).expect("key");
    (
        cert_path.display().to_string(),
        key_path.display().to_string(),
        ca_path.display().to_string(),
    )
}

fn finish(mut builder: CertificateBuilder<'_, SigningKey>) -> x509_cert::Certificate {
    let body = builder.finalize().expect("tbs");
    let signature = builder.signer().sign(&body);
    builder
        .assemble(BitString::from_bytes(signature.to_bytes().as_ref()).expect("bit string"))
        .expect("assemble")
}

fn key_from_seed(seed: u32) -> SigningKey {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&seed.to_le_bytes());
    bytes[31] = 0x5a;
    SigningKey::from_bytes(&bytes)
}
