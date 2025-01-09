use anyhow::{Error, Result, anyhow};
use rcgen::{Certificate, CertificateParams, DnType, IsCa, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Cert {
    pub key: PrivateKeyDer<'static>,
    pub cert: CertificateDer<'static>,
}

impl Cert {
    pub fn self_signed(domains: impl IntoIterator<Item = impl Into<String>>) -> Result<Self, Error> {
        let hosts = domains.into_iter().map(|h| h.into()).collect::<Vec<String>>();
        if hosts.is_empty() {
            return Err(anyhow!("No hosts provided"));
        }

        // Create certificate parameters
        let mut entity = CertificateParams::new(vec![])?;
        entity.is_ca = IsCa::NoCa;
        entity.distinguished_name.push(DnType::CommonName, &hosts[0]);

        // Add SAN entries for each host
        for host in hosts {
            entity.subject_alt_names.push(SanType::DnsName(host.try_into()?));
        }

        entity.not_before = OffsetDateTime::now_utc();
        entity.not_after = OffsetDateTime::now_utc() + time::Duration::days(365);

        let key_pair = KeyPair::generate()?;
        let cert = entity.self_signed(&key_pair)?;

        Ok(Cert { key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der())), cert: cert.der().to_owned() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_self_signed_cert_creation() {
        // Test with single domain
        let result = Cert::self_signed(vec!["example.com"]);
        assert!(result.is_ok());
        let cert = result.unwrap();
        assert!(matches!(cert.key, PrivateKeyDer::Pkcs8(_)));
        assert!(!cert.cert.as_ref().is_empty());

        // Test with multiple domains
        let result = Cert::self_signed(vec!["example.com", "test.com", "localhost"]);
        assert!(result.is_ok());
        let cert = result.unwrap();
        assert!(matches!(cert.key, PrivateKeyDer::Pkcs8(_)));
        assert!(!cert.cert.as_ref().is_empty());

        // Test with empty domains
        let result = Cert::self_signed(Vec::<String>::new());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "No hosts provided");
    }
}
