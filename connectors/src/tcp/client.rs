use std::{sync::Arc, time::Duration};

use futures_util::StreamExt;
use rustls::{
    DigitallySignedStruct, Error as TlsError, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};
use tracing::{debug, info, warn};

use super::{
    connection::{Connection, ConnectionError},
    frame::{Frame, FrameType},
};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls error: {0}")]
    Tls(#[from] TlsError),
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),
    #[error("invalid server name")]
    InvalidServerName,
    #[error("handshake failed")]
    HandshakeFailed,
}

/// A certificate verifier that accepts any certificate
#[derive(Debug)]
struct AcceptAnyCert;

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
        ]
    }
}

pub struct Client {
    config: Arc<rustls::ClientConfig>,
    server_name: ServerName<'static>,
    addr: String,
    connector: TlsConnector,
}

impl Client {
    /// Create a new client with the given server name and address
    pub fn new<N: AsRef<str>>(server_name: N, addr: impl Into<String>) -> Result<Self, ClientError> {
        let server_name = ServerName::try_from(server_name.as_ref()).map_err(|_| ClientError::InvalidServerName)?.to_owned();

        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let config = rustls::ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth();

        let config = Arc::new(config);
        let connector = TlsConnector::from(config.clone());

        Ok(Self { config, server_name, addr: addr.into(), connector })
    }

    /// Create a new client that accepts any server certificate
    pub fn new_insecure<N: AsRef<str>>(server_name: N, addr: impl Into<String>) -> Result<Self, ClientError> {
        let server_name = ServerName::try_from(server_name.as_ref()).map_err(|_| ClientError::InvalidServerName)?.to_owned();

        let config =
            rustls::ClientConfig::builder().dangerous().with_custom_certificate_verifier(Arc::new(AcceptAnyCert)).with_no_client_auth();

        let config = Arc::new(config);
        let connector = TlsConnector::from(config.clone());

        Ok(Self { config, server_name, addr: addr.into(), connector })
    }

    /// Connect to the server
    pub async fn connect(&self) -> Result<Connection<TlsStream<TcpStream>>, ClientError> {
        info!("Connecting to {}", self.addr);
        let stream = TcpStream::connect(&self.addr).await?;
        stream.set_nodelay(true)?;

        debug!("Starting TLS handshake");
        let stream = self.connector.connect(self.server_name.clone(), stream).await?;
        debug!("TLS handshake completed");

        let mut conn = Connection::new(stream);
        debug!("Connection created, waiting for server handshake");

        // Perform protocol handshake
        conn.perform_client_handshake().await?;
        debug!("Protocol handshake completed");

        Ok(conn)
    }
}
