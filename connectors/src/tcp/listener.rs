use std::sync::Arc;

use futures_util::StreamExt;
use rustls::ServerConfig;
use thiserror::Error;
use tokio::{
    io,
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tracing::{debug, error, info, warn};

use super::{
    connection::{Connection, ConnectionError},
    frame::{Frame, FrameType},
};

#[derive(Debug, Error)]
pub enum ListenerError {
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
}

pub struct Listener {
    acceptor: TlsAcceptor,
    tcp: TcpListener,
}

impl Listener {
    /// Create a new TLS listener
    pub async fn bind(config: Arc<ServerConfig>, addr: impl tokio::net::ToSocketAddrs) -> Result<Self, ListenerError> {
        debug!("Attempting to bind TCP listener");
        let tcp = TcpListener::bind(addr).await?;
        let acceptor = TlsAcceptor::from(config);

        info!("Successfully bound listener on {}", tcp.local_addr()?);
        debug!("TLS acceptor initialized");

        Ok(Self { acceptor, tcp })
    }

    /// Accept a new connection
    pub async fn accept(&self) -> Result<Connection<TlsStream<TcpStream>>, ListenerError> {
        debug!("Entering accept loop");
        loop {
            debug!("Waiting for new TCP connection");
            // Accept TCP connection
            let (stream, peer_addr) = match self.tcp.accept().await {
                Ok((stream, addr)) => {
                    debug!("TCP connection accepted from {}", addr);
                    (stream, addr)
                }
                Err(e) => {
                    error!("Failed to accept TCP connection: {}", e);
                    return Err(ListenerError::Io(e));
                }
            };

            if let Err(e) = stream.set_nodelay(true) {
                warn!("Failed to set TCP_NODELAY for {}: {}", peer_addr, e);
            }

            info!("Accepted TCP connection from {}", peer_addr);
            debug!("Starting TLS handshake with {}", peer_addr);

            // Perform TLS handshake
            match self.acceptor.accept(stream).await {
                Ok(stream) => {
                    info!("TLS handshake completed successfully with {}", peer_addr);
                    debug!("Creating new Connection instance for {}", peer_addr);
                    let mut conn = Connection::new(stream);

                    // Perform protocol handshake
                    debug!("Starting protocol handshake with {}", peer_addr);
                    if let Err(e) = conn.perform_server_handshake().await {
                        error!("Protocol handshake failed with {}: {}", peer_addr, e);
                        continue;
                    }
                    debug!("Protocol handshake completed with {}", peer_addr);
                    return Ok(conn);
                }
                Err(e) => {
                    error!("TLS handshake failed with {}: {}", peer_addr, e);
                    continue;
                }
            }
        }
    }

    /// Get the local address we're listening on
    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        debug!("Getting local address");
        self.tcp.local_addr()
    }
}
