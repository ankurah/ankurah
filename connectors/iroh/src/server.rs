use std::{fmt, sync::Arc};

use ankurah_core::{policy::PolicyAgent, storage::StorageEngine, Node};
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint,
};
use tokio::{select, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Acceptor side of the iroh connector.
///
/// Wraps an already-bound [`iroh::Endpoint`] in an [`iroh::protocol::Router`]
/// that accepts connections for the [`crate::ALPN`] protocol. Relay and address
/// lookup configuration are the embedder's responsibility when building the
/// endpoint; this type only speaks the Ankurah presence/message protocol on top.
///
/// For each accepted connection it runs the same handshake as the websocket
/// server: `Presence` is its first outbound frame, it registers the remote peer
/// with the `Node` upon receiving the peer's `Presence`, dispatches subsequent
/// `PeerMessage` frames to the node, and deregisters the peer when the
/// connection closes.
pub struct IrohServer {
    router: Router,
}

impl IrohServer {
    /// Start accepting Ankurah connections for `node` on `endpoint`.
    pub fn new<SE, PA>(node: Node<SE, PA>, endpoint: Endpoint) -> Self
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        info!("Starting iroh server for node {} on endpoint {}", node.id, endpoint.id());
        let router = Router::builder(endpoint)
            .accept(
                crate::ALPN,
                AnkurahProtocol {
                    node,
                    shutdown: CancellationToken::new(),
                    registrations: Arc::new(crate::connection::RegistrationRegistry::default()),
                },
            )
            .spawn();
        Self { router }
    }

    /// The underlying iroh endpoint.
    pub fn endpoint(&self) -> &Endpoint { self.router.endpoint() }

    /// Stop accepting connections and close the endpoint.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.router.shutdown().await.map_err(|e| anyhow::anyhow!("iroh router shutdown failed: {}", e))
    }
}

/// [`ProtocolHandler`] that runs the Ankurah presence handshake and message pump
/// for each accepted connection.
struct AnkurahProtocol<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    node: Node<SE, PA>,
    shutdown: CancellationToken,
    registrations: Arc<crate::connection::RegistrationRegistry>,
}

impl<SE, PA> fmt::Debug for AnkurahProtocol<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_struct("AnkurahProtocol").field("node_id", &self.node.id).finish() }
}

impl<SE, PA> ProtocolHandler for AnkurahProtocol<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let remote = connection.remote_id();
        debug!("Accepted iroh connection from endpoint {}", remote);

        // The dialer opens the bidirectional stream and sends its Presence on it;
        // accept_bi resolves once that stream arrives. We then immediately send our
        // own Presence as our first outbound frame (inside run_connection),
        // mirroring the websocket server's handshake.
        let streams = select! {
            _ = self.shutdown.cancelled() => return Ok(()),
            streams = timeout(crate::connection::HANDSHAKE_TIMEOUT, connection.accept_bi()) => streams,
        };
        let (send_stream, recv_stream) = match streams {
            Ok(streams) => streams?,
            Err(_) => {
                debug!("iroh connection with endpoint {} timed out before opening its Ankurah stream", remote);
                connection.close(1u32.into(), b"ankurah handshake timeout");
                return Ok(());
            }
        };

        if let Err(e) =
            crate::connection::run_connection(&self.node, &self.registrations, send_stream, recv_stream, Some(&self.shutdown), |_| {}).await
        {
            debug!("iroh connection with endpoint {} ended: {}", remote, e);
        }

        connection.close(0u32.into(), b"");
        debug!("iroh connection with endpoint {} closed", remote);
        Ok(())
    }

    async fn shutdown(&self) { self.shutdown.cancel() }
}
