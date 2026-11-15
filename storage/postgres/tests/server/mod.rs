//! A PostgreSQL server of a test's own: a container whose database is
//! published on a port the test chooses, handed to the test once that port
//! accepts connections.

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::LazyLock;
use std::time::Duration;

use ankurah::core::test_helpers::map_conformance::fixtures::HANG;
use anyhow::Context;
use bb8_postgres::PostgresConnectionManager;
use testcontainers::core::IntoContainerPort;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use tokio::time::Instant;
use tokio_postgres::{Config, NoTls};

/// A PostgreSQL server of a test's own, which runs as long as this lives.
pub struct Server {
    _container: ContainerAsync<postgres::Postgres>,
    pub host: String,
    pub port: u16,
}

impl Server {
    /// A server of the test's own, once its port accepts connections.
    pub async fn start() -> anyhow::Result<Self> {
        let port = unused_port().context("choosing the port the database is published on")?;
        let image = postgres::Postgres::default().with_db_name("ankurah").with_user("postgres").with_password("postgres");
        let container = image.with_mapped_port(port, 5432.tcp()).start().await.context("starting the database's container")?;
        let host = container.get_host().await.context("finding the container's host")?.to_string();

        accepting(&host, port).await.with_context(|| format!("waiting for container {} to accept", container.id()))?;
        Ok(Self { _container: container, host, port })
    }

    /// A pool of connections to the server's database, built as bb8 builds
    /// one by default.
    pub async fn pool(&self) -> anyhow::Result<bb8::Pool<PostgresConnectionManager<NoTls>>> {
        let manager = PostgresConnectionManager::new(config(&self.host, self.port), NoTls);
        Ok(bb8::Pool::builder().build(manager).await?)
    }
}

/// The connection settings for the test server at `host` and `port`.
pub fn config(host: &str, port: u16) -> Config {
    let mut config = Config::new();
    config.host(host).port(port).user("postgres").password("postgres").dbname("ankurah");

    config
}

/// A port to publish a database on: below the range this machine takes the
/// local ports of outgoing connections from, 49152 to 65535, and one nothing
/// here listens on.
///
/// Docker would otherwise pick the port, from that range, and these tests
/// open many outgoing connections: when one holds the port Docker picked,
/// Docker Desktop can't listen on it, and the container runs with its port
/// refusing every connection. Each test process walks ports from a place of
/// its own, so that two running at once rarely ask for the same one.
fn unused_port() -> anyhow::Result<u16> {
    static NEXT: LazyLock<AtomicU16> = LazyLock::new(|| AtomicU16::new(40_000 + (std::process::id() % 70) as u16 * 128));

    loop {
        let port = NEXT.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(port < 49_152, "no port below 49152 is left to publish a database on");

        // Free where the tests connect and where Docker Desktop listens.
        let on_loopback = std::net::TcpListener::bind(("127.0.0.1", port)).is_ok();
        if on_loopback && std::net::TcpListener::bind(("0.0.0.0", port)).is_ok() {
            return Ok(port);
        }
    }
}

/// Wait until the database at `host` and `port` accepts a connection.
///
/// Docker reports the database ready by its log, and Docker Desktop opens
/// the published port to this machine a moment after, when many containers
/// start at once: until then a connection is refused, and the test asks
/// again, a moment apart. Any other failure fails at once.
async fn accepting(host: &str, port: u16) -> anyhow::Result<()> {
    let deadline = Instant::now() + HANG;
    loop {
        match config(host, port).connect(NoTls).await {
            Ok(_) => return Ok(()),
            Err(refused) if is_refused(&refused) && Instant::now() < deadline => {}
            Err(refused) if is_refused(&refused) => anyhow::bail!("{host}:{port} still refused connections after {HANG:?}"),
            Err(failed) => return Err(anyhow::Error::new(failed).context("connecting to the database")),
        }

        tokio::time::sleep(PORT_RETRY).await;
    }
}

/// How long the test waits before it asks again of a port that refused it.
/// It paces the asking, and decides nothing.
const PORT_RETRY: Duration = Duration::from_millis(10);

/// Whether `error` is a connection the database's machine refused.
fn is_refused(error: &tokio_postgres::Error) -> bool {
    std::error::Error::source(error)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .is_some_and(|error| error.kind() == std::io::ErrorKind::ConnectionRefused)
}
