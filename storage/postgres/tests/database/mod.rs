//! A PostgreSQL database of a test's own, the nodes on it, a proxy to count
//! their round trips, and what the database reports of their sessions: what
//! the tests of PostgreSQL's maps and of the digest trees on them share. Each
//! of the two test targets compiles this file as a module of its own, so what
//! only one of them uses is marked `#[allow(dead_code)]` for the other.

pub mod proxy;

use std::sync::Arc;

use ankurah::core::error::MutationError;
use ankurah::core::test_helpers::map_conformance::fixtures::HANG;
use ankurah_storage_postgres::{MapError, MapSettings, Postgres};
use anyhow::Context;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::{Client, NoTls};

use self::proxy::Proxy;
use crate::server::{config, Server};

/// A PostgreSQL server of the test's own, and the test's own connection to
/// it, through which it watches what the nodes' sessions do.
pub struct Database {
    server: Server,
    inspector: Client,
}

impl Database {
    pub async fn start() -> anyhow::Result<Self> {
        let server = Server::start().await?;
        let (inspector, connection) = config(&server.host, server.port).connect(NoTls).await.context("connecting the test's inspector")?;
        tokio::spawn(connection);

        Ok(Self { server, inspector })
    }

    /// A node's engine on the database, with connections of its own, as a
    /// node in another process would have.
    pub async fn node(&self) -> anyhow::Result<Arc<Postgres>> { self.node_with(MapSettings::default()).await }

    /// A node's engine on the database, whose map calls connect and wait as
    /// `settings` say.
    pub async fn node_with(&self, settings: MapSettings) -> anyhow::Result<Arc<Postgres>> {
        engine(self.server.pool().await.context("building a node's pool")?, settings).await
    }

    /// A node's engine on the database whose pool holds one connection,
    /// apart from the connections of its map calls.
    #[allow(dead_code)]
    pub async fn node_with_a_pool_of_one(&self) -> anyhow::Result<Arc<Postgres>> {
        let manager = PostgresConnectionManager::new(config(&self.server.host, self.server.port), NoTls);
        let pool = bb8::Pool::builder().max_size(1).build(manager).await.context("building a node's pool")?;
        engine(pool, MapSettings::default()).await
    }

    /// A node's engine that reaches the database through `proxy`.
    pub async fn node_through(&self, proxy: &Proxy) -> anyhow::Result<Arc<Postgres>> {
        let manager = PostgresConnectionManager::new(config("127.0.0.1", proxy.port()), NoTls);
        let pool = bb8::Pool::builder().build(manager).await.context("building a node's pool")?;
        engine(pool, MapSettings::default()).await
    }

    /// A proxy to the database, for nodes to connect through.
    pub async fn proxy(&self) -> anyhow::Result<Proxy> { Proxy::start(&self.server.host, self.server.port).await }

    /// A connection of the test's own to the database, apart from its
    /// inspector's, for a transaction whose locks the test holds.
    #[allow(dead_code)]
    pub async fn connection(&self) -> anyhow::Result<Client> {
        let (client, connection) = config(&self.server.host, self.server.port).connect(NoTls).await.context("connecting the test's own")?;
        tokio::spawn(connection);

        Ok(client)
    }

    /// Wait until `count` sessions wait for a lock, as the database reports
    /// them: an event in these tests only the database can show.
    pub async fn sessions_waiting_for_a_lock(&self, count: i64) -> anyhow::Result<()> {
        self.sessions("wait_event_type = 'Lock'", count).await
    }

    /// Wait until `count` sessions sit idle in a transaction, as the database
    /// reports them.
    #[allow(dead_code)]
    pub async fn sessions_idle_in_a_transaction(&self, count: i64) -> anyhow::Result<()> {
        self.sessions("state LIKE 'idle in transaction%'", count).await
    }

    /// Wait until `count` of the database's sessions are as `condition` says.
    async fn sessions(&self, condition: &str, count: i64) -> anyhow::Result<()> {
        let sessions = format!("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND {condition}");

        tokio::time::timeout(HANG, async {
            while self.inspector.query_one(&sessions, &[]).await?.get::<_, i64>(0) != count {
                tokio::task::yield_now().await;
            }

            anyhow::Ok(())
        })
        .await?
    }
}

/// An engine whose connections `pool` opens, and whose map calls connect and
/// wait as `settings` say.
async fn engine(pool: bb8::Pool<PostgresConnectionManager<NoTls>>, settings: MapSettings) -> anyhow::Result<Arc<Postgres>> {
    Ok(Arc::new(Postgres::with_map_settings(pool, settings).await.context("opening a node's engine")?))
}

/// The engine's own failure of a map call that `error` is, if it is one.
pub fn map_error(error: &MutationError) -> Option<&MapError> {
    match error {
        MutationError::General(error) => error.downcast_ref(),
        _ => None,
    }
}
