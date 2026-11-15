//! Pool capacity, idle retention, snapshot timeouts and transaction cleanup.

use std::sync::Arc;
use std::time::Duration;

use ankurah::core::test_helpers::map_conformance::fixtures::HANG;
use tokio::time::timeout;

use super::{MapSettings, Pool};
use crate::maps::MapError;
use crate::test_server::Server;

#[tokio::test]
async fn the_pool_retains_only_its_idle_limit_after_a_burst() -> anyhow::Result<()> {
    let (_server, pool) = pool(MapSettings { max_idle_connections: 2, ..MapSettings::default() }).await?;

    let mut leased = Vec::new();
    for _ in 0..3 {
        leased.push(timeout(HANG, pool.lease_for_snapshot()).await??);
    }

    drop(leased);

    assert_eq!(pool.idle.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn a_transaction_waits_for_a_connection_at_capacity() -> anyhow::Result<()> {
    let (_server, pool) = pool(MapSettings { max_connections: 1, ..MapSettings::default() }).await?;

    let leased = pool.lease().await?;
    let session = backend(&leased.connection().client).await?;
    let waiting = tokio::spawn({
        let pool = Arc::clone(&pool);
        async move { backend(&pool.lease().await?.connection().client).await }
    });

    tokio::task::yield_now().await;
    drop(leased);

    assert_eq!(timeout(HANG, waiting).await???, session, "the second lease waited for the first's connection");
    Ok(())
}

#[tokio::test]
async fn a_snapshot_waits_for_a_connection_within_its_pool_timeout() -> anyhow::Result<()> {
    let (_server, pool) = pool(MapSettings { max_connections: 1, snapshot_pool_wait_timeout: HANG, ..MapSettings::default() }).await?;

    let leased = pool.lease().await?;
    let session = backend(&leased.connection().client).await?;
    let reading = tokio::spawn({
        let pool = Arc::clone(&pool);
        async move { backend(&pool.lease_for_snapshot().await?.connection().client).await }
    });

    tokio::task::yield_now().await;
    drop(leased);

    assert_eq!(timeout(HANG, reading).await???, session, "the snapshot waited for the transaction's connection");
    Ok(())
}

#[tokio::test]
async fn a_snapshot_with_zero_pool_timeout_fails_at_capacity() -> anyhow::Result<()> {
    let (_server, pool) =
        pool(MapSettings { max_connections: 1, snapshot_pool_wait_timeout: Duration::ZERO, ..MapSettings::default() }).await?;

    let _leased = pool.lease().await?;
    let read = pool.lease_for_snapshot().await;

    assert!(matches!(read, Err(MapError::ConnectionsInUse { most: 1 })));
    Ok(())
}

#[tokio::test]
async fn a_lease_dropped_while_its_transaction_may_be_open_closes_its_connection() -> anyhow::Result<()> {
    let (_server, pool) = pool(MapSettings::default()).await?;

    let lease = pool.lease().await?;
    lease.begin("BEGIN").await?;
    drop(lease);

    assert_eq!(pool.idle.lock().unwrap().len(), 0);
    Ok(())
}

/// A snapshot's transaction ends without its reader waiting: the `ROLLBACK`
/// goes out, the connection goes back at once, and the next lease's
/// statements run after the `ROLLBACK`.
#[tokio::test]
async fn a_transaction_ended_unanswered_gives_its_connection_back_at_once_and_ends_first() -> anyhow::Result<()> {
    let (_server, pool) = pool(MapSettings::default()).await?;

    let lease = pool.lease_for_snapshot().await?;
    lease.begin("BEGIN").await?;
    lease.end_unanswered("ROLLBACK");
    drop(lease);

    let idle = pool.idle.lock().unwrap().len();
    let next = pool.lease().await?;
    let in_transaction = next.connection().client.batch_execute("SAVEPOINT inside").await.is_ok();

    assert_eq!(idle, 1);
    assert!(!in_transaction, "the ROLLBACK ran before the next lease's statement");
    Ok(())
}

/// The server process a connection talks to.
async fn backend(client: &tokio_postgres::Client) -> anyhow::Result<i32> {
    Ok(client.query_one("SELECT pg_backend_pid()", &[]).await?.get(0))
}

/// A pool for map calls over a database of the test's own, connecting and
/// waiting as `settings` say.
async fn pool(settings: MapSettings) -> anyhow::Result<(Server, Arc<Pool>)> {
    let server = Server::start().await?;
    let pool = Pool::new(server.pool().await?, settings);

    Ok((server, pool))
}
