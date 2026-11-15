//! A request sends its steps in their order before it awaits any answer, and
//! stops at a step that fails before it is sent: no step behind that one is
//! sent, a `COMMIT` least of all, so the commit fails as one that landed
//! nothing. A commit whose connection closed may have landed. A map's puts
//! and deletes go in statements of bounded size.

use ankurah::core::test_helpers::map_conformance::fixtures::HANG;
use ankurah_core::storage::{MapId, MapRead, MapRows, MapSnapshot, MapWrite, StorageEngine, StorageTransaction};
use tokio::time::timeout;

use super::{Answer, PendingWrites, Request, MOST_BYTES_WRITTEN};
use crate::test_server::Server;
use crate::{MapError, Postgres, PostgresMapSnapshot};

#[tokio::test]
async fn a_commit_whose_statement_fails_before_it_is_sent_stops_ahead_of_its_commit_and_fails_as_one_that_landed_nothing(
) -> anyhow::Result<()> {
    let (_server, engine) = engine().await?;
    let lease = engine.maps.lease().await?;
    let connection = lease.connection();

    let mut request = Request::on(connection);
    request.then(lease.begin("BEGIN"));
    // The put's value is text where the statement takes bytes, so it fails to encode.
    request.statement(
        &connection.statements.put,
        vec![Box::new(b"m".to_vec()), Box::new(vec![b"a".to_vec()]), Box::new(vec!["text"])],
        Answer::Nothing,
    );
    request.then(lease.end("COMMIT"));
    let failure = MapError::committing(request.send().await.expect_err("the request fails at the step that doesn't encode"));

    let still_open = connection.client.batch_execute("SAVEPOINT after").await.is_ok();
    assert!(still_open, "the transaction is still open: its COMMIT wasn't sent");
    assert!(matches!(failure, MapError::CommitUnsent(_)), "{failure:?}");
    Ok(())
}

/// The connection's task may send a step and then fail, all before the
/// step's first poll returns, so a closed connection then may have sent it.
#[tokio::test]
async fn a_commit_whose_connection_closed_before_its_first_poll_may_have_landed() -> anyhow::Result<()> {
    let (_server, engine) = engine().await?;
    let lease = engine.maps.lease().await?;
    let connection = lease.connection();

    connection.client.batch_execute("SELECT pg_terminate_backend(pg_backend_pid())").await.expect_err("the database ends the session");

    timeout(HANG, async {
        while !connection.client.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let mut request = Request::on(connection);
    request.then(lease.end("COMMIT"));
    let failure = MapError::committing(request.send().await.expect_err("the request fails on the closed connection"));

    assert!(matches!(failure, MapError::OutcomeUnknown(_)), "{failure:?}");
    Ok(())
}

#[tokio::test]
async fn a_maps_puts_past_what_one_statement_puts_go_in_several_of_the_one_request_and_all_land() -> anyhow::Result<()> {
    let (_server, engine) = engine().await?;
    let lease = engine.maps.lease().await?;

    let mut pending = PendingWrites::default();
    pending.add((0..3).map(|n| MapWrite::Put { map: MapId::new("m"), key: vec![n], value: vec![n; MOST_BYTES_WRITTEN / 2] }).collect());

    let mut request = Request::on(lease.connection());
    request.then(lease.begin("BEGIN"));
    request.write(&mut pending);
    request.then(lease.end("COMMIT"));
    let statements = request.steps.len();
    request.send().await.map_err(MapError::request)?;

    let read = MapRead::Get { map: MapId::new("m"), keys: vec![vec![0], vec![1], vec![2]] };
    let landed = PostgresMapSnapshot::begin(&engine.maps).await?.map_read(vec![read]).await?;

    assert_eq!(statements, 5, "the BEGIN, a statement for each entry, more than half of what one statement puts, and the COMMIT");
    assert_eq!(landed, [MapRows::Got((0..3).map(|n| Some(vec![n; MOST_BYTES_WRITTEN / 2])).collect())]);
    Ok(())
}

#[tokio::test]
async fn a_maps_deletes_past_what_one_statement_deletes_go_in_several_of_the_one_request_and_all_land() -> anyhow::Result<()> {
    let (_server, engine) = engine().await?;
    let keys: Vec<Vec<u8>> = (0..=2 * (MOST_BYTES_WRITTEN >> 10)).map(|n| format!("{n:01024}").into_bytes()).collect();

    let mut trx = engine.transaction();
    trx.map_write(keys.iter().map(|key| MapWrite::Put { map: MapId::new("m"), key: key.clone(), value: vec![1] }).collect()).await?;
    trx.commit().await?.committed()?;

    let lease = engine.maps.lease().await?;
    let mut pending = PendingWrites::default();
    pending.add(keys.iter().map(|key| MapWrite::Delete { map: MapId::new("m"), key: key.clone() }).collect());

    let mut request = Request::on(lease.connection());
    request.then(lease.begin("BEGIN"));
    request.write(&mut pending);
    request.then(lease.end("COMMIT"));
    let statements = request.steps.len();
    request.send().await.map_err(MapError::request)?;

    let read = MapRead::Get { map: MapId::new("m"), keys: keys.clone() };
    let landed = PostgresMapSnapshot::begin(&engine.maps).await?.map_read(vec![read]).await?;

    assert_eq!(statements, 5, "the BEGIN, a statement for each of two mebibytes of keys and one for the key past them, and the COMMIT");
    assert_eq!(landed, [MapRows::Got(vec![None; keys.len()])]);
    Ok(())
}

/// An engine over a database of the test's own.
async fn engine() -> anyhow::Result<(Server, Postgres)> {
    let server = Server::start().await?;
    let engine = Postgres::new(server.pool().await?).await?;

    Ok((server, engine))
}
