//! Loss: a push that never arrives, at a Fetch's opening or after a live query
//! is ready, and a server that answers but never pushes.

use super::*;
use ankurah::core::error::RetrievalError;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

#[tokio::test]
async fn a_lost_initial_push_is_repaired_once_the_copy_stops_progressing() -> anyhow::Result<()> {
    // The first update pushing the watched entity to the client is lost.
    let watched: Arc<Mutex<Option<EntityId>>> = Arc::default();
    let lost = Arc::new(AtomicBool::new(false));
    let (server, client, _connection, gate) = {
        let (watched, lost) = (watched.clone(), lost.clone());
        gated(move |message| watched.lock().unwrap().is_some_and(|id| pushes(message, id)) && !lost.swap(true, Ordering::SeqCst)).await?
    };
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", (0..3).map(|n| format!("pet {n}"))).await?;
    *watched.lock().unwrap() = Some(ids[0]);

    let started = Instant::now();
    let fetched = client.fetch::<PetView>("age = '1'").await?;
    assert_eq!(sorted(fetched.iter().map(|pet| pet.id()).collect()), sorted(ids));
    assert_eq!(gate.take_held().len(), 1, "one push was lost");
    assert!(started.elapsed() >= Duration::from_secs(1), "the repair waited out an idle second first");
    Ok(())
}

#[tokio::test]
async fn a_server_that_answers_but_never_pushes_fails_the_fetch_as_stalled() -> anyhow::Result<()> {
    let withhold = Arc::new(AtomicBool::new(false));
    let (server_node, client, _connection, _gate) = {
        let withhold = withhold.clone();
        gated(move |message| withhold.load(Ordering::SeqCst) && matches!(message, proto::NodeMessage::Update(_))).await?
    };
    let server = server_node.context_async(DEFAULT_CONTEXT).await?;
    let client = client.context_async(DEFAULT_CONTEXT).await?;
    create_pets(&server, "1", ["withheld".to_string()]).await?;
    withhold.store(true, Ordering::SeqCst);

    let error = client.fetch::<PetView>("age = '1'").await.expect_err("nothing the server pushes arrives");
    assert!(matches!(error, RetrievalError::SyncStalled { peer } if peer == server_node.id), "a typed error naming the server: {error}");
    Ok(())
}

#[tokio::test]
async fn a_lost_live_push_is_repaired_when_two_checks_in_a_row_disagree() -> anyhow::Result<()> {
    let watched: Arc<Mutex<Option<EntityId>>> = Arc::default();
    let (server, client_node, _connection, _gate) = {
        let watched = watched.clone();
        gated(move |message| watched.lock().unwrap().is_some_and(|id| pushes(message, id))).await?
    };
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", ["renamed".to_string(), "kept".to_string()]).await?;
    let query = client.query_wait::<PetView>(nocache("age = '1'")?).await?;
    let name = || query.peek().iter().find(|pet| pet.id() == ids[0]).map(|pet| pet.name().unwrap());
    assert_eq!(name().as_deref(), Some("renamed"));

    // The live push of the rename is lost; later pushes arrive.
    *watched.lock().unwrap() = Some(ids[0]);
    let trx = server.begin();
    trx.get::<Pet>(&ids[0]).await?.name()?.replace("new name")?;
    trx.commit().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    *watched.lock().unwrap() = None;
    assert_eq!(name().as_deref(), Some("renamed"), "the copy missed the rename");

    client_node.check_remote_queries_now().await;
    assert_eq!(name().as_deref(), Some("renamed"), "one disagreement may be a push in flight, so it repairs nothing");
    client_node.check_remote_queries_now().await;
    let repaired = tokio::time::timeout(Duration::from_secs(5), async {
        while name().as_deref() != Some("new name") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(repaired.is_ok(), "the second disagreement repaired the copy");
    Ok(())
}
