//! Busy results: a live query becomes ready while the server keeps writing to
//! its result throughout the sync.

use super::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

/// Run `writes` on the server every 300 ms until `query_ready` ends, and
/// fail unless it ends within `within`.
async fn ready_while_writing<T>(
    within: Duration,
    query_ready: impl std::future::Future<Output = T>,
    mut writes: impl FnMut(usize) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>,
) -> anyhow::Result<T> {
    let writing = async {
        for n in 0.. {
            tokio::time::sleep(Duration::from_millis(300)).await;
            writes(n).await?;
        }
        anyhow::Ok(())
    };
    tokio::select! {
        ready = tokio::time::timeout(within, query_ready) => ready.map_err(|_| anyhow::anyhow!("the query is ready while its result keeps changing")),
        written = writing => Err(written.expect_err("the writes go on until the query is ready")),
    }
}

#[tokio::test]
async fn a_live_query_whose_result_keeps_gaining_members_becomes_ready() -> anyhow::Result<()> {
    // The first push of `lost` is lost, so the sync waits for a repair while
    // the members the server creates every 300 ms arrive live.
    let lost: Arc<Mutex<Option<EntityId>>> = Arc::default();
    let losing = Arc::new(AtomicBool::new(true));
    let (server, client, _connection, _gate) = {
        let (lost, losing) = (lost.clone(), losing.clone());
        gated(move |message| lost.lock().unwrap().is_some_and(|id| pushes(message, id)) && losing.swap(false, Ordering::SeqCst)).await?
    };
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", ["kept".to_string(), "lost".to_string()]).await?;
    *lost.lock().unwrap() = Some(ids[1]);

    let entrants = server.clone();
    let query = ready_while_writing(Duration::from_secs(5), client.query_wait::<PetView>(nocache("age = '1'")?), move |n| {
        let server = entrants.clone();
        Box::pin(async move { create_pets(&server, "1", [format!("entrant {n}")]).await.map(drop) })
    })
    .await??;
    let held: Vec<_> = query.peek().iter().map(|pet| pet.id()).collect();
    assert!(ids.iter().all(|id| held.contains(id)), "the lost member was repaired: {held:?}");
    Ok(())
}
