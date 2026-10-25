//! Live queries: readiness before the first result, the subscription's own
//! updates once ready, and a sync that stops once its query is dropped.

use super::*;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

/// Wait until `query`'s members are `expected`.
async fn members(query: &LiveQuery<PetView>, expected: &[EntityId]) {
    let expected = sorted(expected.to_vec());
    let settled = tokio::time::timeout(Duration::from_secs(5), async {
        while sorted(query.peek().iter().map(|pet| pet.id()).collect()) != expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(settled.is_ok(), "the live query holds {expected:?}");
}

#[tokio::test]
async fn a_live_query_is_ready_with_the_servers_result_and_follows_its_changes() -> anyhow::Result<()> {
    let (server, client, _connection) = connected().await?;
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", ["leaves".to_string(), "stays".to_string()]).await?;

    let query = client.query_wait::<PetView>(nocache("age = '1'")?).await?;
    assert_eq!(sorted(query.peek().iter().map(|pet| pet.id()).collect()), sorted(ids.clone()), "ready means the whole result is here");

    let joins = create_pets(&server, "1", ["joins".to_string()]).await?;
    let trx = server.begin();
    trx.get::<Pet>(&ids[0]).await?.age()?.replace("2")?;
    trx.commit().await?;
    members(&query, &[ids[1], joins[0]]).await;
    Ok(())
}

#[tokio::test]
async fn a_limited_live_query_fills_the_gap_a_departure_leaves() -> anyhow::Result<()> {
    let (server, client, _connection) = connected().await?;
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_albums(&server, 2020..=2025).await?;
    let query = client.query_wait::<AlbumView>(nocache("year >= '2020' ORDER BY year ASC LIMIT 3")?).await?;
    assert_eq!(years(&query), ["2020", "2021", "2022"]);

    // The departure leaves a gap the client fills with a limited Fetch.
    let trx = server.begin();
    trx.get::<Album>(&ids[1]).await?.year()?.replace("1999")?;
    trx.commit().await?;
    let filled = tokio::time::timeout(Duration::from_secs(5), async {
        while years(&query) != ["2020", "2022", "2023"] {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(filled.is_ok(), "the gap filled with the next album: {:?}", years(&query));
    Ok(())
}

#[tokio::test]
async fn a_dropped_live_query_stops_syncing_at_its_next_wake() -> anyhow::Result<()> {
    // Once the pet exists the server's pushes never arrive, so a subscribe's
    // sync waits for the pet, and on waking idle it would ask for it again.
    let pet: Arc<Mutex<Option<EntityId>>> = Arc::default();
    let subscribed: Arc<Mutex<HashSet<proto::QueryId>>> = Arc::default();
    let asked_again = Arc::new(AtomicBool::new(false));
    let (server, client, _connection, _gate) = {
        let (pet, subscribed, asked_again) = (pet.clone(), subscribed.clone(), asked_again.clone());
        gated(move |message| {
            let Some(pet) = *pet.lock().unwrap() else { return false };
            if let proto::NodeMessage::Response(response) = message {
                match &response.body {
                    proto::NodeResponseBody::QuerySubscribed { query_id, .. } => {
                        subscribed.lock().unwrap().insert(*query_id);
                    }
                    proto::NodeResponseBody::EntitiesSynced { held, .. } if held.iter().any(|entity| entity.entity_id == pet) => {
                        asked_again.store(true, Ordering::SeqCst)
                    }
                    _ => {}
                }
            }
            matches!(message, proto::NodeMessage::Update(_))
        })
        .await?
    };
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    *pet.lock().unwrap() = Some(create_pets(&server, "1", ["withheld".to_string()]).await?[0]);

    let query = client.query::<PetView>(nocache("age = '1'")?)?;
    let answered = tokio::time::timeout(Duration::from_secs(5), async {
        while !subscribed.lock().unwrap().contains(&query.query_id()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(answered.is_ok(), "the server answered the subscribe");
    drop(query);
    // Past the first idle second, when a sync still running asks for the pet again.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(!asked_again.load(Ordering::SeqCst), "the dropped query's sync stops at its next wake, without asking for the pet again");
    Ok(())
}
