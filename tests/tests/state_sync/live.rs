//! Live queries: readiness before the first result, and the subscription's
//! own updates once ready.

use super::*;
use std::time::Duration;

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
