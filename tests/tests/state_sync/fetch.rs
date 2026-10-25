//! Fetches: what a client's copy holds once a Fetch returns, cold or warm, for
//! small and large results, members whose key moved, entities that left the
//! result, and limits.

use super::*;

#[tokio::test]
async fn a_cold_fetch_brings_every_member() -> anyhow::Result<()> {
    let (server, client, _connection) = connected().await?;
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", (0..5).map(|n| format!("pet {n}"))).await?;

    let fetched = client.fetch::<PetView>("age = '1'").await?;
    assert_eq!(sorted(fetched.iter().map(|pet| pet.id()).collect()), sorted(ids));
    Ok(())
}

#[tokio::test]
async fn a_warm_fetch_brings_new_members_and_newer_heads_of_held_ones() -> anyhow::Result<()> {
    let (server, client, _connection) = connected().await?;
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let mut ids = create_pets(&server, "1", (0..3).map(|n| format!("pet {n}"))).await?;
    assert_eq!(client.fetch::<PetView>("age = '1'").await?.len(), 3);

    ids.extend(create_pets(&server, "1", (3..5).map(|n| format!("pet {n}"))).await?);
    let trx = server.begin();
    trx.get::<Pet>(&ids[0]).await?.name()?.replace("renamed")?;
    trx.commit().await?;

    let fetched = client.fetch::<PetView>("age = '1'").await?;
    assert_eq!(sorted(fetched.iter().map(|pet| pet.id()).collect()), sorted(ids.clone()));
    let renamed = fetched.iter().find(|pet| pet.id() == ids[0]).unwrap();
    assert_eq!(renamed.name()?, "renamed", "a member the copy held at an older head arrives at the newer one");
    Ok(())
}

#[tokio::test]
async fn a_warm_fetch_of_a_large_result_brings_new_members_and_newer_heads_of_held_ones() -> anyhow::Result<()> {
    let (server, client, _connection) = connected().await?;
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    // More members than one account's heads, so both accounts are digest frontiers.
    let mut ids = Vec::new();
    for batch in 0..5 {
        ids.extend(create_pets(&server, "1", (0..450).map(|n| format!("pet {batch}-{n}"))).await?);
    }
    assert_eq!(client.fetch::<PetView>("age = '1'").await?.len(), ids.len());

    ids.extend(create_pets(&server, "1", ["late one".to_string(), "late two".to_string()]).await?);
    let trx = server.begin();
    trx.get::<Pet>(&ids[7]).await?.name()?.replace("renamed")?;
    trx.commit().await?;

    let fetched = client.fetch::<PetView>("age = '1'").await?;
    assert_eq!(sorted(fetched.iter().map(|pet| pet.id()).collect()), sorted(ids.clone()));
    assert_eq!(fetched.iter().find(|pet| pet.id() == ids[7]).unwrap().name()?, "renamed");
    Ok(())
}

#[tokio::test]
async fn a_warm_fetch_of_a_large_result_brings_a_member_whose_key_moved() -> anyhow::Result<()> {
    let (server, client, _connection) = connected().await?;
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    // More members than one account's heads, under a range its tree keys by
    // age, so both accounts are digest frontiers addressed by age. The copy
    // holds members at both ages, so the offer describes each age by a
    // nonempty digest, within which the server pushes nothing.
    let mut ids = Vec::new();
    for batch in 0..5 {
        let age = if batch == 4 { "2" } else { "1" };
        ids.extend(create_pets(&server, age, (0..420).map(|n| format!("pet {batch}-{n}"))).await?);
    }
    assert_eq!(client.fetch::<PetView>("age >= '1'").await?.len(), ids.len());

    // The client holds no live query, so only its next Fetch learns of this.
    // Its descent pages both the member's old address, where only the copy
    // holds it, and its new one, where the server lists it.
    let trx = server.begin();
    trx.get::<Pet>(&ids[7]).await?.age()?.replace("2")?;
    trx.commit().await?;

    let fetched = client.fetch::<PetView>("age >= '1'").await?;
    assert_eq!(fetched.len(), ids.len());
    assert_eq!(fetched.iter().find(|pet| pet.id() == ids[7]).unwrap().age()?, "2", "the member arrives at its new key");
    Ok(())
}

#[tokio::test]
async fn an_entity_that_left_the_result_while_the_client_was_away_leaves_its_copy() -> anyhow::Result<()> {
    let (server_node, client_node, _connection) = connected().await?;
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", ["leaves".to_string(), "stays".to_string()]).await?;
    assert_eq!(client.fetch::<PetView>("age = '1'").await?.len(), 2);

    // The client holds no live query, so only its next Fetch can learn of this.
    let trx = server.begin();
    trx.get::<Pet>(&ids[0]).await?.age()?.replace("2")?;
    trx.commit().await?;

    let fetched = client.fetch::<PetView>("age = '1'").await?;
    assert_eq!(fetched.iter().map(|pet| pet.id()).collect::<Vec<_>>(), vec![ids[1]], "the stale copy is not returned");
    let (held, current) = (client_node.storage.get_state(ids[0]).await?, server_node.storage.get_state(ids[0]).await?);
    assert_eq!(held.payload.state.head, current.payload.state.head, "its newer state reached the client");
    Ok(())
}

#[tokio::test]
async fn a_limited_fetch_returns_the_servers_cut_even_over_a_stale_copy() -> anyhow::Result<()> {
    let (server, client, _connection) = connected().await?;
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_albums(&server, 2020..=2025).await?;
    let query = "year >= '2020' ORDER BY year ASC LIMIT 3";
    let years = |albums: Vec<AlbumView>| albums.iter().map(|album| album.year().unwrap()).collect::<Vec<_>>();
    assert_eq!(years(client.fetch::<AlbumView>(query).await?), ["2020", "2021", "2022"]);

    let trx = server.begin();
    trx.get::<Album>(&ids[1]).await?.year()?.replace("1999")?;
    trx.commit().await?;

    assert_eq!(years(client.fetch::<AlbumView>(query).await?), ["2020", "2022", "2023"], "the copy's stale member left the cut");
    Ok(())
}
