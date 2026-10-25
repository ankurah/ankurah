//! What a server answers and pushes for a client's offer: an account that
//! lists every head of a result that fits one offer, and pushes of only the
//! members the copy lacks or holds at another head, as states where the
//! offer named no head to bridge from.

use super::*;

fn is_state(item: &proto::SubscriptionUpdateItem) -> bool { matches!(item.content, proto::UpdateContent::StateAndEvent(..)) }

#[tokio::test]
async fn an_account_lists_every_head_of_a_result_that_fits_one_offer() -> anyhow::Result<()> {
    let received = Received::default();
    let (server_node, client, _connection, _gate) = {
        let received = received.clone();
        gated(move |message| {
            received.record(message);
            false
        })
        .await?
    };
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    // More members than one page of heads holds, and far fewer than an offer's.
    let ids = create_pets(&server, "1", (0..256).map(|n| format!("pet {n}"))).await?;
    let heads = heads(&server_node, &ids).await?;

    assert_eq!(client.fetch::<PetView>("age = '1'").await?.len(), 256);
    let query = client.query_wait::<PetView>(nocache("age = '1'")?).await?;

    let responses = received.responses();
    let fetched = responses.iter().filter_map(|body| match body {
        proto::NodeResponseBody::Fetch(account) => listed(account),
        _ => None,
    });
    assert!(fetched.into_iter().any(|listed| listed == heads), "the Fetch's account lists each member at the server's head");
    let subscribed = responses.iter().find_map(|body| match body {
        proto::NodeResponseBody::QuerySubscribed { query_id, target } if *query_id == query.query_id() => Some(target),
        _ => None,
    });
    assert_eq!(subscribed.and_then(listed), Some(heads), "and so does the subscribe's");
    Ok(())
}

#[tokio::test]
async fn a_warm_fetch_pushes_only_the_members_whose_heads_differ() -> anyhow::Result<()> {
    let received = Received::default();
    let (server, client, _connection, _gate) = {
        let received = received.clone();
        gated(move |message| {
            received.record(message);
            false
        })
        .await?
    };
    let (server, client) = (server.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", (0..256).map(|n| format!("pet {n}"))).await?;

    assert_eq!(client.fetch::<PetView>("age = '1'").await?.len(), 256);
    let cold = received.items_for(&ids);
    assert_eq!(sorted(cold.iter().map(|item| item.entity_id).collect()), sorted(ids.clone()), "a cold copy is pushed each member once");
    assert!(cold.iter().all(is_state), "as its state, since the copy holds no head to bridge from");

    received.clear();
    assert_eq!(client.fetch::<PetView>("age = '1'").await?.len(), 256);
    assert!(received.items_for(&ids).is_empty(), "a copy that holds every head is pushed nothing");

    let trx = server.begin();
    trx.get::<Pet>(&ids[117]).await?.name()?.replace("renamed")?;
    trx.commit().await?;
    received.clear();
    let fetched = client.fetch::<PetView>("age = '1'").await?;
    assert_eq!(fetched.iter().find(|pet| pet.id() == ids[117]).unwrap().name()?, "renamed");
    let items = received.items_for(&ids);
    assert_eq!(items.iter().map(|item| item.entity_id).collect::<Vec<_>>(), [ids[117]], "only the changed member is pushed");
    assert!(
        matches!(&items[0].content, proto::UpdateContent::EventOnly(events) if events.len() == 1),
        "as the one event that bridges from the head the offer listed"
    );
    Ok(())
}

#[tokio::test]
async fn an_offer_that_lists_nothing_is_pushed_each_members_state() -> anyhow::Result<()> {
    let received = Received::default();
    let (server_node, client, _connection, _gate) = {
        let received = received.clone();
        gated(move |message| {
            received.record(message);
            false
        })
        .await?
    };
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client.context_async(DEFAULT_CONTEXT).await?);
    let held = create_pets(&server, "1", (0..96).map(|n| format!("held {n}"))).await?;
    assert_eq!(client.fetch::<PetView>("age = '1'").await?.len(), 96);

    // The copy holds the moved pets at age 1, outside the result it offers
    // for age 2, so its offer for age 2 lists no member.
    let trx = server.begin();
    for id in &held {
        trx.get::<Pet>(id).await?.age()?.replace("2")?;
    }
    trx.commit().await?;
    let mut ids = held.clone();
    ids.extend(create_pets(&server, "2", (0..96).map(|n| format!("new {n}"))).await?);
    received.clear();

    let fetched = client.fetch::<PetView>("age = '2'").await?;
    assert_eq!(sorted(fetched.iter().map(|pet| pet.id()).collect()), sorted(ids.clone()));
    let items = received.items_for(&ids);
    assert_eq!(items.len(), ids.len(), "each member is pushed once");
    assert!(items.iter().all(is_state), "as its state, even where the copy held an older head the offer did not list");
    let current = heads(&server_node, &held).await?;
    for pet in fetched.iter().filter(|pet| held.contains(&pet.id())) {
        assert_eq!(pet.entity().head(), current[&pet.id()], "the copy adopted the newer state of a member it held elsewhere");
    }
    Ok(())
}
