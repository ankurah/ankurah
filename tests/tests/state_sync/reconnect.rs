//! Live queries that reconnect: the relay subscribes again with an offer of
//! the copy it kept, and the server brings that copy level.

use super::*;
use ankurah::signals::Signal;

#[tokio::test]
async fn a_reconnecting_live_query_offers_the_copy_it_kept_and_catches_up() -> anyhow::Result<()> {
    let (server_node, client_node, connection) = connected().await?;
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    let ids = create_pets(&server, "1", (0..6).map(|n| format!("pet {n}"))).await?;
    let query = client.query_wait::<PetView>(nocache("age = '1'")?).await?;
    let kept = heads(&client_node, &ids).await?;
    drop(connection);

    // While the client is away, one member changes, one leaves and one joins.
    let trx = server.begin();
    trx.get::<Pet>(&ids[0]).await?.name()?.replace("renamed")?;
    trx.get::<Pet>(&ids[1]).await?.age()?.replace("2")?;
    trx.commit().await?;
    let joined = create_pets(&server, "1", ["joined".to_string()]).await?[0];

    // The connection back records the offers the client's subscribes carry.
    let offers = Arc::new(Mutex::new(Vec::new()));
    let (_connection, _gate) = {
        let (offers, query_id) = (offers.clone(), query.query_id());
        GatedConnection::new(&client_node, &server_node, move |message| {
            if let proto::NodeMessage::Request { request, .. } = message {
                if let proto::NodeRequestBody::SubscribeQuery { query_id: id, known_state, .. } = &request.body {
                    if *id == query_id {
                        offers.lock().unwrap().push(known_state.clone());
                    }
                }
            }
            false
        })
        .await
    };
    let mut members: Vec<_> = ids.iter().copied().filter(|id| *id != ids[1]).collect();
    members.push(joined);
    let members = sorted(members);
    wait_until("the query catches up with the server's result", || {
        sorted(query.ids()) == members && query.peek().iter().any(|pet| pet.name().unwrap() == "renamed")
    })
    .await;
    let offer = offers.lock().unwrap().first().cloned().expect("the query subscribed again");
    assert_eq!(listed(&offer), Some(kept), "its offer lists the copy it kept, at the heads it kept");
    assert_eq!(heads(&client_node, &members).await?, heads(&server_node, &members).await?, "and the copy is level with the server");
    Ok(())
}

#[tokio::test]
async fn a_resubscribe_brings_a_held_member_only_the_events_it_missed() -> anyhow::Result<()> {
    let (server_node, client_node, connection) = connected().await?;
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    let id = create_pets(&server, "1", ["rex".to_string()]).await?[0];
    let query = client.query_wait::<PetView>(nocache("age >= '1'")?).await?;
    drop(connection);

    let trx = server.begin();
    trx.get::<Pet>(&id).await?.age()?.replace("2")?;
    trx.commit().await?;
    assert_eq!(query.peek()[0].age()?, "1", "the client missed the edit");

    let received = Received::default();
    let (_connection, _gate) = {
        let received = received.clone();
        GatedConnection::new(&server_node, &client_node, move |message| {
            received.record(message);
            false
        })
        .await
    };
    wait_until("the copy catches up", || query.peek()[0].age().unwrap() == "2").await;
    let items = received.items_for(&[id]);
    assert_eq!(items.len(), 1, "the member is pushed once");
    assert!(
        matches!(&items[0].content, proto::UpdateContent::EventOnly(events) if events.len() == 1),
        "as a bridge of the one event after the head its copy held, not its history from creation"
    );
    assert_eq!(query.peek()[0].entity().head(), heads(&server_node, &[id]).await?[&id]);
    Ok(())
}

/// A live query for the first ten pets in `direction` order, whose sixth
/// member's sort key grows longer while the client is away, staying between
/// its neighbors: the first result installed once the query is ready again
/// keeps that member's rank.
async fn a_reconnect_keeps_the_rank_of_a_sort_key_that_grew(direction: &str) -> anyhow::Result<()> {
    let (server_node, client_node, connection) = connected().await?;
    let (server, client) = (server_node.context_async(DEFAULT_CONTEXT).await?, client_node.context_async(DEFAULT_CONTEXT).await?);
    let trx = server.begin();
    let mut ids = Vec::new();
    for position in 0..32 {
        ids.push(trx.create(&Pet { name: format!("pet {position}"), age: format!("row-{position:08}") }).await?.id());
    }
    trx.commit().await?;
    let query = client.query_wait::<PetView>(nocache(format!("age >= 'row-' ORDER BY age {direction} LIMIT 10"))?).await?;
    let ranked: Vec<usize> = if direction == "ASC" { (0..10).collect() } else { (22..32).rev().collect() };
    let expected: Vec<_> = ranked.iter().map(|position| ids[*position]).collect();
    assert_eq!(query.ids(), expected);
    drop(connection);

    let edited = ranked[5];
    let grown = format!("row-{edited:08}-grown-while-the-client-was-away");
    let trx = server.begin();
    trx.get::<Pet>(&ids[edited]).await?.age()?.replace(&grown)?;
    trx.commit().await?;

    // The query activates again once its sync after the reconnect is ready.
    let resultset = (*query.resultset()).clone();
    resultset.set_loaded(false);
    let installed = Arc::new(Mutex::new(None));
    let _guard = resultset.listen(Arc::new({
        let (observed, installed) = (resultset.clone(), installed.clone());
        move |_| {
            let mut installed = installed.lock().unwrap();
            if observed.is_loaded() && installed.is_none() {
                *installed = Some(observed.keys().collect::<Vec<_>>());
            }
        }
    }));
    let _connection = LocalProcessConnection::new(&server_node, &client_node).await?;
    wait_until("the query is ready again", || installed.lock().unwrap().is_some()).await;
    assert_eq!(installed.lock().unwrap().as_ref(), Some(&expected), "{direction}: the grown key keeps its member's rank");
    assert_eq!(query.peek()[5].age()?, grown);
    Ok(())
}

#[tokio::test]
async fn a_reconnect_keeps_the_ascending_rank_of_a_sort_key_that_grew() -> anyhow::Result<()> {
    a_reconnect_keeps_the_rank_of_a_sort_key_that_grew("ASC").await
}

#[tokio::test]
async fn a_reconnect_keeps_the_descending_rank_of_a_sort_key_that_grew() -> anyhow::Result<()> {
    a_reconnect_keeps_the_rank_of_a_sort_key_that_grew("DESC").await
}
