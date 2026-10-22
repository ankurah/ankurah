//! Real peers reuse compatible trees while comparing only each query's scope.
mod common;
use ankurah::signals::Signal;
use ankurah::Mutable;
use anyhow::Result;
use common::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

async fn selection_update(initial: &str, updated: &str, expected_keys: &[&str], expected_titles: &[&str], shares: bool) -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    for (title, artist) in [("row-a", "artist-c"), ("row-b", "artist-b"), ("row-c", "artist-a")] {
        trx.create(&Record { title: title.into(), artist: artist.into() }).await?;
    }
    trx.commit().await?;

    let offers = Arc::new(Mutex::new(Vec::new()));
    let (_connection, _) = GatedConnection::new(&client, &server, {
        let offers = offers.clone();
        move |message| {
            if let proto::NodeMessage::Request { request, .. } = message {
                if let proto::NodeRequestBody::SubscribeQuery { query_id, known_state, .. } = &request.body {
                    offers.lock().unwrap().push((*query_id, known_state.spec.clone()));
                }
            }
            false
        }
    })
    .await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let query = reader.query::<RecordView>(nocache(initial)?)?;
    tokio::time::timeout(Duration::from_secs(10), query.wait_durable_answered()).await??;
    let first = offers.lock().unwrap().iter().find(|(id, _)| *id == query.query_id()).unwrap().1.clone();
    assert_eq!(first.key_spec().keyparts.len(), 1);
    assert_eq!(first.key_spec().keyparts[0].key, resolved_prop(&client, Record::descriptor(), "title"));

    query.update_selection(updated)?;
    tokio::time::timeout(Duration::from_secs(10), query.wait_durable_answered()).await??;
    let actual = query.peek().iter().map(|row| row.title()).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(actual, expected_titles);
    let offers = offers.lock().unwrap();
    let current = &offers.iter().rev().find(|(id, _)| *id == query.query_id()).unwrap().1;
    assert_eq!(current.predicate(), first.predicate(), "only order/limit changed");
    let keys = current.key_spec().keyparts.iter().map(|part| part.key).collect::<Vec<_>>();
    let expected = expected_keys.iter().map(|name| resolved_prop(&client, Record::descriptor(), name)).collect::<Vec<_>>();
    assert_eq!(keys, expected, "use the existing layout when it can serve the new Selection");
    assert_eq!(current == &first, shares);
    Ok(())
}

#[tokio::test]
async fn unlimited_order_update_reuses_compatible_cached_tree() -> Result<()> {
    selection_update(
        "title >= 'row-' AND artist >= 'artist-' ORDER BY title",
        "title >= 'row-' AND artist >= 'artist-' ORDER BY artist",
        &["title"],
        &["row-c", "row-b", "row-a"],
        true,
    )
    .await
}

#[tokio::test]
async fn adding_limit_replaces_incompatible_cached_tree() -> Result<()> {
    selection_update(
        "title >= 'row-' ORDER BY title, artist",
        "title >= 'row-' ORDER BY title, artist LIMIT 2",
        &["title", "artist"],
        &["row-a", "row-b"],
        false,
    )
    .await
}

type TestNode = Node<SledStorageEngine, PermissiveAgent>;
type Offers = Arc<Mutex<Vec<(Option<proto::QueryId>, proto::KnownState)>>>;

async fn connect(client: &TestNode, server: &TestNode, offers: &Offers) -> GatedConnection {
    GatedConnection::new(client, server, {
        let offers = offers.clone();
        move |message| {
            if let proto::NodeMessage::Request { request, .. } = message {
                match &request.body {
                    proto::NodeRequestBody::Fetch { known_state, .. } => offers.lock().unwrap().push((None, known_state.clone())),
                    proto::NodeRequestBody::SubscribeQuery { query_id, known_state, .. } => {
                        offers.lock().unwrap().push((Some(*query_id), known_state.clone()));
                    }
                    _ => {}
                }
            }
            false
        }
    })
    .await
    .0
}

fn offer(offers: &Offers, query: Option<proto::QueryId>, key: proto::PropertyId) -> proto::KnownState {
    offers
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|(id, known)| *id == query && known.spec.key_spec().keyparts.iter().any(|part| part.key == key))
        .expect("the requested user query must send KnownState")
        .1
        .clone()
}

async fn wait_for(mut condition: impl FnMut() -> bool) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn broad_fetch_tree_serves_narrow_fetch_and_distinct_live_scopes() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let mut pets = Vec::new();
    for (name, age) in [("a", "08"), ("b", "10"), ("c", "12"), ("d", "15")] {
        pets.push(trx.create(&Pet { name: name.into(), age: age.into() }).await?.read()?);
    }
    trx.commit().await?;
    let offers = Offers::default();
    let _connection = connect(&client, &server, &offers).await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let broad = "age > '07' ORDER BY age";
    let rows = reader.fetch::<PetView>(broad).await?;
    let age = resolved_prop(&client, Pet::descriptor(), "age");
    assert_eq!(rows.iter().map(|row| row.id()).collect::<Vec<_>>(), pets.iter().map(|row| row.id()).collect::<Vec<_>>());
    let original = offer(&offers, None, age).spec;

    let rows = reader.fetch::<PetView>("age > '10' ORDER BY age").await?;
    assert_eq!(rows.iter().map(|row| row.id()).collect::<Vec<_>>(), [pets[2].id(), pets[3].id()]);
    assert_eq!(offer(&offers, None, age).spec, original, "narrow Fetch offers the resident tree's broader admission");

    let first_two = reader.query::<PetView>(nocache("age > '07' ORDER BY age ASC LIMIT 2")?)?;
    let last_two = reader.query::<PetView>(nocache("age > '10' ORDER BY age DESC LIMIT 2")?)?;
    tokio::time::timeout(Duration::from_secs(10), first_two.wait_durable_answered()).await??;
    tokio::time::timeout(Duration::from_secs(10), last_two.wait_durable_answered()).await??;
    assert_eq!(first_two.ids(), [pets[0].id(), pets[1].id()]);
    assert_eq!(last_two.ids(), [pets[3].id(), pets[2].id()]);
    for query in [&first_two, &last_two] {
        assert_eq!(
            offer(&offers, Some(query.query_id()), age).spec,
            original,
            "both subscriptions reuse the Fetch tree's layout and admission"
        );
    }

    let trx = writer.begin();
    pets[0].edit(&trx)?.age()?.replace("20")?;
    pets[2].edit(&trx)?.age()?.replace("06")?;
    trx.commit().await?;
    wait_for(|| first_two.ids() == [pets[1].id(), pets[3].id()] && last_two.ids() == [pets[0].id(), pets[3].id()]).await?;
    assert_eq!(first_two.peek()[1].age()?, "15", "unchanged rank entrant arrives from the full resident tree");
    assert_eq!(last_two.peek()[0].age()?, "20");
    tokio::time::timeout(Duration::from_secs(10), first_two.refresh()).await??;
    tokio::time::timeout(Duration::from_secs(10), last_two.refresh()).await??;
    assert_eq!(first_two.ids(), [pets[1].id(), pets[3].id()]);
    assert_eq!(last_two.ids(), [pets[0].id(), pets[3].id()]);
    Ok(())
}

#[tokio::test]
async fn differing_local_and_remote_limit_cuts_converge_using_a_shared_broad_tree() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let mut pets = Vec::new();
    for (name, age) in [("a", "08"), ("b", "10"), ("c", "12"), ("d", "15")] {
        pets.push(trx.create(&Pet { name: name.into(), age: age.into() }).await?.read()?);
    }
    trx.commit().await?;
    let offers = Offers::default();
    let connection = connect(&client, &server, &offers).await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let cached = reader.fetch::<PetView>("age > '07' ORDER BY age").await?;
    let age = resolved_prop(&client, Pet::descriptor(), "age");
    let original = offer(&offers, None, age).spec;
    drop(connection);
    let trx = writer.begin();
    pets[0].edit(&trx)?.age()?.replace("20")?;
    pets[3].edit(&trx)?.age()?.replace("09")?;
    trx.commit().await?;
    assert_eq!(cached[0].age()?, "08");
    assert_eq!(cached[3].age()?, "15");

    let _connection = connect(&client, &server, &offers).await;
    let query = reader.query::<PetView>(nocache("age > '07' ORDER BY age LIMIT 2")?)?;
    tokio::time::timeout(Duration::from_secs(10), query.wait_durable_answered()).await??;
    let known = offer(&offers, Some(query.query_id()), age);
    assert_eq!(known.spec, original);
    let [proto::SpanContent::Heads(inventory)] = known.spans.as_slice() else { panic!("small local scope sends one inventory") };
    assert_eq!(
        inventory.heads.to_vec().iter().map(|head| head.entity.entity_id).collect::<Vec<_>>(),
        [pets[0].id(), pets[1].id()],
        "the initial offer describes the requester's older top-two cut"
    );
    assert_eq!(query.ids(), [pets[3].id(), pets[1].id()], "the response must discover the responder's different top-two cut");
    assert_eq!(query.peek()[0].age()?, "09");
    let fetched = reader.fetch::<PetView>("age > '07' ORDER BY age LIMIT 2").await?;
    assert_eq!(fetched.iter().map(|row| row.id()).collect::<Vec<_>>(), [pets[3].id(), pets[1].id()]);
    assert_eq!(offer(&offers, None, age).spec, original);
    Ok(())
}

async fn reconnect_after_sort_key_grows(direction: &str) -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    server.system.create().await?;
    server.wait_ready().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let writer = server.context(DEFAULT_CONTEXT)?;
    let trx = writer.begin();
    let mut pets = Vec::new();
    for position in 0..32 {
        pets.push(trx.create(&Pet { name: format!("pet-{position}"), age: format!("row-{position:08}") }).await?.read()?);
    }
    trx.commit().await?;
    let offers = Offers::default();
    let connection = connect(&client, &server, &offers).await;
    let reader = client.context_async(DEFAULT_CONTEXT).await?;
    let query = reader.query::<PetView>(nocache(format!("age >= 'row-' ORDER BY age {direction} LIMIT 10"))?)?;
    tokio::time::timeout(Duration::from_secs(10), query.wait_durable_answered()).await??;
    let positions = if direction == "ASC" { (0..10).collect::<Vec<_>>() } else { (22..32).rev().collect() };
    let expected = positions.iter().map(|position| pets[*position].id()).collect::<Vec<_>>();
    assert_eq!(query.ids(), expected);
    client.deregister_peer(server.id);
    server.deregister_peer(client.id);
    drop(connection);

    let edited = positions[5];
    let changed = format!("row-{edited:08}-offline-edit");
    let trx = writer.begin();
    pets[edited].edit(&trx)?.age()?.replace(&changed)?;
    trx.commit().await?;
    assert_ne!(query.peek()[5].age()?, changed, "the client has not received the offline edit");

    // Check the first installed reconnect result, before later traffic can repair it.
    let resultset = query.resultset();
    resultset.set_loaded(false);
    let installed = Arc::new(Mutex::new(None));
    let _guard = resultset.listen(Arc::new({
        let observed = resultset.clone();
        let installed = installed.clone();
        move |_| {
            if observed.is_loaded() {
                let mut installed = installed.lock().unwrap();
                if installed.is_none() {
                    *installed = Some(observed.read().iter_entities().map(|(id, _)| id).collect::<Vec<_>>());
                }
            }
        }
    }));
    let _connection = connect(&client, &server, &offers).await;
    wait_for(|| installed.lock().unwrap().is_some()).await?;
    assert_eq!(installed.lock().unwrap().as_ref(), Some(&expected), "{direction} reconnect must preserve the edited row's rank");
    assert_eq!(query.peek()[5].age()?, changed);
    Ok(())
}

#[tokio::test]
async fn reconnect_keeps_ascending_order_when_a_sort_key_outgrows_inline_storage() -> Result<()> {
    reconnect_after_sort_key_grows("ASC").await
}

#[tokio::test]
async fn reconnect_keeps_descending_order_when_a_sort_key_outgrows_inline_storage() -> Result<()> {
    reconnect_after_sort_key_grows("DESC").await
}
