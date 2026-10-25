//! Resident trees and resident entities shared across a client's queries: a
//! compatible tree serves later and narrower queries, an incompatible one is
//! replaced, and entities the client caches outside a query join its result
//! without losing what the client holds of them.

use super::*;

/// The offer each of a client's Fetches (no query id) and subscribes carried.
type Offers = Arc<Mutex<Vec<(Option<proto::QueryId>, proto::KnownState)>>>;

/// An ephemeral client connected to `server` through a gate that records its
/// offers. Connected only once the server holds its data, so the client's
/// catalog types each property before its first query: a query that cannot
/// type its keys falls back to a tree keyed by entity id.
async fn connect_recording_offers(server: &Sled) -> anyhow::Result<(Sled, GatedConnection, Offers)> {
    let client = ephemeral_sled_setup().await?;
    let offers = Offers::default();
    let (connection, _gate) = {
        let offers = offers.clone();
        GatedConnection::new(&client, server, move |message| {
            if let proto::NodeMessage::Request { request, .. } = message {
                match &request.body {
                    proto::NodeRequestBody::Fetch { known_state, .. } => offers.lock().unwrap().push((None, known_state.clone())),
                    proto::NodeRequestBody::SubscribeQuery { query_id, known_state, .. } => {
                        offers.lock().unwrap().push((Some(*query_id), known_state.clone()))
                    }
                    _ => {}
                }
            }
            false
        })
        .await
    };
    client.wait_ready().await?;
    Ok((client, connection, offers))
}

/// The tree of each offer `query` carried (none for a Fetch), in order.
fn specs(offers: &Offers, query: Option<proto::QueryId>) -> Vec<proto::TreeSpec> {
    offers.lock().unwrap().iter().filter(|(id, _)| *id == query).map(|(_, known)| known.spec.clone()).collect()
}

fn keys(spec: &proto::TreeSpec) -> Vec<proto::PropertyId> { spec.key_spec().keyparts.iter().map(|part| part.key).collect() }

/// The trees a live query over three records offers before and after its
/// selection changes from `initial` to `updated`, and its titles once ready.
async fn trees_across_a_selection_change(initial: &str, updated: &str) -> anyhow::Result<(Sled, Vec<proto::TreeSpec>, Vec<String>)> {
    let server = durable_sled_setup().await?;
    let trx = server.context_async(DEFAULT_CONTEXT).await?.begin();
    for (title, artist) in [("row-a", "artist-c"), ("row-b", "artist-b"), ("row-c", "artist-a")] {
        trx.create(&Record { title: title.into(), artist: artist.into() }).await?;
    }
    trx.commit().await?;
    let (client_node, _connection, offers) = connect_recording_offers(&server).await?;
    let client = client_node.context_async(DEFAULT_CONTEXT).await?;

    let query = client.query_wait::<RecordView>(nocache(initial)?).await?;
    query.update_selection_wait(updated).await?;
    let titles = query.peek().iter().map(|record| record.title()).collect::<Result<_, _>>()?;
    Ok((client_node, specs(&offers, Some(query.query_id())), titles))
}

#[tokio::test]
async fn reordering_an_unlimited_query_keeps_offering_its_tree() -> anyhow::Result<()> {
    let (client, specs, titles) = trees_across_a_selection_change(
        "title >= 'row-' AND artist >= 'artist-' ORDER BY title",
        "title >= 'row-' AND artist >= 'artist-' ORDER BY artist",
    )
    .await?;
    assert_eq!(titles, ["row-c", "row-b", "row-a"]);
    assert_eq!(keys(&specs[0]), [resolved_prop(&client, Record::descriptor(), "title")], "the tree is keyed by the ranged order");
    assert_eq!(specs.last(), specs.first(), "an order does not change an unlimited result's members, so the same tree serves it");
    Ok(())
}

#[tokio::test]
async fn limiting_a_query_offers_a_tree_keyed_for_its_cut() -> anyhow::Result<()> {
    let (client, specs, titles) =
        trees_across_a_selection_change("title >= 'row-' ORDER BY title, artist", "title >= 'row-' ORDER BY title, artist LIMIT 2").await?;
    assert_eq!(titles, ["row-a", "row-b"]);
    let (first, last) = (specs.first().unwrap(), specs.last().unwrap());
    let key = |name| resolved_prop(&client, Record::descriptor(), name);
    assert_eq!(keys(first), [key("title")]);
    assert_eq!(keys(last), [key("title"), key("artist")], "a cut needs every order key, which the first tree lacks");
    assert_eq!(last.predicate(), first.predicate(), "over the same predicate");
    Ok(())
}

#[tokio::test]
async fn a_broad_fetchs_tree_serves_a_narrower_fetch_and_live_queries_with_their_own_cuts() -> anyhow::Result<()> {
    let server_node = durable_sled_setup().await?;
    let server = server_node.context_async(DEFAULT_CONTEXT).await?;
    let trx = server.begin();
    let mut pets = Vec::new();
    for (name, age) in [("a", "08"), ("b", "10"), ("c", "12"), ("d", "15")] {
        pets.push(trx.create(&Pet { name: name.into(), age: age.into() }).await?.id());
    }
    trx.commit().await?;
    let (client_node, _connection, offers) = connect_recording_offers(&server_node).await?;
    let client = client_node.context_async(DEFAULT_CONTEXT).await?;
    let ids = |pets: Vec<PetView>| pets.iter().map(|pet| pet.id()).collect::<Vec<_>>();

    assert_eq!(ids(client.fetch("age > '07' ORDER BY age").await?), pets);
    let broad = specs(&offers, None).pop().unwrap();
    assert_eq!(ids(client.fetch("age > '10' ORDER BY age").await?), [pets[2], pets[3]]);
    assert_eq!(specs(&offers, None).pop().unwrap(), broad, "the narrower Fetch offers the broad Fetch's tree");

    let first_two = client.query_wait::<PetView>(nocache("age > '07' ORDER BY age ASC LIMIT 2")?).await?;
    let last_two = client.query_wait::<PetView>(nocache("age > '10' ORDER BY age DESC LIMIT 2")?).await?;
    assert_eq!(first_two.ids(), [pets[0], pets[1]]);
    assert_eq!(last_two.ids(), [pets[3], pets[2]]);
    for query in [&first_two, &last_two] {
        assert_eq!(specs(&offers, Some(query.query_id())), [broad.clone()], "each live query offers the broad tree too");
    }

    // Changes that move each cut differently: a member of the first leaves
    // its predicate, and the gap fills from the server's members; then an
    // entrant tops the second and stays out of the first.
    let trx = server.begin();
    trx.get::<Pet>(&pets[0]).await?.age()?.replace("06")?;
    trx.commit().await?;
    wait_until("the first cut fills the gap the departure left", || first_two.ids() == [pets[1], pets[2]]).await;
    let entrant = create_pets(&server, "18", ["e".to_string()]).await?[0];
    wait_until("the second cut takes the entrant", || last_two.ids() == [entrant, pets[3]]).await;
    assert_eq!(first_two.ids(), [pets[1], pets[2]]);
    Ok(())
}

/// A live query for the first `count` records by title reconnects to a result
/// that an entity the client caches at an older head has joined and a member
/// has left, while the client edited its own copy of another member past the
/// server's head. Once the query is ready, each copy the client caches
/// matches or exceeds the server's.
async fn cached_copies_join_a_reconnected_result(count: usize) -> anyhow::Result<()> {
    let server_node = durable_sled_setup().await?;
    let server = server_node.context_async(DEFAULT_CONTEXT).await?;
    let trx = server.begin();
    let mut rows = Vec::new();
    for n in 0..count {
        rows.push(trx.create(&Record { title: format!("a-{n:06}"), artist: "row".into() }).await?.id());
    }
    // Both outside the result at first: they rank past its last member.
    let older = trx.create(&Record { title: "z-older".into(), artist: "row".into() }).await?.id();
    let newer = trx.create(&Record { title: "z-newer".into(), artist: "row".into() }).await?.id();
    trx.commit().await?;
    let client_node = ephemeral_sled_setup().await?;
    let connection = LocalProcessConnection::new(&server_node, &client_node).await?;
    client_node.wait_ready().await?;
    let client = client_node.context_async(DEFAULT_CONTEXT).await?;
    let cached_older = client.get::<RecordView>(older).await?;
    let cached_newer = client.get::<RecordView>(newer).await?;
    let selection = format!("artist >= 'row' ORDER BY title ASC LIMIT {count}");
    let query = client.query_wait::<RecordView>(nocache(selection.as_str())?).await?;
    assert_eq!(sorted(query.ids()), sorted(rows.clone()));

    // The server ranks `newer` into the result, which the client follows live.
    let trx = server.begin();
    trx.get::<Record>(&newer).await?.title()?.set(&format!("a-{:06}5", count * 3 / 4))?;
    trx.commit().await?;
    wait_for_heads(&client_node, &heads(&server_node, &[newer]).await?).await?;
    drop(connection);

    // While the client is away it edits its copy of `newer`, and the server
    // ranks `older` into the result and moves a member out of it.
    let trx = client.begin();
    cached_newer.edit(&trx)?.artist()?.set(&"row edited while away".to_string())?;
    trx.commit().await?;
    let edited = heads(&client_node, &[newer]).await?;
    let former = rows[count / 6];
    let older_title = format!("a-{:06}5", count / 4);
    let trx = server.begin();
    trx.get::<Record>(&older).await?.title()?.set(&older_title)?;
    trx.get::<Record>(&former).await?.artist()?.set(&"gone".to_string())?;
    trx.commit().await?;
    let current = heads(&server_node, &[older, former]).await?;

    // A new version of the same selection is ready only once its own sync is.
    query.update_selection(selection.as_str())?;
    let _connection = LocalProcessConnection::new(&server_node, &client_node).await?;
    tokio::time::timeout(Duration::from_secs(10), query.wait_initialized()).await??;

    assert_eq!(cached_older.title()?, older_title, "the older cached copy is brought level");
    assert_eq!(cached_newer.artist()?, "row edited while away", "the server's older state leaves the client's edit in place");
    let mut held = heads(&client_node, &[older, former]).await?;
    held.extend(heads(&client_node, &[newer]).await?);
    let mut expected = current;
    expected.extend(edited);
    assert_eq!(held, expected, "each copy is stored at a head that matches or exceeds the server's");
    // The two entrants push the last row past the result's end.
    let mut members: Vec<_> = rows[..count - 1].iter().copied().filter(|id| *id != former).collect();
    members.extend([older, newer]);
    assert_eq!(sorted(query.ids()), sorted(members));
    Ok(())
}

#[tokio::test]
async fn cached_copies_join_a_reconnected_result_whose_account_lists_its_heads() -> anyhow::Result<()> {
    cached_copies_join_a_reconnected_result(256).await
}

#[tokio::test]
async fn cached_copies_join_a_reconnected_result_whose_account_is_a_digest_frontier() -> anyhow::Result<()> {
    // More members than an account lists heads for.
    cached_copies_join_a_reconnected_result(2049).await
}
