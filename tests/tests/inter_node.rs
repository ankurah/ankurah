mod common;

use ankurah::core::node::nocache;
use ankurah::core::{
    connector::{PeerSender, SendError},
    error::{RequestError, RetrievalError},
};
use ankurah::signals::Subscribe;
use ankurah::{changes::ChangeKind, policy::DEFAULT_CONTEXT as c, EntityId, Mutable, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use common::{Album, AlbumView, Pet, PetView};

use crate::common::*;

#[derive(Clone)]
struct FailingPeerSender {
    recipient: EntityId,
}

impl PeerSender for FailingPeerSender {
    fn send_message(&self, _message: ankurah::proto::NodeMessage) -> std::result::Result<(), SendError> { Err(SendError::ConnectionClosed) }

    fn recipient_node_id(&self) -> EntityId { self.recipient }

    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

pub fn names(resultset: Vec<AlbumView>) -> Vec<String> { resultset.iter().map(|r| r.name().unwrap_or_default()).collect::<Vec<String>>() }

#[tokio::test]
async fn inter_node_fetch() -> Result<()> {
    let node1 = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let node2 = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    info!("Node1 root: {:?}", node1.system.root());
    info!("Node2 root: {:?}", node2.system.root());

    // Initialize the system on the durable node
    node1.system.create().await?;

    info!("After initializing - Node1 root: {:?}", node1.system.root());
    info!("After initializing - Node2 root: {:?}", node2.system.root());

    // Verify node2 is not ready before connection
    assert!(!node2.system.is_system_ready());

    // Create connection between nodes
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;

    // Note: No need to explicitly wait for node2 to be ready here.
    // context_async() will handle waiting for system readiness after join_system completes

    // Now get context for durable node after system is ready
    let ctx1 = node1.context_async(c).await;

    info!("After context - Node1 root: {:?}", node1.system.root());
    info!("After context - Node2 root: {:?}", node2.system.root());

    {
        let trx = ctx1.begin();
        trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await?;
        trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await?;
        trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await?;
        trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await?;
        trx.commit().await?;
    };

    info!("After transaction - Node1 root: {:?}", node1.system.root());
    info!("After transaction - Node2 root: {:?}", node2.system.root());

    let p = "name = 'Walking on a Dream'";
    // Should already be on node1
    assert_eq!(names(ctx1.fetch(p).await?), ["Walking on a Dream"]);

    // Now get context for ephemeral node
    let ctx2 = node2.context_async(c).await;

    // Now node2 should now successfully fetch the entity
    assert_eq!(names(ctx2.fetch::<AlbumView>(p).await?), ["Walking on a Dream"]);

    info!("After fetch - Node1 root: {:?}", node1.system.root());
    info!("After fetch - Node2 root: {:?}", node2.system.root());

    Ok(())
}

#[tokio::test]
async fn server_edits_subscription() -> Result<()> {
    // Create two nodes
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    // Connect the nodes
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let server = server.context(c)?;
    let client = client.context(c)?;

    use ankurah::View;
    // Create initial entities on node1
    let (rex, snuffy, jasper) = {
        let trx = server.begin();
        let rex = trx.create(&Pet { name: "Rex".to_string(), age: "1".to_string() }).await?;
        let snuffy = trx.create(&Pet { name: "Snuffy".to_string(), age: "2".to_string() }).await?;
        let jasper = trx.create(&Pet { name: "Jasper".to_string(), age: "6".to_string() }).await?;

        let read = (rex.read(), snuffy.read(), jasper.read());
        trx.commit().await?;
        read
    };

    info!("rex: {}, snuffy: {}, jasper: {}", rex.entity(), snuffy.entity(), jasper.entity());

    // Set up subscription on node2
    let client_watcher = TestWatcher::changeset_with_event_ids();
    let cached_watcher = TestWatcher::changeset();
    let pred = "name = 'Rex' OR (age > 2 and age < 5)";

    let cached_query = client.query::<PetView>(pred)?; // Cached behavior: subscribe before remote initialization
    assert_eq!(cached_query.ids(), vec![]); // didn't wait for initialization AND the client cache is empty, so should be empty for two reasons
    let _cached_handle = cached_query.subscribe(&cached_watcher);
    assert_eq!(cached_watcher.count(), 0); // we don't notify on subscribe so there should be no changes yet

    // Use nocache - so we have to call the server before it's considered initialized
    let client_query = client.query_wait::<PetView>(nocache(pred)?).await?;
    assert_eq!(client_query.ids(), vec![rex.id()]); // client query should immediately include rex because we awaited initialization
    let _client_handle = client_query.subscribe(&client_watcher);
    assert_eq!(client_watcher.count(), 0); // we don't notify on subscribe so there should be no changes yet

    info!("cached_query: {}, client_query: {}", cached_query.query_id(), client_query.query_id()); // for debugging

    // Update Rex's age to 7 on node1
    {
        let trx = server.begin();
        rex.edit(&trx)?.age().overwrite(0, 1, "7")?;
        info!("COMMITING REX UPDATE");
        trx.commit().await?;
    }

    // Client watcher doesn't get an ::Initial notification because we waited for initialization to be completed before subscribing
    assert_eq!(client_watcher.take_one().await, vec![(rex.id(), ChangeKind::Update, rex.entity().head().to_vec())]);
    assert_eq!(client_query.ids(), vec![rex.id()]);
    assert_eq!(client_query.peek().iter().map(|r| r.age().unwrap_or_default()).collect::<Vec<String>>(), vec!["7"]);
    assert_eq!(client_query.loaded(), true);

    // Cached watcher subscribes before initialization, so should have an initial [], then Add when the server deltas arrive, then an Update when the age is updated
    assert_eq!(cached_watcher.take(3).await?, vec![vec![], vec![(rex.id(), ChangeKind::Add)], vec![(rex.id(), ChangeKind::Update)]]);
    assert_eq!(cached_query.ids(), vec![rex.id()]);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(cached_query.peek().get(0).unwrap().id(), client_query.peek().get(0).unwrap().id());
    assert_eq!(cached_query.peek().get(0).unwrap(), client_query.peek().get(0).unwrap());
    assert_eq!(cached_query.peek().iter().map(|r| r.age().unwrap_or_default()).collect::<Vec<String>>(), vec!["7"]);
    println!("cached_query loaded: {}", cached_query.loaded());

    // Update Snuffy's age to 3 on node1
    {
        let trx = server.begin();

        // rex.edit(&trx)?.age().overwrite(0, 1, "8")?;
        snuffy.edit(&trx)?.age().overwrite(0, 1, "3")?;
        trx.commit().await?;
    }

    // Wait for and verify Snuffy being added (now matches age > 2 and age < 5)
    assert_eq!(client_watcher.take_one().await, vec![(snuffy.id(), ChangeKind::Add, snuffy.entity().head().to_vec())]);
    // assert_eq!(one.into_iter().map(|(e, k, _)| (e, k)).collect::<Vec<(EntityId, ChangeKind)>>(), vec![(snuffy.id(), ChangeKind::Add)]);

    // Ensure no additional unexpected changes
    assert_eq!(client_watcher.quiesce().await, 0);
    Ok(())
}

#[tokio::test]
async fn cached_livequery_survives_disconnect_and_catches_up_on_reconnect() -> Result<()> {
    let server_node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server_node.system.create().await?;
    let client_node = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let conn = LocalProcessConnection::new(&server_node, &client_node).await?;
    client_node.system.wait_system_ready().await;

    let initial_album = {
        let server = server_node.context(c)?;
        let trx = server.begin();
        let album = trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await?.read();
        trx.commit().await?;
        album
    };

    let client = client_node.context(c)?;
    let query = client.query_wait::<AlbumView>(nocache("year >= '2020'")?).await?;
    assert_eq!(query.ids(), vec![initial_album.id()]);
    assert_eq!(names(query.peek()), vec!["Ask That God".to_string()]);

    drop(conn);

    let new_album = {
        let server = server_node.context(c)?;
        let trx = server.begin();
        let album = trx.create(&Album { name: "Future Dust".into(), year: "2027".into() }).await?.read();
        trx.commit().await?;
        album
    };

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(query.ids(), vec![initial_album.id()], "Disconnected livequery should retain cached resultset");
    assert_eq!(names(query.peek()), vec!["Ask That God".to_string()]);

    let offline_watcher = TestWatcher::changeset();
    let _offline_handle = query.subscribe(&offline_watcher);
    assert_eq!(offline_watcher.quiesce().await, 0, "Resubscribing offline should not fabricate new changes");
    assert_eq!(query.ids(), vec![initial_album.id()], "Offline resubscribe should still expose cached results");

    let _reconn = LocalProcessConnection::new(&server_node, &client_node).await?;

    assert_eq!(offline_watcher.take_one().await, vec![(new_album.id(), ChangeKind::Add)]);

    let mut ids = query.ids();
    ids.sort();
    let mut expected = vec![initial_album.id(), new_album.id()];
    expected.sort();
    assert_eq!(ids, expected, "Existing livequery should catch up after reconnect");
    assert_eq!(names(query.peek()), vec!["Ask That God".to_string(), "Future Dust".to_string()]);

    Ok(())
}

#[tokio::test]
async fn test_client_server_propagation() -> Result<()> {
    // Create server (durable) and two client nodes
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;

    let client_a = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let client_b = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    // Connect both clients to the server
    let _conn_a = LocalProcessConnection::new(&client_a, &server).await?;
    let _conn_b = LocalProcessConnection::new(&client_b, &server).await?;

    client_a.system.wait_system_ready().await;
    client_b.system.wait_system_ready().await;

    let server = server.context(c)?;
    let client_a = client_a.context(c)?;
    let client_b = client_b.context(c)?;

    info!("Server: {}, client_a: {}, client_b: {}", server.node_id(), client_a.node_id(), client_b.node_id());

    // Create an entity on client_a
    {
        let trx = client_a.begin();
        trx.create(&Album { name: "Origin of Symmetry".into(), year: "2001".into() }).await?;
        trx.commit().await?;
    }

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify entity is queryable on server
    let query = "name = 'Origin of Symmetry'";
    assert_eq!(names(server.fetch(query).await?), ["Origin of Symmetry"]);

    // Wait for propagation to client_b
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify entity is queryable on client_b
    assert_eq!(names(client_b.fetch(query).await?), ["Origin of Symmetry"]);

    Ok(())
}

#[tokio::test]
async fn test_client_server_subscription_propagation() -> Result<()> {
    // Create server (durable) and two client nodes
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client_a = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let client_b = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    // Connect both clients to the server
    let _conn_a = LocalProcessConnection::new(&client_a, &server).await?;
    let _conn_b = LocalProcessConnection::new(&client_b, &server).await?;

    let server = server.context(c)?;

    client_a.system.wait_system_ready().await;
    client_b.system.wait_system_ready().await;
    let client_a = client_a.context(c)?;
    let client_b = client_b.context(c)?;

    // Set up watchers for server and client_b
    let server_watcher = TestWatcher::changeset();
    let client_b_watcher = TestWatcher::changeset();

    // Set up subscriptions
    let _server_sub = server.query_wait::<AlbumView>("name = 'Origin of Symmetry'").await?.subscribe(&server_watcher);
    let _client_b_sub = client_b.query_wait::<AlbumView>("name = 'Origin of Symmetry'").await?.subscribe(&client_b_watcher);

    // Create an entity on client_a
    let album_id = {
        let trx = client_a.begin();
        let album = trx.create(&Album { name: "Origin of Symmetry".into(), year: "2001".into() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Wait for and verify server received the change (should get empty initialization + add)
    assert_eq!(server_watcher.take_one().await, vec![(album_id, ChangeKind::Add)]);
    // Wait for and verify client_b received the change (should get empty initialization + add)
    assert_eq!(client_b_watcher.take_one().await, vec![(album_id, ChangeKind::Add)]);

    assert_eq!(server_watcher.quiesce().await, 0);
    assert_eq!(client_b_watcher.quiesce().await, 0);
    Ok(())
}

#[tokio::test]
async fn test_view_field_subscriptions_with_query_lifecycle() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let server = server.context(c)?;
    let client = client.context(c)?;

    // Create initial entity on server
    let pet_id = {
        let trx = server.begin();
        let pet = trx.create(&Pet { name: "Buddy".to_string(), age: "3".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // PART 1: Test that Livequery/View subscriptions work when active

    // Set up query subscription on client that matches our pet
    let lq_watcher = TestWatcher::changeset();

    // Also exercise cached behavior by subscribing to a LiveQuery that uses cached initialization
    let cached_watcher = TestWatcher::changeset();
    let _cached_guard = client.query::<PetView>("name = 'Buddy'")?.subscribe(&cached_watcher);

    // This is the actual livequery, which has a predicate subscription with the server because the client node is ephemeral.
    // Using query_wait ensures the LiveQuery is fully initialized before we subscribe to it.

    let client_livequery = client.query_wait::<PetView>(nocache("name = 'Buddy'")?).await?;
    // LOCALLY subscribe to notifications from the livequery. This is a different kind of subscription than above
    // the guard does keep the livequery alive, but dropping it does not necessarily drop the LiveQuery and unsubscribe from the server.
    let lq_subguard = client_livequery.subscribe(&lq_watcher);

    // NOTE!
    // Since we used query_wait (which waits for initialization before returning), the LiveQuery is already
    // initialized when we subscribe to it. Therefore, we don't receive an Initial change notification.
    assert_eq!(lq_watcher.drain(), vec![] as Vec<Vec<(EntityId, ChangeKind)>>);

    // Get the pet view from client and set up View/field subscriptions
    let client_pet = client.get::<PetView>(pet_id).await?;
    let view_watcher = TestWatcher::new();
    // subscribe to the View directly - this gets notified when any change to this view arrives from any source
    let _view_subguard = client_pet.subscribe(&view_watcher);

    // The view subscription is a good example of this, because there's no such thing as an uninitialized view
    assert_eq!(view_watcher.quiesce().await, 0);
    assert_eq!(client_pet.age().unwrap(), "3");

    // Make an edit on the server
    {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.age().replace("4")?;
        trx.commit().await?;
    }

    // Wait for and verify the update notification
    assert_eq!(lq_watcher.take_one().await, vec![(pet_id, ChangeKind::Update)]); // Buddy's age changed to 4 - still matches the query

    // Check the cached watcher only after the above - to avoid screwing with the lq_watcher timing, which would happen if we checked before now
    // it gets two additional notifications:
    // - first a blank one because cached_livequery.subscribe happens before initialization,
    // - then an Add when the entity arrives from the server, and then an Update when the age is updated
    // The nocache watcher only receives one ::Update because the record is already arrived from the server before the signal subscribe
    // due to a combination of using query_wait and nocache(). if the cached version used query_wait instead, it should have
    // received only ::Add and ::Update - not the blank initial notification
    assert_eq!(cached_watcher.take(3).await?, vec![vec![], vec![(pet_id, ChangeKind::Add)], vec![(pet_id, ChangeKind::Update)]]);
    // Drop cached subscription before testing later lifecycle expectations
    drop(_cached_guard);

    // Verify that View/field subscriptions received the update
    assert_eq!(view_watcher.take_one().await, client_pet.clone());

    drop(lq_subguard); // This should stop the local listener

    // Make another edit on the server
    {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.age().replace("4")?;
        trx.commit().await?;
    }

    // Verify that subscriptions did NOT receive the update after being dropped
    assert_eq!(
        lq_watcher.quiesce_drain().await,
        vec![] as Vec<Vec<(EntityId, ChangeKind)>>,
        "subscription to LiveQuery signal should not receive updates after dropping lq_subguard"
    );
    assert_eq!(
        view_watcher.take_one().await,
        client_pet.clone(),
        "Subscription to View signal should still receive updates because both client_livequery and _view_subguard are still alive"
    );

    drop(client_livequery);
    // give the server time to apply the unsubscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.age().replace("5")?;
        trx.commit().await?;
    }
    assert_eq!(lq_watcher.quiesce().await, 0, "subscription to LiveQuery signal should still be dead");
    assert_eq!(
        view_watcher.take_one().await,
        client_pet.clone(),
        "Resident entity should continue receiving updates after dropping client_livequery"
    );

    drop(_view_subguard);
    {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.age().replace("6")?;
        trx.commit().await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(client_pet.age().unwrap(), "6");
    assert_eq!(view_watcher.quiesce().await, 0, "View signal should stop notifying after dropping view_subguard");

    Ok(())
}

#[tokio::test]
async fn resident_entity_from_get_resubscribes_after_reconnect() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let server_ctx = server.context(c)?;
    let client_ctx = client.context(c)?;

    let pet_id = {
        let trx = server_ctx.begin();
        let pet = trx.create(&Pet { name: "Echo".to_string(), age: "1".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    let client_pet = client_ctx.get::<PetView>(pet_id).await?;
    let watcher = TestWatcher::new();
    let _guard = client_pet.subscribe(&watcher);
    assert_eq!(client_pet.age().unwrap(), "1");
    assert_eq!(watcher.quiesce().await, 0);

    drop(conn);

    {
        let trx = server_ctx.begin();
        let server_pet = server_ctx.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.age().replace("2")?;
        trx.commit().await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(client_pet.age().unwrap(), "1", "Disconnected resident entity should keep cached state");
    assert_eq!(watcher.quiesce().await, 0, "Disconnected resident entity should not receive updates");

    let _reconn = LocalProcessConnection::new(&client, &server).await?;

    assert_eq!(watcher.take_one().await, client_pet.clone());
    assert_eq!(client_pet.age().unwrap(), "2", "Resident entity fetched via get should catch up after reconnect");

    Ok(())
}

#[tokio::test]
async fn cached_reads_fall_back_to_local_on_transient_peer_failures() -> Result<()> {
    let durable_engine = Arc::new(SledStorageEngine::new_test().unwrap());
    let ephemeral_engine = Arc::new(SledStorageEngine::new_test().unwrap());

    let server = Node::new_durable(durable_engine, PermissiveAgent::new());
    server.system.create().await?;

    let pet_id = {
        let server_ctx = server.context(c)?;
        let trx = server_ctx.begin();
        let pet = trx.create(&Pet { name: "LieFi".to_string(), age: "1".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    {
        let client = Node::new(ephemeral_engine.clone(), PermissiveAgent::new());
        let _conn = LocalProcessConnection::new(&client, &server).await?;
        client.system.wait_system_ready().await;

        let client_ctx = client.context(c)?;
        assert_eq!(client_ctx.get::<PetView>(pet_id).await?.age().unwrap(), "1");
        assert_eq!(client_ctx.fetch::<PetView>("name = 'LieFi'").await?.len(), 1);
    }

    let client = Node::new(ephemeral_engine, PermissiveAgent::new());
    client.system.wait_loaded().await;
    assert!(client.system.is_system_ready(), "Cached root should keep restarted ephemeral node usable");

    client.register_peer(
        ankurah::proto::Presence { node_id: server.id, durable: true, system_root: server.system.root() },
        Box::new(FailingPeerSender { recipient: server.id }),
    );

    let client_ctx = client.context(c)?;

    match client_ctx.get::<PetView>(pet_id).await {
        Err(RetrievalError::RequestError(RequestError::SendError(_))) => {}
        other => panic!("Expected uncached get to fail with a send error, got {other:?}"),
    }

    assert_eq!(client_ctx.get_cached::<PetView>(pet_id).await?.age().unwrap(), "1");
    assert_eq!(client_ctx.fetch::<PetView>("name = 'LieFi'").await?.len(), 1);

    match client_ctx.fetch::<PetView>(nocache("name = 'LieFi'")?).await {
        Err(RetrievalError::RequestError(RequestError::SendError(_))) => {}
        other => panic!("Expected nocache fetch to fail with a send error, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn test_lineage_event_bridge() -> Result<()> {
    // Tests that EventBridge efficiently handles cases where the client is missing many intermediate events.
    // The server sends all needed events in the EventBridge response rather than requiring recursive fetches.
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    // Connect the nodes
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let server = server.context(c)?;
    let client = client.context(c)?;

    // Create initial entity on server
    let pet_id = {
        let trx = server.begin();
        let pet = trx.create(&Pet { name: "BudgetTest".to_string(), age: "1".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // Client gets the entity (no subscription established)
    let client_pet = client.get::<PetView>(pet_id).await?;
    assert_eq!(client_pet.age().unwrap(), "1");

    // Server makes 11 changes (exceeds retrieval budget of 10)
    for i in 2..=12 {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.age().replace(&i.to_string())?;
        trx.commit().await?;
    }

    // Client fetches - EventBridge provides all missing events efficiently
    let results = client.fetch::<PetView>("name = 'BudgetTest'").await?;

    assert_eq!(results.len(), 1);
    let client_pet = &results[0];
    assert_eq!(client_pet.age().unwrap(), "12", "Client should have final state after EventBridge");

    Ok(())
}

#[tokio::test]
async fn test_fetch_view_field_subscriptions_behavior() -> Result<()> {
    // Create server (durable) and client nodes
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    // Connect the nodes
    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let server = server.context(c)?;
    let client = client.context(c)?;

    // Create initial entity on server
    let pet_id = {
        let trx = server.begin();
        let pet = trx.create(&Pet { name: "Luna".to_string(), age: "2".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // === Test fetch() behavior ===

    // Use fetch() to get the entity on client (no ongoing subscription)
    let fetch_result = client.fetch::<PetView>("name = 'Luna'").await?;
    assert_eq!(fetch_result.len(), 1, "Should fetch one pet");

    let client_pet = fetch_result.iter().next().unwrap();

    // Set up View/field subscriptions on the fetched entity
    let view_watcher = TestWatcher::new();

    let _view_handle = client_pet.subscribe(&view_watcher);

    // Make an edit on the server
    {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.name().replace("Stella")?;
        trx.commit().await?;
    }

    // Verify that View/field subscriptions did NOT receive updates
    // This documents the current behavior - fetch() doesn't establish ongoing subscriptions
    assert_eq!(view_watcher.quiesce().await, 0, "View subscription should NOT receive updates with fetch() only (current behavior)");

    Ok(())
}
