mod common;

use ankurah::signals::Subscribe;
use ankurah::{changes::ChangeKind, policy::DEFAULT_CONTEXT as c, EntityId, Mutable, Node, PermissiveAgent, ResultSet};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use common::{Album, AlbumView, Pet, PetView};

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

    // let (server_watcher, check_server) = common::changeset_watcher::<PetView>();

    // let server_query = server.query("name = 'Rex' OR (age > 2 and age < 5)")?;
    // let _server_handle = server_query.subscribe(server_watcher);

    // // Wait for the subscription to be fully initialized
    // server_query.wait_initialized().await;

    // assert_eq!(check_server(), vec![vec![]] as Vec<Vec<(EntityId, ChangeKind)>>);

    use ankurah::View;
    // Create initial entities on node1
    let (rex, snuffy, jasper) = {
        let trx = server.begin();
        let rex = trx.create(&Pet { name: "Rex".to_string(), age: "1".to_string() }).await?;
        let snuffy = trx.create(&Pet { name: "Snuffy".to_string(), age: "2".to_string() }).await?;
        let jasper = trx.create(&Pet { name: "Jasper".to_string(), age: "6".to_string() }).await?;

        let read = (rex.read(), snuffy.read(), jasper.read());
        tracing::info!("MARK 1");
        trx.commit().await?;
        read
    };

    info!("rex: {}, snuffy: {}, jasper: {}", rex.entity(), snuffy.entity(), jasper.entity());
    // assert_eq!(check_server(), vec![vec![(rex.id(), ChangeKind::Add)]]);

    // Set up subscription on node2
    let (client_watcher, check_client) = common::changeset_watcher::<PetView>();
    // FIXME: Does the SubscriptionGuard keep the LiveQuery<PetView> alive? I think it should, but we need to test it
    // In this example, the LiveQuery<PetView> returned by client.query() gets immediately dropped, so hopefully the subscribe closure keeps a clone alive
    let client_query = client.query("name = 'Rex' OR (age > 2 and age < 5)")?;
    let _client_handle = client_query.subscribe(client_watcher);
    client_query.wait_initialized().await;

    // Initial state should include Rex
    assert_eq!(check_client(), vec![vec![(rex.id(), ChangeKind::Initial)]]);

    // Update Rex's age to 7 on node1
    {
        let trx: ankurah::transaction::Transaction = server.begin();
        rex.edit(&trx)?.age().overwrite(0, 1, "7")?;
        info!("MARK 2");
        trx.commit().await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    // assert_eq!(check_server(), vec![vec![(rex.id(), ChangeKind::Update)]]);
    assert_eq!(check_client(), vec![vec![(rex.id(), ChangeKind::Update)]]); // Rex still matches the predicate, but the age has changed

    // Update Snuffy's age to 3 on node1
    {
        let trx = server.begin();

        // rex.edit(&trx)?.age().overwrite(0, 1, "8")?;
        snuffy.edit(&trx)?.age().overwrite(0, 1, "3")?;
        trx.commit().await?;
    }

    // Sleep for a bit to ensure the change is propagated
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Should receive notification about Snuffy being added (now matches age > 2 and age < 5)
    // assert_eq!(check_server(), vec![vec![(snuffy.id(), ChangeKind::Add)]]);
    assert_eq!(check_client(), vec![vec![(snuffy.id(), ChangeKind::Add)]]);

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
    let (server_watcher, check_server) = common::changeset_watcher::<AlbumView>();
    let (client_b_watcher, check_client_b) = common::changeset_watcher::<AlbumView>();

    // Set up subscriptions
    let _server_sub = server.query("name = 'Origin of Symmetry'")?.subscribe(server_watcher);
    let _client_b_sub = client_b.query("name = 'Origin of Symmetry'")?.subscribe(client_b_watcher);

    // Create an entity on client_a
    let album_id = {
        let trx = client_a.begin();
        let album = trx.create(&Album { name: "Origin of Symmetry".into(), year: "2001".into() }).await?;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Wait for propagation to server
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check server received the change
    assert_eq!(check_server(), vec![vec![], vec![(album_id, ChangeKind::Add)]]);

    // Wait for propagation to client_b
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check client_b received the change
    assert_eq!(check_client_b(), vec![vec![], vec![(album_id, ChangeKind::Add)]]);

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

    info!("Created pet with id: {}", pet_id);

    // PART 1: Test that Livequery/View subscriptions work when active

    // Set up query subscription on client that matches our pet
    let (lq_listener, check_lq_notifs) = common::changeset_watcher::<PetView>();
    // This is the actual livequery, which has a predicate subscription with the server because the client node is ephemeral.
    let client_livequery = client.query("name = 'Buddy'")?;
    // LOCALLY subscribe to notifications from the livequery. This is a different kind of subscription than above
    // the guard does keep the livequery alive, but dropping it does not necessarily drop the LiveQuery and unsubscribe from the server.
    let lq_subguard = client_livequery.subscribe(lq_listener);

    // Wait for initial state to be received from the server
    client_livequery.wait_initialized().await;

    // NOTE!
    // calling .subscribe does not immediately call the listener. We are receiving the Initial change because every
    // LiveQuery starts empty and is initialized asynchronously, and crucially for reproducibility here
    // there are no awaits between the creation of the LiveQuery and the subscription.
    assert_eq!(check_lq_notifs(), vec![vec![(pet_id, ChangeKind::Initial)]]);

    // Get the pet view from client and set up View/field subscriptions
    let client_pet = client.get::<PetView>(pet_id).await?;
    let (view_watcher, check_view_changes) = common::generic_watcher::<PetView>();
    // subscribe to the View directly - this gets notified when any change to this view arrives from any source
    let view_subguard = client_pet.subscribe(view_watcher);

    // The view subscription is a good example of this, because there's no such thing as an uninitialized view
    assert_eq!(check_view_changes(), vec![]);

    // Make an edit on the server
    {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.age().replace("4")?;
        trx.commit().await?;
    }

    // Wait for propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify that View/field subscriptions received the update
    assert_eq!(check_view_changes(), vec![client_pet.clone()]);
    assert_eq!(check_lq_notifs(), vec![vec![(pet_id, ChangeKind::Update)]]); // Buddy's age changed to 4 - still matches the query

    info!("Dropping query subscription handle...");
    drop(lq_subguard); // This should stop the local listener

    // Make another edit on the server
    {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.age().replace("4")?;
        trx.commit().await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // wait for propagation to client

    // Verify that subscriptions did NOT receive the update after being dropped
    assert_eq!(
        check_lq_notifs(),
        Vec::<Vec<(EntityId, ChangeKind)>>::new(),
        "subscription to LiveQuery signal should not receive updates after dropping lq_subguard"
    );
    assert_eq!(
        check_view_changes(),
        vec![client_pet.clone()],
        "Subscription to View signal should still receive updates because both client_livequery and _view_subguard are still alive"
    );

    drop(client_livequery);

    {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.age().replace("5")?;
        trx.commit().await?;
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // wait for propagation to client
    assert_eq!(check_lq_notifs(), Vec::<Vec<(EntityId, ChangeKind)>>::new(), "subscription to LiveQuery signal should still be dead");
    assert_eq!(check_view_changes(), vec![], "The current expected behavior (though undesirable) is that the Entity itself should not receive updates after dropping client_livequery");

    // TODO: After implementing implicit entity subscriptions, the entity should continue receiving updates after dropping client_livequery
    // at which point the above assertion should change, and then
    // drop(view_subguard);
    // {
    //     let trx = server.begin();
    //     let server_pet = server.get::<PetView>(pet_id).await?;
    //     server_pet.edit(&trx)?.age().replace("6")?;
    //     trx.commit().await?;
    // }
    // tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // wait for propagation to client
    // assert_eq!(pet.age(), "6"); // Updates to the underlying entity should continue even after everything (other than the Entity itself) is dropped
    // assert_eq!(check_view_changes(), vec![],"But the subscription to the View signal should be dead with the dropping of the view_subguard"

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

    info!("Created pet with id: {}", pet_id);

    // === Test fetch() behavior ===

    // Use fetch() to get the entity on client (no ongoing subscription)
    let fetch_result = client.fetch::<PetView>("name = 'Luna'").await?;
    assert_eq!(fetch_result.len(), 1, "Should fetch one pet");

    let client_pet = fetch_result.iter().next().unwrap();

    // Set up View/field subscriptions on the fetched entity
    let (view_watcher, check_view_changes) = common::generic_watcher::<PetView>();
    // let (name_watcher, check_name_changes) = common::generic_watcher::<String>();
    // let (age_watcher, check_age_changes) = common::generic_watcher::<String>();

    let _view_handle = client_pet.subscribe(view_watcher);
    // let _name_handle = client_pet.name().subscribe(name_watcher);
    // let _age_handle = client_pet.age().subscribe(age_watcher);

    // Make an edit on the server
    {
        let trx = server.begin();
        let server_pet = server.get::<PetView>(pet_id).await?;
        server_pet.edit(&trx)?.name().replace("Stella")?;
        trx.commit().await?;
    }

    // Wait for potential propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Verify that View/field subscriptions did NOT receive updates
    let view_changes = check_view_changes();
    // let name_changes = check_name_changes();
    // let age_changes = check_age_changes();

    info!("After server edit with fetch() only:");
    info!("View changes: {}", view_changes.len());
    // info!("Name changes: {}", name_changes.len());
    // info!("Age changes: {}", age_changes.len());

    // This documents the current behavior - fetch() doesn't establish ongoing subscriptions
    assert_eq!(view_changes.len(), 0, "View subscription should NOT receive updates with fetch() only (current behavior)");
    // assert_eq!(name_changes.len(), 0, "Name field subscription should NOT receive updates with fetch() only (current behavior)");
    // assert_eq!(age_changes.len(), 0, "Age field subscription should NOT receive updates with fetch() only (current behavior)");

    Ok(())
}
