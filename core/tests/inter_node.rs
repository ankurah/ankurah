mod common;

use ankurah_core::changes::ChangeKind;
use ankurah_core::connector::local_process::LocalProcessConnection;
use ankurah_core::node::{FetchArgs, Node};
use ankurah_core::storage::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use ankurah_core::model::Mutable;
use ankurah_core::resultset::ResultSet;
use common::{Album, AlbumView, Pet, PetView};

pub fn names(resultset: ResultSet<AlbumView>) -> Vec<String> { resultset.items.iter().map(|r| r.name()).collect::<Vec<String>>() }

#[tokio::test]
async fn inter_node_fetch() -> Result<()> {
    let node1 = Arc::new(Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap())));
    let node2 = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    {
        let trx = node1.begin();
        trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await;
        trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await;
        trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await;
        trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await;
        trx.commit().await?;
    };

    let p = "name = 'Walking on a Dream'";
    // Should already be on node1
    assert_eq!(names(node1.fetch(p).await?), ["Walking on a Dream"]);

    // But node2 because they arent connected
    assert_eq!(names(node2.fetch(FetchArgs { predicate: p.try_into()?, cached: true }).await?), [] as [&str; 0]);

    // Connect the nodes
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;

    // Now node2 should now successfully fetch the entity
    assert_eq!(names(node2.fetch(p).await?), ["Walking on a Dream"]);

    Ok(())
}

#[tokio::test]
async fn inter_node_subscription() -> Result<()> {
    // Create two nodes
    let node1 = Arc::new(Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap())));
    let node2 = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    // Connect the nodes
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;

    use ankurah_core::model::View;
    // Create initial entities on node1
    let (rex, snuffy, jasper) = {
        let trx = node1.begin();
        let rex = trx.create(&Pet { name: "Rex".to_string(), age: "1".to_string() }).await;
        let snuffy = trx.create(&Pet { name: "Snuffy".to_string(), age: "2".to_string() }).await;
        let jasper = trx.create(&Pet { name: "Jasper".to_string(), age: "6".to_string() }).await;

        let read = (rex.read(), snuffy.read(), jasper.read());
        trx.commit().await?;
        read
    };

    info!("rex: {}, snuffy: {}, jasper: {}", rex.entity(), snuffy.entity(), jasper.entity());

    // Set up subscription on node2
    let (watcher, check_node2) = common::changeset_watcher::<PetView>();
    let _handle = node2.subscribe("name = 'Rex' OR (age > 2 and age < 5)", watcher).await?;

    // Initial state should include Rex
    assert_eq!(check_node2(), vec![(rex.id(), ChangeKind::Initial)]);

    // Update Rex's age to 7 on node1
    {
        let trx = node1.begin();
        rex.edit(&trx).await?.age().overwrite(0, 1, "7");
        trx.commit().await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    assert_eq!(check_node2(), vec![(rex.id(), ChangeKind::Update)]); // Rex still matches the predicate, but the age has changed

    // short circuit to simplify debugging
    // Update Snuffy's age to 3 on node1
    {
        let trx = node1.begin();
        snuffy.edit(&trx).await?.age().overwrite(0, 1, "3");
        trx.commit().await?;
    }

    // Sleep for a bit to ensure the change is propagated

    // Should receive notification about Snuffy being added (now matches age > 2 and age < 5)
    let changes = check_node2();
    assert_eq!(changes, vec![(snuffy.id(), ChangeKind::Add)]);

    Ok(())
}

#[tokio::test]
async fn test_client_server_propagation() -> Result<()> {
    // Create server (durable) and two client nodes
    let server = Arc::new(Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap())));
    let client_a = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));
    let client_b = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    // Connect both clients to the server
    let _conn_a = LocalProcessConnection::new(&client_a, &server).await?;
    let _conn_b = LocalProcessConnection::new(&client_b, &server).await?;

    // Create an entity on client_a
    {
        let trx = client_a.begin();
        trx.create(&Album { name: "Origin of Symmetry".into(), year: "2001".into() }).await;
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
    let server = Arc::new(Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap())));
    let client_a = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));
    let client_b = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    // Connect both clients to the server
    let _conn_a = LocalProcessConnection::new(&client_a, &server).await?;
    let _conn_b = LocalProcessConnection::new(&client_b, &server).await?;

    // Set up watchers for server and client_b
    let (server_watcher, check_server) = common::changeset_watcher::<AlbumView>();
    let (client_b_watcher, check_client_b) = common::changeset_watcher::<AlbumView>();

    // Set up subscriptions
    let _server_sub = server.subscribe("name = 'Origin of Symmetry'", server_watcher).await?;
    let _client_b_sub = client_b.subscribe("name = 'Origin of Symmetry'", client_b_watcher).await?;

    // Create an entity on client_a
    let album_id = {
        let trx = client_a.begin();
        let album = trx.create(&Album { name: "Origin of Symmetry".into(), year: "2001".into() }).await;
        let id = album.id();
        trx.commit().await?;
        id
    };

    // Wait for propagation to server
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check server received the change
    assert_eq!(check_server(), vec![(album_id, ChangeKind::Add)]);

    // Wait for propagation to client_b
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Check client_b received the change
    assert_eq!(check_client_b(), vec![(album_id, ChangeKind::Add)]);

    Ok(())
}
