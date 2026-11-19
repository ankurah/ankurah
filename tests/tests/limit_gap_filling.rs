mod common;
use common::*;

#[tokio::test]
async fn ordered_queries_use_validated_bindings_before_catalog_subscription_delivery() -> anyhow::Result<()> {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    for (subscribe, offline) in [(false, false), (true, false), (true, true)] {
        let server = durable_sled_setup().await?;
        let client = ephemeral_sled_setup().await?;
        let hold = Arc::new(AtomicBool::new(false));
        let gated = hold.clone();
        let (connection, _gate) = GatedConnection::new(&server, &client, move |message| {
            gated.load(Ordering::SeqCst) && matches!(message, proto::NodeMessage::Update(_))
        })
        .await;
        let server_ctx = server.context_async(DEFAULT_CONTEXT).await?;
        let client_ctx = client.context_async(DEFAULT_CONTEXT).await?;
        hold.store(true, Ordering::SeqCst);
        create_albums(&server_ctx, 2020..=2024).await?;
        client_ctx.resolve_model_id::<Album>().await?;
        let property = Album::descriptor().field_by_name("year").unwrap().resolved.get(client.system.system_epoch().unwrap())?;
        let proto::PropertyId::EntityId(property) = property else { unreachable!() };
        assert!(matches!(client.storage.get_state(property).await, Err(ankurah::core::error::RetrievalError::EntityNotFound(_))));

        if offline {
            assert_eq!(client_ctx.fetch::<AlbumView>("true").await?.len(), 5);
            drop(connection);
            client.deregister_peer(server.id);
            server.deregister_peer(client.id);
            let query = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client_ctx.query_wait::<AlbumView>("true ORDER BY year DESC LIMIT 2"),
            )
            .await??;
            assert_eq!(years(&query), vec!["2024", "2023"]);
        } else if subscribe {
            let query = client_ctx.query_wait::<AlbumView>(nocache("true ORDER BY year DESC LIMIT 2")?).await?;
            assert_eq!(years(&query), vec!["2024", "2023"]);
        } else {
            let albums = client_ctx.fetch::<AlbumView>("true ORDER BY year DESC LIMIT 2").await?;
            assert_eq!(albums.iter().map(|album| album.year().unwrap()).collect::<Vec<_>>(), vec!["2024", "2023"]);
        }
        assert!(
            matches!(client.storage.get_state(property).await, Err(ankurah::core::error::RetrievalError::EntityNotFound(_))),
            "tree preparation must not fetch a missing catalog record"
        );
        if offline {
            use ankurah::core::{
                property::backend::{LWWBackend, PropertyBackend},
                storage::StorageTransaction,
            };
            let mut definition = server.storage.get_state(property).await?;
            definition.payload.state.state_buffers.0.insert("lww".into(), LWWBackend::new().to_state_buffer()?);
            let mut trx = client.storage.transaction();
            trx.set_state(&proto::Clock::default(), &definition).await?;
            trx.commit().await?.committed()?;
            assert!(
                matches!(
                    client_ctx.query_wait::<AlbumView>("true ORDER BY year ASC LIMIT 1").await,
                    Err(ankurah::core::error::RetrievalError::InvalidState(_))
                ),
                "an available but malformed definition must not fall back to its registered type"
            );
        }
    }
    Ok(())
}

/// Test ORDER BY + LIMIT gap filling on a single durable node
///
/// This test verifies that when entities are removed from a limited result set,
/// the gaps are automatically filled by fetching additional entities from local storage.
#[tokio::test]
async fn test_single_node_gap_filling() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ctx = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await.unwrap();
    let ids = create_albums(&ctx, 2020..=2024).await?;

    let watcher = TestWatcher::changeset();
    let query = ctx.query_wait::<AlbumView>("year >= '2020' ORDER BY year ASC LIMIT 3").await?;
    let _handle = query.subscribe(&watcher);

    // Initial state should have the first 3 albums (2020, 2021, 2022)
    assert_eq!(watcher.quiesce().await, 0);
    assert_eq!(query.peek().len(), 3);
    assert_eq!(years(&query), vec!["2020", "2021", "2022"]);

    // Update the middle album (2021) to no longer match - this should trigger gap filling
    {
        let trx = ctx.begin();
        trx.get::<Album>(&ids[1]).await?.year()?.replace("1999")?; // no longer matches year >= '2020'
        trx.commit().await?;
    }

    // Wait for gap filling: remove notification for updated album, then add notification for 2023
    assert_eq!(watcher.take_one().await, vec![(ids[1], ChangeKind::Remove), (ids[3], ChangeKind::Add)]);

    // Final state should have 2020, 2022, 2023 (gap filled with 2023)
    assert_eq!(years(&query), vec!["2020", "2022", "2023"]);
    assert_eq!(watcher.quiesce().await, 0);
    Ok(())
}

/// Test ORDER BY + LIMIT gap filling with multiple deletions
///
/// This test verifies gap filling works correctly when multiple entities are removed,
/// requiring multiple entities to be fetched to fill the gaps.
#[tokio::test]
async fn test_single_node_multiple_gap_filling() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ctx = durable_sled_setup().await?.context_async(DEFAULT_CONTEXT).await.unwrap();
    let ids = create_albums(&ctx, 2020..=2030).await?;
    let watcher = TestWatcher::changeset();
    let query = ctx.query_wait::<AlbumView>("year >= '2020' ORDER BY year ASC LIMIT 5").await?;
    let _handle = query.subscribe(&watcher);

    assert_eq!(watcher.quiesce().await, 0);
    assert_eq!(years(&query), vec!["2020", "2021", "2022", "2023", "2024"]);

    // Update two albums (2021 and 2023) to no longer match - this should trigger gap filling for both
    let trx = ctx.begin();
    trx.get::<Album>(&ids[1]).await?.year()?.replace("1999")?;
    trx.get::<Album>(&ids[3]).await?.year()?.replace("1999")?;
    trx.commit().await?;

    // Wait for consolidated gap filling update: 2 removes + 2 adds in one update
    assert_eq!(
        watcher.take_one().await,
        vec![
            (ids[1], ChangeKind::Remove), // 2021
            (ids[3], ChangeKind::Remove), // 2023
            (ids[5], ChangeKind::Add),    // 2025
            (ids[6], ChangeKind::Add),    // 2026
        ]
    );
    assert_eq!(years(&query), vec!["2020", "2022", "2024", "2025", "2026"]);
    assert_eq!(watcher.quiesce().await, 0);
    Ok(())
}

/// Test ORDER BY + LIMIT gap filling between ephemeral and durable nodes
///
/// This test verifies that when entities are removed from a limited result set on an ephemeral node,
/// the gaps are automatically filled by fetching additional entities from the remote durable node.
#[tokio::test]
async fn test_inter_node_gap_filling() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    let server = server.context_async(DEFAULT_CONTEXT).await.unwrap();
    let client = client.context_async(DEFAULT_CONTEXT).await.unwrap();
    let ids = create_albums(&server, 2020..=2025).await?;

    let watcher = TestWatcher::changeset();
    let query = client.query_wait::<AlbumView>(nocache("year >= '2020' ORDER BY year ASC LIMIT 3")?).await?;
    let _handle = query.subscribe(&watcher);

    // Initial state should have the first 3 albums (2020, 2021, 2022)
    assert_eq!(watcher.quiesce().await, 0);
    assert_eq!(years(&query), vec!["2020", "2021", "2022"]);

    let trx = server.begin();
    trx.get::<Album>(&ids[1]).await?.year()?.replace("1999")?; // no longer matches year >= '2020'
    trx.commit().await?;

    assert_eq!(watcher.take_one().await, vec![(ids[1], ChangeKind::Remove), (ids[3], ChangeKind::Add)]);
    assert_eq!(years(&query), vec!["2020", "2022", "2023"]);

    assert_eq!(watcher.quiesce().await, 0);
    Ok(())
}

/// Test ORDER BY + LIMIT gap filling with DESC ordering between nodes
///
/// This test verifies gap filling works correctly with descending order between ephemeral and durable nodes.
#[tokio::test]
async fn test_inter_node_gap_filling_desc() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    let server = server.context_async(DEFAULT_CONTEXT).await.unwrap();
    let client = client.context_async(DEFAULT_CONTEXT).await.unwrap();
    let ids = create_albums(&server, 2020..=2027).await?;

    let watcher = TestWatcher::changeset();
    let query = client.query_wait::<AlbumView>(nocache("year >= '2020' ORDER BY year DESC LIMIT 4")?).await?;
    let _handle = query.subscribe(&watcher);

    assert_eq!(watcher.quiesce().await, 0);
    assert_eq!(years(&query), vec!["2027", "2026", "2025", "2024"]);

    let trx = server.begin();
    trx.get::<Album>(&ids[4]).await?.year()?.replace("1999")?;
    trx.get::<Album>(&ids[6]).await?.year()?.replace("1999")?;
    trx.commit().await?;

    assert_eq!(
        watcher.take_one().await,
        vec![(ids[4], ChangeKind::Remove), (ids[6], ChangeKind::Remove), (ids[3], ChangeKind::Add), (ids[2], ChangeKind::Add),]
    );
    assert_eq!(years(&query), vec!["2027", "2025", "2023", "2022"]);
    assert_eq!(watcher.quiesce().await, 0);
    Ok(())
}
