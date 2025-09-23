mod common;

use ankurah::{
    changes::ChangeKind, error::MutationError, policy::DEFAULT_CONTEXT as ctx_data, Context, EntityId, LiveQuery, Mutable, Node,
    PermissiveAgent,
};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;
use tracing::info;

use crate::common::TestWatcher;
use ankurah::signals::{Peek, Subscribe};
use common::{create_albums, years, Album, AlbumView};

/// Test ORDER BY + LIMIT gap filling on a single durable node
///
/// This test verifies that when entities are removed from a limited result set,
/// the gaps are automatically filled by fetching additional entities from local storage.
#[tokio::test]
async fn test_single_node_gap_filling() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(ctx_data)?;

    let ids = create_albums(&ctx, 2020..=2024).await?;

    // Set up subscription with ORDER BY year ASC LIMIT 3
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
        trx.get::<Album>(&ids[1]).await?.year().replace("1999")?; // no longer matches year >= '2020'
        trx.commit().await?;
    }

    // Wait for gap filling: remove notification for updated album, then add notification for 2023
    assert_eq!(watcher.take_when(2).await, vec![vec![(ids[1], ChangeKind::Remove)], vec![(ids[3], ChangeKind::Add)]]);

    // Final state should have 2020, 2022, 2023 (gap filled with 2023)
    assert_eq!(years(&query), vec!["2020", "2022", "2023"]);

    Ok(())
}

/// Test ORDER BY + LIMIT gap filling with multiple deletions
///
/// This test verifies gap filling works correctly when multiple entities are removed,
/// requiring multiple entities to be fetched to fill the gaps.
#[tokio::test]
async fn test_single_node_multiple_gap_filling() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(ctx_data)?;

    // Create more test albums
    let _albums = {
        let trx = ctx.begin();
        for year in 2020..=2030 {
            trx.create(&Album { name: format!("Album {}", year), year: year.to_string() }).await?;
        }
        trx.commit().await?;
    };

    info!("Created 11 test albums (2020-2030)");

    // Set up subscription with ORDER BY year ASC LIMIT 5
    let watcher = TestWatcher::changeset();
    use ankurah::signals::{Peek, Subscribe};
    let query = ctx.query_wait::<AlbumView>("year >= '2020' ORDER BY year ASC LIMIT 5").await?;
    let _handle = query.subscribe(&watcher);

    // Initial state should have 2020, 2021, 2022, 2023, 2024
    assert_eq!(watcher.quiesce().await, 0);
    assert_eq!(query.peek().len(), 5);
    assert_eq!(
        query.peek().iter().map(|a| a.year().unwrap_or_default()).collect::<Vec<String>>(),
        vec!["2020", "2021", "2022", "2023", "2024"]
    );

    // Update two albums (2021 and 2023) to no longer match - this should trigger gap filling for both
    {
        let trx = ctx.begin();
        let albums_to_update: Vec<AlbumView> = ctx.fetch("year = '2021' OR year = '2023'").await?;
        assert_eq!(albums_to_update.len(), 2);
        for album_view in albums_to_update {
            album_view.edit(&trx)?.year().overwrite(0, 4, "1999")?; // Change to 1999 so it no longer matches
        }
        trx.commit().await?;
    }

    // Wait for gap filling: 2 remove notifications, then 2 add notifications
    let changes = watcher.take_when(4).await;
    let flat_changes: Vec<_> = changes.into_iter().flatten().collect();
    assert_eq!(flat_changes.iter().filter(|(_, kind)| *kind == ChangeKind::Remove).count(), 2);
    assert_eq!(flat_changes.iter().filter(|(_, kind)| *kind == ChangeKind::Add).count(), 2);

    // Final state should have 2020, 2022, 2024, 2025, 2026 (gaps filled with 2025, 2026)
    assert_eq!(query.peek().len(), 5);
    assert_eq!(
        query.peek().iter().map(|a| a.year().unwrap_or_default()).collect::<Vec<String>>(),
        vec!["2020", "2022", "2024", "2025", "2026"]
    );

    Ok(())
}

/// Test ORDER BY + LIMIT gap filling between ephemeral and durable nodes
///
/// This test verifies that when entities are removed from a limited result set on an ephemeral node,
/// the gaps are automatically filled by fetching additional entities from the remote durable node.
#[tokio::test]
async fn test_inter_node_gap_filling() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create durable and ephemeral nodes
    let durable_node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let ephemeral_node = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    info!("Durable node root: {:?}", durable_node.system.root());
    info!("Ephemeral node root: {:?}", ephemeral_node.system.root());

    // Initialize the system on the durable node
    durable_node.system.create().await?;

    // Create connection between nodes
    let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;

    // Get contexts for both nodes
    let durable_ctx = durable_node.context_async(ctx_data).await;
    let ephemeral_ctx = ephemeral_node.context_async(ctx_data).await;

    // Create test albums on the durable node
    let albums = {
        let trx = durable_ctx.begin();
        let album1 = trx.create(&Album { name: "Album 2020".into(), year: "2020".into() }).await?.read();
        let album2 = trx.create(&Album { name: "Album 2021".into(), year: "2021".into() }).await?.read();
        let album3 = trx.create(&Album { name: "Album 2022".into(), year: "2022".into() }).await?.read();
        let album4 = trx.create(&Album { name: "Album 2023".into(), year: "2023".into() }).await?.read();
        let album5 = trx.create(&Album { name: "Album 2024".into(), year: "2024".into() }).await?.read();
        let album6 = trx.create(&Album { name: "Album 2025".into(), year: "2025".into() }).await?.read();
        trx.commit().await?;
        vec![album1, album2, album3, album4, album5, album6]
    };

    info!("Created {} test albums on durable node", albums.len());

    // Set up subscription on ephemeral node with ORDER BY year ASC LIMIT 3
    let watcher = TestWatcher::changeset();
    use ankurah::signals::{Peek, Subscribe};
    let query = ephemeral_ctx.query_wait::<AlbumView>("year >= '2020' ORDER BY year ASC LIMIT 3").await?;
    let _handle = query.subscribe(&watcher);

    // Initial state should have the first 3 albums (2020, 2021, 2022)
    assert_eq!(watcher.quiesce().await, 0);
    assert_eq!(query.peek().len(), 3);
    assert_eq!(query.peek().iter().map(|a| a.year().unwrap_or_default()).collect::<Vec<String>>(), vec!["2020", "2021", "2022"]);

    // Update an album on the durable node to no longer match (this will propagate to ephemeral node)
    {
        let trx = durable_ctx.begin();
        let albums_to_update: Vec<AlbumView> = durable_ctx.fetch("year = '2021'").await?;
        assert_eq!(albums_to_update.len(), 1);
        albums_to_update[0].edit(&trx)?.year().overwrite(0, 4, "1999")?; // Change to 1999 so it no longer matches
        trx.commit().await?;
    }

    // Wait for gap filling on ephemeral node: should get both remove and add notifications
    let changes = watcher.take_when(2).await;
    let flat_changes: Vec<_> = changes.into_iter().flatten().collect();
    assert_eq!(flat_changes.len(), 2);
    // Events may arrive in any order due to async nature of inter-node communication
    assert_eq!(flat_changes.iter().filter(|(_, kind)| *kind == ChangeKind::Remove).count(), 1);
    assert_eq!(flat_changes.iter().filter(|(_, kind)| *kind == ChangeKind::Add).count(), 1);

    // Final state should have 2020, 2022, 2023 (gap filled with 2023 from durable node)
    // Wait a bit for all changes to propagate
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    assert_eq!(query.peek().len(), 3);
    assert_eq!(query.peek().iter().map(|a| a.year().unwrap_or_default()).collect::<Vec<String>>(), vec!["2020", "2022", "2023"]);

    Ok(())
}

/// Test ORDER BY + LIMIT gap filling with DESC ordering between nodes
///
/// This test verifies gap filling works correctly with descending order between ephemeral and durable nodes.
#[tokio::test]
async fn test_inter_node_gap_filling_desc() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create durable and ephemeral nodes
    let durable_node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    let ephemeral_node = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    // Initialize the system on the durable node
    durable_node.system.create().await?;

    // Create connection between nodes
    let _conn = LocalProcessConnection::new(&durable_node, &ephemeral_node).await?;

    // Get contexts for both nodes
    let durable_ctx = durable_node.context_async(ctx_data).await;
    let ephemeral_ctx = ephemeral_node.context_async(ctx_data).await;

    // Create test albums on the durable node
    {
        let trx = durable_ctx.begin();
        for year in 2020..=2027 {
            trx.create(&Album { name: format!("Album {}", year), year: year.to_string() }).await?;
        }
        trx.commit().await?;
    }

    info!("Created 8 test albums (2020-2027) on durable node");

    // Set up subscription on ephemeral node with ORDER BY year DESC LIMIT 4
    let watcher = TestWatcher::changeset();
    use ankurah::signals::{Peek, Subscribe};
    let query = ephemeral_ctx.query_wait::<AlbumView>("year >= '2020' ORDER BY year DESC LIMIT 4").await?;
    let _handle = query.subscribe(&watcher);

    // Initial state should have the last 4 albums in DESC order (2027, 2026, 2025, 2024)
    assert_eq!(watcher.quiesce().await, 0);
    assert_eq!(query.peek().len(), 4);
    assert_eq!(query.peek().iter().map(|a| a.year().unwrap_or_default()).collect::<Vec<String>>(), vec!["2027", "2026", "2025", "2024"]);

    // Update two albums on the durable node to no longer match
    {
        let trx = durable_ctx.begin();
        let albums_to_update: Vec<AlbumView> = durable_ctx.fetch("year = '2026' OR year = '2024'").await?;
        assert_eq!(albums_to_update.len(), 2);
        for album_view in albums_to_update {
            album_view.edit(&trx)?.year().overwrite(0, 4, "1999")?; // Change to 1999 so it no longer matches
        }
        trx.commit().await?;
    }

    // Wait for gap filling on ephemeral node: should get 2 removes and 2 adds (may be interleaved)
    let changes = watcher.take_when(4).await;
    let flat_changes: Vec<_> = changes.into_iter().flatten().collect();
    // Due to async inter-node communication, we may get more than 4 individual changes
    // but we should have at least 2 removes and 2 adds
    assert!(flat_changes.iter().filter(|(_, kind)| *kind == ChangeKind::Remove).count() >= 2);
    assert!(flat_changes.iter().filter(|(_, kind)| *kind == ChangeKind::Add).count() >= 2);

    // Final state should have 2027, 2025, 2023, 2022 (gaps filled with 2023, 2022 from durable node)
    assert_eq!(query.peek().len(), 4);
    assert_eq!(query.peek().iter().map(|a| a.year().unwrap_or_default()).collect::<Vec<String>>(), vec!["2027", "2025", "2023", "2022"]);

    Ok(())
}
