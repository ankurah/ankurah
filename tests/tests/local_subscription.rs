use ankurah::{changes::ChangeKind, policy::DEFAULT_CONTEXT as c, EntityId, Mutable, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

mod common;
use common::{Album, AlbumView, Pet, PetView};

use crate::common::TestWatcher;

#[tokio::test]
async fn basic_local_subscription() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    // Create some initial entities
    let (two_vines, ask_that_god, ice_on_the_dune) = {
        let trx = ctx.begin();
        trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await?;
        let ice_on_the_dune = trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await?.read();
        let two_vines = trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await?.read();
        let ask_that_god = trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await?.read();
        trx.commit().await?;

        (two_vines, ask_that_god, ice_on_the_dune)
    };

    // Set up subscription using the watcher pattern
    let watcher = TestWatcher::changeset();

    let predicate = ankql::parser::parse_selection("year > '2015'").unwrap();
    use ankurah::signals::Subscribe;
    let query = ctx.query_wait::<AlbumView>(predicate).await?;
    let _handle = query.subscribe(&watcher);

    // Initial state should have Two Vines and Ask That God
    assert_eq!(watcher.quiesce().await, 0);
    use ankurah::signals::Peek;
    // Order depends on ULID random component when created in same millisecond
    let mut actual_ids = query.peek().iter().map(|p| p.id()).collect::<Vec<EntityId>>();
    actual_ids.sort();
    let mut expected_ids = vec![two_vines.id(), ask_that_god.id()];
    expected_ids.sort();
    assert_eq!(actual_ids, expected_ids);

    // Update an entity
    {
        let trx = ctx.begin();
        let albums: Vec<AlbumView> = ctx.fetch("name = 'Ice on the Dune'").await?;
        let album = albums[0].edit(&trx)?;
        album.year().overwrite(0, 4, "2020")?;
        trx.commit().await?;
    }

    // After update, should have all three albums
    // Order depends on ULID random component when created in same millisecond
    let mut actual_ids = query.peek().iter().map(|p| p.id()).collect::<Vec<EntityId>>();
    actual_ids.sort();
    let mut expected_ids = vec![two_vines.id(), ask_that_god.id(), ice_on_the_dune.id()];
    expected_ids.sort();
    assert_eq!(actual_ids, expected_ids);

    // Should have received a notification about Ice on the Dune being added
    assert_eq!(watcher.drain(), vec![vec![(ice_on_the_dune.id(), ChangeKind::Add)]]);

    Ok(())
}

#[tokio::test]
async fn complex_local_subscription() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create a new node
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let watcher = TestWatcher::changeset();

    // Subscribe to changes
    use ankurah::signals::Subscribe;
    let query = ctx.query_wait::<PetView>("name = 'Rex' OR (age > 2 and age < 5)").await?;
    let _handle = query.subscribe(&watcher);

    let (rex, snuffy, jasper);
    {
        // Create some test entities
        let trx = ctx.begin();
        rex = trx.create(&Pet { name: "Rex".to_string(), age: "1".to_string() }).await?.read();

        snuffy = trx.create(&Pet { name: "Snuffy".to_string(), age: "2".to_string() }).await?.read();

        jasper = trx.create(&Pet { name: "Jasper".to_string(), age: "6".to_string() }).await?.read();

        trx.commit().await.unwrap();
    };

    // Verify initial state
    assert_eq!(watcher.drain(), vec![vec![(rex.id(), ChangeKind::Add)]]); // Initial state should be an Add

    {
        // Update Rex's age to 7
        let trx = ctx.begin();
        rex.edit(&trx).unwrap().age().overwrite(0, 1, "7")?;
        trx.commit().await.unwrap();
    }

    // Verify Rex's update was received - should be Edit since it still matches name = 'Rex'
    assert_eq!(watcher.drain(), vec![vec![(rex.id(), ChangeKind::Update)]]);

    {
        // Update Snuffy's age to 3
        let trx = ctx.begin();
        snuffy.edit(&trx).unwrap().age().overwrite(0, 1, "3")?;
        trx.commit().await.unwrap();
    }

    // Verify Snuffy's update was received (now matches age > 2 and age < 5)
    assert_eq!(watcher.drain(), vec![vec![(snuffy.id(), ChangeKind::Add)]]);

    // Update Jasper's age to 4
    {
        let trx = ctx.begin();
        jasper.edit(&trx).unwrap().age().overwrite(0, 1, "4")?;
        trx.commit().await.unwrap();
    }

    // Verify Jasper's update was received (now matches age > 2 and age < 5)
    assert_eq!(watcher.drain(), vec![vec![(jasper.id(), ChangeKind::Add)]]);

    // Update Snuffy and Jasper to ages outside the range
    let trx = ctx.begin();
    let snuffy_edit = snuffy.edit(&trx).unwrap();
    snuffy_edit.age().overwrite(0, 1, "5")?;
    let jasper_edit = jasper.edit(&trx).unwrap();
    jasper_edit.age().overwrite(0, 1, "6")?;
    trx.commit().await.unwrap();

    // Verify both updates were received as removals
    assert_eq!(watcher.drain(), vec![vec![(snuffy.id(), ChangeKind::Remove), (jasper.id(), ChangeKind::Remove)]]);

    // Update Rex to no longer match the query (instead of deleting)
    // This should still trigger a ChangeKind::Remove since it no longer matches
    let trx = ctx.begin();
    let rex_edit = rex.edit(&trx).unwrap();
    rex_edit.name().overwrite(0, 3, "NotRex")?;
    trx.commit().await.unwrap();

    // Verify Rex's "removal" was received
    assert_eq!(watcher.drain(), vec![vec![(rex.id(), ChangeKind::Remove)]]);
    Ok(())
}

/// Demonstrates the difference between ResultSet Signal and LiveQuery Signal notification semantics:
/// - ResultSet Signal fires only on membership changes (entities entering/leaving the query)
/// - LiveQuery Signal fires on all entity changes (including property updates for entities already in the query)
#[tokio::test]
async fn resultset_vs_livequery_signal_semantics() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    // Create initial entities
    let (album_a, album_b) = {
        let trx = ctx.begin();
        let a = trx.create(&Album { name: "Album A".into(), year: "2020".into() }).await?.read();
        let b = trx.create(&Album { name: "Album B".into(), year: "2015".into() }).await?.read();
        trx.commit().await?;
        (a, b)
    };

    // Set up query for albums where year >= 2020
    let predicate = ankql::parser::parse_selection("year >= '2020'").unwrap();
    let query = ctx.query_wait::<AlbumView>(predicate).await?;

    // Set up watchers for ResultSet Signal vs LiveQuery Signal
    let resultset_watcher = TestWatcher::new();
    let livequery_watcher = TestWatcher::new();

    use ankurah::signals::Signal;
    let _resultset_guard = {
        let watcher = resultset_watcher.clone();
        query.resultset().listen(Arc::new(move |()| watcher.notify(())))
    };
    let _livequery_guard = {
        let watcher = livequery_watcher.clone();
        query.listen(Arc::new(move |()| watcher.notify(())))
    };

    // Initial state - no notifications yet
    assert_eq!(resultset_watcher.drain().len(), 0);
    assert_eq!(livequery_watcher.drain().len(), 0);

    // Test 1: Property update (no membership change)
    // album_a is in the query, and stays in the query
    {
        let trx = ctx.begin();
        let album = album_a.edit(&trx)?;
        album.name().overwrite(0, 7, "Album A Updated")?;
        trx.commit().await?;
    }

    // ResultSet Signal should NOT fire (no membership change)
    // LiveQuery Signal SHOULD fire (entity property changed)
    assert_eq!(resultset_watcher.drain().len(), 0, "ResultSet signal should not fire on property updates within query");
    assert_eq!(livequery_watcher.drain().len(), 1, "LiveQuery signal should fire on property updates");

    // Test 2: Entity enters the query (membership change)
    {
        let trx = ctx.begin();
        let album = album_b.edit(&trx)?;
        album.year().overwrite(0, 4, "2021")?; // Now year >= 2020, so it enters the query
        trx.commit().await?;
    }

    // Both signals should fire (membership changed)
    let resultset_count = resultset_watcher.drain().len();
    let livequery_count = livequery_watcher.drain().len();
    assert!(resultset_count >= 1, "ResultSet signal should fire on membership change (got {})", resultset_count);
    assert!(livequery_count >= 1, "LiveQuery signal should fire on membership change (got {})", livequery_count);

    // Test 3: Another property update (no membership change)
    {
        let trx = ctx.begin();
        let album = album_a.edit(&trx)?;
        album.name().overwrite(0, 15, "Album A Changed Again")?;
        trx.commit().await?;
    }

    // ResultSet Signal should NOT fire (no membership change)
    // LiveQuery Signal SHOULD fire (entity property changed)
    assert_eq!(resultset_watcher.drain().len(), 0, "ResultSet signal should not fire on property updates within query");
    assert_eq!(livequery_watcher.drain().len(), 1, "LiveQuery signal should fire on property updates");

    Ok(())
}
