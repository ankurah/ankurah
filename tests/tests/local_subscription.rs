use ankurah::{changes::ChangeKind, policy::DEFAULT_CONTEXT as c, EntityId, Mutable, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

mod common;
use common::{Album, AlbumView, Pet, PetView};

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
    let (watcher, check_changes) = common::changeset_watcher::<AlbumView>();

    let predicate = ankql::parser::parse_selection("year > '2015'").unwrap();
    use ankurah::signals::Subscribe;
    let query = ctx.query(predicate)?;
    query.wait_initialized().await;
    let _handle = query.subscribe(watcher);

    // Wait a moment for initialization to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Initial state should have Two Vines and Ask That God
    assert_eq!(check_changes(), Vec::<Vec<(EntityId, ChangeKind)>>::new());
    use ankurah::signals::Peek;
    // TODO - implement deterministic ordering
    let mut ids = query.peek().iter().map(|p| p.id()).collect::<Vec<EntityId>>();
    ids.sort();
    let mut expected_ids = vec![two_vines.id(), ask_that_god.id()];
    expected_ids.sort();
    assert_eq!(ids, expected_ids);

    // Update an entity
    {
        let trx = ctx.begin();
        let albums: Vec<AlbumView> = ctx.fetch("name = 'Ice on the Dune'").await?;
        let album = albums[0].edit(&trx)?;
        album.year().overwrite(0, 4, "2020")?;
        trx.commit().await?;
    }

    // TODO - implement deterministic ordering
    let mut ids = query.peek().iter().map(|p| p.id()).collect::<Vec<EntityId>>();
    ids.sort();
    let mut expected_ids = vec![two_vines.id(), ask_that_god.id(), ice_on_the_dune.id()];
    expected_ids.sort();
    assert_eq!(ids, expected_ids);

    // Should have received a notification about Ice on the Dune being added
    assert_eq!(check_changes(), vec![vec![(ice_on_the_dune.id(), ChangeKind::Add)]]);

    Ok(())
}

#[tokio::test]
async fn complex_local_subscription() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create a new node
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    node.system.create().await?;
    let ctx = node.context(c)?;

    let (watcher, check) = common::changeset_watcher::<PetView>();

    // Subscribe to changes
    use ankurah::signals::Subscribe;
    let query = ctx.query("name = 'Rex' OR (age > 2 and age < 5)")?;
    query.wait_initialized().await;
    let _handle = query.subscribe(watcher);

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
    assert_eq!(check(), vec![vec![(rex.id(), ChangeKind::Add)]]); // Initial state should be an Add

    {
        // Update Rex's age to 7
        let trx = ctx.begin();
        rex.edit(&trx).unwrap().age().overwrite(0, 1, "7")?;
        trx.commit().await.unwrap();
    }

    // Verify Rex's update was received - should be Edit since it still matches name = 'Rex'
    assert_eq!(check(), vec![vec![(rex.id(), ChangeKind::Update)]]);

    {
        // Update Snuffy's age to 3
        let trx = ctx.begin();
        snuffy.edit(&trx).unwrap().age().overwrite(0, 1, "3")?;
        trx.commit().await.unwrap();
    }

    // Verify Snuffy's update was received (now matches age > 2 and age < 5)
    assert_eq!(check(), vec![vec![(snuffy.id(), ChangeKind::Add)]]);

    // Update Jasper's age to 4
    {
        let trx = ctx.begin();
        jasper.edit(&trx).unwrap().age().overwrite(0, 1, "4")?;
        trx.commit().await.unwrap();
    }

    // Verify Jasper's update was received (now matches age > 2 and age < 5)
    assert_eq!(check(), vec![vec![(jasper.id(), ChangeKind::Add)]]);

    // Update Snuffy and Jasper to ages outside the range
    let trx = ctx.begin();
    let snuffy_edit = snuffy.edit(&trx).unwrap();
    snuffy_edit.age().overwrite(0, 1, "5")?;
    let jasper_edit = jasper.edit(&trx).unwrap();
    jasper_edit.age().overwrite(0, 1, "6")?;
    trx.commit().await.unwrap();

    // Verify both updates were received as removals
    // TODO - implement deterministic ordering
    let mut changes = check();
    assert_eq!(changes.len(), 1);
    changes[0].sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected_changes = vec![vec![(snuffy.id(), ChangeKind::Remove), (jasper.id(), ChangeKind::Remove)]];
    expected_changes[0].sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(changes, expected_changes);

    // Update Rex to no longer match the query (instead of deleting)
    // This should still trigger a ChangeKind::Remove since it no longer matches
    let trx = ctx.begin();
    let rex_edit = rex.edit(&trx).unwrap();
    rex_edit.name().overwrite(0, 3, "NotRex")?;
    trx.commit().await.unwrap();

    // Verify Rex's "removal" was received
    assert_eq!(check(), vec![vec![(rex.id(), ChangeKind::Remove)]]);
    Ok(())
}
