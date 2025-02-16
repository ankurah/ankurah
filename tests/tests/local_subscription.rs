use ankurah::{
    changes::{ChangeKind, ChangeSet},
    policy::DEFAULT_CONTEXT as c,
    Mutable, Node, PermissiveAgent, ResultSet,
};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::{Arc, Mutex};

mod common;
use common::{Album, AlbumView, Pet, PetView};

#[tokio::test]
async fn basic_local_subscription() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new()).context(c);

    // Create some initial entities
    {
        let trx = client.begin();
        trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await;
        trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await;
        trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await;
        trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await;
        trx.commit().await?;
    }

    // Set up subscription
    let received_changesets = Arc::new(Mutex::new(Vec::new()));
    let received_changesets_clone = received_changesets.clone();

    let predicate = ankql::parser::parse_selection("year > '2015'").unwrap();
    let _handle = client
        .subscribe(predicate, move |changeset: ChangeSet<AlbumView>| {
            let mut received = received_changesets_clone.lock().unwrap();
            received.push(changeset);
        })
        .await?;

    // Initial state should have Two Vines and Ask That God
    {
        let changesets = received_changesets.lock().unwrap();
        assert_eq!(changesets.len(), 1);
        let changeset = &changesets[0];
        assert_eq!(changeset.changes.len(), 2);
        // TODO: Add more specific assertions about the changes
    }

    // Update an entity
    {
        let trx = client.begin();
        let albums: ResultSet<AlbumView> = client.fetch("name = 'Ice on the Dune'").await?;
        let album = albums.items[0].edit(&trx).await?;
        album.year().overwrite(0, 4, "2020");
        trx.commit().await?;
    }

    // Should have received a notification about Ice on the Dune being added
    {
        let changesets = received_changesets.lock().unwrap();
        assert_eq!(changesets.len(), 2);
        let changeset = &changesets[1];
        assert_eq!(changeset.changes.len(), 1);
        // TODO: Add more specific assertions about the changes
    }

    Ok(())
}

#[tokio::test]
async fn complex_local_subscription() {
    // Create a new node
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new()).context(c);
    let (watcher, check) = common::changeset_watcher::<PetView>();

    // Subscribe to changes
    let _handle = node.subscribe("name = 'Rex' OR (age > 2 and age < 5)", watcher).await.unwrap();

    let (rex, snuffy, jasper);
    {
        // Create some test entities
        let trx = node.begin();
        rex = trx.create(&Pet { name: "Rex".to_string(), age: "1".to_string() }).await.read();

        snuffy = trx.create(&Pet { name: "Snuffy".to_string(), age: "2".to_string() }).await.read();

        jasper = trx.create(&Pet { name: "Jasper".to_string(), age: "6".to_string() }).await.read();

        trx.commit().await.unwrap();
    };

    // Verify initial state
    assert_eq!(check(), vec![vec![(rex.id(), ChangeKind::Add)]]); // Initial state should be an Add

    {
        // Update Rex's age to 7
        let trx = node.begin();
        rex.edit(&trx).await.unwrap().age().overwrite(0, 1, "7");
        trx.commit().await.unwrap();
    }

    // Verify Rex's update was received - should be Edit since it still matches name = 'Rex'
    assert_eq!(check(), vec![vec![(rex.id(), ChangeKind::Update)]]);

    {
        // Update Snuffy's age to 3
        let trx = node.begin();
        snuffy.edit(&trx).await.unwrap().age().overwrite(0, 1, "3");
        trx.commit().await.unwrap();
    }

    // Verify Snuffy's update was received (now matches age > 2 and age < 5)
    assert_eq!(check(), vec![vec![(snuffy.id(), ChangeKind::Add)]]);

    // Update Jasper's age to 4
    {
        let trx = node.begin();
        jasper.edit(&trx).await.unwrap().age().overwrite(0, 1, "4");
        trx.commit().await.unwrap();
    }

    // Verify Jasper's update was received (now matches age > 2 and age < 5)
    assert_eq!(check(), vec![vec![(jasper.id(), ChangeKind::Add)]]);

    // Update Snuffy and Jasper to ages outside the range
    let trx = node.begin();
    let snuffy_edit = snuffy.edit(&trx).await.unwrap();
    snuffy_edit.age().overwrite(0, 1, "5");
    let jasper_edit = jasper.edit(&trx).await.unwrap();
    jasper_edit.age().overwrite(0, 1, "6");
    trx.commit().await.unwrap();

    // Verify both updates were received as removals
    assert_eq!(check(), vec![vec![(snuffy.id(), ChangeKind::Remove), (jasper.id(), ChangeKind::Remove)]]);

    // Update Rex to no longer match the query (instead of deleting)
    // This should still trigger a ChangeKind::Remove since it no longer matches
    let trx = node.begin();
    let rex_edit = rex.edit(&trx).await.unwrap();
    rex_edit.name().overwrite(0, 3, "NotRex");
    trx.commit().await.unwrap();

    // Verify Rex's "removal" was received
    assert_eq!(check(), vec![vec![(rex.id(), ChangeKind::Remove)]]);
}
