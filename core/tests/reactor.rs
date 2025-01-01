#[cfg(feature = "derive")]
use ankurah_core::changes::{ChangeSet, RecordChangeKind};
use ankurah_core::model::Model;
use ankurah_core::property::YrsString;
use ankurah_core::storage::SledStorageEngine;
use ankurah_core::{model::ScopedRecord, node::Node};
use ankurah_derive::Model;
use std::sync::{Arc, Mutex};

mod common;
use anyhow::Result;
use common::{Album, AlbumRecord};

#[derive(Debug, Clone, Model)]
pub struct Pet {
    #[active_value(YrsString)]
    pub name: String,
    #[active_value(YrsString)]
    pub age: String,
}

#[tokio::test]
async fn test_subscription_and_notification() -> Result<()> {
    let client = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    // Create some initial records
    {
        let trx = client.begin();
        trx.create(&Album {
            name: "Walking on a Dream".into(),
            year: "2008".into(),
        })
        .await;
        trx.create(&Album {
            name: "Ice on the Dune".into(),
            year: "2013".into(),
        })
        .await;
        trx.create(&Album {
            name: "Two Vines".into(),
            year: "2016".into(),
        })
        .await;
        trx.create(&Album {
            name: "Ask That God".into(),
            year: "2024".into(),
        })
        .await;
        trx.commit().await?;
    }

    // Set up subscription
    let received_changesets = Arc::new(Mutex::new(Vec::new()));
    let received_changesets_clone = received_changesets.clone();

    let predicate = ankql::parser::parse_selection("year > '2015'").unwrap();
    let _handle = client
        .subscribe(Album::bucket_name(), &predicate, move |changeset| {
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

    // Update a record
    {
        let trx = client.begin();
        let albums: ankurah_core::resultset::ResultSet<AlbumRecord> =
            client.fetch("name = 'Ice on the Dune'").await?;
        let album = albums.records[0].edit(&trx).await?;
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
async fn test_complex_subscription_conditions() {
    // Create a new node
    let node = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    // Set up a subscription to watch for records with name = 'Rex' OR (age > 2 and age < 5)
    let predicate =
        ankql::parser::parse_selection("name = 'Rex' OR (age > 2 and age < 5)").unwrap();

    // Track received changesets
    let received_changesets = Arc::new(Mutex::new(Vec::new()));
    let received_changesets_clone = received_changesets.clone();

    // Helper function to check received changesets
    let check_received = move || {
        let mut changesets = received_changesets.lock().unwrap();
        let result: Vec<RecordChangeKind> = (*changesets)
            .iter()
            .flat_map(|c: &ChangeSet| c.changes.iter().map(|change| change.kind.clone()))
            .collect();
        changesets.clear();
        result
    };

    // Subscribe to changes
    let _handle = node
        .subscribe("pets", &predicate, move |changeset| {
            let mut received = received_changesets_clone.lock().unwrap();
            received.push(changeset);
        })
        .await
        .unwrap();

    // Create some test records
    let trx = node.begin();
    let rex = trx
        .create(&Pet {
            name: "Rex".to_string(),
            age: "1".to_string(),
        })
        .await;
    let rex = rex.read();

    let snuffy = trx
        .create(&Pet {
            name: "Snuffy".to_string(),
            age: "2".to_string(),
        })
        .await;
    let snuffy = snuffy.read();

    let jasper = trx
        .create(&Pet {
            name: "Jasper".to_string(),
            age: "6".to_string(),
        })
        .await;
    let jasper = jasper.read();

    trx.commit().await.unwrap();

    // Verify initial state
    let received = check_received();
    assert_eq!(received.len(), 1); // Should have received one changeset for Rex (matches by name)
    assert_eq!(received[0], RecordChangeKind::Add); // Initial state should be an Add

    // Update Rex's age to 7
    let trx = node.begin();
    let mut rex_edit = rex.edit(&trx).await.unwrap();
    rex_edit.age().overwrite(0, 1, "7");
    trx.commit().await.unwrap();

    // Verify Rex's update was received
    let received = check_received();
    assert_eq!(received.len(), 1); // Should have received one changeset for Rex's age change
    assert_eq!(received[0], RecordChangeKind::Edit);

    // Update Snuffy's age to 3
    let trx = node.begin();
    let mut snuffy_edit = snuffy.edit(&trx).await.unwrap();
    snuffy_edit.age().overwrite(0, 1, "3");
    trx.commit().await.unwrap();

    // Verify Snuffy's update was received (now matches age > 2 and age < 5)
    let received = check_received();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0], RecordChangeKind::Add);

    // Update Jasper's age to 4
    let trx = node.begin();
    let mut jasper_edit = jasper.edit(&trx).await.unwrap();
    jasper_edit.age().overwrite(0, 1, "4");
    trx.commit().await.unwrap();

    // Verify Jasper's update was received (now matches age > 2 and age < 5)
    let received = check_received();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0], RecordChangeKind::Add);

    // Update Snuffy and Jasper to ages outside the range
    let trx = node.begin();
    let mut snuffy_edit = snuffy.edit(&trx).await.unwrap();
    snuffy_edit.age().overwrite(0, 1, "5");
    let mut jasper_edit = jasper.edit(&trx).await.unwrap();
    jasper_edit.age().overwrite(0, 1, "6");
    trx.commit().await.unwrap();

    // Verify both updates were received as removals
    let received = check_received();
    assert_eq!(received.len(), 2); // Should have received two changesets for removals
    assert_eq!(received[0], RecordChangeKind::Remove);
    assert_eq!(received[1], RecordChangeKind::Remove);

    // Update Rex to no longer match the query (instead of deleting)
    // This should still trigger a RecordChangeKind::Remove since it no longer matches
    let trx = node.begin();
    let mut rex_edit = rex.edit(&trx).await.unwrap();
    rex_edit.name().overwrite(0, 3, "NotRex");
    trx.commit().await.unwrap();

    // Verify Rex's "removal" was received
    let received = check_received();
    assert_eq!(received.len(), 1); // Should have received one changeset for Rex no longer matching
    assert_eq!(received[0], RecordChangeKind::Remove); // Should be a removal (from the result set)
}
