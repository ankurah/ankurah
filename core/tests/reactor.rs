mod common;
use ankurah_core::storage::SledStorageEngine;
use ankurah_core::{model::Record, node::Node};
use anyhow::Result;
use std::sync::{Arc, Mutex};

use common::{Album, AlbumRecord};

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
        album.year().set("2020".into());
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
