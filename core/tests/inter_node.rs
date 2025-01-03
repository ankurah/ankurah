mod common;

use ankurah_core::connector::local_process::LocalProcessConnection;
use ankurah_core::node::Node;
use ankurah_core::storage::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;

use ankurah_core::changes::{RecordChange, RecordChangeKind};
use ankurah_core::model::ScopedRecord;
use common::{Album, AlbumRecord, Pet};

#[tokio::test]
async fn inter_node_fetch() -> Result<()> {
    let remote_node = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));
    let local_node = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    let _local_connector = LocalProcessConnection::new(&local_node, &remote_node).await?;

    {
        let trx = local_node.begin();
        let _album = trx
            .create(&Album {
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
    };

    // Mock client - This works:
    {
        let albums: ankurah_core::resultset::ResultSet<AlbumRecord> =
            local_node.fetch("name = 'Walking on a Dream'").await?;

        assert_eq!(
            albums
                .records
                .iter()
                .map(|active_record| active_record.name())
                .collect::<Vec<String>>(),
            vec!["Walking on a Dream".to_string()]
        );
    }

    // mock server - The next step is to make this work:
    {
        let albums: ankurah_core::resultset::ResultSet<AlbumRecord> =
            remote_node.fetch("name = 'Walking on a Dream'").await?;

        assert_eq!(
            albums
                .records
                .iter()
                .map(|active_record| active_record.name())
                .collect::<Vec<String>>(),
            vec!["Walking on a Dream".to_string()]
        );
    }

    Ok(())
}

#[tokio::test]
async fn inter_node_subscription() -> Result<()> {
    // Create two nodes
    let node1 = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));
    let node2 = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    // Connect the nodes
    let _connector = LocalProcessConnection::new(&node1, &node2).await?;

    // Create initial records on node1
    let (rex, snuffy, jasper);
    {
        let trx = node1.begin();
        rex = trx
            .create(&Pet {
                name: "Rex".to_string(),
                age: "1".to_string(),
            })
            .await
            .read();

        snuffy = trx
            .create(&Pet {
                name: "Snuffy".to_string(),
                age: "2".to_string(),
            })
            .await
            .read();

        jasper = trx
            .create(&Pet {
                name: "Jasper".to_string(),
                age: "6".to_string(),
            })
            .await
            .read();

        trx.commit().await?;
    }

    // Set up subscription on node2
    let (watcher, check_node2) = common::changeset_watcher();
    let _handle = node2
        .subscribe("pets", "name = 'Rex' OR (age > 2 and age < 5)", watcher)
        .await?;

    // Initial state should include Rex (name = 'Rex')
    let changes = check_node2();
    assert_eq!(changes, vec![RecordChangeKind::Initial]);

    // Update Rex's age to 7 on node1
    {
        let trx = node1.begin();
        rex.edit(&trx).await?.age().overwrite(0, 1, "7");
        trx.commit().await?;
    }

    // Should receive notification about Rex being removed (no longer matches either condition)
    let changes = check_node2();
    assert_eq!(changes, vec![RecordChangeKind::Remove]);

    // Update Snuffy's age to 3 on node1
    {
        let trx = node1.begin();
        snuffy.edit(&trx).await?.age().overwrite(0, 1, "3");
        trx.commit().await?;
    }

    // Should receive notification about Snuffy being added (now matches age > 2 and age < 5)
    let changes = check_node2();
    assert_eq!(changes, vec![RecordChangeKind::Add]);

    Ok(())
}
