mod common;

use ankurah_core::connector::local_process::LocalProcessConnection;
use ankurah_core::node::{FetchArgs, Node};
use ankurah_core::storage::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

use ankurah_core::changes::ItemChange;
use ankurah_core::model::ScopedRecord;
use ankurah_core::resultset::ResultSet;
use common::{Album, AlbumRecord, Pet};

use common::ChangeKind;

pub fn names(resultset: ResultSet<AlbumRecord>) -> Vec<String> {
    resultset
        .records
        .iter()
        .map(|r| r.name())
        .collect::<Vec<String>>()
}

#[tokio::test]
async fn inter_node_fetch() -> Result<()> {
    let node1 = Arc::new(Node::new_durable(Arc::new(
        SledStorageEngine::new_test().unwrap(),
    )));
    let node2 = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    {
        let trx = node1.begin();
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
    };

    let p = "name = 'Walking on a Dream'";
    // Should already be on node1
    assert_eq!(names(node1.fetch(p).await?), ["Walking on a Dream"]);

    // But node2 because they arent connected
    assert_eq!(
        names(
            node2
                .fetch(FetchArgs {
                    predicate: p.try_into()?,
                    cached: true
                })
                .await?
        ),
        [] as [&str; 0]
    );

    // Connect the nodes
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;

    // Now node2 should now successfully fetch the record
    assert_eq!(names(node2.fetch(p).await?), ["Walking on a Dream"]);

    Ok(())
}

#[tokio::test]
async fn inter_node_subscription() -> Result<()> {
    // Create two nodes
    let node1 = Arc::new(Node::new_durable(Arc::new(
        SledStorageEngine::new_test().unwrap(),
    )));
    let node2 = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    // Connect the nodes
    let _conn = LocalProcessConnection::new(&node1, &node2).await?;

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

    info!(
        "rex: {:?}, snuffy: {:?}, jasper: {:?}",
        rex.id(),
        snuffy.id(),
        jasper.id()
    );

    // Set up subscription on node2
    let (watcher, check_node2) = common::changeset_watcher();
    let _handle = node2
        .subscribe("pet", "name = 'Rex' OR (age > 2 and age < 5)", watcher)
        .await?;

    // Initial state should include Rex
    assert_eq!(check_node2(), vec![(rex.id(), ChangeKind::Initial)]);

    // Update Rex's age to 7 on node1
    {
        let trx = node1.begin();
        rex.edit(&trx).await?.age().overwrite(0, 1, "7");
        trx.commit().await?;
    }

    assert_eq!(check_node2(), vec![(rex.id(), ChangeKind::Update)]); // Rex still matches the predicate, but the age has changed

    // Update Snuffy's age to 3 on node1
    {
        let trx = node1.begin();
        snuffy.edit(&trx).await?.age().overwrite(0, 1, "3");
        trx.commit().await?;
    }

    // Sleep for a bit to ensure the change is propagated
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

    // Should receive notification about Snuffy being added (now matches age > 2 and age < 5)
    let changes = check_node2();
    assert_eq!(changes, vec![(snuffy.id(), ChangeKind::Add)]);

    Ok(())
}
