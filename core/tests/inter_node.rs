mod common;

use ankurah_core::storage::SledStorageEngine;
use ankurah_core::{model::ScopedRecord, node::Node};
use anyhow::Result;

use common::{Album, AlbumRecord};
use std::sync::Arc;

#[tokio::test]
async fn basic_inter_node() -> Result<()> {
    let server = Arc::new(Node::new(Box::new(SledStorageEngine::new_test().unwrap())));
    let client = Arc::new(Node::new(Box::new(SledStorageEngine::new_test().unwrap())));

    // this doesn't work yet
    client.local_connect(&server);

    let id = {
        let trx = client.begin();
        let album = trx
            .create(&Album {
                name: "Walking on a Dream".into(),
                year: "2008".into(),
            })
            .await;
        let id = album.id();

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
        id
    };

    // Mock client - This works:
    {
        let albums: ankurah_core::resultset::ResultSet<AlbumRecord> =
            client.fetch("name = 'Walking on a Dream'").await?;

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
            server.fetch("name = 'Walking on a Dream'").await?;

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
