mod common;

use ankurah_core::connector::local_process::LocalProcessConnection;
use ankurah_core::node::Node;
use ankurah_core::storage::SledStorageEngine;
use anyhow::Result;

use common::{Album, AlbumRecord};
use std::sync::Arc;

#[tokio::test]
async fn basic_inter_node() -> Result<()> {
    let remote_node = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));
    let local_node = Arc::new(Node::new(Arc::new(SledStorageEngine::new_test().unwrap())));

    let _local_connector = LocalProcessConnection::new(&local_node, &remote_node).await?;

    {
        let trx = local_node.begin();
        let album = trx
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
