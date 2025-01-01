#![cfg(feature = "postgres")]
mod common;
use anyhow::Result;
use std::sync::Arc;
#[cfg(feature = "postgres")]
mod pg_common;
use ankurah_core::node::Node;

#[tokio::test]
async fn test_postgres() -> Result<()> {
    use common::*;

    let (_container, storage_engine) = pg_common::create_postgres_container().await?;
    let node = Arc::new(Node::new(Arc::new(storage_engine)));

    let trx = node.begin();
    let _album = trx
        .create(&Album {
            name: "The rest of the owl".to_owned(),
            year: "2024".to_owned(),
        })
        .await;

    trx.commit().await?;

    Ok(())
}
