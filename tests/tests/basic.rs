#![cfg(feature = "postgres")]
mod common;
use anyhow::Result;
use std::sync::Arc;
#[cfg(feature = "postgres")]
mod pg_common;
use ankurah::{Node, PermissiveAgent};

#[tokio::test]
async fn test_postgres() -> Result<()> {
    use common::*;

    let (_container, storage_engine) = pg_common::create_postgres_container().await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());

    let trx = node.begin();
    let _album = trx.create(&Album { name: "The rest of the owl".to_owned(), year: "2024".to_owned() }).await;

    trx.commit().await?;

    Ok(())
}
