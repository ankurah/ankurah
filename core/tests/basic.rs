#![cfg(feature = "postgres")]
mod common;
use anyhow::Result;
use std::sync::Arc;
#[cfg(feature = "postgres")]
mod pg_common;

#[test]
fn test_postgres() -> Result<()> {
    use common::*;
    let (_container, storage_engine) = pg_common::create_postgres_container()?;
    let node = Arc::new(Node::new(Box::new(storage_engine)));

    let trx = node.begin();
    let _album = trx.create(&Album {
        name: "The rest of the owl".to_owned(),
    });

    trx.commit()?;

    Ok(())
}
