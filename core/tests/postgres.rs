use ankurah_core::property::value::YrsString;
use ankurah_core::storage::PostgresStorageEngine;
use ankurah_core::{model::ScopedRecord, node::Node};
use ankurah_derive::Model;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio_postgres::{Error, NoTls};
use tracing::{info, Level};

use std::sync::Arc;

#[derive(Model, Debug, Serialize, Deserialize)] // This line now uses the Model derive macro
pub struct Album {
    // DECISION: The model always contains projected types, which will initally be just the native types.
    // We will leave the door open to backend specific projected newtypes in the future, but we will not have the active value types in the model.
    // Implication: we will still need to have a native type to active type lookup in the Model macro for now.
    // We have the option of adding override attributes to switch backends in the future.
    // We will initially only use Model structs for initial construction of the record (or a property group thereof) but we may later consider
    // using them for per-property group retrieval binding, but preferably only via an immutable borrow.
    #[active_value(YrsString)]
    pub name: String,
}

#[tokio::test]
async fn postgres() -> Result<()> {
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=postgres password=postgres", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let storage_engine = PostgresStorageEngine::new(client)?;
    let node = Arc::new(Node::new(storage_engine));

    Ok(())
}
