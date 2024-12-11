use ankurah_core::property::value::YrsString;
use ankurah_core::storage::Postgres;
use ankurah_core::{model::ScopedRecord, node::Node};
use ankurah_derive::Model;
use anyhow::Result;
use postgres::{Error, NoTls};
use r2d2_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};
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

#[test]
fn postgres() -> Result<()> {
    let manager = PostgresConnectionManager::new(
        "host=localhost user=postgres password=postgres dbname=ankurah"
            .parse()
            .unwrap(),
        NoTls,
    );
    let pool = r2d2::Pool::new(manager).unwrap();

    let storage_engine = Postgres::new(pool)?;
    let node = Arc::new(Node::new(storage_engine));

    println!("mark 1");
    let trx = node.begin();
    println!("mark 2");
    let album = trx.create(&Album {
        name: "The rest of the owl".to_owned(),
    });
    println!("mark 3");

    trx.commit().unwrap();
    println!("mark 4");

    Ok(())
}
