use ankurah_core::property::value::YrsString;
use ankurah_core::storage::SledStorageEngine;
use ankurah_core::{model::ScopedRecord, node::Node};
use ankurah_derive::Model;
use anyhow::Result;
use serde::{Deserialize, Serialize};

use std::sync::Arc;

#[derive(Model, Debug, Serialize, Deserialize)] // This line now uses the Model derive macro
pub struct Album {
    #[active_value(YrsString)]
    pub name: String,
    #[active_value(YrsString)]
    pub year: String,
}

#[test]
fn basic_where_clause() -> Result<()> {
    let client = Arc::new(Node::new(SledStorageEngine::new_test().unwrap()));

    let id = {
        let trx = client.begin();
        let id = trx
            .create(&Album {
                name: "Walking on a Dream".into(),
                year: "2008".into(),
            })
            .id();
        trx.create(&Album {
            name: "Ice on the Dune".into(),
            year: "2013".into(),
        });
        trx.create(&Album {
            name: "Two Vines".into(),
            year: "2016".into(),
        });
        trx.create(&Album {
            name: "Ask That God".into(),
            year: "2024".into(),
        });
        trx.commit()?;
        id
    };

    // This works:
    let album_record: AlbumRecord = client.get_record(id).unwrap();
    println!("album_record: {:?}", album_record);

    // but we don't want to get the record by id, we want to get the record by the where clause
    // this works
    let predicate = ankql::parser::parse_selection("name = 'Walking on a Dream'").unwrap();
    println!("{:?}", predicate);

    // The next step is to make this work:
    let albums: ankurah_core::resultset::ResultSet<AlbumRecord> =
        client.fetch("name = 'Walking on a Dream'")?;

    assert_eq!(
        albums
            .records
            .iter()
            .map(|active_record| active_record.name())
            .collect::<Vec<String>>(),
        vec!["Walking on a Dream".to_string()]
    );

    Ok(())
}
