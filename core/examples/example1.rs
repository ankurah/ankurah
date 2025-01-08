use std::sync::Arc;

use ankurah_core::property::backend::{PropertyBackend, YrsBackend};
use ankurah_core::property::value::YrsString;
use ankurah_core::storage::SledStorageEngine;
use ankurah_core::{model::Mutable, node::Node};
use ankurah_derive::Model;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{info, Level};

#[derive(Model, Debug, Serialize, Deserialize)] // This line now uses the Model derive macro
pub struct Album {
    #[active_value(YrsString)]
    pub name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let client = Arc::new(Node::new(Arc::new(SledStorageEngine::new().unwrap())));

    {
        let trx = client.begin();
        let album = trx.create(&Album { name: "The Dark Sid of the Moon".into() }).await;

        info!("Album created: {:?}", album);

        let yrs = album.backends().get::<YrsBackend>().unwrap();
        yrs.properties();

        album.name().insert(12, "e");

        assert_eq!(album.name().value(), Some("The Dark Side of the Moon".to_string()));

        let album_id = album.id();

        trx.commit().await?;

        let trx = client.begin();
        let album = trx.edit::<Album>(album_id).await?;
        println!("{:?}", album.name().value());
        //let album = album.entity();
        trx.rollback();

        //album
    };

    // Both client and server signals should trigger
    std::thread::sleep(std::time::Duration::from_millis(500));
    Ok(())
}
