use std::sync::Arc;

use ankurah::Model;
use ankurah::{error::MutationError, policy::DEFAULT_CONTEXT, Context, Node, PermissiveAgent};

use ankurah_storage_sled::SledStorageEngine;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::Level;

// Initialize tracing for tests
#[ctor::ctor]
fn init_tracing() {
    // if LOG_LEVEL env var is set, use it
    if let Ok(level) = std::env::var("LOG_LEVEL") {
        tracing_subscriber::fmt().with_max_level(Level::from_str(&level).unwrap()).with_test_writer().init();
    } else {
        tracing_subscriber::fmt().with_max_level(Level::INFO).with_test_writer().init();
    }
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    #[active_type(LWW)]
    pub name: String,
    pub year: String,
}

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Book {
    #[active_type(LWW)]
    pub name: String,
    pub year: String,
}

pub fn names(albums: &[AlbumView]) -> Vec<String> { albums.iter().map(|a| a.name().unwrap()).collect() }

#[allow(unused)]
pub fn sort_names(albums: &[AlbumView]) -> Vec<String> {
    let mut names = names(albums);
    names.sort();
    names
}

#[allow(unused)]
pub fn years(albums: &[AlbumView]) -> Vec<String> { albums.iter().map(|a| a.year().unwrap()).collect() }

#[allow(unused)]
pub async fn setup_context() -> Result<Context, anyhow::Error> {
    let storage_engine = SledStorageEngine::new_test()?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    Ok(node.context_async(DEFAULT_CONTEXT).await)
}

#[allow(unused)]
pub async fn create_albums(ctx: &Context, vec: Vec<(&'static str, &'static str)>) -> Result<Vec<AlbumView>, MutationError> {
    use ankurah::Mutable;
    let trx = ctx.begin();
    let mut albums = Vec::new();
    for (name, year) in vec {
        let album = Album { name: name.to_owned(), year: year.to_owned() };
        let album = trx.create(&album).await?.read();
        albums.push(album);
    }
    trx.commit().await?;
    Ok(albums)
}

#[allow(unused)]
pub async fn create_books(ctx: &Context, vec: Vec<(&'static str, &'static str)>) -> Result<(), MutationError> {
    let trx = ctx.begin();
    for (name, year) in vec {
        let book = Book { name: name.to_owned(), year: year.to_owned() };
        let _book = trx.create(&book).await?;
    }
    trx.commit().await?;
    Ok(())
}

// Convenience fetch API to mirror wasm tests style
#[allow(dead_code)]
pub async fn fetch(ctx: &Context, q: &str) -> Result<Vec<AlbumView>, anyhow::Error> { ctx.fetch::<AlbumView>(q).await.map_err(Into::into) }
