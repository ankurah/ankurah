use std::sync::Arc;

use ankurah::{error::MutationError, policy::DEFAULT_CONTEXT, Context, Model, Node, PermissiveAgent};
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use serde::{Deserialize, Serialize};
use tracing_subscriber::layer::SubscriberExt;
use tracing_wasm::{ConsoleConfig, WASMLayerConfigBuilder};
use wasm_bindgen_test::*;

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

wasm_bindgen_test_configure!(run_in_browser);

pub fn names(albums: &[AlbumView]) -> Vec<String> { albums.iter().map(|a| a.name().unwrap()).collect() }

pub fn sort_names(albums: &[AlbumView]) -> Vec<String> {
    let mut names = names(albums);
    names.sort();
    names
}

pub fn years(albums: &[AlbumView]) -> Vec<String> { albums.iter().map(|a| a.year().unwrap()).collect() }

pub fn setup() -> () {
    console_error_panic_hook::set_once();

    std::panic::set_hook(Box::new(|info| {
        let msg = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &s[..],
                None => "Box<dyn Any>",
            },
        };
        let location = info.location().unwrap();
        web_sys::console::error_2(&format!("panic at {}:{}", location.file(), location.line()).into(), &msg.into());
    }));

    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::registry::Registry::default().with(tracing_wasm::WASMLayer::new(
            WASMLayerConfigBuilder::new()
                .set_report_logs_in_timings(true)
                .set_console_config(ConsoleConfig::ReportWithoutConsoleColor)
                .set_max_level(tracing::Level::INFO)
                .build(),
        )),
    );
}

pub async fn setup_context() -> Result<(Context, String), anyhow::Error> {
    setup();
    let db_name = format!("test_db_{}", ulid::Ulid::new());
    let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
    let node = Node::new_durable(Arc::new(storage_engine), PermissiveAgent::new());
    node.system.create().await?;
    Ok((node.context_async(DEFAULT_CONTEXT).await, db_name))
}

pub async fn create_albums(ctx: &Context, vec: Vec<(&'static str, &'static str)>) -> Result<(), MutationError> {
    let trx = ctx.begin();
    for (name, year) in vec {
        let album = Album { name: name.to_owned(), year: year.to_owned() };
        let _album = trx.create(&album).await?;
    }
    trx.commit().await?;
    Ok(())
}

pub async fn create_books(ctx: &Context, vec: Vec<(&'static str, &'static str)>) -> Result<(), MutationError> {
    let trx = ctx.begin();
    for (name, year) in vec {
        let book = Book { name: name.to_owned(), year: year.to_owned() };
        let _book = trx.create(&book).await?;
    }
    trx.commit().await?;
    Ok(())
}
