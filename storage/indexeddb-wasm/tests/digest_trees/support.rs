//! An IndexedDB database and its test's tabs, using core's digest-tree fixtures.

use std::sync::{Arc, Mutex};

use ankurah::core::sync::conformance::fixtures;
use ankurah_digest_tree::Ristretto255;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use futures::FutureExt;

/// The trees the stories keep: trees of items, summed in Ristretto255.
pub type Tree = fixtures::Tree<IndexedDBStorageEngine, Ristretto255>;

/// An IndexedDB database of the test's own, under a name no other test's
/// has, deleted as the test ends.
pub struct Database {
    name: String,

    /// Every tab opened on the database, whose connections close as the test
    /// ends, so that the database can be deleted.
    tabs: Mutex<Vec<Arc<IndexedDBStorageEngine>>>,
}

impl Database {
    pub fn new() -> Self { Self { name: format!("stores_{}", ulid::Ulid::new()), tabs: Mutex::default() } }

    /// An engine on the database, as another tab has one: a connection, and
    /// locks within the tab, of its own.
    pub async fn tab(&self) -> anyhow::Result<Arc<IndexedDBStorageEngine>> {
        let tab = Arc::new(IndexedDBStorageEngine::open(&self.name).await?);
        self.tabs.lock().unwrap().push(Arc::clone(&tab));

        Ok(tab)
    }
}

/// Every tab's connection closes, and the browser deletes the database once
/// they have.
impl Drop for Database {
    fn drop(&mut self) {
        for tab in self.tabs.get_mut().unwrap().drain(..) {
            tab.db.close().now_or_never().expect("no call on a tab under way as its test ends");
        }

        let factory = web_sys::window().and_then(|window| window.indexed_db().ok().flatten()).expect("the browser's IndexedDB");
        factory.delete_database(&self.name).expect("the test's database deleted");
    }
}
