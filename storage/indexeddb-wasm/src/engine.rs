use ankurah_core::{
    error::{MutationError, RetrievalError},
    property::PropertyResolver,
    storage::{StorageCollection, StorageEngine},
};
use ankurah_proto::{self as proto};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use send_wrapper::SendWrapper;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use wasm_bindgen::prelude::*;

use crate::{
    collection::IndexedDBBucket,
    database::Database,
    util::{cb_future::cb_future, require::WBGRequire},
};
#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicBool, Ordering};

pub struct IndexedDBStorageEngine {
    // We need SendWrapper because despite the ability to declare an async trait as ?Send,
    // we can't actually define StorageEngine and StorageCollection as optionally Send or !Send.
    // This appears not to be an issue with the macro, but rather the inability to add supplemental bindings on Generic associated types?
    // A lot of time could be potentially burned on this, so we're just going to use SendWrapper for now.
    // See this thread for more information
    // https://users.rust-lang.org/t/send-not-send-variant-of-async-trait-object-without-duplication/115294
    pub db: Database,
    /// The catalog resolver, injected post-construction by `Node` (see
    /// `StorageEngine::set_property_resolver`). Shared with every bucket: the
    /// name SOURCE for the engine-owned durable id-to-field map (the
    /// `property_columns` object store). Weak so storage never keeps the node
    /// alive. (Wasm is single-threaded, but the trait signature requires these
    /// `Sync` types; `std::sync` works fine here.)
    resolver: Arc<RwLock<Option<std::sync::Weak<dyn PropertyResolver>>>>,
    #[cfg(debug_assertions)]
    pub prefix_guard_disabled: std::sync::Arc<AtomicBool>,
}

impl IndexedDBStorageEngine {
    pub async fn open(name: &str) -> anyhow::Result<Self> {
        let db = Database::open(name).await?;
        Ok(Self {
            db,
            resolver: Arc::new(RwLock::new(None)),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: std::sync::Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn cleanup(name: &str) -> anyhow::Result<()> { Database::cleanup(name).await }

    /// Get the database name
    pub fn name(&self) -> &str { self.db.name() }

    /// For wasm tests: enable/disable prefix guard at runtime
    #[cfg(debug_assertions)]
    pub fn set_prefix_guard_disabled(&self, disabled: bool) { self.prefix_guard_disabled.store(disabled, Ordering::Relaxed); }
}

#[async_trait]
impl StorageEngine for IndexedDBStorageEngine {
    type Value = JsValue;
    async fn collection(&self, collection_id: &proto::CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        Ok(Arc::new(IndexedDBBucket {
            db: self.db.clone(),
            collection_id: collection_id.clone(),
            mutex: tokio::sync::Mutex::new(()),
            invocation_count: std::sync::atomic::AtomicUsize::new(0),
            resolver: self.resolver.clone(),
            property_columns: Arc::new(RwLock::new(BTreeMap::new())),
            property_columns_loaded: std::sync::atomic::AtomicBool::new(false),
            #[cfg(debug_assertions)]
            prefix_guard_disabled: self.prefix_guard_disabled.clone(),
        }))
    }

    fn set_property_resolver(&self, resolver: std::sync::Weak<dyn PropertyResolver>) { *self.resolver.write().unwrap() = Some(resolver); }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            // Clear entities store
            let entities_transaction = db_connection
                .transaction_with_str_and_mode("entities", web_sys::IdbTransactionMode::Readwrite)
                .require("create entities transaction")?;
            let entities_store = entities_transaction.object_store("entities").require("get entities store")?;
            let entities_request = entities_store.clear().require("clear entities store")?;
            cb_future(&entities_request, "success", "error").await.require("await entities clear")?;
            cb_future(&entities_transaction, "complete", "error").await.require("complete entities transaction")?;

            // Clear events store
            let events_transaction = db_connection
                .transaction_with_str_and_mode("events", web_sys::IdbTransactionMode::Readwrite)
                .require("create events transaction")?;
            let events_store = events_transaction.object_store("events").require("get events store")?;
            let events_request = events_store.clear().require("clear events store")?;
            cb_future(&events_request, "success", "error").await.require("await events clear")?;
            cb_future(&events_transaction, "complete", "error").await.require("complete events transaction")?;

            // Clear property_columns store (the engine-owned durable id-to-field
            // map). Wiping every collection must wipe its id-to-field
            // assignments too, or a re-created collection would find stale rows.
            let columns_transaction = db_connection
                .transaction_with_str_and_mode("property_columns", web_sys::IdbTransactionMode::Readwrite)
                .require("create property_columns transaction")?;
            let columns_store = columns_transaction.object_store("property_columns").require("get property_columns store")?;
            let columns_request = columns_store.clear().require("clear property_columns store")?;
            cb_future(&columns_request, "success", "error").await.require("await property_columns clear")?;
            cb_future(&columns_transaction, "complete", "error").await.require("complete property_columns transaction")?;

            // Return true since we cleared everything
            Ok(true)
        })
        .await
    }

    /// Non-creating collection discovery. The trait default returns nothing,
    /// which would make a durable node warm an empty catalog on restart.
    /// Unlike sled/SQL, IndexedDB keeps every collection's entities
    /// in one shared `entities` store addressed by the compound
    /// `(__collection, id)` index, so the collections are the distinct
    /// `__collection` values. Reads the entities via the shared cursor scanner
    /// and dedupes their `__collection`; creates nothing. (A distinct-key cursor
    /// walk would avoid reading full records -- a follow-up optimization.)
    async fn list_collections(&self) -> Result<Vec<proto::CollectionId>, RetrievalError> {
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("entities").require("create entities transaction")?;
            let store = transaction.object_store("entities").require("get entities store")?;
            let index = store.index("__collection__id").require("get collection index")?;
            let scanner = crate::scanner::IdbIndexScanner::new(index, None, web_sys::IdbCursorDirection::Next, 0, Vec::new());
            let mut stream = std::pin::pin!(scanner.scan());
            let mut seen = std::collections::HashSet::new();
            let mut collections = Vec::new();
            while let Some(result) = stream.next().await {
                let entity_obj = result?;
                if let Some(name) = js_sys::Reflect::get(&entity_obj, &JsValue::from_str("__collection")).ok().and_then(|v| v.as_string()) {
                    if seen.insert(name.clone()) {
                        collections.push(proto::CollectionId::from(name));
                    }
                }
            }
            Ok(collections)
        })
        .await
    }
}
