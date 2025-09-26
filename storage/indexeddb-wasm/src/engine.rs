use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::{StorageCollection, StorageEngine},
};
use ankurah_proto::{self as proto};
use anyhow::Result;
use async_trait::async_trait;
use send_wrapper::SendWrapper;
use std::sync::Arc;
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
    #[cfg(debug_assertions)]
    pub prefix_guard_disabled: std::sync::Arc<AtomicBool>,
}

impl IndexedDBStorageEngine {
    pub async fn open(name: &str) -> anyhow::Result<Self> {
        let db = Database::open(name).await?;
        Ok(Self {
            db,
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
            #[cfg(debug_assertions)]
            prefix_guard_disabled: self.prefix_guard_disabled.clone(),
        }))
    }

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

            // Return true since we cleared everything
            Ok(true)
        })
        .await
    }
}
