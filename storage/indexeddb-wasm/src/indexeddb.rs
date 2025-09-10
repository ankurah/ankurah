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

use crate::{collection::IndexedDBBucket, database::Database};

pub struct IndexedDBStorageEngine {
    // We need SendWrapper because despite the ability to declare an async trait as ?Send,
    // we can't actually define StorageEngine and StorageCollection as optionally Send or !Send.
    // This appears not to be an issue with the macro, but rather the inability to add supplemental bindings on Generic associated types?
    // A lot of time could be potentially burned on this, so we're just going to use SendWrapper for now.
    // See this thread for more information
    // https://users.rust-lang.org/t/send-not-send-variant-of-async-trait-object-without-duplication/115294
    pub db: Database,
}

impl IndexedDBStorageEngine {
    pub async fn open(name: &str) -> anyhow::Result<Self> {
        let db = Database::open(name).await?;
        Ok(Self { db })
    }

    pub async fn cleanup(name: &str) -> anyhow::Result<()> { Database::cleanup(name).await }

    /// Get the database name
    pub fn name(&self) -> &str { self.db.name() }
}

#[async_trait]
impl StorageEngine for IndexedDBStorageEngine {
    type Value = JsValue;
    async fn collection(&self, collection_id: &proto::CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        Ok(Arc::new(IndexedDBBucket::new(self.db.clone(), collection_id.clone())))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, MutationError> {
            res.map_err(|e| MutationError::FailedStep(msg, e.into().as_string().unwrap_or_default()))
        }

        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            // Clear entities store
            let entities_transaction = step(
                db_connection.transaction_with_str_and_mode("entities", web_sys::IdbTransactionMode::Readwrite),
                "create entities transaction",
            )?;
            let entities_store = step(entities_transaction.object_store("entities"), "get entities store")?;
            let entities_request = step(entities_store.clear(), "clear entities store")?;
            step(crate::cb_future::CBFuture::new(&entities_request, "success", "error").await, "await entities clear")?;
            step(crate::cb_future::CBFuture::new(&entities_transaction, "complete", "error").await, "complete entities transaction")?;

            // Clear events store
            let events_transaction = step(
                db_connection.transaction_with_str_and_mode("events", web_sys::IdbTransactionMode::Readwrite),
                "create events transaction",
            )?;
            let events_store = step(events_transaction.object_store("events"), "get events store")?;
            let events_request = step(events_store.clear(), "clear events store")?;
            step(crate::cb_future::CBFuture::new(&events_request, "success", "error").await, "await events clear")?;
            step(crate::cb_future::CBFuture::new(&events_transaction, "complete", "error").await, "complete events transaction")?;

            // Return true since we cleared everything
            Ok(true)
        })
        .await
    }
}
