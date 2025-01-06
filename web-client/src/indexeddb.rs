use ankql::selection::filter::evaluate_predicate;
use ankurah_core::error::RetrievalError;
use ankurah_core::model::RecordInner;
use ankurah_core::storage::{StorageBucket, StorageEngine};
use ankurah_proto as proto;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use js_sys::Function;
use send_wrapper::SendWrapper;
use std::any::Any;
use std::sync::Arc;
use tracing::info;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    Event, IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest, IdbVersionChangeEvent,
};

pub struct IndexedDBStorageEngine {
    // We need SendWrapper because despite the ability to declare an async trait as ?Send,
    // we can't actually define StorageEngine and StorageBucket as optionally Send or !Send.
    // This appears not to be an issue with the macro, but rather the inability to add supplemental bindings on Generic associated types?
    // A lot of time could be potentially burned on this, so we're just going to use SendWrapper for now.
    // See this thread for more information
    // https://users.rust-lang.org/t/send-not-send-variant-of-async-trait-object-without-duplication/115294
    db: SendWrapper<IdbDatabase>,
    _callbacks: SendWrapper<Vec<Box<dyn Any>>>,
}

#[derive(Debug)]
pub struct IndexedDBBucket {
    db: SendWrapper<IdbDatabase>,
    name: String,
}

impl IndexedDBStorageEngine {
    pub async fn open(name: &str) -> anyhow::Result<Self> {
        info!("Opening database: {}", name);
        // Validate database name
        if name.is_empty() {
            return Err(anyhow::anyhow!("Database name cannot be empty"));
        }

        let window = web_sys::window().ok_or_else(|| anyhow::anyhow!("No window found"))?;
        let idb: IdbFactory = window
            .indexed_db()
            .map_err(|e| anyhow::anyhow!("IndexedDB error: {:?}", e))?
            .ok_or_else(|| anyhow::anyhow!("IndexedDB not available"))?;

        let open_request: IdbOpenDbRequest = idb
            .open_with_u32(name, 1)
            .map_err(|e| anyhow::anyhow!("Failed to open DB: {:?}", e))?;

        let mut callbacks: Vec<Box<dyn Any>> = Vec::new();
        let promise = js_sys::Promise::new(&mut |resolve: Function, reject: Function| {
            let onupgradeneeded = Closure::wrap(Box::new(move |event: IdbVersionChangeEvent| {
                let target: IdbRequest = event.target().unwrap().unchecked_into();
                let db: SendWrapper<IdbDatabase> =
                    SendWrapper::new(target.result().unwrap().unchecked_into());

                // Create records store with index on collection
                let store = match db.create_object_store("records") {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::warn!("Error creating store (may already exist): {:?}", e);
                        return;
                    }
                };

                // Create index on collection field
                if let Err(e) = store.create_index_with_str("by_collection", "collection") {
                    tracing::error!("Failed to create collection index: {:?}", e);
                }
            }) as Box<dyn FnMut(_)>);

            let onsuccess = Closure::wrap(Box::new(move |event: Event| {
                let target: IdbRequest = event.target().unwrap().unchecked_into();
                let db: IdbDatabase = target.result().unwrap().unchecked_into();
                resolve.call1(&JsValue::NULL, &JsValue::from(db)).unwrap();
            }) as Box<dyn FnMut(_)>);

            let onerror = Closure::wrap(Box::new(move |event: Event| {
                let target: IdbRequest = event.target().unwrap().unchecked_into();
                let error = target.error().unwrap();
                reject.call1(&JsValue::NULL, &error.into()).unwrap();
            }) as Box<dyn FnMut(_)>);

            open_request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));
            open_request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
            open_request.set_onerror(Some(onerror.as_ref().unchecked_ref()));

            // Keep closures alive
            callbacks.push(Box::new(onupgradeneeded));
            callbacks.push(Box::new(onsuccess));
            callbacks.push(Box::new(onerror));
        });

        let db = SendWrapper::new(
            JsFuture::from(promise)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open database: {:?}", e))?
                .unchecked_into::<IdbDatabase>(),
        );

        Ok(Self {
            db,
            _callbacks: SendWrapper::new(callbacks),
        })
    }

    pub async fn cleanup(name: &str) -> anyhow::Result<()> {
        let window = web_sys::window().ok_or_else(|| anyhow::anyhow!("No window found"))?;
        let idb = window
            .indexed_db()
            .map_err(|e| anyhow::anyhow!("IndexedDB error: {:?}", e))?
            .ok_or_else(|| anyhow::anyhow!("IndexedDB not available"))?;

        let delete_request = idb
            .delete_database(name)
            .map_err(|e| anyhow::anyhow!("Failed to delete database: {:?}", e))?;

        let future =
            crate::cb_future::CBFuture::new(&delete_request, &["success", "blocked"], "error");

        future
            .await
            .map_err(|e| anyhow::anyhow!("Failed to delete database: {:?}", e))?;

        Ok(())
    }
}

#[async_trait]
impl StorageEngine for IndexedDBStorageEngine {
    async fn bucket(&self, name: &str) -> anyhow::Result<Arc<dyn StorageBucket>> {
        Ok(Arc::new(IndexedDBBucket {
            db: self.db.clone(),
            name: name.to_owned(),
        }))
    }

    async fn fetch_states(
        &self,
        bucket_name: String,
        predicate: &ankql::ast::Predicate,
    ) -> anyhow::Result<Vec<(proto::ID, proto::RecordState)>> {
        SendWrapper::new(async move {
            let transaction = self
                .db
                .transaction_with_str("records")
                .map_err(|_e| anyhow::anyhow!("Failed to create transaction"))?;

            let store = transaction
                .object_store("records")
                .map_err(|_e| anyhow::anyhow!("Failed to get object store"))?;

            let index = store
                .index("by_collection")
                .map_err(|_e| anyhow::anyhow!("Failed to get collection index"))?;

            let key_range = web_sys::IdbKeyRange::only(&(&bucket_name).into())
                .map_err(|_e| anyhow::anyhow!("Failed to create key range"))?;

            let request = index
                .open_cursor_with_range(&key_range)
                .map_err(|_e| anyhow::anyhow!("Failed to open cursor"))?;

            let mut records = Vec::new();
            let mut stream = crate::cb_stream::CBStream::new(&request, "success", "error");

            while let Some(result) = stream.next().await {
                let cursor_result = result.map_err(|e| anyhow::anyhow!("Cursor error: {}", e))?;

                // Check if we've reached the end
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }

                let cursor: web_sys::IdbCursorWithValue = cursor_result
                    .dyn_into()
                    .map_err(|_| anyhow::anyhow!("Failed to cast cursor"))?;

                let record = cursor
                    .value()
                    .map_err(|e| anyhow::anyhow!("Failed to get cursor value: {:?}", e))?;

                let id_str = js_sys::Reflect::get(&record, &"id".into())
                    .map_err(|_e| anyhow::anyhow!("Failed to get record id"))?;
                let id =
                    proto::ID::from_ulid(ulid::Ulid::from_string(&id_str.as_string().unwrap())?);

                let state_buffer = js_sys::Reflect::get(&record, &"state_buffer".into())
                    .map_err(|_e| anyhow::anyhow!("Failed to get state buffer"))?;
                let array: js_sys::Uint8Array = state_buffer
                    .dyn_into()
                    .map_err(|_e| anyhow::anyhow!("Failed to convert state buffer"))?;

                let mut buffer = vec![0; array.length() as usize];
                array.copy_to(&mut buffer);

                let state_buffers: std::collections::BTreeMap<String, Vec<u8>> =
                    bincode::deserialize(&buffer)?;
                let record_state = proto::RecordState { state_buffers };

                // Create record to evaluate predicate
                let record_inner = RecordInner::from_record_state(id, &bucket_name, &record_state)?;

                // Apply predicate filter
                if evaluate_predicate(&record_inner, predicate)? {
                    records.push((id, record_state));
                }

                cursor
                    .continue_()
                    .map_err(|_e| anyhow::anyhow!("Failed to advance cursor"))?;
            }

            Ok(records)
        })
        .await
    }
}

#[async_trait]
impl StorageBucket for IndexedDBBucket {
    async fn set_record(&self, id: proto::ID, state: &proto::RecordState) -> anyhow::Result<()> {
        SendWrapper::new(async move {
            // Serialize the state
            let state_buffer = bincode::serialize(&state.state_buffers)?;
            // Create transaction and get object store
            let transaction = self
                .db
                .transaction_with_str_and_mode("records", web_sys::IdbTransactionMode::Readwrite)
                .map_err(|_e| anyhow::anyhow!("Failed to create transaction"))?;

            let store = transaction
                .object_store("records")
                .map_err(|_e| anyhow::anyhow!("Failed to get object store"))?;

            // Create a JS object to store our data
            let record = js_sys::Object::new();
            js_sys::Reflect::set(&record, &"id".into(), &id.as_string().into())
                .map_err(|_e| anyhow::anyhow!("Failed to set id on record"))?;
            js_sys::Reflect::set(&record, &"collection".into(), &self.name.clone().into())
                .map_err(|_e| anyhow::anyhow!("Failed to set collection on record"))?;
            js_sys::Reflect::set(
                &record,
                &"state_buffer".into(),
                &js_sys::Uint8Array::from(&state_buffer[..]).into(),
            )
            .map_err(|_e| anyhow::anyhow!("Failed to set data on record"))?;

            // Put the record in the store
            let request = store
                .put_with_key(&record, &id.as_string().into())
                .map_err(|_e| anyhow::anyhow!("Failed to put record in store"))?;

            let request_fut = crate::cb_future::CBFuture::new(&request, "success", "error");
            request_fut
                .await
                .map_err(|_e| anyhow::anyhow!("Failed to put record in store"))?;

            let trx_fut = crate::cb_future::CBFuture::new(&transaction, "complete", "error");
            trx_fut
                .await
                .map_err(|_e| anyhow::anyhow!("Failed to complete transaction"))?;

            Ok(())
        })
        .await
    }

    async fn get_record(&self, id: proto::ID) -> Result<proto::RecordState, RetrievalError> {
        SendWrapper::new(async move {
            // Create transaction and get object store
            let transaction = self.db.transaction_with_str("records").map_err(|_e| {
                RetrievalError::StorageError(anyhow::anyhow!("Failed to create transaction").into())
            })?;

            let store = transaction.object_store("records").map_err(|_e| {
                RetrievalError::StorageError(anyhow::anyhow!("Failed to get object store").into())
            })?;

            // Get the record
            let request = store.get(&id.as_string().into()).map_err(|_e| {
                RetrievalError::StorageError(anyhow::anyhow!("Failed to get record 1").into())
            })?;

            crate::cb_future::CBFuture::new(&request, "success", "error")
                .await
                .map_err(|_e| {
                    RetrievalError::StorageError(anyhow::anyhow!("Failed to get record 2").into())
                })?;

            let result = request.result().unwrap();

            // Check if the record exists
            if result.is_undefined() || result.is_null() {
                return Err(RetrievalError::NotFound(id));
            }

            // Get the data from the JS object
            let record: web_sys::js_sys::Object = result.dyn_into().map_err(|_e| {
                RetrievalError::StorageError(anyhow::anyhow!("Failed to get record 3").into())
            })?;

            let data = js_sys::Reflect::get(&record, &"state_buffer".into()).map_err(|_e| {
                RetrievalError::StorageError(anyhow::anyhow!("Failed to get record 4").into())
            })?;

            let array: js_sys::Uint8Array = data.dyn_into().map_err(|_e| {
                RetrievalError::StorageError(anyhow::anyhow!("Failed to get record 5").into())
            })?;

            let mut buffer = vec![0; array.length() as usize];
            array.copy_to(&mut buffer);

            // Deserialize the state
            let state_buffers = bincode::deserialize(&buffer)?;

            Ok(proto::RecordState { state_buffers })
        })
        .await
    }
}

#[cfg(target_arch = "wasm32")]
#[cfg(test)]
mod tests {
    #![allow(unused)]

    use super::*;
    use ankurah_core::property::value::YrsString;
    use ankurah_core::{model::ScopedRecord, node::Node};
    use ankurah_derive::Model;
    use serde::{Deserialize, Serialize};
    use wasm_bindgen_test::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Album {
        #[active_value(YrsString)]
        name: String,
        #[active_value(YrsString)]
        year: String,
    }

    wasm_bindgen_test_configure!(run_in_browser);

    async fn setup() -> anyhow::Result<()> {
        console_error_panic_hook::set_once();
        let _ = tracing_wasm::try_set_as_global_default();
        tracing::debug!("Test setup complete");
        Ok(())
    }

    #[wasm_bindgen_test]
    async fn test_open_database() {
        setup().await.expect("Failed to setup test");

        let db_name = format!("test_db_{}", ulid::Ulid::new());
        tracing::info!("Starting test_open_database");
        let result = IndexedDBStorageEngine::open(&db_name).await;
        tracing::info!("Open result: {:?}", result.is_ok());

        assert!(
            result.is_ok(),
            "Failed to open database: {:?}",
            result.err()
        );

        let engine = result.unwrap();
        tracing::info!("Successfully opened database: {}", engine.db.name());
        assert_eq!(engine.db.name(), db_name, "Database name mismatch");

        // Test reopening existing database
        tracing::info!("Attempting to reopen database 'test_db'");
        let result2 = IndexedDBStorageEngine::open(&db_name).await;

        assert!(
            result2.is_ok(),
            "Failed to reopen database: {:?}",
            result2.err()
        );
        tracing::info!("Test completed successfully");

        // Drop both engine instances
        drop(engine);
        if let Ok(engine2) = result2 {
            drop(engine2);
        }

        // Cleanup
        IndexedDBStorageEngine::cleanup(&db_name)
            .await
            .expect("Failed to cleanup database");
    }

    #[wasm_bindgen_test]
    async fn test_set_and_get_record() {
        setup().await.expect("Failed to setup test");

        let db_name = format!("test_db_{}", ulid::Ulid::new());
        let engine = IndexedDBStorageEngine::open(&db_name)
            .await
            .expect("Failed to open database");

        let bucket = engine
            .bucket("albums")
            .await
            .expect("Failed to create bucket");

        // Create a test record
        let id = proto::ID::from_ulid(ulid::Ulid::new());
        let mut state_buffers = std::collections::BTreeMap::new();
        state_buffers.insert("propertybackend_yrs".to_string(), vec![1, 2, 3]);
        let state = proto::RecordState { state_buffers };

        // Set the record
        bucket
            .set_record(id.clone(), &state)
            .await
            .expect("Failed to set record");

        // Get the record back
        let retrieved_state = bucket.get_record(id).await.expect("Failed to get record 6");
        tracing::info!("Retrieved state: {:?}", retrieved_state);

        assert_eq!(state.state_buffers, retrieved_state.state_buffers);

        // Drop the bucket and engine to close connections
        drop(bucket);
        drop(engine);

        // Cleanup
        IndexedDBStorageEngine::cleanup(&db_name)
            .await
            .expect("Failed to cleanup database");
    }

    #[wasm_bindgen_test]
    async fn test_basic_workflow() -> Result<(), anyhow::Error> {
        setup().await.expect("Failed to setup test");

        let db_name = format!("test_db_{}", ulid::Ulid::new());
        tracing::info!("Starting test_basic_workflow");
        let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
        tracing::info!("Storage engine opened");
        let node = Arc::new(Node::new(Box::new(storage_engine)));

        let id;
        {
            tracing::info!("Creating transaction");
            let trx = node.begin();
            tracing::info!("Transaction created");
            let album = trx
                .create(&Album {
                    name: "The rest of the owl".to_owned(),
                    year: "2024".to_owned(),
                })
                .await;
            assert_eq!(
                album.name().value(),
                Some("The rest of the owl".to_string())
            );

            id = album.id();
            tracing::info!("Album created");

            trx.commit().await?;
            tracing::info!("Transaction committed");
        }

        // Retrieve the record
        let album_ro: AlbumRecord = node.get_record(id).await?;
        assert_eq!(album_ro.name(), "The rest of the owl");
        assert_eq!(album_ro.year(), "2024");

        // Drop the node to close the connection
        drop(node);

        // Cleanup
        IndexedDBStorageEngine::cleanup(&db_name).await?;

        Ok(())
    }

    #[wasm_bindgen_test]
    async fn test_basic_where_clause() -> Result<(), anyhow::Error> {
        setup().await.expect("Failed to setup test");

        let db_name = format!("test_db_{}", ulid::Ulid::new());
        let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
        let node = Arc::new(Node::new(Box::new(storage_engine)));

        {
            let trx = node.begin();

            trx.create(&Album {
                name: "Walking on a Dream".into(),
                year: "2008".into(),
            })
            .await;

            trx.create(&Album {
                name: "Ice on the Dune".into(),
                year: "2013".into(),
            })
            .await;

            trx.create(&Album {
                name: "Two Vines".into(),
                year: "2016".into(),
            })
            .await;

            trx.create(&Album {
                name: "Ask That God".into(),
                year: "2024".into(),
            })
            .await;

            trx.commit().await?;
        }

        let albums: ankurah_core::resultset::ResultSet<AlbumRecord> =
            node.fetch("name = 'Walking on a Dream'").await?;

        assert_eq!(
            albums
                .records
                .iter()
                .map(|active_record| active_record.name())
                .collect::<Vec<String>>(),
            vec!["Walking on a Dream".to_string()]
        );

        // Drop the node to close the connection
        drop(node);

        // Cleanup
        IndexedDBStorageEngine::cleanup(&db_name).await?;

        Ok(())
    }
}
