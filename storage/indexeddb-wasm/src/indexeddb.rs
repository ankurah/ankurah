use ankql::selection::filter::evaluate_predicate;
use ankurah_core::error::RetrievalError;
use ankurah_core::model::Entity;
use ankurah_core::storage::{StorageCollection, StorageEngine};
use ankurah_proto as proto;
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use js_sys::Function;
use send_wrapper::SendWrapper;
use std::any::Any;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tracing::info;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Event, IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest, IdbVersionChangeEvent};

pub struct IndexedDBStorageEngine {
    // We need SendWrapper because despite the ability to declare an async trait as ?Send,
    // we can't actually define StorageEngine and StorageCollection as optionally Send or !Send.
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
    collection_id: proto::CollectionId,
    mutex: tokio::sync::Mutex<()>,
    invocation_count: AtomicUsize,
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

        let open_request: IdbOpenDbRequest = idb.open_with_u32(name, 1).map_err(|e| anyhow::anyhow!("Failed to open DB: {:?}", e))?;

        let mut callbacks: Vec<Box<dyn Any>> = Vec::new();
        let promise = js_sys::Promise::new(&mut |resolve: Function, reject: Function| {
            let onupgradeneeded = Closure::wrap(Box::new(move |event: IdbVersionChangeEvent| {
                let target: IdbRequest = event.target().unwrap().unchecked_into();
                let db: SendWrapper<IdbDatabase> = SendWrapper::new(target.result().unwrap().unchecked_into());

                // Create entities store with index on collection
                let store = match db.create_object_store("entities") {
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
            JsFuture::from(promise).await.map_err(|e| anyhow::anyhow!("Failed to open database: {:?}", e))?.unchecked_into::<IdbDatabase>(),
        );

        Ok(Self { db, _callbacks: SendWrapper::new(callbacks) })
    }

    pub async fn cleanup(name: &str) -> anyhow::Result<()> {
        let window = web_sys::window().ok_or_else(|| anyhow::anyhow!("No window found"))?;
        let idb = window
            .indexed_db()
            .map_err(|e| anyhow::anyhow!("IndexedDB error: {:?}", e))?
            .ok_or_else(|| anyhow::anyhow!("IndexedDB not available"))?;

        let delete_request = idb.delete_database(name).map_err(|e| anyhow::anyhow!("Failed to delete database: {:?}", e))?;

        let future = crate::cb_future::CBFuture::new(&delete_request, &["success", "blocked"], "error");

        future.await.map_err(|e| anyhow::anyhow!("Failed to delete database: {:?}", e))?;

        Ok(())
    }
}

#[async_trait]
impl StorageEngine for IndexedDBStorageEngine {
    async fn collection(&self, collection_id: &proto::CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        Ok(Arc::new(IndexedDBBucket {
            collection_id: collection_id,
            db: self.db.clone(),
            collection_id: collection_id.clone(),
            mutex: tokio::sync::Mutex::new(()),
            invocation_count: AtomicUsize::new(0),
        }))
    }

    async fn fetch_states(
        &self,
        collection_id: proto::CollectionId,
        predicate: &ankql::ast::Predicate,
    ) -> Result<Vec<(proto::ID, proto::State)>, RetrievalError> {
        SendWrapper::new(async move {
            let transaction = self.db.transaction_with_str("entities").map_err(|_e| anyhow::anyhow!("Failed to create transaction"))?;

            let store = transaction.object_store("entities").map_err(|_e| anyhow::anyhow!("Failed to get object store"))?;

            let index = store.index("by_collection").map_err(|_e| anyhow::anyhow!("Failed to get collection index"))?;

            let key_range = web_sys::IdbKeyRange::only(&(&collection_id).as_str().into())
                .map_err(|_e| anyhow::anyhow!("Failed to create key range"))?;

            let request = index.open_cursor_with_range(&key_range).map_err(|_e| anyhow::anyhow!("Failed to open cursor"))?;

            let mut tuples = Vec::new();
            let mut stream = crate::cb_stream::CBStream::new(&request, "success", "error");

            while let Some(result) = stream.next().await {
                let cursor_result = result.map_err(|e| anyhow::anyhow!("Cursor error: {}", e))?;

                // Check if we've reached the end
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }

                let cursor: web_sys::IdbCursorWithValue = cursor_result.dyn_into().map_err(|_| anyhow::anyhow!("Failed to cast cursor"))?;

                let entity = cursor.value().map_err(|e| anyhow::anyhow!("Failed to get cursor value: {:?}", e))?;

                let id_str = js_sys::Reflect::get(&entity, &"id".into()).map_err(|_e| anyhow::anyhow!("Failed to get entity id"))?;
                let id: proto::ID = id_str.try_into().map_err(|_e| anyhow::anyhow!("Failed to convert id to proto::ID"))?;

                let state_buffer =
                    js_sys::Reflect::get(&entity, &"state_buffer".into()).map_err(|_e| anyhow::anyhow!("Failed to get state buffer"))?;
                let array: js_sys::Uint8Array = state_buffer.dyn_into().map_err(|_e| anyhow::anyhow!("Failed to convert state buffer"))?;

                let mut buffer = vec![0; array.length() as usize];
                array.copy_to(&mut buffer);

                let state_buffers: std::collections::BTreeMap<String, Vec<u8>> = bincode::deserialize(&buffer)?;

                // Get the head array
                let head_data = js_sys::Reflect::get(&entity, &"head".into()).map_err(|_e| anyhow::anyhow!("Failed to get head"))?;
                let head: proto::Clock = head_data.try_into().map_err(|e| anyhow::anyhow!("Failed to deserialize head: {}", e))?;

                let entity_state = proto::State { state_buffers, head };

                // Create entity to evaluate predicate
                let entity = Entity::from_state(id, collection_id.clone(), &entity_state)?;

                // Apply predicate filter
                if evaluate_predicate(&entity, predicate)? {
                    tuples.push((id, entity_state));
                }

                cursor.continue_().map_err(|_e| anyhow::anyhow!("Failed to advance cursor"))?;
            }

            Ok(tuples)
        })
        .await
    }
}

#[async_trait]
impl StorageCollection for IndexedDBBucket {
    async fn set_state(&self, id: proto::ID, state: &proto::State) -> anyhow::Result<bool> {
        let invocation = self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        info!("IndexedDBBucket({}) set_state({invocation}) {id} {state}", self.collection_id);
        // Lock the mutex to prevent concurrent updates
        let _lock = self.mutex.lock().await;
        info!("IndexedDBBucket({}) set_state({invocation}) LOCKED", self.collection_id);

        SendWrapper::new(async move {
            // Get the old entity if it exists to check for changes
            let transaction = self
                .db
                .transaction_with_str_and_mode("entities", web_sys::IdbTransactionMode::Readwrite)
                .map_err(|_e| anyhow::anyhow!("Failed to create transaction"))?;

            let store = transaction.object_store("entities").map_err(|_e| anyhow::anyhow!("Failed to get object store"))?;

            let old_request = store.get(&id.as_string().into()).map_err(|_e| anyhow::anyhow!("Failed to get old entity"))?;

            crate::cb_future::CBFuture::new(&old_request, "success", "error")
                .await
                .map_err(|_e| anyhow::anyhow!("Failed to get old entity"))?;

            let old_entity: JsValue = old_request.result().unwrap();

            // Check if the entity changed
            if !old_entity.is_undefined() && !old_entity.is_null() {
                let old_head = js_sys::Reflect::get(&old_entity, &"head".into()).map_err(|_e| anyhow::anyhow!("Failed to get old head"))?;
                if !old_head.is_undefined() && !old_head.is_null() {
                    let old_clock: proto::Clock = old_head.try_into().map_err(|e| anyhow::anyhow!("Failed to parse old head: {}", e))?;
                    info!(
                        "IndexedDBBucket({}) set_state({invocation}) MARK 1 {} {} old_clock {}",
                        self.collection_id, id, state, old_clock
                    );
                    if old_clock == state.head {
                        info!(
                            "IndexedDBBucket({}) set_state({invocation}) MARK 2 {} {} old_clock {}",
                            self.collection_id, id, state, old_clock
                        );
                        // No change in head, skip update
                        // HACK - we still need to lie and return true because there are good odds the other browser is yours
                        // and has already updated the entity 🤦
                        // ...andd this breaks subscription notification
                        // Ideally we'd use the node to check for changes, but we can't assume that the subscriber is keeping the entities resident
                        // and the node is using weak references so they might be freed
                        return Ok(true);
                    }
                }
            }
            info!("IndexedDBBucket({}) set_state({invocation}) MARK 3 ", self.collection_id);

            // Create a JS object to store our data
            let entity = js_sys::Object::new();
            js_sys::Reflect::set(&entity, &"id".into(), &id.as_string().into())
                .map_err(|_e| anyhow::anyhow!("Failed to set id on entity"))?;
            js_sys::Reflect::set(&entity, &"collection".into(), &self.collection_id.as_str().into())
                .map_err(|_e| anyhow::anyhow!("Failed to set collection on entity"))?;

            // Store state_buffers
            let state_buffer = bincode::serialize(&state.state_buffers)?;
            js_sys::Reflect::set(&entity, &"state_buffer".into(), &js_sys::Uint8Array::from(&state_buffer[..]).into())
                .map_err(|_e| anyhow::anyhow!("Failed to set data on entity"))?;

            js_sys::Reflect::set(&entity, &"head".into(), &(&(state.head)).into())
                .map_err(|_e| anyhow::anyhow!("Failed to set head on entity"))?;

            // Put the entity in the store
            let request =
                store.put_with_key(&entity, &id.as_string().into()).map_err(|_e| anyhow::anyhow!("Failed to put entity in store"))?;

            let request_fut = crate::cb_future::CBFuture::new(&request, "success", "error");
            request_fut.await.map_err(|_e| anyhow::anyhow!("Failed to put entity in store"))?;

            let trx_fut = crate::cb_future::CBFuture::new(&transaction, "complete", "error");
            trx_fut.await.map_err(|_e| anyhow::anyhow!("Failed to complete transaction"))?;

            Ok(true) // It was updated
        })
        .await
    }

    async fn get_state(&self, id: proto::ID) -> Result<proto::State, RetrievalError> {
        SendWrapper::new(async move {
            // Create transaction and get object store
            let transaction = self
                .db
                .transaction_with_str("entities")
                .map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to create transaction").into()))?;

            let store = transaction
                .object_store("entities")
                .map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to get object store").into()))?;

            // Get the entity
            let request = store
                .get(&id.as_string().into())
                .map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to get entity 1").into()))?;

            crate::cb_future::CBFuture::new(&request, "success", "error")
                .await
                .map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to get entity 2").into()))?;

            let result = request.result().unwrap();

            // Check if the entity exists
            if result.is_undefined() || result.is_null() {
                return Err(RetrievalError::NotFound(id));
            }

            // Get the data from the JS object
            let entity: web_sys::js_sys::Object =
                result.dyn_into().map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to get entity 3").into()))?;

            let data = js_sys::Reflect::get(&entity, &"state_buffer".into())
                .map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to get entity 4").into()))?;

            let array: js_sys::Uint8Array =
                data.dyn_into().map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to get entity 5").into()))?;

            let mut buffer = vec![0; array.length() as usize];
            array.copy_to(&mut buffer);

            // Deserialize the state
            let state_buffers = bincode::deserialize(&buffer)?;

            // Get the head array
            let head_data = js_sys::Reflect::get(&entity, &"head".into())
                .map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to get head").into()))?;
            let head: proto::Clock = head_data
                .try_into()
                .map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("Failed to deserialize head: {}", e).into()))?;

            Ok(proto::State { state_buffers, head })
        })
        .await
    }

    async fn fetch_states(&self, predicate: &ankql::ast::Predicate) -> Result<Vec<(proto::ID, proto::State)>, RetrievalError> {
        let collection = self.name.clone();
        SendWrapper::new(async move {
            let transaction = self.db.transaction_with_str("entities").map_err(|_e| anyhow::anyhow!("Failed to create transaction"))?;

            let store = transaction.object_store("entities").map_err(|_e| anyhow::anyhow!("Failed to get object store"))?;

            let index = store.index("by_collection").map_err(|_e| anyhow::anyhow!("Failed to get collection index"))?;

            let key_range =
                web_sys::IdbKeyRange::only(&(&collection).into()).map_err(|_e| anyhow::anyhow!("Failed to create key range"))?;

            let request = index.open_cursor_with_range(&key_range).map_err(|_e| anyhow::anyhow!("Failed to open cursor"))?;

            let mut tuples = Vec::new();
            let mut stream = crate::cb_stream::CBStream::new(&request, "success", "error");

            while let Some(result) = stream.next().await {
                let cursor_result = result.map_err(|e| anyhow::anyhow!("Cursor error: {}", e))?;

                // Check if we've reached the end
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }

                let cursor: web_sys::IdbCursorWithValue = cursor_result.dyn_into().map_err(|_| anyhow::anyhow!("Failed to cast cursor"))?;

                let entity = cursor.value().map_err(|e| anyhow::anyhow!("Failed to get cursor value: {:?}", e))?;

                let id_str = js_sys::Reflect::get(&entity, &"id".into()).map_err(|_e| anyhow::anyhow!("Failed to get entity id"))?;
                let id = proto::ID::from_ulid(ulid::Ulid::from_string(&id_str.as_string().unwrap()).map_err(RetrievalError::storage)?);

                let state_buffer =
                    js_sys::Reflect::get(&entity, &"state_buffer".into()).map_err(|_e| anyhow::anyhow!("Failed to get state buffer"))?;
                let array: js_sys::Uint8Array = state_buffer.dyn_into().map_err(|_e| anyhow::anyhow!("Failed to convert state buffer"))?;

                let mut buffer = vec![0; array.length() as usize];
                array.copy_to(&mut buffer);

                let state_buffers: std::collections::BTreeMap<String, Vec<u8>> = bincode::deserialize(&buffer)?;

                // Get the head array
                let head_data = js_sys::Reflect::get(&entity, &"head".into()).map_err(|_e| anyhow::anyhow!("Failed to get head"))?;
                let head: proto::Clock = head_data.try_into().map_err(|e| anyhow::anyhow!("Failed to deserialize head: {}", e))?;

                let entity_state = proto::State { state_buffers, head };

                // Create entity to evaluate predicate
                let entity = Entity::from_state(id, collection.clone(), &entity_state)?;

                // Apply predicate filter
                if evaluate_predicate(&entity, predicate)? {
                    tuples.push((id, entity_state));
                }

                cursor.continue_().map_err(|_e| anyhow::anyhow!("Failed to advance cursor"))?;
            }

            Ok(tuples)
        })
        .await
    }

    async fn add_event(&self, entity_event: &ankurah_proto::Event) -> anyhow::Result<bool> { todo!() }
    async fn get_events(&self, id: ankurah_proto::ID) -> Result<Vec<ankurah_proto::Event>, ankurah_core::error::RetrievalError> { todo!() }
}

// #[cfg(target_arch = "wasm32")]
#[cfg(test)]
mod tests {
    #![allow(unused)]

    use super::*;
    use ankurah::Model;
    use ankurah::{Mutable, Node};
    use serde::{Deserialize, Serialize};
    use wasm_bindgen_test::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct Album {
        name: String,
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

        assert!(result.is_ok(), "Failed to open database: {:?}", result.err());

        let engine = result.unwrap();
        tracing::info!("Successfully opened database: {}", engine.db.name());
        assert_eq!(engine.db.name(), db_name, "Database name mismatch");

        // Test reopening existing database
        tracing::info!("Attempting to reopen database 'test_db'");
        let result2 = IndexedDBStorageEngine::open(&db_name).await;

        assert!(result2.is_ok(), "Failed to reopen database: {:?}", result2.err());
        tracing::info!("Test completed successfully");

        // Drop both engine instances
        drop(engine);
        if let Ok(engine2) = result2 {
            drop(engine2);
        }

        // Cleanup
        IndexedDBStorageEngine::cleanup(&db_name).await.expect("Failed to cleanup database");
    }

    #[wasm_bindgen_test]
    async fn test_set_and_get_entity() {
        setup().await.expect("Failed to setup test");

        let db_name = format!("test_db_{}", ulid::Ulid::new());
        let engine = IndexedDBStorageEngine::open(&db_name).await.expect("Failed to open database");

        let bucket = engine.collection(&"albums".into()).await.expect("Failed to create bucket");

        // Create a test entity
        let id = proto::ID::new();
        let mut state_buffers = std::collections::BTreeMap::new();
        state_buffers.insert("propertybackend_yrs".to_string(), vec![1, 2, 3]);
        let state = proto::State { state_buffers, head: proto::Clock::default() };

        // Set the entity
        bucket.set_state(id.clone(), &state).await.expect("Failed to set entity");

        // Get the entity back
        let retrieved_state = bucket.get_state(id).await.expect("Failed to get entity 6");
        tracing::info!("Retrieved state: {:?}", retrieved_state);

        assert_eq!(state.state_buffers, retrieved_state.state_buffers);

        // Drop the bucket and engine to close connections
        drop(bucket);
        drop(engine);

        // Cleanup
        IndexedDBStorageEngine::cleanup(&db_name).await.expect("Failed to cleanup database");
    }

    #[wasm_bindgen_test]
    async fn test_basic_workflow() -> Result<(), anyhow::Error> {
        setup().await.expect("Failed to setup test");

        let db_name = format!("test_db_{}", ulid::Ulid::new());
        tracing::info!("Starting test_basic_workflow");
        let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
        tracing::info!("Storage engine opened");
        let node = Node::new(Arc::new(storage_engine));

        let id;
        {
            tracing::info!("Creating transaction");
            let trx = node.begin();
            tracing::info!("Transaction created");
            let album = trx.create(&Album { name: "The rest of the owl".to_owned(), year: "2024".to_owned() }).await;
            assert_eq!(album.name().value(), Some("The rest of the owl".to_string()));

            id = album.id();
            tracing::info!("Album created");

            trx.commit().await?;
            tracing::info!("Transaction committed");
        }

        // Retrieve the entity
        let album_ro: AlbumView = node.get_entity(id).await?;
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
        let node = Node::new(Arc::new(storage_engine));

        {
            let trx = node.begin();

            trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await;

            trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await;

            trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await;

            trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await;

            trx.commit().await?;
        }

        let albums: ankurah_core::resultset::ResultSet<AlbumView> = node.fetch("name = 'Walking on a Dream'").await?;

        assert_eq!(
            albums.items.iter().map(|active_entity| active_entity.name()).collect::<Vec<String>>(),
            vec!["Walking on a Dream".to_string()]
        );

        // Drop the node to close the connection
        drop(node);

        // Cleanup
        IndexedDBStorageEngine::cleanup(&db_name).await?;

        Ok(())
    }
}
