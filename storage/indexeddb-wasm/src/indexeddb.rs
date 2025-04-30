use ankql::selection::filter::evaluate_predicate;
use ankurah_core::entity::TemporaryEntity;
use ankurah_core::error::{MutationError, RetrievalError};
use ankurah_core::storage::{StorageCollection, StorageEngine};
use ankurah_proto::{self as proto, Attested, EntityState, EventId, State};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use js_sys::Function;
use lazy_static::lazy_static;
use send_wrapper::SendWrapper;
use std::any::Any;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tracing::debug;
use tracing::info;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest, IdbVersionChangeEvent};

use crate::cb_future::{cb_future, CBFuture};

// Cache the property keys so we only have to create the js values once
lazy_static! {
    static ref ID_KEY: Property = Property::new("id");
    static ref HEAD_KEY: Property = Property::new("head");
    static ref COLLECTION_KEY: Property = Property::new("collection");
    static ref STATE_BUFFER_KEY: Property = Property::new("state_buffer");
    static ref ENTITY_ID_KEY: Property = Property::new("entity_id");
    static ref OPERATIONS_KEY: Property = Property::new("operations");
    static ref ATTESTATIONS_KEY: Property = Property::new("attestations");
    static ref PARENT_KEY: Property = Property::new("parent");
}

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
        info!("IndexedDBStorageEngine.open({})", name);
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

                // Create events store with index on entity_id
                let events_store = match db.create_object_store("events") {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::warn!("Error creating events store (may already exist): {:?}", e);
                        return;
                    }
                };

                // Create index on entity_id field for efficient event lookups
                if let Err(e) = events_store.create_index_with_str("by_entity_id", "entity_id") {
                    tracing::error!("Failed to create entity_id index: {:?}", e);
                }
            }) as Box<dyn FnMut(_)>);

            let onsuccess = Closure::wrap(Box::new(move |event: web_sys::Event| {
                let target: IdbRequest = event.target().unwrap().unchecked_into();
                let db: IdbDatabase = target.result().unwrap().unchecked_into();
                resolve.call1(&JsValue::NULL, &JsValue::from(db)).unwrap();
            }) as Box<dyn FnMut(_)>);

            let onerror = Closure::wrap(Box::new(move |event: web_sys::Event| {
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

        let future = CBFuture::new(&delete_request, &["success", "blocked"], "error");

        future.await.map_err(|e| anyhow::anyhow!("Failed to delete database: {:?}", e))?;

        Ok(())
    }
}

#[async_trait]
impl StorageEngine for IndexedDBStorageEngine {
    type Value = JsValue;
    async fn collection(&self, collection_id: &proto::CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        Ok(Arc::new(IndexedDBBucket {
            db: self.db.clone(),
            collection_id: collection_id.clone(),
            mutex: tokio::sync::Mutex::new(()),
            invocation_count: AtomicUsize::new(0),
        }))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, MutationError> {
            res.map_err(|e| MutationError::FailedStep(msg, e.into().as_string().unwrap_or_default()))
        }

        SendWrapper::new(async move {
            // Clear entities store
            let entities_transaction = step(
                self.db.transaction_with_str_and_mode("entities", web_sys::IdbTransactionMode::Readwrite),
                "create entities transaction",
            )?;
            let entities_store = step(entities_transaction.object_store("entities"), "get entities store")?;
            let entities_request = step(entities_store.clear(), "clear entities store")?;
            step(CBFuture::new(&entities_request, "success", "error").await, "await entities clear")?;
            step(CBFuture::new(&entities_transaction, "complete", "error").await, "complete entities transaction")?;

            // Clear events store
            let events_transaction =
                step(self.db.transaction_with_str_and_mode("events", web_sys::IdbTransactionMode::Readwrite), "create events transaction")?;
            let events_store = step(events_transaction.object_store("events"), "get events store")?;
            let events_request = step(events_store.clear(), "clear events store")?;
            step(CBFuture::new(&events_request, "success", "error").await, "await events clear")?;
            step(CBFuture::new(&events_transaction, "complete", "error").await, "complete events transaction")?;

            // Return true since we cleared everything
            Ok(true)
        })
        .await
    }
}

#[async_trait]
impl StorageCollection for IndexedDBBucket {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, MutationError> {
            res.map_err(|e| MutationError::FailedStep(msg, e.into().as_string().unwrap_or_default()))
        }

        self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Lock the mutex to prevent concurrent updates
        let _lock = self.mutex.lock().await;

        SendWrapper::new(async move {
            use web_sys::IdbTransactionMode::Readwrite;
            // Get the old entity if it exists to check for changes
            let transaction = step(self.db.transaction_with_str_and_mode("entities", Readwrite), "create transaction")?;
            let store = step(transaction.object_store("entities"), "get object store")?;
            let old_request = step(store.get(&state.payload.entity_id.as_string().into()), "get old entity")?;
            let foo = CBFuture::new(&old_request, "success", "error").await;
            let _: () = step(foo, "get old entity")?;

            let old_entity: JsValue = step(old_request.result(), "get old entity result")?;

            // Check if the entity changed
            if !old_entity.is_undefined() && !old_entity.is_null() {
                let old_head = get_property(&old_entity, &HEAD_KEY)?;

                if !old_head.is_undefined() && !old_head.is_null() {
                    let old_clock: proto::Clock = old_head.try_into()?;
                    if old_clock == state.payload.state.head {
                        // return false if the head is the same. This was formerly disabled for IndexedDB because it was breaking things
                        // by *accurately* reporting that the stored entity had not changed, because it was applied by another browser window moments earlier.
                        // Now we are checking the resident entity to see if it has been updated, which is more correct.
                        return Ok(false);
                    }
                }
            }

            let entity = js_sys::Object::new();
            set_property(&entity, &ID_KEY, &state.payload.entity_id.as_string().into())?;
            set_property(&entity, &COLLECTION_KEY, &self.collection_id.as_str().into())?;
            set_property(&entity, &STATE_BUFFER_KEY, &(&state.payload.state.state_buffers).try_into()?)?;
            set_property(&entity, &HEAD_KEY, &(&(state.payload.state.head)).into())?;
            set_property(&entity, &ATTESTATIONS_KEY, &(&state.attestations).try_into()?)?;

            // Put the entity in the store
            let request = step(store.put_with_key(&entity, &state.payload.entity_id.as_string().into()), "put entity in store")?;

            step(CBFuture::new(&request, "success", "error").await, "put entity in store")?;
            step(CBFuture::new(&transaction, "complete", "error").await, "complete transaction")?;

            Ok(true) // It was updated
        })
        .await
    }

    async fn get_state(&self, id: proto::EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, RetrievalError> {
            res.map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("{}: {}", msg, e.into().as_string().unwrap_or_default()).into()))
        }

        SendWrapper::new(async move {
            // Create transaction and get object store
            let transaction = step(self.db.transaction_with_str("entities"), "create transaction")?;
            let store = step(transaction.object_store("entities"), "get object store")?;
            let request = step(store.get(&id.as_string().into()), "get entity")?;

            step(CBFuture::new(&request, "success", "error").await, "await request")?;

            let result = step(request.result(), "get result")?;

            // Check if the entity exists
            if result.is_undefined() || result.is_null() {
                return Err(RetrievalError::EntityNotFound(id));
            }

            let entity: web_sys::js_sys::Object = step(result.dyn_into(), "dyn into")?;

            Ok(Attested {
                payload: EntityState {
                    entity_id: id,
                    collection: self.collection_id.clone(),
                    state: State {
                        state_buffers: get_property(&entity, &STATE_BUFFER_KEY)?.try_into()?,
                        head: get_property(&entity, &HEAD_KEY)?.try_into()?,
                    },
                },
                attestations: get_property(&entity, &ATTESTATIONS_KEY)?.try_into()?,
            })
        })
        .await
    }

    async fn fetch_states(&self, predicate: &ankql::ast::Predicate) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let collection_id = self.collection_id.clone();

        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, RetrievalError> {
            res.map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("{}: {}", msg, e.into().as_string().unwrap_or_default()).into()))
        }

        SendWrapper::new(async move {
            let transaction = step(self.db.transaction_with_str("entities"), "create transaction")?;
            let store = step(transaction.object_store("entities"), "get object store")?;
            let index = step(store.index("by_collection"), "get collection index")?;
            let key_range = step(web_sys::IdbKeyRange::only(&collection_id.as_str().into()), "create key range")?;
            let request = step(index.open_cursor_with_range(&key_range), "open cursor")?;

            let mut output: Vec<Attested<EntityState>> = Vec::new();
            let mut stream = crate::cb_stream::CBStream::new(&request, "success", "error");

            while let Some(result) = stream.next().await {
                let cursor_result = step(result, "cursor error")?;

                // Check if we've reached the end
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }

                let cursor: web_sys::IdbCursorWithValue = step(cursor_result.dyn_into(), "cast cursor")?;
                let entity = step(cursor.value(), "get cursor value")?;
                let id = get_property(&entity, &ID_KEY)?.try_into()?;
                let entity_state = EntityState {
                    collection: collection_id.clone(),
                    entity_id: id,
                    state: State {
                        state_buffers: get_property(&entity, &STATE_BUFFER_KEY)?.try_into()?,
                        head: get_property(&entity, &HEAD_KEY)?.try_into()?,
                    },
                };
                let attestations = get_property(&entity, &ATTESTATIONS_KEY)?.try_into()?;
                let attested_state = Attested { payload: entity_state, attestations };

                // Create entity to evaluate predicate
                let entity = TemporaryEntity::new(id, collection_id.clone(), &attested_state.payload.state)?;
                // Apply predicate filter
                if evaluate_predicate(&entity, predicate)? {
                    output.push(attested_state);
                }

                step(cursor.continue_(), "advance cursor")?;
            }

            Ok(output)
        })
        .await
    }

    async fn add_event(&self, attested_event: &Attested<ankurah_proto::Event>) -> Result<bool, MutationError> {
        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, MutationError> {
            res.map_err(|e| MutationError::FailedStep(msg, e.into().as_string().unwrap_or_default()))
        }

        let invocation = self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug!("IndexedDBBucket({}).add_event({})", self.collection_id, invocation);
        let _lock = self.mutex.lock().await;
        debug!("IndexedDBBucket({}).add_event({}) LOCKED", self.collection_id, invocation);

        SendWrapper::new(async move {
            let transaction =
                step(self.db.transaction_with_str_and_mode("events", web_sys::IdbTransactionMode::Readwrite), "create transaction")?;

            let store = step(transaction.object_store("events"), "get object store")?;

            // Create a JS object to store the event data
            let event_obj = js_sys::Object::new();
            let payload = &attested_event.payload;
            let id: JsValue = (&payload.id()).into();
            set_property(&event_obj, &ID_KEY, &id)?;
            set_property(&event_obj, &ENTITY_ID_KEY, &(&payload.entity_id).into())?;
            set_property(&event_obj, &OPERATIONS_KEY, &(&payload.operations).try_into()?)?;
            set_property(&event_obj, &ATTESTATIONS_KEY, &(&attested_event.attestations).try_into()?)?;
            set_property(&event_obj, &PARENT_KEY, &(&payload.parent).into())?;

            let request = step(store.put_with_key(&event_obj, &(&payload.id()).into()), "put event in store")?;

            step(cb_future(&request, "success", "error").await, "await request")?;
            step(cb_future(&transaction, "complete", "error").await, "complete transaction")?;

            Ok(true)
        })
        .await
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<ankurah_proto::Event>>, RetrievalError> {
        if event_ids.is_empty() {
            return Ok(Vec::new());
        }

        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, RetrievalError> {
            res.map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("{}: {}", msg, e.into().as_string().unwrap_or_default()).into()))
        }

        SendWrapper::new(async move {
            let transaction = step(self.db.transaction_with_str("events"), "create transaction")?;
            let store = step(transaction.object_store("events"), "get object store")?;

            // TODO - do we want to use a cursor? The id space is pretty sparse, so we would probably need benchmarks to see if it's worth it
            let mut events = Vec::new();
            for event_id in event_ids {
                let request = step(store.get(&event_id.to_string().into()), "get event")?;
                step(CBFuture::new(&request, "success", "error").await, "await event request")?;
                let result = step(request.result(), "get result")?;

                // Skip if event not found
                if result.is_undefined() || result.is_null() {
                    continue;
                }

                let event_obj: web_sys::js_sys::Object = step(result.dyn_into(), "cast event object")?;

                let event = Attested {
                    payload: ankurah_proto::Event {
                        collection: get_property(&event_obj, &COLLECTION_KEY)?.try_into()?,
                        entity_id: get_property(&event_obj, &ENTITY_ID_KEY)?.try_into()?,
                        operations: get_property(&event_obj, &OPERATIONS_KEY)?.try_into()?,
                        parent: get_property(&event_obj, &PARENT_KEY)?.try_into()?,
                    },
                    attestations: get_property(&event_obj, &ATTESTATIONS_KEY)?.try_into()?,
                };
                events.push(event);
            }

            Ok(events)
        })
        .await
    }

    async fn dump_entity_events(&self, id: ankurah_proto::EntityId) -> Result<Vec<Attested<ankurah_proto::Event>>, RetrievalError> {
        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, RetrievalError> {
            res.map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("{}: {}", msg, e.into().as_string().unwrap_or_default()).into()))
        }

        SendWrapper::new(async move {
            let transaction = step(self.db.transaction_with_str("events"), "create transaction")?;
            let store = step(transaction.object_store("events"), "get object store")?;
            let index = step(store.index("by_entity_id"), "get entity_id index")?;
            let key_range = step(web_sys::IdbKeyRange::only(&id.into()), "create key range")?;
            let request = step(index.open_cursor_with_range(&key_range), "open cursor")?;

            let mut events = Vec::new();
            let mut stream = crate::cb_stream::CBStream::new(&request, "success", "error");

            while let Some(result) = stream.next().await {
                let cursor_result = step(result, "Cursor error")?;

                // Check if we've reached the end
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }

                let cursor: web_sys::IdbCursorWithValue = step(cursor_result.dyn_into(), "cast cursor")?;
                let event_obj = step(cursor.value(), "get cursor value")?;

                let event = Attested {
                    payload: ankurah_proto::Event {
                        collection: get_property(&event_obj, &COLLECTION_KEY)?.try_into()?,
                        // id: get_property(&event_obj, &ID_KEY)?.try_into()?,
                        entity_id: get_property(&event_obj, &ENTITY_ID_KEY)?.try_into()?,
                        operations: get_property(&event_obj, &OPERATIONS_KEY)?.try_into()?,
                        parent: get_property(&event_obj, &PARENT_KEY)?.try_into()?,
                    },
                    attestations: get_property(&event_obj, &ATTESTATIONS_KEY)?.try_into()?,
                };
                events.push(event);

                step(cursor.continue_(), "Failed to advance cursor")?;
            }

            Ok(events)
        })
        .await
    }
}

fn get_property(obj: &JsValue, key: &Property) -> Result<JsValue, RetrievalError> {
    js_sys::Reflect::get(obj, key).map_err(|_e| RetrievalError::StorageError(anyhow::anyhow!("Failed to get {}", key).into()))
}

fn set_property(obj: &JsValue, key: &Property, value: &JsValue) -> Result<bool, MutationError> {
    js_sys::Reflect::set(obj, key, value).map_err(|_e| MutationError::FailedToSetProperty(key.name, value.as_string().unwrap_or_default()))
}

struct Property {
    key: SendWrapper<JsValue>,
    name: &'static str,
}

impl Property {
    fn new(key: &'static str) -> Self { Self { key: SendWrapper::new(key.into()), name: key } }
}
impl std::ops::Deref for Property {
    type Target = JsValue;
    fn deref(&self) -> &Self::Target { &self.key }
}
impl std::fmt::Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.name) }
}

// #[cfg(target_arch = "wasm32")]
#[cfg(test)]
mod tests {
    #![allow(unused)]

    use std::collections::BTreeMap;

    use super::*;
    use ankurah::{policy::DEFAULT_CONTEXT as c, Model, Mutable, Node, PermissiveAgent};
    use ankurah_proto::{AttestationSet, StateBuffers};
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
        let id = proto::EntityId::new();
        let mut state_buffers: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        state_buffers.insert("propertybackend_yrs".to_string(), vec![1, 2, 3]);
        let state = proto::State { state_buffers: StateBuffers(state_buffers), head: proto::Clock::default() };
        let attested_state = Attested {
            payload: EntityState { entity_id: id, collection: "albums".into(), state },
            attestations: AttestationSet::default(),
        };
        // Set the entity
        bucket.set_state(attested_state.clone()).await.expect("Failed to set entity");

        // Get the entity back
        let retrieved_state = bucket.get_state(id).await.expect("Failed to get entity 6");
        tracing::info!("Retrieved state: {:?}", retrieved_state);

        assert_eq!(attested_state.payload.state.state_buffers, retrieved_state.payload.state.state_buffers);

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
        let node = Node::new(Arc::new(storage_engine), PermissiveAgent::new());
        node.system.create().await?;
        let ctx = node.context(c)?;

        let id;
        {
            tracing::info!("Creating transaction");
            let trx = ctx.begin();
            tracing::info!("Transaction created");
            let album = trx.create(&Album { name: "The rest of the owl".to_owned(), year: "2024".to_owned() }).await?;
            assert_eq!(album.name().value(), Some("The rest of the owl".to_string()));

            id = album.id();
            tracing::info!("Album created");

            trx.commit().await?;
            tracing::info!("Transaction committed");
        }

        // Retrieve the entity
        let album_ro: AlbumView = ctx.get(id).await?;
        assert_eq!(album_ro.name().unwrap(), "The rest of the owl");
        assert_eq!(album_ro.year().unwrap(), "2024");

        // Drop the node to close the connection
        drop(ctx);

        // Cleanup
        IndexedDBStorageEngine::cleanup(&db_name).await?;

        Ok(())
    }

    #[wasm_bindgen_test]
    async fn test_basic_where_clause() -> Result<(), anyhow::Error> {
        setup().await.expect("Failed to setup test");

        let db_name = format!("test_db_{}", ulid::Ulid::new());
        let storage_engine = IndexedDBStorageEngine::open(&db_name).await?;
        let node = Node::new(Arc::new(storage_engine), PermissiveAgent::new());
        node.system.create().await?;
        let ctx = node.context(c)?;

        {
            let trx = ctx.begin();

            trx.create(&Album { name: "Walking on a Dream".into(), year: "2008".into() }).await?;

            trx.create(&Album { name: "Ice on the Dune".into(), year: "2013".into() }).await?;

            trx.create(&Album { name: "Two Vines".into(), year: "2016".into() }).await?;

            trx.create(&Album { name: "Ask That God".into(), year: "2024".into() }).await?;

            trx.commit().await?;
        }

        let albums: ankurah_core::resultset::ResultSet<AlbumView> = ctx.fetch("name = 'Walking on a Dream'").await?;

        assert_eq!(
            albums.items.iter().map(|active_entity| active_entity.name().unwrap()).collect::<Vec<String>>(),
            vec!["Walking on a Dream".to_string()]
        );

        // Drop the node to close the connection
        drop(ctx);

        // Cleanup
        IndexedDBStorageEngine::cleanup(&db_name).await?;

        Ok(())
    }
}
