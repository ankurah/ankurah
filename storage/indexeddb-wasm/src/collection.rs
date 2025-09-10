use std::sync::atomic::AtomicUsize;

use ankurah_core::{
    action_debug,
    error::{MutationError, RetrievalError},
    storage::StorageCollection,
};
use ankurah_proto::{self as proto, Attested, EntityState, EventId, State};
use async_trait::async_trait;
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue};

use crate::{cb_future::CBFuture, cb_stream::CBStream, database::Database, object::Object, statics::*};

// Import tracing for debug macro and futures for StreamExt
use futures::StreamExt;
use tracing::debug;

#[derive(Debug)]
pub struct IndexedDBBucket {
    db: Database,
    collection_id: proto::CollectionId,
    mutex: tokio::sync::Mutex<()>, // should probably be implemented by Database, but not certain
    invocation_count: AtomicUsize,
}

impl IndexedDBBucket {
    pub fn new(db: Database, collection_id: proto::CollectionId) -> Self {
        Self { db, collection_id, mutex: tokio::sync::Mutex::new(()), invocation_count: AtomicUsize::new(0) }
    }
}

impl std::fmt::Display for IndexedDBBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "IndexedDBBucket({})", self.collection_id) }
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

        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            use web_sys::IdbTransactionMode::Readwrite;
            action_debug!(self, "set_state {}", "{}", &self.collection_id);
            // Get the old entity if it exists to check for changes
            let transaction = step(db_connection.transaction_with_str_and_mode("entities", Readwrite), "create transaction")?;
            let store = step(transaction.object_store("entities"), "get object store")?;
            let old_request = step(store.get(&state.payload.entity_id.to_string().into()), "get old entity")?;
            let foo = CBFuture::new(&old_request, "success", "error").await;
            let _: () = step(foo, "get old entity")?;

            let old_entity: JsValue = step(old_request.result(), "get old entity result")?;

            // Check if the entity changed
            if !old_entity.is_undefined() && !old_entity.is_null() {
                let old_entity_obj = Object::new(old_entity);

                if let Some(old_clock) = old_entity_obj.get_opt::<proto::Clock>(&HEAD_KEY)? {
                    // let old_clock: proto::Clock = old_head.try_into()?;
                    if old_clock == state.payload.state.head {
                        // return false if the head is the same. This was formerly disabled for IndexedDB because it was breaking things
                        // by *accurately* reporting that the stored entity had not changed, because it was applied by another browser window moments earlier.
                        // Now we are checking the resident entity to see if it has been updated, which is more correct.
                        return Ok(false);
                    }
                }
            }

            let entity = Object::new(js_sys::Object::new().into());
            entity.set(&*ID_KEY, state.payload.entity_id.to_string())?;
            entity.set(&*COLLECTION_KEY, self.collection_id.as_str())?;
            entity.set(&*STATE_BUFFER_KEY, &state.payload.state.state_buffers)?;
            entity.set(&*HEAD_KEY, &state.payload.state.head)?;
            entity.set(&*ATTESTATIONS_KEY, &state.attestations)?;

            // Extract all fields for indexing
            extract_all_fields(&entity, &state.payload)?;

            // Put the entity in the store
            let request = step(store.put_with_key(&entity, &state.payload.entity_id.to_string().into()), "put entity in store")?;

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

        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            // Create transaction and get object store
            let transaction = step(db_connection.transaction_with_str("entities"), "create transaction")?;
            let store = step(transaction.object_store("entities"), "get object store")?;
            let request = step(store.get(&id.to_string().into()), "get entity")?;

            step(CBFuture::new(&request, "success", "error").await, "await request")?;

            let result = step(request.result(), "get result")?;

            // Check if the entity exists
            if result.is_undefined() || result.is_null() {
                return Err(RetrievalError::EntityNotFound(id));
            }

            let entity = Object::new(result);

            Ok(Attested {
                payload: EntityState {
                    entity_id: id,
                    collection: self.collection_id.clone(),
                    state: State { state_buffers: entity.get(&STATE_BUFFER_KEY)?, head: entity.get(&HEAD_KEY)? },
                },
                attestations: entity.get(&ATTESTATIONS_KEY)?,
            })
        })
        .await
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, RetrievalError> {
            res.map_err(|e| RetrievalError::StorageError(format!("{}: {}", msg, e.into().as_string().unwrap_or_default()).into()))
        }

        let _invocation = self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _lock = self.mutex.lock().await;

        // Step 1: Use optimizer to select the best index
        let optimizer = crate::indexes::Optimizer::new();
        let (index_spec, scan_direction, range_constraint) =
            optimizer.pick_index(selection).map_err(|e| RetrievalError::StorageError(format!("index selection: {}", e).into()))?;

        // Step 2: Ensure the index exists
        self.db
            .assure_index_exists(&index_spec)
            .await
            .map_err(|e| RetrievalError::StorageError(format!("ensure index exists: {}", e).into()))?;

        // Step 3: Execute the query using the selected index
        let db_connection = self.db.get_connection().await;
        let collection_id = self.collection_id.clone();
        let limit = selection.limit;

        SendWrapper::new(async move {
            let transaction = step(db_connection.transaction_with_str("entities"), "create transaction")?;
            let store = step(transaction.object_store("entities"), "get object store")?;

            // Always use index cursor - we never use the object store cursor directly
            // All our indexes are composite: [__collection, field_name]
            let index = step(store.index(index_spec.name()), "get index")?;

            // Convert range constraint to IndexedDB key range
            let key_range = if let Some(constraint) = range_constraint {
                Some(step(constraint.to_idb_key_range(&collection_id), "create constraint key range")?)
            } else {
                let collection_js_value: JsValue = collection_id.clone().into();

                // For composite indexes, we need to create a range that matches the collection prefix
                // Following IndexedDB best practices for composite key ranges:
                // Lower bound: ['album', ''] - collection + empty string (lowest possible)
                // Upper bound: ['album', Infinity] - collection + Infinity (highest possible)
                let lower_bound = js_sys::Array::new_with_length(2);
                lower_bound.set(0, collection_js_value.clone());
                lower_bound.set(1, JsValue::from_str("")); // Empty string for lowest possible second key

                let upper_bound = js_sys::Array::new_with_length(2);
                upper_bound.set(0, collection_js_value);
                upper_bound.set(1, JsValue::from_str("\u{10FFFF}")); // Highest Unicode code point for string comparison

                Some(step(web_sys::IdbKeyRange::bound(&lower_bound, &upper_bound), "create collection prefix key range")?)
            };

            let results = execute_index_query(&index, key_range, &selection.predicate, scan_direction, limit, &collection_id).await?;

            Ok(results)
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

        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction =
                step(db_connection.transaction_with_str_and_mode("events", web_sys::IdbTransactionMode::Readwrite), "create transaction")?;

            let store = step(transaction.object_store("events"), "get object store")?;

            // Create a JS object to store the event data
            let event_obj = Object::new(js_sys::Object::new().into());
            let payload = &attested_event.payload;
            event_obj.set(&*ID_KEY, &payload.id())?;
            event_obj.set(&*ENTITY_ID_KEY, &payload.entity_id)?;
            event_obj.set(&*OPERATIONS_KEY, &payload.operations)?;
            event_obj.set(&*ATTESTATIONS_KEY, &attested_event.attestations)?;
            event_obj.set(&*PARENT_KEY, &payload.parent)?;

            let request = step(store.put_with_key(&event_obj, &(&payload.id()).into()), "put event in store")?;

            step(CBFuture::new(&request, "success", "error").await, "await request")?;
            step(CBFuture::new(&transaction, "complete", "error").await, "complete transaction")?;

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

        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = step(db_connection.transaction_with_str("events"), "create transaction")?;
            let store = step(transaction.object_store("events"), "get object store")?;

            // TODO - do we want to use a cursor? The id space is pretty sparse, so we would probably need benchmarks to see if it's worth it
            let mut events = Vec::new();
            for event_id in event_ids {
                let request = step(store.get(&event_id.to_base64().into()), "get event")?;
                step(CBFuture::new(&request, "success", "error").await, "await event request")?;
                let result = step(request.result(), "get result")?;

                // Skip if event not found
                if result.is_undefined() || result.is_null() {
                    continue;
                }

                let event_obj = Object::new(result);

                let event = Attested {
                    payload: ankurah_proto::Event {
                        collection: self.collection_id.clone(),
                        entity_id: event_obj.get(&ENTITY_ID_KEY)?,
                        operations: event_obj.get(&OPERATIONS_KEY)?,
                        parent: event_obj.get(&PARENT_KEY)?,
                    },
                    attestations: event_obj.get(&ATTESTATIONS_KEY)?,
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

        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = step(db_connection.transaction_with_str("events"), "create transaction")?;
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
                let event_obj = Object::new(step(cursor.value(), "get cursor value")?);

                let event = Attested {
                    payload: ankurah_proto::Event {
                        collection: self.collection_id.clone(),
                        // id: event_obj.get(&ID_KEY)?.try_into()?,
                        entity_id: event_obj.get(&ENTITY_ID_KEY)?,
                        operations: event_obj.get(&OPERATIONS_KEY)?,
                        parent: event_obj.get(&PARENT_KEY)?,
                    },
                    attestations: event_obj.get(&ATTESTATIONS_KEY)?,
                };
                events.push(event);

                step(cursor.continue_(), "Failed to advance cursor")?;
            }

            Ok(events)
        })
        .await
    }
}

impl IndexedDBBucket {
    // Helper methods for query execution
}

// We always use index cursors for fetch operations
// Store cursors are only used for direct ID lookups (get_state, not fetch_states)

/// Execute queries using index cursors (we always use indexes for fetch operations)
/// Convert IndexDirection to IdbCursorDirection
pub fn to_idb_cursor_direction(direction: crate::indexes::IndexDirection) -> web_sys::IdbCursorDirection {
    match direction {
        crate::indexes::IndexDirection::Asc => web_sys::IdbCursorDirection::Next,
        crate::indexes::IndexDirection::Desc => web_sys::IdbCursorDirection::Prev,
    }
}

async fn execute_index_query(
    index: &web_sys::IdbIndex,
    key_range: Option<web_sys::IdbKeyRange>,
    predicate: &ankql::ast::Predicate,
    direction: crate::indexes::IndexDirection,
    limit: Option<u64>,
    collection_id: &ankurah_proto::CollectionId,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, RetrievalError> {
        res.map_err(|e| RetrievalError::StorageError(format!("{}: {}", msg, e.into().as_string().unwrap_or_default()).into()))
    }

    // Convert direction to IndexedDB cursor direction
    let cursor_direction = to_idb_cursor_direction(direction);

    // Open index cursor with optional key range and direction
    // Convert IdbKeyRange to JsValue for the API call
    let cursor_request = if let Some(range) = &key_range {
        step(index.open_cursor_with_range_and_direction(range.as_ref(), cursor_direction), "open index cursor with range and direction")?
    } else {
        step(
            index.open_cursor_with_range_and_direction(&wasm_bindgen::JsValue::NULL, cursor_direction),
            "open index cursor with direction only",
        )?
    };

    let mut results = Vec::new();
    let mut stream = CBStream::new(&cursor_request, "success", "error");
    let mut count = 0u64;

    while let Some(result) = stream.next().await {
        let cursor_result = step(result, "cursor error")?;
        if cursor_result.is_null() || cursor_result.is_undefined() {
            break;
        }

        let cursor: web_sys::IdbCursorWithValue = step(cursor_result.dyn_into(), "cast cursor")?;
        let entity_obj = Object::new(step(cursor.value(), "get cursor value")?);

        // Create a wrapper that provides collection context for filtering
        let filterable_entity = FilterableObject { object: &entity_obj, collection_id: &collection_id };

        // Apply predicate filtering first (cheaper than entity conversion)
        if ankql::selection::filter::evaluate_predicate(&filterable_entity, predicate)
            .map_err(|e| RetrievalError::StorageError(format!("Predicate evaluation failed: {}", e).into()))?
        {
            // Only convert to EntityState if predicate matches
            if let Ok(entity_state) = js_object_to_entity_state(&entity_obj, &collection_id) {
                results.push(entity_state);
                count += 1;

                if let Some(limit_val) = limit {
                    if count >= limit_val {
                        break;
                    }
                }
            } else {
            }
        } else {
        }

        step(cursor.continue_(), "advance cursor")?;
    }

    Ok(results)
}

/// Extract ID range from predicate for primary key queries
fn extract_id_range(predicate: &ankql::ast::Predicate) -> Option<web_sys::IdbKeyRange> {
    // TODO: Implement proper ID range extraction from predicate
    // For now, return None to scan all records
    let _ = predicate;
    None
}

/// Convert JS object to EntityState using the correct field extraction
fn js_object_to_entity_state(
    entity_obj: &Object,
    collection_id: &ankurah_proto::CollectionId,
) -> Result<Attested<EntityState>, RetrievalError> {
    use crate::statics::{ATTESTATIONS_KEY, HEAD_KEY, ID_KEY, STATE_BUFFER_KEY};
    use ankurah_proto::{Attested, EntityId, EntityState, State};

    // Extract the specific fields that are stored in IndexedDB using Object::get
    let id: EntityId = entity_obj.get(&ID_KEY)?;

    let entity_state = EntityState {
        collection: collection_id.clone(),
        entity_id: id,
        state: State { state_buffers: entity_obj.get(&STATE_BUFFER_KEY)?, head: entity_obj.get(&HEAD_KEY)? },
    };

    let attestations = entity_obj.get(&ATTESTATIONS_KEY)?;
    let attested_state = Attested { payload: entity_state, attestations };

    Ok(attested_state)
}

/// Extract all fields from entity state and set them directly on the IndexedDB entity object
fn extract_all_fields(entity_obj: &Object, entity_state: &EntityState) -> Result<(), MutationError> {
    use ankurah_core::property::backend::backend_from_string;
    use std::collections::HashSet;

    let mut seen_fields = HashSet::new();

    // Process all property values from state buffers
    for (backend_name, state_buffer) in entity_state.state.state_buffers.iter() {
        let backend = backend_from_string(backend_name, Some(state_buffer)).map_err(|e| MutationError::General(Box::new(e)))?;

        for (field_name, value) in backend.property_values() {
            // Use first occurrence (like Postgres) to handle field name collisions
            if !seen_fields.insert(field_name.clone()) {
                continue;
            }

            // Set field directly on entity object (no prefix - they become the primary fields)
            let js_value = match value {
                Some(ref prop_value) => JsValue::from(prop_value),
                None => JsValue::NULL,
            };
            entity_obj.set(&field_name, js_value)?;
        }
    }

    Ok(())
}

/// Wrapper struct to provide collection context for predicate filtering
struct FilterableObject<'a> {
    object: &'a Object,
    collection_id: &'a ankurah_proto::CollectionId,
}

/// Implement Filterable for FilterableObject to enable direct predicate evaluation on JS objects
impl<'a> ankql::selection::filter::Filterable for FilterableObject<'a> {
    fn collection(&self) -> &str { self.collection_id.as_str() }

    fn value(&self, name: &str) -> Option<String> { self.object.get_opt::<String>(&name.into()).unwrap_or(None) }
}
