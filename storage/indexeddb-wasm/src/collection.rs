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
use ankurah_storage_common::Plan;
// Import tracing for debug macro and futures for StreamExt
use futures::StreamExt;
use tracing::{debug, info};

#[derive(Debug)]
pub struct IndexedDBBucket {
    pub(crate) db: Database,
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) mutex: tokio::sync::Mutex<()>, // should probably be implemented by Database, but not certain
    pub(crate) invocation_count: AtomicUsize,
    #[cfg(debug_assertions)]
    pub(crate) prefix_guard_disabled: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
        let _lock = self.mutex.lock().await; // TODO why are we locking here?

        // Step 1: Amend predicate with __collection comparison
        let amended_selection = add_collection(selection, &self.collection_id);

        // Step 2: Use planner to generate query plans
        let planner = ankurah_storage_common::planner::Planner::new(ankurah_storage_common::planner::PlannerConfig::indexeddb());
        let plans = planner.plan(&amended_selection);

        // Step 3: Pick the first plan (always)
        let plan = plans.first().ok_or_else(|| RetrievalError::StorageError("No plan generated".into()))?;

        // Handle Plan enum
        match plan {
            Plan::EmptyScan => {
                // Empty scan - return empty results immediately
                return Ok(Vec::new());
            }
            Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, .. } => {
                // Step 4: Ensure index exists using plan's IndexSpec
                self.db
                    .assure_index_exists(index_spec)
                    .await
                    .map_err(|e| RetrievalError::StorageError(format!("ensure index exists: {}", e).into()))?;

                info!("LOOK fetching with bounds: {:?}", bounds);

                // Step 6: Execute the query using the plan
                let db_connection = self.db.get_connection().await;
                let collection_id = self.collection_id.clone();
                let limit = selection.limit;

                SendWrapper::new(async move {
                    let transaction = step(db_connection.transaction_with_str("entities"), "create transaction")?;
                    let store = step(transaction.object_store("entities"), "get object store")?;

                    // Get the index specified by the plan
                    let index = step(store.index(&index_spec.name_with("", "__")), "get index")?;

                    // Convert plan bounds to IndexedDB key range using new pipeline
                    let (key_range, upper_open_ended, eq_prefix_len, eq_prefix_values) =
                        crate::planner_integration::plan_bounds_to_idb_range(bounds)
                            .map_err(|e| RetrievalError::StorageError(format!("bounds conversion: {}", e).into()))?;

                    // Convert scan direction to cursor direction
                    let cursor_direction = crate::planner_integration::scan_direction_to_cursor_direction(scan_direction);

                    let results = self
                        .execute_plan_query(
                            &index,
                            Some(key_range),
                            remaining_predicate,
                            cursor_direction,
                            limit,
                            &collection_id,
                            upper_open_ended,
                            eq_prefix_len,
                            eq_prefix_values,
                        )
                        .await?;

                    Ok(results)
                })
            }
        }
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

// We always use index cursors for fetch operations
// Store cursors are only used for direct ID lookups (get_state, not fetch_states)

/// Execute queries using index cursors (we always use indexes for fetch operations)
/// Convert IndexDirection to IdbCursorDirection
pub fn to_idb_cursor_direction(direction: ankurah_storage_common::index_spec::IndexDirection) -> web_sys::IdbCursorDirection {
    match direction {
        ankurah_storage_common::index_spec::IndexDirection::Asc => web_sys::IdbCursorDirection::Next,
        ankurah_storage_common::index_spec::IndexDirection::Desc => web_sys::IdbCursorDirection::Prev,
    }
}

impl IndexedDBBucket {
    async fn execute_plan_query(
        &self,
        index: &web_sys::IdbIndex,
        key_range: Option<web_sys::IdbKeyRange>,
        predicate: &ankql::ast::Predicate,
        cursor_direction: web_sys::IdbCursorDirection,
        limit: Option<u64>,
        collection_id: &ankurah_proto::CollectionId,
        upper_open_ended: bool,
        eq_prefix_len: usize,
        eq_prefix_values: Vec<ankurah_core::property::PropertyValue>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        fn step<T, E: Into<JsValue>>(res: Result<T, E>, msg: &'static str) -> Result<T, RetrievalError> {
            res.map_err(|e| RetrievalError::StorageError(format!("{}: {}", msg, e.into().as_string().unwrap_or_default()).into()))
        }

        // cursor_direction is already provided as parameter

        // Open index cursor with optional key range and direction
        // Convert IdbKeyRange to JsValue for the API call
        let cursor_request = if let Some(range) = &key_range {
            step(
                index.open_cursor_with_range_and_direction(range.as_ref(), cursor_direction),
                "open index cursor with range and direction",
            )?
        } else {
            step(
                index.open_cursor_with_range_and_direction(&wasm_bindgen::JsValue::NULL, cursor_direction),
                "open index cursor with direction only",
            )?
        };

        let mut results = Vec::new();
        let mut stream = CBStream::new(&cursor_request, "success", "error");
        let mut count = 0u64;

        // Equality-prefix guard: enabled when scanning open-ended ranges with a non-empty equality prefix
        #[cfg(debug_assertions)]
        let use_prefix_guard =
            upper_open_ended && eq_prefix_len > 0 && !self.prefix_guard_disabled.load(std::sync::atomic::Ordering::Relaxed);
        #[cfg(not(debug_assertions))]
        let use_prefix_guard = upper_open_ended && eq_prefix_len > 0;

        let eq_prefix_js: Vec<JsValue> =
            if use_prefix_guard { eq_prefix_values.iter().map(|p| propertyvalue_to_js(p)).collect() } else { Vec::new() };

        'scan: while let Some(result) = stream.next().await {
            let cursor_result = step(result, "cursor error")?;
            if cursor_result.is_null() || cursor_result.is_undefined() {
                break;
            }

            let cursor: web_sys::IdbCursorWithValue = step(cursor_result.dyn_into(), "cast cursor")?;

            if use_prefix_guard {
                let key_js = step(cursor.key(), "get cursor key")?;
                if !key_js.is_undefined() && !key_js.is_null() {
                    let key_arr = js_sys::Array::from(&key_js);
                    for i in 0..(eq_prefix_len as u32) {
                        let lhs = key_arr.get(i);
                        let rhs = &eq_prefix_js[i as usize];
                        if !js_sys::Object::is(&lhs, rhs) {
                            break 'scan;
                        }
                    }
                }
            }
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
}
// FIXME: use (or update and use) the Into<JsValue> impl from core/src/property/mod.rs
// Convert PropertyValue to JsValue using the same mapping as key encoding
fn propertyvalue_to_js(p: &ankurah_core::property::PropertyValue) -> JsValue {
    match p {
        ankurah_core::property::PropertyValue::I16(x) => JsValue::from_f64(*x as f64),
        ankurah_core::property::PropertyValue::I32(x) => JsValue::from_f64(*x as f64),
        ankurah_core::property::PropertyValue::I64(x) => {
            // Order-preserving string encoding for i64 (matches planner_integration)
            let u = (*x as i128) - (i64::MIN as i128);
            JsValue::from_str(&format!("{:020}", u))
        }
        ankurah_core::property::PropertyValue::Bool(b) => JsValue::from_f64(if *b { 1.0 } else { 0.0 }),
        ankurah_core::property::PropertyValue::String(s) => JsValue::from_str(s),
        ankurah_core::property::PropertyValue::Object(bytes) | ankurah_core::property::PropertyValue::Binary(bytes) => {
            let u8_array = unsafe { js_sys::Uint8Array::view(bytes) };
            u8_array.buffer().into()
        }
    }
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

/// Amend a selection with __collection = 'value' comparison
pub fn add_collection(selection: &ankql::ast::Selection, collection_id: &ankurah_proto::CollectionId) -> ankql::ast::Selection {
    use ankql::ast::{ComparisonOperator, Expr, Identifier, Literal, Predicate};

    let collection_comparison = Predicate::Comparison {
        left: Box::new(Expr::Identifier(Identifier::Property("__collection".to_string()))),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Literal::String(collection_id.to_string()))),
    };

    ankql::ast::Selection {
        predicate: Predicate::And(Box::new(collection_comparison), Box::new(selection.predicate.clone())),
        order_by: selection.order_by.clone(),
        limit: selection.limit,
    }
}
