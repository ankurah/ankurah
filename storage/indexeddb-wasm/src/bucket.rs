use ankql::ast::{Predicate, Resolved};
use ankurah_storage_common::naming;
use ankurah_storage_common::{ColumnPath, EngineColumns};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use ankql::ast::PropertyId;
use ankurah_core::{
    error::{MutationError, RetrievalError},
    schema::CatalogResolver,
    selection::filter::evaluate_predicate,
    ModelId,
};
use ankurah_proto::{self as proto, Attested, EntityState};
use send_wrapper::SendWrapper;
use wasm_bindgen::{JsCast, JsValue};

use crate::{
    database::Database,
    statics::*,
    util::{cb_future::cb_future, cb_stream::cb_stream, object::Object, require::WBGRequire},
};
use ankurah_storage_common::{filtering::ValueSetStream, OrderByComponents, Plan};
// Import tracing for debug macro and futures for StreamExt
use futures::StreamExt;

#[derive(Debug)]
/// Private query/materialization handle for one model within IndexedDB's
/// shared stores.
///
/// `materialization_name` is assigned by the engine's durable model registry;
/// property fields are likewise addressed through its durable property map.
pub struct IndexedDBBucket {
    pub(crate) db: Database,
    pub(crate) model_id: ModelId,
    pub(crate) materialization_name: String,
    /// Serializes query planning and index creation for this materialization.
    pub(crate) mutex: tokio::sync::Mutex<()>,
    /// The injected catalog resolver (shared with the engine): the NAME SOURCE
    /// for [`Self::column_for_key`]. Weak so storage never keeps the node alive.
    pub(crate) resolver: Arc<RwLock<Option<std::sync::Weak<dyn CatalogResolver>>>>,
    /// This materialization's slice of the engine-owned durable identity-to-field map
    /// (the `property_columns` object store), cached in memory and keyed by
    /// durable [`PropertyId`] (NOT `EntityId`) so the write side (a backend's
    /// `property_values()` id) and the read side (a `PropertyId` off the resolved
    /// AST) address the same row. The map -- not the display name -- is what
    /// addresses a property's field once assigned: renames never move fields,
    /// collisions were deduped at assignment.
    pub(crate) property_columns: Arc<RwLock<BTreeMap<PropertyId, String>>>,
    /// Whether [`Self::ensure_property_columns_loaded`] has hydrated
    /// `property_columns` from the store; cleared when a query needs a missing field.
    pub(crate) property_columns_loaded: AtomicBool,
    #[cfg(debug_assertions)]
    pub(crate) prefix_guard_disabled: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// A projection whose durable property fields are already assigned.
///
/// Publishing it is one IndexedDB `put`, so the engine can include it in the
/// same transaction as canonical state and entity-model associations.
pub(crate) struct PreparedIndexedDbMaterialization {
    /// Composite key selecting one model projection of one entity.
    pub(crate) key: String,
    /// Fully projected object ready for transactional publication.
    pub(crate) object: Object,
}

/// Reserved materialization-object fields a property field must never shadow.
const RESERVED_FIELDS: [&str; 2] = ["id", "__materialization"];

/// Store key for a property identity's field-name assignment in a model:
/// `model\0{serialized ModelId}\0{serialized PropertyId}`. The suffix is the property's durable
/// serde identity (JSON, see [`property_key_text`]) -- NOT an `EntityId` -- so
/// the write side (a backend's `property_values()` id) and the read side (a
/// `PropertyId` off the resolved AST) address the byte-identical row, and a
/// system property gets a row exactly like a registered one. A concatenated
/// string key (matching the store's other string keys) so a materialization's
/// whole slice is one prefix range; serialized model ids carry no raw NUL and
/// JSON escapes any
/// control byte, so the NUL cleanly separates the scope from the identity suffix.
fn property_columns_prefix(model: &ModelId) -> String {
    format!("model\0{}\0", serde_json::to_string(model).expect("ModelId always serializes"))
}

fn property_columns_key(model: &ModelId, id: &PropertyId) -> String {
    format!("{}{}", property_columns_prefix(model), property_key_text(id))
}

fn property_field_key(model: &ModelId, field: &str) -> String {
    format!("field\0{}\0{field}", serde_json::to_string(model).expect("ModelId always serializes"))
}

fn decode_property_column_row(prefix: &str, key: &str, field: String) -> Result<(PropertyId, String), MutationError> {
    let suffix = key.strip_prefix(prefix).ok_or_else(|| {
        MutationError::General(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("property_columns key {key:?} is outside the requested model prefix"),
            )
            .into(),
        )
    })?;
    let property_id = serde_json::from_str::<PropertyId>(suffix).map_err(|error| {
        MutationError::General(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("property_columns key {key:?} has an invalid PropertyId suffix: {error}"),
            )
            .into(),
        )
    })?;
    Ok((property_id, field))
}

fn materialization_entity_key(materialization_name: &str, entity_id: proto::EntityId) -> String {
    format!("{materialization_name}\0{}", entity_id.to_base64())
}

/// The durable, serialized address of a property identity: the JSON a
/// [`PropertyId`] serializes to (mirrors the sqlite engine's `property_key`
/// text). Legible and stable across the serde boundary the durable map key
/// crosses; the write side (the id a backend yields) and the read side (an id
/// straight off the resolved AST) both go through this, so a field assigned on
/// write is found by the byte-identical key on read.
fn property_key_text(id: &PropertyId) -> String { serde_json::to_string(id).expect("PropertyId always serializes") }

impl std::fmt::Display for IndexedDBBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "IndexedDBBucket({})", self.materialization_name) }
}

impl IndexedDBBucket {
    /// Assign any missing durable fields and build a projection ready for the
    /// engine's atomic state transaction.
    pub(crate) async fn prepare_state(&self, state: &Attested<EntityState>) -> Result<PreparedIndexedDbMaterialization, MutationError> {
        self.ensure_property_columns_loaded().await?;

        let materialized = Object::new(js_sys::Object::new().into());
        materialized.set(&*ID_KEY, state.payload.entity_id.to_base64())?;
        materialized.set(&*MATERIALIZATION_KEY, self.materialization_name.as_str())?;
        self.extract_fields(&materialized, &state.payload).await?;

        Ok(PreparedIndexedDbMaterialization {
            key: materialization_entity_key(&self.materialization_name, state.payload.entity_id),
            object: materialized,
        })
    }

    /// Query this model's projection and hydrate matching canonical states.
    pub(crate) async fn fetch_states(
        &self,
        selection: &ankql::ast::Selection<Resolved>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let _lock = self.mutex.lock().await;

        // Resolve, through this engine's own durable map, the field for every
        // property the selection references (assigned on write, sticky under
        // rename). There is NO name fallback: a property with no assigned field
        // is absent (evaluates NULL, folded below), never re-derived from a raw
        // name. The planner then translates each surviving
        // identity to its field at build time via `column_of`, and the in-memory
        // post-filter/sort keep reading by identity, so a rename is harmless end
        // to end.
        self.ensure_property_columns_loaded().await.map_err(|e| RetrievalError::storage(format!("{e:?}")))?;
        let referenced = selection.referenced_properties();
        let missing = {
            let assigned = self.property_columns.read().expect("RwLock poisoned");
            referenced.iter().any(|id| *id != PropertyId::Id && !assigned.contains_key(id))
        };
        if missing {
            // Another engine may have assigned the field since this cache loaded.
            self.property_columns_loaded.store(false, Ordering::Relaxed);
            self.ensure_property_columns_loaded().await?;
        }
        let assigned = std::sync::Arc::new(self.property_columns.read().expect("RwLock poisoned").clone());

        // Translate a resolved identity to this engine's physical field through
        // the durable map (a miss = absent, no name fallback); `PropertyId::Id`
        // is pinned to the reserved "id" field so a primary-key read is a uniform
        // map hit and never wrongly absent.
        let column_of = {
            let assigned = assigned.clone();
            move |pid: &PropertyId| -> Option<String> {
                if matches!(pid, PropertyId::Id) {
                    return Some("id".to_string());
                }
                assigned.get(pid).cloned()
            }
        };

        // Read-side absence, by durable identity: fold every referenced property
        // this engine never materialized to NULL, and drop any ORDER BY key on
        // one.
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|pid| column_of(pid).is_none()).collect();
        if selection.predicate.referenced_properties().iter().any(|property| absent.contains(property)) {
            // Preserve fail-closed comparisons under NOT; NULL folding can change their meaning.
            let mut selection = selection.clone();
            selection.predicate = Predicate::And(Box::new(Predicate::MemberOf(self.model_id)), Box::new(selection.predicate));
            return crate::engine::query::fetch(&self.db, &selection).await;
        }
        let selection = if absent.is_empty() { selection.clone() } else { selection.assume_null(&absent) };

        let amended_selection = crate::lower::lower(&selection, &assigned, &self.materialization_name);
        let planner = ankurah_storage_common::Planner::new(ankurah_storage_common::PlannerConfig::indexeddb());
        let plans = planner.plan(&amended_selection, "id");
        let plan = plans.first().ok_or_else(|| RetrievalError::Other("No plan generated".into()))?;

        let states = match plan {
            Plan::EmptyScan => Vec::new(),
            Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill } => {
                // Step 4: Ensure index exists using plan's IndexSpec
                self.db
                    .assure_index_exists(index_spec)
                    .await
                    .map_err(|e| RetrievalError::storage(format!("ensure index exists: {}", e)))?;

                // Step 6: Execute the query using the plan
                let db_connection = self.db.get_connection().await;
                let limit = selection.limit;

                SendWrapper::new(async move {
                    let stores = js_sys::Array::of3(
                        &JsValue::from_str("materializations"),
                        &JsValue::from_str("entities"),
                        &JsValue::from_str("entity_models"),
                    );
                    let transaction = db_connection.transaction_with_str_sequence(&stores).require("create indexed read transaction")?;
                    let store = transaction.object_store("materializations").require("get materializations store")?;
                    let entities = transaction.object_store("entities").require("get entities store")?;
                    let membership_store = transaction.object_store("entity_models").require("get entity-model association store")?;

                    // Get the index specified by the plan
                    let index = store.index(&index_spec.name_with("", "__")).require("get index")?;

                    // Convert plan bounds to IndexedDB key range using new pipeline
                    let (key_range, upper_open_ended, eq_prefix_len, eq_prefix_values) =
                        crate::planner_integration::plan_bounds_to_idb_range(bounds, scan_direction)
                            .map_err(|e| RetrievalError::storage(format!("bounds conversion: {}", e)))?;
                    // Convert scan direction to cursor direction
                    let cursor_direction = crate::planner_integration::scan_direction_to_cursor_direction(scan_direction);

                    let results = self
                        .execute_plan_query(
                            &index,
                            &entities,
                            &membership_store,
                            Some(key_range),
                            remaining_predicate,
                            cursor_direction,
                            limit,
                            upper_open_ended,
                            eq_prefix_len,
                            eq_prefix_values,
                            &order_by_spill,
                        )
                        .await?;

                    Ok::<_, RetrievalError>(results)
                })
                .await?
            }
            Plan::TableScan { .. } => {
                return Err(RetrievalError::Other(
                    "IndexedDB planner returned a table scan after materialization scoping; no safe execution path exists".to_owned(),
                ));
            }
        };
        Ok(states)
    }
}

impl IndexedDBBucket {
    /// Hydrate `property_columns` with this model's durable
    /// identity-to-field assignments. Runs BEFORE any write
    /// transaction so the loaded cache is the authoritative taken-set for
    /// [`Self::column_for_key`] (and so a field lookup never has to read the
    /// store while the entities transaction is open -- IndexedDB auto-commits a
    /// transaction the moment an await doesn't belong to it).
    async fn ensure_property_columns_loaded(&self) -> Result<(), MutationError> {
        if self.property_columns_loaded.load(Ordering::Relaxed) {
            return Ok(());
        }
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection.transaction_with_str("property_columns").require("create property_columns transaction")?;
            let store = transaction.object_store("property_columns").require("get property_columns store")?;

            // This model's rows are exactly the keys under its serialized
            // ModelId prefix; the serialized-PropertyId suffix is JSON
            // (all chars sort below U+FFFF), so this bound captures the slice and
            // nothing else.
            let prefix = property_columns_prefix(&self.model_id);
            let upper = format!("{}{}", prefix, '\u{ffff}');
            let range = web_sys::IdbKeyRange::bound(&JsValue::from_str(&prefix), &JsValue::from_str(&upper))
                .require("create property_columns key range")?;
            let request = store.open_cursor_with_range(&range).require("open property_columns cursor")?;

            let mut map = BTreeMap::new();
            let mut stream = cb_stream(&request, "success", "error");
            while let Some(result) = stream.next().await {
                let cursor_result = result.require("property_columns cursor error")?;
                if cursor_result.is_null() || cursor_result.is_undefined() {
                    break;
                }
                let cursor = cursor_result.dyn_into::<web_sys::IdbCursorWithValue>().require("cast property_columns cursor")?;
                let key = cursor.key().require("get property_columns cursor key")?;
                let value = cursor.value().require("get property_columns cursor value")?;
                // This map is authoritative durable addressing metadata.
                // Refuse malformed rows: skipping one could make an existing
                // property appear unassigned and allocate a second field.
                let key_str = key.as_string().ok_or_else(|| {
                    MutationError::General(
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "property_columns key is not a string").into(),
                    )
                })?;
                let name = value.as_string().ok_or_else(|| {
                    MutationError::General(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("property_columns value for {key_str:?} is not a string"),
                        )
                        .into(),
                    )
                })?;
                let (property_id, name) = decode_property_column_row(&prefix, &key_str, name)?;
                map.insert(property_id, name);
                cursor.continue_().require("advance property_columns cursor")?;
            }

            *self.property_columns.write().unwrap() = map;
            self.property_columns_loaded.store(true, Ordering::Relaxed);
            Ok(())
        })
        .await
    }

    /// Assign a sticky physical name on a cache miss, using a label or an ID prefix.
    /// Call after loading the name map and before opening the publication transaction.
    async fn column_for_key(&self, property_id: &PropertyId) -> Result<String, MutationError> {
        if let Some(field) = self.property_columns.read().unwrap().get(property_id) {
            return Ok(field.clone());
        }

        let label = match property_id {
            PropertyId::EntityId(_) => {
                let resolver = self.resolver.read().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
                match resolver {
                    Some(resolver) => resolver.get_property_label(property_id).await,
                    None => None,
                }
            }
            _ => None,
        };
        // A readwrite transaction serializes claims in the durable registry.
        // The cache supplies the ordinary fast path; on a collision with a
        // different tab/engine instance, reload and re-dedupe.
        for _attempt in 0..3 {
            let field = match property_id {
                // System property: its sanitized name is the field directly.
                // It has no identity suffix with which to recover from a
                // collision, so any other owner is a hard error.
                PropertyId::System(property) => {
                    let field = naming::sanitize(property.as_str());
                    if RESERVED_FIELDS.contains(&field.as_str()) {
                        return Err(MutationError::General(
                            anyhow::anyhow!(
                                "system property {:?} sanitizes to {:?}, a reserved record field of materialization {}; refusing the assignment",
                                property,
                                field,
                                self.materialization_name
                            )
                            .into(),
                        ));
                    }
                    let assigned = self.property_columns.read().unwrap();
                    if let Some((owner, _)) = assigned.iter().find(|(owner, taken)| *owner != property_id && taken.as_str() == field) {
                        return Err(MutationError::General(
                            anyhow::anyhow!(
                                "system property {:?} sanitizes to field {:?}, which is already assigned to property {} in materialization {}; refusing the assignment",
                                property,
                                field,
                                property_key_text(owner),
                                self.materialization_name
                            )
                            .into(),
                        ));
                    }
                    drop(assigned);
                    field
                }
                // Registered property: seed from the resolver, dedupe by id.
                PropertyId::EntityId(id) => {
                    let assigned = self.property_columns.read().unwrap();
                    let is_taken = |candidate: &str| {
                        RESERVED_FIELDS.contains(&candidate)
                            || assigned.iter().any(|(other, name)| other != property_id && name == candidate)
                    };
                    match label.as_deref() {
                        Some(label) => naming::dedupe(&naming::sanitize(label), id, is_taken),
                        None => naming::fallback("p", id, is_taken),
                    }
                    .map_err(|error| MutationError::General(anyhow::anyhow!(error).into()))?
                }
                PropertyId::Id => {
                    return Err(MutationError::General(
                        anyhow::anyhow!("the id pseudo-property is never materialized as a stored value").into(),
                    ))
                }
            };

            match self.persist_property_column(property_id, &field).await? {
                PropertyFieldClaim::Stored(stored) => {
                    self.property_columns.write().unwrap().insert(*property_id, stored.clone());
                    return Ok(stored);
                }
                PropertyFieldClaim::Collision if matches!(property_id, PropertyId::System(_)) => {
                    return Err(MutationError::General(
                        anyhow::anyhow!(
                            "system property {} cannot claim field {:?} in materialization {}; another property owns it",
                            property_key_text(property_id),
                            field,
                            self.materialization_name
                        )
                        .into(),
                    ));
                }
                PropertyFieldClaim::Collision => {
                    self.property_columns_loaded.store(false, Ordering::Relaxed);
                    self.ensure_property_columns_loaded().await?;
                }
            }
        }

        Err(MutationError::General(
            anyhow::anyhow!(
                "could not assign a field for property {} in materialization {} after repeated concurrent collisions",
                property_key_text(property_id),
                self.materialization_name
            )
            .into(),
        ))
    }

    /// Persist an identity-to-field assignment to the `property_columns` store,
    /// returning the durable claim result. Forward and reverse rows are written
    /// in one readwrite transaction, so separate tabs or engine instances
    /// cannot assign the same field to different property identities.
    async fn persist_property_column(&self, property_id: &PropertyId, proposed: &str) -> Result<PropertyFieldClaim, MutationError> {
        let map_key = property_columns_key(&self.model_id, property_id);
        let field_key = property_field_key(&self.model_id, proposed);
        let proposed = proposed.to_string();
        let db_connection = self.db.get_connection().await;
        SendWrapper::new(async move {
            let transaction = db_connection
                .transaction_with_str_and_mode("property_columns", web_sys::IdbTransactionMode::Readwrite)
                .require("create property_columns transaction")?;
            let store = transaction.object_store("property_columns").require("get property_columns store")?;

            let get_request = store.get(&JsValue::from_str(&map_key)).require("get property_columns entry")?;
            cb_future(&get_request, "success", "error").await.require("await property_columns get")?;
            let existing = get_request.result().require("get property_columns result")?;
            if let Some(name) = existing.as_string() {
                return Ok(PropertyFieldClaim::Stored(name));
            }

            let reverse_request = store.get(&JsValue::from_str(&field_key)).require("get property field owner")?;
            cb_future(&reverse_request, "success", "error").await.require("await property field owner")?;
            let owner = reverse_request.result().require("get property field owner result")?;
            if let Some(owner) = owner.as_string() {
                if owner != map_key {
                    return Ok(PropertyFieldClaim::Collision);
                }
            }

            let put_request =
                store.put_with_key(&JsValue::from_str(&proposed), &JsValue::from_str(&map_key)).require("put property_columns entry")?;
            cb_future(&put_request, "success", "error").await.require("await property_columns put")?;
            let reverse_put =
                store.put_with_key(&JsValue::from_str(&map_key), &JsValue::from_str(&field_key)).require("put property field owner")?;
            cb_future(&reverse_put, "success", "error").await.require("await property field owner put")?;
            cb_future(&transaction, "complete", "error").await.require("complete property_columns transaction")?;
            Ok(PropertyFieldClaim::Stored(proposed))
        })
        .await
    }

    /// Extract property values from canonical entity state and
    /// place them on the private materialization object.
    async fn extract_fields(&self, entity_obj: &Object, entity_state: &EntityState) -> Result<(), MutationError> {
        use ankurah_core::property::backend::backend_from_string;
        use std::collections::HashSet;

        let mut seen_fields = HashSet::new();

        // Process all property values from state buffers
        for (backend_name, state_buffer) in entity_state.state.state_buffers.iter() {
            let backend = backend_from_string(backend_name, Some(state_buffer)).map_err(|e| MutationError::General(Box::new(e)))?;

            for (property_id, value) in backend.property_values() {
                // This object is prepared before the engine opens its publication transaction.
                let field_name = self.column_for_key(&property_id).await?;
                // First occurrence wins on same-field collisions (a cross-backend
                // duplicate). Same-materialization identity collisions can't reach
                // here: assignment deduped them to distinct fields (EntityId) or
                // refused the state outright (System), so no property field ever
                // lands on a reserved record field or another identity's field.
                if !seen_fields.insert(field_name.clone()) {
                    continue;
                }

                // Set field directly on entity object (no prefix - they become the primary fields)
                // Use IdbValue encoding to ensure fields are IndexedDB-key-compatible (bool as 0/1, etc.)
                let js_value = match value {
                    Some(ref prop_value) => crate::idb_value::IdbValue::from(prop_value).into(),
                    None => JsValue::NULL,
                };
                entity_obj.set(&field_name, js_value)?;
            }
        }

        Ok(())
    }

    async fn execute_plan_query(
        &self,
        index: &web_sys::IdbIndex,
        entities: &web_sys::IdbObjectStore,
        membership_store: &web_sys::IdbObjectStore,
        key_range: Option<web_sys::IdbKeyRange>,
        predicate: &ankql::ast::Predicate<EngineColumns>,
        cursor_direction: web_sys::IdbCursorDirection,
        limit: Option<u64>,
        upper_open_ended: bool,
        eq_prefix_len: usize,
        eq_prefix_values: Vec<ankurah_core::value::Value>,
        order_by_spill: &OrderByComponents,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let needs_spill_sort = !order_by_spill.spill.is_empty();
        let needs_memberships = !predicate.referenced_models().is_empty();

        // Determine effective prefix guard config (can be disabled in debug builds for testing)
        #[cfg(debug_assertions)]
        let effective_prefix_len =
            if upper_open_ended && eq_prefix_len > 0 && !self.prefix_guard_disabled.load(std::sync::atomic::Ordering::Relaxed) {
                eq_prefix_len
            } else {
                0
            };
        #[cfg(not(debug_assertions))]
        let effective_prefix_len = if upper_open_ended && eq_prefix_len > 0 { eq_prefix_len } else { 0 };

        // Use IdbIndexScanner for cursor iteration with prefix guard
        let scanner =
            crate::scanner::IdbIndexScanner::new(index.clone(), key_range, cursor_direction, effective_prefix_len, eq_prefix_values);

        let mut stream = std::pin::pin!(scanner.scan());
        let mut rows: Vec<IdbRecord> = Vec::new();

        while let Some(result) = stream.next().await {
            let entity_obj = result?;

            let id = entity_obj.get(&ID_KEY)?;
            let memberships = if needs_memberships {
                crate::engine::associated_models_in_store(membership_store, id).await?
            } else {
                BTreeSet::new()
            };
            let record = IdbRecord { id, object: entity_obj, memberships };

            // Apply predicate filtering (uses lazy extraction from IdbRecord)
            if evaluate_predicate(&record, predicate).map_err(|e| RetrievalError::storage(format!("Predicate evaluation failed: {}", e)))? {
                rows.push(record);
                if !needs_spill_sort && limit.is_some_and(|limit| rows.len() as u64 >= limit) {
                    break;
                }
            }
        }

        // If we need to sort by spilled columns, use partition-aware sorting
        let selected = if needs_spill_sort {
            // Use ValueSetStream trait methods for partition-aware sorting
            match limit {
                Some(limit_val) => {
                    // Use partition-aware TopK
                    futures::stream::iter(rows).top_k(order_by_spill.clone(), limit_val as usize).collect().await
                }
                None => {
                    // Use partition-aware sort
                    futures::stream::iter(rows).sort_by(order_by_spill.clone()).collect().await
                }
            }
        } else {
            rows
        };
        let mut states = Vec::with_capacity(selected.len());
        for record in selected {
            states.push(crate::engine::load_state_in_store(entities, record.id).await?);
        }
        Ok(states)
    }
}

enum PropertyFieldClaim {
    Stored(String),
    Collision,
}

/// A materialized row, evaluated against physical column paths.
struct IdbRecord {
    id: ankurah_proto::EntityId,
    object: Object,
    memberships: BTreeSet<ModelId>,
}

impl IdbRecord {
    fn field_value(&self, field: &str) -> Option<ankurah_core::value::Value> {
        let value: crate::idb_value::IdbValue = self.object.get_opt(&field.into()).ok()??;
        Some(value.into_value())
    }
}

impl ankurah_core::selection::filter::ValueLookup<EngineColumns> for IdbRecord {
    fn is_member_of(&self, model: &ModelId) -> Result<bool, ankurah_core::selection::filter::Error> {
        Ok(self.memberships.contains(model))
    }

    fn value_at(&self, path: &ColumnPath) -> Option<ankurah_core::value::Value> {
        let value = if path.column == "id" { ankurah_core::value::Value::EntityId(self.id) } else { self.field_value(&path.column)? };
        if path.subpath.is_empty() {
            Some(value)
        } else {
            value.extract_at_path(&path.subpath)
        }
    }
}

impl ankurah_storage_common::filtering::HasEntityId for IdbRecord {
    fn entity_id(&self) -> ankurah_proto::EntityId { self.id }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::EntityId;

    #[test]
    fn corrupt_property_column_rows_are_rejected() {
        let model = ModelId::EntityId(EntityId::from_bytes([4; EntityId::BYTE_LEN]));
        let prefix = property_columns_prefix(&model);
        let error = decode_property_column_row(&prefix, &format!("{prefix}not-json"), "field".to_owned()).unwrap_err();
        assert!(error.to_string().contains("invalid PropertyId suffix"), "{error}");
    }
}
