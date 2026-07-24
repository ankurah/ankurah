use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use ankql::ast::PropertyId;
use ankurah_core::{
    error::{MutationError, RetrievalError},
    schema::CatalogResolver,
    selection::filter::{evaluate_predicate, Filterable},
    storage::naming,
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
    pub(crate) mutex: tokio::sync::Mutex<()>, // should probably be implemented by Database, but not certain
    pub(crate) invocation_count: AtomicUsize,
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
    /// `property_columns` from the store yet (lazy, once per bucket).
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
        self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _lock = self.mutex.lock().await;
        let resolver = self
            .resolver
            .read()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| MutationError::General(format!("catalog resolver is unavailable for model {}", self.model_id).into()))?;
        let projected: std::collections::HashSet<PropertyId> = resolver
            .model_properties(&self.model_id)
            .map_err(|error| MutationError::General(error.to_string().into()))?
            .into_iter()
            .collect();

        // Assign + persist durable field names for this model's projected
        // properties BEFORE opening the materializations transaction. This does every
        // `property_columns` write up front on its own transaction, so the
        // materialization transaction never awaits a foreign transaction (IndexedDB
        // auto-commits a transaction the moment an await doesn't belong to it),
        // and it warms the cache so `extract_projected_fields` only ever hits it.
        self.assign_property_fields(&projected).await?;

        let materialized = Object::new(js_sys::Object::new().into());
        materialized.set(&*ID_KEY, state.payload.entity_id.to_base64())?;
        materialized.set(&*MATERIALIZATION_KEY, self.materialization_name.as_str())?;
        self.extract_projected_fields(&materialized, &state.payload, &projected).await?;

        Ok(PreparedIndexedDbMaterialization {
            key: materialization_entity_key(&self.materialization_name, state.payload.entity_id),
            object: materialized,
        })
    }

    /// Query this model's projection and hydrate matching canonical states.
    pub(crate) async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        // This engine can only address a property by identity. Refuse a
        // selection that still carries a name, before taking the lock.
        selection.check()?;
        let _invocation = self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _lock = self.mutex.lock().await; // TODO why are we locking here?

        // Resolve, through this engine's own durable map, the field for every
        // property the selection references (assigned on write, sticky under
        // rename). There is NO name fallback: a property with no assigned field
        // is ABSENT (evaluates NULL, folded below), never re-derived from a raw
        // name (the #374 fix). The planner then translates each surviving
        // identity to its field at build time via `column_of`, and the in-memory
        // post-filter/sort keep reading by identity, so a rename is harmless end
        // to end.
        self.ensure_property_columns_loaded().await.map_err(|e| RetrievalError::StorageError(format!("{e:?}").into()))?;
        let resolver = {
            let resolver = self.resolver.read().expect("RwLock poisoned");
            match resolver.as_ref() {
                None => None,
                Some(weak) => Some(
                    weak.upgrade()
                        .ok_or_else(|| RetrievalError::Other(format!("catalog resolver is unavailable for model {}", self.model_id)))?,
                ),
            }
        };
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
        let referenced = selection.referenced_properties();
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|pid| column_of(pid).is_none()).collect();
        let selection = if absent.is_empty() { selection.clone() } else { selection.assume_null(&absent)? };

        // Bind every surviving resolved property to its registered type, keyed
        // by the private JSON field. The planner re-casts predicate bounds at
        // execution and uses the same map for ORDER BY collation.
        let mut physical_types = BTreeMap::new();
        for property_id in selection.referenced_properties() {
            let Some(column) = column_of(&property_id) else { continue };
            let value_type = match property_id {
                PropertyId::Id => Some(ankurah_core::value::ValueType::EntityId),
                _ => resolver
                    .as_deref()
                    .map(|resolver| resolver.property_value_type(&property_id))
                    .transpose()
                    .map_err(|error| RetrievalError::Other(error.to_string()))?,
            };
            if let Some(value_type) = value_type {
                physical_types.insert(column, value_type);
            }
        }

        // Step 1: Scope the plan to this model's physical materialization.
        let amended_selection = add_materialization(&selection, &self.materialization_name);

        // Step 2: Use planner to generate query plans. ORDER BY key parts collate
        // in each property's canonical type (keyed by physical field), both for
        // boundary casting and collation. `column_of` resolves each identity the
        // planner touches to its field (no name fallback), so a rename never
        // moves the field a plan addresses.
        let physical_type_of = |column: &str| -> Option<ankurah_core::value::ValueType> { physical_types.get(column).copied() };
        let planner = ankurah_storage_common::planner::Planner::new(ankurah_storage_common::planner::PlannerConfig::indexeddb());
        let plans = planner.plan_with_types(&amended_selection, "id", &physical_type_of, &column_of);

        // Step 3: Pick the first plan (always)
        let plan = plans.first().ok_or_else(|| RetrievalError::StorageError("No plan generated".into()))?;

        let ids = match plan {
            Plan::EmptyScan => Vec::new(),
            Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill } => {
                // Step 4: Ensure index exists using plan's IndexSpec
                self.db
                    .assure_index_exists(index_spec)
                    .await
                    .map_err(|e| RetrievalError::StorageError(format!("ensure index exists: {}", e).into()))?;

                // Step 6: Execute the query using the plan
                let db_connection = self.db.get_connection().await;
                let materialization_name = self.materialization_name.clone();
                let limit = selection.limit;
                // The durable identity-to-field snapshot the post-filter/sort use
                // to read each referenced property by resolved identity.
                let columns = assigned.clone();

                SendWrapper::new(async move {
                    let transaction = db_connection.transaction_with_str("materializations").require("create transaction")?;
                    let store = transaction.object_store("materializations").require("get materializations store")?;

                    // Get the index specified by the plan
                    let index = store.index(&index_spec.name_with("", "__")).require("get index")?;

                    // Convert plan bounds to IndexedDB key range using new pipeline
                    let (key_range, upper_open_ended, eq_prefix_len, eq_prefix_values) =
                        crate::planner_integration::plan_bounds_to_idb_range(bounds, scan_direction)
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
                            &materialization_name,
                            upper_open_ended,
                            eq_prefix_len,
                            eq_prefix_values,
                            &order_by_spill,
                            columns,
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
        self.load_states(&ids).await
    }
}

// We always use index cursors for fetch operations
// Store cursors are only used for direct ID lookups (get_state, not fetch_states)

/// Execute queries using index cursors (we always use indexes for fetch operations)
/// Convert IndexDirection to IdbCursorDirection
// pub fn to_idb_cursor_direction(direction: ankurah_core::indexing::IndexDirection) -> web_sys::IdbCursorDirection {
//     match direction {
//         ankurah_core::indexing::IndexDirection::Asc => web_sys::IdbCursorDirection::Next,
//         ankurah_core::indexing::IndexDirection::Desc => web_sys::IdbCursorDirection::Prev,
//     }
// }

impl IndexedDBBucket {
    /// Hydrate `property_columns` with this model's durable
    /// identity-to-field assignments, once per bucket. Runs BEFORE any write
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

    /// Assign + persist a durable field name for every property projected by
    /// this model, hydrating the cache first. Runs BEFORE the materialization
    /// transaction: all of the assignment path's
    /// `property_columns` writes happen here, on their own transaction, so the
    /// entities transaction never awaits a foreign transaction (which would
    /// silently auto-commit it). After this, every [`Self::column_for_key`] call
    /// inside `extract_projected_fields` is a pure cache hit.
    async fn assign_property_fields(&self, projected: &std::collections::HashSet<PropertyId>) -> Result<(), MutationError> {
        self.ensure_property_columns_loaded().await?;
        for property_id in projected {
            self.column_for_key(property_id).await?;
        }
        Ok(())
    }

    /// The durable field name for a property identity in this model.
    ///
    /// Both arms route through the durable map (there is no name short-circuit),
    /// so a system property is addressed by identity on reads exactly like a
    /// registered one. A cache miss assigns a field NOW, mirroring the sqlite
    /// engine:
    /// - a **system** property (`PropertyId::System(SystemProperty)`) has a
    ///   closed, globally unique identity and stable label. It has no entity id
    ///   to suffix-dedupe with, so its sanitized label is the field directly;
    ///   a collision with a reserved record field or a field already assigned
    ///   to a different identity is a hard error rather than silent absorption.
    ///   Re-assignment by the same identity is idempotent (the cache hit above);
    /// - a **registered** property (`PropertyId::EntityId`) seeds from the catalog resolver's
    ///   display name (sanitized), deduped against the reserved fields and this
    ///   model's other assignments (`{name}_{trailing id chars}`, the ratified
    ///   collision rule). A missing label is an error rather than an
    ///   identity-derived placeholder;
    /// - the **`id`** pseudo-property is the primary key, never a stored value, so
    ///   it never reaches field assignment (it is pinned to the `"id"` field in
    ///   the read path's `column_of`).
    ///
    /// PRECONDITION: the cache is already loaded (via
    /// [`Self::ensure_property_columns_loaded`]). A cache HIT returns WITHOUT
    /// awaiting, so it is safe to call while the entities transaction is open;
    /// the miss/assignment path opens the `property_columns` transaction and so
    /// must only be reached before that (see [`Self::assign_property_fields`]).
    async fn column_for_key(&self, property_id: &PropertyId) -> Result<String, MutationError> {
        if let Some(field) = self.property_columns.read().unwrap().get(property_id) {
            return Ok(field.clone());
        }

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
                    let resolver = self.resolver.read().unwrap().as_ref().and_then(std::sync::Weak::upgrade).ok_or_else(|| {
                        MutationError::General(anyhow::anyhow!("catalog resolver is unavailable for property {property_id}").into())
                    })?;
                    let label = resolver.property_name(property_id).map_err(|error| MutationError::General(error.to_string().into()))?;
                    let seed = naming::sanitize(&label);
                    let assigned = self.property_columns.read().unwrap();
                    let is_taken = |candidate: &str| {
                        RESERVED_FIELDS.contains(&candidate)
                            || assigned.iter().any(|(other, name)| other != property_id && name == candidate)
                    };
                    naming::dedupe(&seed, id, is_taken).map_err(|error| MutationError::General(anyhow::anyhow!(error).into()))?
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

    /// Extract this model's projected fields from canonical entity state and
    /// place them on the private materialization object.
    async fn extract_projected_fields(
        &self,
        entity_obj: &Object,
        entity_state: &EntityState,
        projected: &std::collections::HashSet<PropertyId>,
    ) -> Result<(), MutationError> {
        use ankurah_core::property::backend::backend_from_string;
        use std::collections::HashSet;

        let mut seen_fields = HashSet::new();

        // Process all property values from state buffers
        for (backend_name, state_buffer) in entity_state.state.state_buffers.iter() {
            let backend = backend_from_string(backend_name, Some(state_buffer)).map_err(|e| MutationError::General(Box::new(e)))?;

            for (property_id, value) in backend.property_values() {
                if !projected.contains(&property_id) {
                    continue;
                }
                // Resolve the durable field name through the engine-owned map by
                // identity. The cache was warmed by `assign_property_fields`
                // before the entities transaction opened, so this is always a hit
                // here and never opens a transaction that would evict the entities
                // one.
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

    async fn load_states(&self, ids: &[proto::EntityId]) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let mut states = Vec::with_capacity(ids.len());
        for id in ids {
            match crate::engine::load_state(&self.db, *id).await {
                Ok(state) => states.push(state),
                Err(RetrievalError::EntityNotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(states)
    }

    async fn execute_plan_query(
        &self,
        index: &web_sys::IdbIndex,
        key_range: Option<web_sys::IdbKeyRange>,
        predicate: &ankql::ast::Predicate,
        cursor_direction: web_sys::IdbCursorDirection,
        limit: Option<u64>,
        materialization_name: &str,
        upper_open_ended: bool,
        eq_prefix_len: usize,
        eq_prefix_values: Vec<ankurah_core::value::Value>,
        order_by_spill: &OrderByComponents,
        columns: std::sync::Arc<BTreeMap<PropertyId, String>>,
    ) -> Result<Vec<proto::EntityId>, RetrievalError> {
        let needs_spill_sort = !order_by_spill.spill.is_empty();

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
        let mut count = 0u64;
        let mut rows: Vec<IdbRecord> = Vec::new();
        let mut direct_results: Vec<proto::EntityId> = Vec::new();

        while let Some(result) = stream.next().await {
            let entity_obj = result?;

            // Create IdbRecord - wraps JS object with lazy value extraction,
            // carrying the identity-to-field snapshot so it reads by identity.
            // A row returned by this engine's own index must have the record
            // shape the materializer writes. Silently skipping a malformed row
            // turns durable corruption (or a writer/reader shape mismatch)
            // into a false "no matches" result, which is especially dangerous
            // for a database. Surface the decoding error instead.
            let record = IdbRecord::new(entity_obj, materialization_name.to_owned(), columns.clone())?;

            // Apply predicate filtering (uses lazy extraction from IdbRecord)
            if evaluate_predicate(&record, predicate)
                .map_err(|e| RetrievalError::StorageError(format!("Predicate evaluation failed: {}", e).into()))?
            {
                if needs_spill_sort {
                    // Collect for sorting
                    rows.push(record);
                } else {
                    direct_results.push(record.id);
                    count += 1;

                    if let Some(limit_val) = limit {
                        if count >= limit_val {
                            break;
                        }
                    }
                }
            }
        }

        // If we need to sort by spilled columns, use partition-aware sorting
        if needs_spill_sort {
            // Use ValueSetStream trait methods for partition-aware sorting
            let results: Vec<proto::EntityId> = match limit {
                Some(limit_val) => {
                    // Use partition-aware TopK
                    futures::stream::iter(rows).top_k(order_by_spill.clone(), limit_val as usize).map(|record| record.id).collect().await
                }
                None => {
                    // Use partition-aware sort
                    futures::stream::iter(rows).sort_by(order_by_spill.clone()).map(|record| record.id).collect().await
                }
            };
            Ok(results)
        } else {
            Ok(direct_results)
        }
    }
}

enum PropertyFieldClaim {
    Stored(String),
    Collision,
}

/// A record from the IndexedDB materializations store.
///
/// Wraps the raw JS object with lazy extraction for filtering and sorting.
/// Implements `Filterable` and `HasEntityId` for use with stream combinators.
/// The object's fields are keyed by physical field name, so the `Filterable`
/// impl translates each resolved IDENTITY to its field through the durable map
/// snapshot ([`Self::columns`]) -- reading by identity exactly like the entity's
/// own evaluation, with NO fallback to a name (the #374 fix).
struct IdbRecord {
    id: ankurah_proto::EntityId,
    object: Object,
    materialization_name: String,
    /// This model's durable identity-to-field map (snapshot), the sole
    /// translation from a resolved property identity to the physical field the
    /// JS object carries it under.
    columns: std::sync::Arc<BTreeMap<PropertyId, String>>,
}

impl IdbRecord {
    /// Create a new IdbRecord from a JS object
    fn new(
        object: Object,
        materialization_name: String,
        columns: std::sync::Arc<BTreeMap<PropertyId, String>>,
    ) -> Result<Self, RetrievalError> {
        let id: ankurah_proto::EntityId = object.get(&ID_KEY)?;
        Ok(Self { id, object, materialization_name, columns })
    }

    /// Lazily decode the physical field `field` off the JS object, if present.
    fn field_value(&self, field: &str) -> Option<ankurah_core::value::Value> {
        let idb_val: crate::idb_value::IdbValue = self.object.get_opt(&field.into()).ok()??;
        Some(idb_val.into_value())
    }
}

impl Filterable for IdbRecord {
    fn collection(&self) -> &str { self.materialization_name.as_str() }

    /// Read the `id` pseudo-property, a reserved system object field
    /// (`__materialization`, stored verbatim), or a SYSTEM property. `id`
    /// is the record's own EntityId; a system property is addressed through the
    /// durable map by identity (a miss is absent, no name fallback). A registered
    /// property is read by id via [`Filterable::value_by_id`], not here.
    fn value(&self, name: &str) -> Option<ankurah_core::value::Value> {
        if name == "id" {
            return Some(ankurah_core::value::Value::EntityId(self.id));
        }
        if name.starts_with("__") {
            return self.field_value(name);
        }
        let property = ankql::ast::SystemProperty::from_name(name)?;
        let field = self.columns.get(&PropertyId::System(property))?;
        self.field_value(field)
    }

    /// Read a registered property by its stable id -- the identity a resolved
    /// `Expr::PropertyPath` evaluates through. Identity -> field via the
    /// durable map; a miss is absent (NULL), no name fallback.
    fn value_by_id(&self, property_id: ankurah_proto::EntityId) -> Option<ankurah_core::value::Value> {
        let field = self.columns.get(&PropertyId::EntityId(property_id))?;
        self.field_value(field)
    }
}

impl ankurah_storage_common::filtering::HasEntityId for IdbRecord {
    fn entity_id(&self) -> ankurah_proto::EntityId { self.id }
}

/// Amend a selection with the private materialization discriminator.
pub fn add_materialization(selection: &ankql::ast::Selection, materialization_name: &str) -> ankql::ast::Selection {
    use ankql::ast::{ComparisonOperator, Expr, PathExpr, Predicate, Value};

    let materialization_comparison = Predicate::Comparison {
        left: Box::new(Expr::Path(PathExpr::simple("__materialization"))),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::String(materialization_name.to_owned()))),
    };

    ankql::ast::Selection {
        predicate: Predicate::And(Box::new(materialization_comparison), Box::new(selection.predicate.clone())),
        order_by: selection.order_by.clone(),
        limit: selection.limit,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::{OrderByItem, OrderDirection, OrderKey, Predicate, PropertyPath, Selection};
    use ankurah_core::value::ValueType;
    use ankurah_proto::EntityId;

    struct RenamedCatalogResolver {
        property: EntityId,
    }

    #[test]
    fn corrupt_property_column_rows_are_rejected() {
        let model = ModelId::EntityId(EntityId::from_bytes([4; 16]));
        let prefix = property_columns_prefix(&model);
        let error = decode_property_column_row(&prefix, &format!("{prefix}not-json"), "field".to_owned()).unwrap_err();
        assert!(error.to_string().contains("invalid PropertyId suffix"), "{error}");
    }

    impl CatalogResolver for RenamedCatalogResolver {
        fn resolve_model_property(&self, _model: &ModelId, name: &str) -> anyhow::Result<Option<PropertyId>> {
            Ok((name == "score").then_some(PropertyId::EntityId(self.property)))
        }

        fn model_properties(&self, _model: &ModelId) -> anyhow::Result<Vec<PropertyId>> { Ok(vec![PropertyId::EntityId(self.property)]) }

        fn model_name(&self, _model: &ModelId) -> anyhow::Result<String> { Ok("record".to_string()) }

        fn property_name(&self, id: &PropertyId) -> anyhow::Result<String> {
            if *id == PropertyId::EntityId(self.property) {
                Ok("score".to_string())
            } else {
                anyhow::bail!("unknown property {id:?}")
            }
        }

        fn property_value_type(&self, id: &PropertyId) -> anyhow::Result<ValueType> {
            if *id == PropertyId::EntityId(self.property) {
                Ok(ValueType::I32)
            } else {
                anyhow::bail!("unknown property {id:?}")
            }
        }
    }

    /// The durable field an ORDER BY property was assigned is sticky across a
    /// catalog rename and may be collision-suffixed, so it is NOT catalog
    /// resolvable. Binding the canonical value_type by IDENTITY (before touching
    /// the physical field) and keying `order_type_of` by that field must still
    /// give the planner the canonical numeric collation -- never the String
    /// fallback, and never a name-derived field (the #374 fix).
    #[test]
    fn resolved_order_type_survives_sticky_physical_field() {
        let property = EntityId::from_bytes([7; 16]);
        let resolver = RenamedCatalogResolver { property };

        // A resolved ORDER BY on the registered property. Its catalog display
        // name is now "score", but the resolved identity carries only the id (the
        // "score" label is Display-only and never seeds a read).
        let selection = Selection {
            predicate: Predicate::True,
            order_by: Some(vec![OrderByItem {
                key: OrderKey::Property(PropertyPath::registered(property, "score", vec![])),
                direction: OrderDirection::Asc,
            }]),
            limit: None,
        };

        // A field assigned before the catalog display name changed (or a
        // collision-suffixed assignment that was never a catalog name), keyed by
        // durable identity exactly like the engine's own cache.
        let assigned: BTreeMap<PropertyId, String> = BTreeMap::from([(PropertyId::EntityId(property), "legacy_score".to_string())]);
        let column_of = |pid: &PropertyId| -> Option<String> {
            if matches!(pid, PropertyId::Id) {
                return Some("id".to_string());
            }
            assigned.get(pid).cloned()
        };

        // Bind the ORDER BY canonical value_type by identity, keyed by the
        // physical field (mirrors `fetch_states`).
        let order_types: BTreeMap<String, ValueType> = selection
            .order_by
            .iter()
            .flatten()
            .filter_map(|item| {
                let OrderKey::Property(pp) = &item.key else { return None };
                let property_id = pp.id();
                let PropertyId::EntityId(_) = property_id else { return None };
                let value_type = resolver.property_value_type(&property_id).ok()?;
                let column = column_of(&property_id)?;
                Some((column, value_type))
            })
            .collect();

        assert_eq!(order_types.get("legacy_score"), Some(&ValueType::I32));
        assert_eq!(
            resolver.resolve_model_property(&ModelId::System(proto::SystemModel::System), "legacy_score").unwrap(),
            None,
            "the physical field is not catalog-resolvable"
        );

        // Pin the actual planner boundary: the physical field still gets the
        // canonical numeric collation rather than String fallback.
        let amended = add_materialization(&selection, "record");
        let order_type_of = |column: &str| order_types.get(column).copied();
        let planner = ankurah_storage_common::planner::Planner::new(ankurah_storage_common::planner::PlannerConfig::indexeddb());
        let plans = planner.plan_with_types(&amended, "id", &order_type_of, &column_of);
        let index_spec = plans
            .iter()
            .find_map(|plan| match plan {
                Plan::Index { index_spec, .. } => Some(index_spec),
                _ => None,
            })
            .expect("ORDER BY should produce an IndexedDB index plan");
        let score_key = index_spec.keyparts.iter().find(|part| part.key == "legacy_score").expect("physical ORDER BY key");
        assert_eq!(score_key.value_type, ValueType::I32);
    }
}
