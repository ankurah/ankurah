#[cfg(debug_assertions)]
use std::sync::atomic::AtomicBool;

use ankql::ast::{OrderKey, Predicate, PropertyId};
use ankurah_core::indexing::KeySpec;
use ankurah_core::{
    error::{MutationError, RetrievalError},
    storage::StorageCollection,
    EntityId,
};
use ankurah_proto::{Attested, CollectionId, EntityState, Event, EventId, StateFragment};
use ankurah_storage_common::{filtering::ValueSetStream, KeyBounds, OrderByComponents, Plan, Planner, PlannerConfig, ScanDirection};
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;

use tokio::task;

use crate::entity::{SledEntityExt, SledEntityExtFromMats, SledEntityLookup};
// TODO: Will need bounds_to_sled_range and normalize when implementing scanner logic
use crate::scan_collection::SledCollectionLookup;
use crate::scan_collection::{SledCollectionKeyScanner, SledCollectionScanner};
use crate::scan_index::SledIndexScanner;
use crate::{
    database::Database,
    error::{sled_error, SledRetrievalError},
};
use ankurah_storage_common::traits::{EntityIdStream, EntityStateStream};

#[derive(Clone)]
pub struct SledStorageCollectionInner {
    pub collection_id: CollectionId,
    pub database: Arc<Database>,
    pub tree: sled::Tree,
    /// The injected catalog resolver (shared with the engine): the name
    /// source for column assignment at materialization time.
    pub(crate) resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn ankurah_core::schema::CatalogResolver>>>>,
    #[cfg(debug_assertions)]
    pub prefix_guard_disabled: Arc<AtomicBool>,
}

pub struct SledStorageCollection(SledStorageCollectionInner);

impl SledStorageCollection {
    pub fn new(
        collection_id: CollectionId,
        database: Arc<Database>,
        tree: sled::Tree,
        resolver: Arc<std::sync::RwLock<Option<std::sync::Weak<dyn ankurah_core::schema::CatalogResolver>>>>,
        #[cfg(debug_assertions)] prefix_guard_disabled: Arc<AtomicBool>,
    ) -> Self {
        Self(SledStorageCollectionInner {
            collection_id,
            database,
            tree,
            resolver,
            #[cfg(debug_assertions)]
            prefix_guard_disabled,
        })
    }
}

#[async_trait]
impl StorageCollection for SledStorageCollection {
    // stub functions in the trait impl should all call out to their blocking counterparts
    // in order to keep this tidy
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let inner = self.0.clone();
        // Use spawn_blocking since sled operations are not async
        Ok(task::spawn_blocking(move || inner.set_state_blocking(state)).await??)
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let inner = self.0.clone();
        Ok(task::spawn_blocking(move || inner.get_state_blocking(id)).await??)
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        // This engine can only address a property by identity. Refuse a
        // selection that still carries a name, before spawning any work.
        selection.check()?;
        let inner = self.0.clone();
        let selection = selection.clone();
        Ok(task::spawn_blocking(move || inner.fetch_states_blocking(selection)).await??)
    }

    async fn add_event(&self, event: &Attested<Event>) -> Result<bool, MutationError> {
        let inner = self.0.clone();
        let event = event.clone();
        Ok(task::spawn_blocking(move || inner.add_event_blocking(&event)).await??)
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let inner = self.0.clone();
        Ok(task::spawn_blocking(move || inner.get_events_blocking(event_ids)).await??)
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let inner = self.0.clone();
        Ok(task::spawn_blocking(move || inner.dump_entity_events_blocking(entity_id)).await??)
    }
}

impl SledStorageCollectionInner {
    /// The model id written on envelopes this bucket reconstructs (#330):
    /// well-knowns, then the injected catalog resolver.
    fn model_id(&self) -> Result<EntityId, ankurah_core::error::RetrievalError> {
        let resolver = self.resolver.read().unwrap().as_ref().and_then(|weak| weak.upgrade());
        ankurah_core::storage::bucket_model_id(&self.collection_id, resolver.as_deref())
    }

    /// [`Self::model_id`] as a clonable outcome for the scan streams, which
    /// check it only when a row actually hydrates: a scan that matches
    /// nothing must not fail for want of a model id (cold catalog, e.g. the
    /// ephemeral known_matches pre-fetch on a never-stored collection).
    fn model_id_lazy(&self) -> Result<EntityId, String> { self.model_id().map_err(|e| e.to_string()) }

    // I think this one is done - did it myself
    fn set_state_blocking(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        // The envelope carries a model id (#330); the node resolved it to this
        // bucket before routing, so there is no name to cross-check here.
        let (entity_id, _model, sfrag) = state.to_parts();

        let binary_state = bincode::serialize(&sfrag)?;
        let id_bytes = entity_id.to_bytes();
        // 1) Write canonical state
        let last = self.database.entities_tree.insert(id_bytes, binary_state.clone()).map_err(sled_error)?;
        let changed = if let Some(last_bytes) = last { last_bytes != binary_state } else { true };

        // 2) Write-time materialization into collection_{collection}: each
        // property identity is assigned a physical column in the durable
        // id-to-column map (system ids by their sanitized name, registered ids
        // seeded from the catalog resolver and deduped) -- populating the map so
        // reads can find the column -- and its value is stored under the compact
        // numeric slot keyed by the SAME durable identity.
        let resolver = self.resolver.read().unwrap().as_ref().and_then(|weak| weak.upgrade());
        let mut mat: Vec<(u32, ankurah_core::value::Value)> = Vec::new();
        let mut seen_columns = std::collections::HashSet::new();
        for (backend_name, state_buffer) in sfrag.state.state_buffers.iter() {
            let backend = ankurah_core::property::backend::backend_from_string(backend_name, Some(state_buffer))?;
            for (property_id, opt_val) in backend.property_values() {
                let column =
                    self.database.property_manager.column_for_key(self.collection_id.as_str(), &property_id, resolver.as_deref())?;
                if !seen_columns.insert(column) {
                    // Same column from another backend of this entity: first
                    // occurrence wins (same-collection id collisions were
                    // deduped at column assignment and cannot land here).
                    continue;
                }
                if let Some(val) = opt_val {
                    mat.push((self.database.property_manager.get_property_id(&property_id)?, val));
                }
            }
        }

        let mat_bytes = bincode::serialize(&mat)?;
        let old_mat_bytes = self.tree.insert(entity_id.to_bytes(), mat_bytes).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;
        let old_mat = match old_mat_bytes {
            Some(ivec) => Some(bincode::deserialize::<Vec<(u32, ankurah_core::value::Value)>>(&ivec)?),
            None => None,
        };

        // 2c) Update indexes for this collection based on old/new mats
        self.database.index_manager.update_indexes_for_entity(self.collection_id.as_str(), &entity_id, old_mat.as_deref(), &mat)?;

        Ok(changed)
    }
    // I think this one is done - did it myself
    fn get_state_blocking(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        match self.database.entities_tree.get(id.to_bytes()).map_err(sled_error)? {
            Some(ivec) => {
                let sfrag: StateFragment = bincode::deserialize(ivec.as_ref())?;
                let es = Attested::<EntityState>::from_parts(id, self.model_id()?, sfrag);
                Ok(es)
            }
            None => Err(RetrievalError::EntityNotFound(id)),
        }
    }
    // this is the one that needs the most work
    // unlike IndexedDB, we are using separate Trees for each materialized collection - which is what we scan over
    // so there will be some times when there is no predicate or range restriction - just a full scan
    // I don't know if this produces zero plans, or a plan with an empty IndexSpec and predicate::True
    // I actually don't have a preference which way the planner goes, but we should understand it either way.
    // TODO: make a test for that in storage/common/planner.rs
    // Whatever way that goes, we need to handle it here.
    // ideally we would DRY a bit between exec_index_plan and exec_fallback_scan (which we should call full_scan or table_scan I think)
    // the difference being that exec_index_plan iterates over the index iterator, while exec_fallback_scan iterates over the collection iterator
    // They BOTH need to then do a secondary lookup in the entities tree to get the state fragment
    // (and we need to make sure that both are using industry best practices for that sort of index -> record scan)

    fn fetch_states_blocking(&self, selection: ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        // Type resolution (Literal -> Json for non-simple paths) is handled by TypeResolver
        // at the entry points (Context/Node). The selection here is already type-resolved.

        let resolver = self.resolver.read().unwrap().as_ref().and_then(|weak| weak.upgrade());
        let collection = self.collection_id.as_str();
        let manager = &self.database.property_manager;

        // Pre-resolve every referenced identity to its column BEFORE planning.
        // `column_of` returns a Result, so a real storage or decode failure
        // surfaces here (propagated) rather than reading as "property absent".
        // The planner's ColumnOf closure is infallible, so it reads from this
        // pre-resolved map; whatever is missing from the map is a true absence.
        let referenced = selection.referenced_properties();
        let mut resolved_columns: std::collections::HashMap<PropertyId, String> = std::collections::HashMap::new();
        for pid in &referenced {
            if let Some(column) = manager.column_of(collection, pid)? {
                resolved_columns.insert(pid.clone(), column);
            }
        }

        // Read-side absence, by durable identity: a referenced property this
        // engine never materialized (no column in the durable map) is ABSENT --
        // there is NO fallback to a name (the #374 fix). Fold those to NULL and
        // drop any ORDER BY key on one. `PropertyId::Id` is pinned to the "id"
        // column, so it is never absent.
        let absent: Vec<PropertyId> = referenced.into_iter().filter(|pid| !resolved_columns.contains_key(pid)).collect();
        let selection = if absent.is_empty() { selection } else { selection.assume_null(&absent) };

        // Bind each resolved ORDER BY key's canonical value_type (the canonical
        // value_type ruling), keyed by the PHYSICAL column the planner will
        // resolve it to via `column_of`, so ordered index scans collate numerics
        // numerically. A key whose canonical type is unknown (system properties,
        // the primary key) keeps the historical String collation.
        let order_types: std::collections::HashMap<String, ankurah_core::value::ValueType> = selection
            .order_by
            .iter()
            .flatten()
            .filter_map(|item| {
                let OrderKey::Property(pp) = &item.key else { return None };
                let property_id = pp.id();
                let PropertyId::EntityId(ulid) = property_id else { return None };
                let resolver = resolver.as_deref()?;
                let value_type =
                    ankurah_core::value::ValueType::from_property_str(&resolver.canonical_value_type(&EntityId::from_ulid(ulid))?)?;
                let column = resolved_columns.get(&property_id)?.clone();
                Some((column, value_type))
            })
            .collect();

        // The planner's two engine-supplied lookups: `column_of` translates a
        // resolved identity to its physical column through the pre-resolved map (a
        // miss = absent, no name fallback), and `order_type_of` supplies the
        // canonical collation for a physical column.
        let column_of = |pid: &PropertyId| -> Option<String> { resolved_columns.get(pid).cloned() };
        let order_type_of = |column: &str| -> Option<ankurah_core::value::ValueType> { order_types.get(column).copied() };

        // Generate query plans and choose the first non-empty one
        let plans = Planner::new(PlannerConfig::full_support()).plan_with_types(&selection, "id", &order_type_of, &column_of);

        let plan = plans.into_iter().next().ok_or_else(|| RetrievalError::StorageError("No plan generated".into()))?;

        // Execute the chosen plan using streaming pipeline architecture
        match plan {
            Plan::EmptyScan => Ok(Vec::new()),

            Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill } =>
            //
            {
                self.exec_index_scan_plan(index_spec, bounds, scan_direction, remaining_predicate, order_by_spill, selection.limit)
            }

            Plan::TableScan { bounds, scan_direction, remaining_predicate, order_by_spill } => {
                self.exec_table_scan_plan(bounds, scan_direction, remaining_predicate, order_by_spill, selection.limit)
            }
        }
    }
    fn exec_index_scan_plan(
        &self,
        index_spec: KeySpec<String>,
        bounds: KeyBounds,
        scan_direction: ScanDirection,
        remaining_predicate: Predicate,
        order_by_spill: OrderByComponents,
        limit: Option<u64>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let model = self.model_id_lazy();
        // Debug flag for disabling equality-prefix guard (testing only)
        let prefix_guard_disabled = {
            #[cfg(debug_assertions)]
            {
                use std::sync::atomic::Ordering;
                self.prefix_guard_disabled.load(Ordering::Relaxed)
            }
            #[cfg(not(debug_assertions))]
            false
        };

        let (index, match_type) = self.database.index_manager.assure_index_exists(
            self.collection_id.as_str(),
            &index_spec,
            &self.database.db,
            &self.database.property_manager,
        )?;

        let ids = SledIndexScanner::new(&index, &bounds, scan_direction, match_type, prefix_guard_disabled)?;

        if remaining_predicate == Predicate::True && order_by_spill.is_satisfied() {
            return futures::executor::block_on(ids.limit(limit).entities(&self.database.entities_tree, model).collect_states());
        }

        // Values path: ids → materialized lookup → filter/sort/topk/limit → hydrate → collect
        let e_tree = &self.database.entities_tree;
        let needs_spill = !order_by_spill.spill.is_empty();
        let mats = SledCollectionLookup::new(&self.tree, &self.database.property_manager, ids);

        match remaining_predicate {
            Predicate::True => {
                match (needs_spill, limit) {
                    // order by + limit: use partition-aware TopK
                    (true, Some(limit)) => {
                        futures::executor::block_on(mats.top_k(order_by_spill, limit as usize).entities(e_tree, model).collect_states())
                    }
                    // order by only: use partition-aware sort
                    (true, None) => futures::executor::block_on(mats.sort_by(order_by_spill).entities(e_tree, model).collect_states()),
                    // limit only
                    (false, limit) => futures::executor::block_on(mats.limit(limit).entities(e_tree, model).collect_states()),
                }
            }
            _ => {
                let filtered = mats.filter_predicate(&remaining_predicate);
                match (needs_spill, limit) {
                    // filter + order by + limit: use partition-aware TopK
                    (true, Some(limit)) => {
                        futures::executor::block_on(filtered.top_k(order_by_spill, limit as usize).entities(e_tree, model).collect_states())
                    }
                    // filter + order by: use partition-aware sort
                    (true, None) => futures::executor::block_on(filtered.sort_by(order_by_spill).entities(e_tree, model).collect_states()),
                    // filter + limit
                    (false, limit) => futures::executor::block_on(filtered.limit(limit).entities(e_tree, model).collect_states()),
                }
            }
        }
    }
    fn exec_table_scan_plan(
        &self,
        bounds: KeyBounds,
        scan_direction: ScanDirection,
        remaining_predicate: Predicate,
        order_by_spill: OrderByComponents,
        limit: Option<u64>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let model = self.model_id_lazy();
        if remaining_predicate == Predicate::True && order_by_spill.is_satisfied() {
            let ids = SledCollectionKeyScanner::new(&self.tree, &bounds, scan_direction)?;
            let states = SledEntityLookup::new(&self.database.entities_tree, model, ids.limit(limit));
            return futures::executor::block_on(states.collect_states());
        }

        // Values path: kvs → decode mats → filter/sort/topk/limit → hydrate → collect
        let e_tree = &self.database.entities_tree;
        let needs_spill = !order_by_spill.spill.is_empty();
        let scanner = SledCollectionScanner::new(&self.tree, &bounds, scan_direction, &self.database.property_manager)?;

        match remaining_predicate {
            Predicate::True => {
                match (needs_spill, limit) {
                    // order by + limit: use partition-aware TopK
                    (true, Some(limit)) => {
                        futures::executor::block_on(scanner.top_k(order_by_spill, limit as usize).entities(e_tree, model).collect_states())
                    }
                    // order by only: use partition-aware sort
                    (true, None) => futures::executor::block_on(scanner.sort_by(order_by_spill).entities(e_tree, model).collect_states()),
                    // limit only
                    (false, limit) => futures::executor::block_on(scanner.limit(limit).entities(e_tree, model).collect_states()),
                }
            }
            _ => {
                // Collect items from scanner first, then filter
                let collection_items: Vec<_> = futures::executor::block_on(scanner.collect());
                let filtered = futures::stream::iter(collection_items).filter_predicate(&remaining_predicate);
                match (needs_spill, limit) {
                    // filter + order by + limit: use partition-aware TopK
                    (true, Some(limit)) => {
                        futures::executor::block_on(filtered.top_k(order_by_spill, limit as usize).entities(e_tree, model).collect_states())
                    }
                    // filter + order by: use partition-aware sort
                    (true, None) => futures::executor::block_on(filtered.sort_by(order_by_spill).entities(e_tree, model).collect_states()),
                    // filter + limit
                    (false, limit) => futures::executor::block_on(filtered.limit(limit).entities(e_tree, model).collect_states()),
                }
            }
        }
    }

    fn get_events_blocking(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let mut events = Vec::new();
        for event_id in event_ids {
            match self.database.events_tree.get(event_id.as_bytes()).map_err(SledRetrievalError::StorageError)? {
                Some(event) => {
                    let event: Attested<Event> = bincode::deserialize(&event)?;
                    events.push(event);
                }
                None => continue,
            }
        }
        Ok(events)
    }

    fn dump_entity_events_blocking(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let mut events = Vec::new();

        // TODO: this is a full table scan. If we actually need this for more than just tests, we should index the events by entity_id
        for event_data in self.database.events_tree.iter() {
            let (_key, data) = event_data.map_err(SledRetrievalError::StorageError)?;
            let event: Attested<Event> = bincode::deserialize(&data)?;
            if event.payload.entity_id == entity_id {
                events.push(event);
            }
        }

        Ok(events)
    }

    fn add_event_blocking(&self, event: &Attested<Event>) -> Result<bool, MutationError> {
        let binary_state = bincode::serialize(event)?;

        let last = self
            .database
            .events_tree
            .insert(event.payload.id().as_bytes(), binary_state.clone())
            .map_err(|err| MutationError::UpdateFailed(Box::new(err)))?;

        if let Some(last_bytes) = last {
            Ok(last_bytes != binary_state)
        } else {
            Ok(true)
        }
    }
}
