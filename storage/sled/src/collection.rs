#[cfg(debug_assertions)]
use std::sync::atomic::AtomicBool;

use ankql::ast::Predicate;
use ankurah_core::indexing::KeySpec;
use ankurah_core::{
    entity::TemporaryEntity,
    error::StorageError,
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
    #[cfg(debug_assertions)]
    pub prefix_guard_disabled: Arc<AtomicBool>,
}

pub struct SledStorageCollection(SledStorageCollectionInner);

impl SledStorageCollection {
    pub fn new(
        collection_id: CollectionId,
        database: Arc<Database>,
        tree: sled::Tree,
        #[cfg(debug_assertions)] prefix_guard_disabled: Arc<AtomicBool>,
    ) -> Self {
        Self(SledStorageCollectionInner {
            collection_id,
            database,
            tree,
            #[cfg(debug_assertions)]
            prefix_guard_disabled,
        })
    }
}

#[async_trait]
impl StorageCollection for SledStorageCollection {
    // stub functions in the trait impl should all call out to their blocking counterparts
    // in order to keep this tidy
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, StorageError> {
        let inner = self.0.clone();
        // Use spawn_blocking since sled operations are not async
        Ok(task::spawn_blocking(move || inner.set_state_blocking(state)).await??)
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, StorageError> {
        let inner = self.0.clone();
        Ok(task::spawn_blocking(move || inner.get_state_blocking(id)).await??)
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, StorageError> {
        let inner = self.0.clone();
        let selection = selection.clone();
        Ok(task::spawn_blocking(move || inner.fetch_states_blocking(selection)).await??)
    }

    async fn add_event(&self, event: &Attested<Event>) -> Result<bool, StorageError> {
        let inner = self.0.clone();
        let event = event.clone();
        Ok(task::spawn_blocking(move || inner.add_event_blocking(&event)).await??)
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, StorageError> {
        let inner = self.0.clone();
        Ok(task::spawn_blocking(move || inner.get_events_blocking(event_ids)).await??)
    }

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, StorageError> {
        let inner = self.0.clone();
        Ok(task::spawn_blocking(move || inner.dump_entity_events_blocking(entity_id)).await??)
    }
}

impl SledStorageCollectionInner {
    // I think this one is done - did it myself
    fn set_state_blocking(&self, state: Attested<EntityState>) -> Result<bool, StorageError> {
        let (entity_id, collection, sfrag) = state.to_parts();
        if self.collection_id != collection {
            return Err(StorageError::BackendError(anyhow::anyhow!("Collection ID mismatch").into()));
        }

        let binary_state = bincode::serialize(&sfrag)?;
        let id_bytes = entity_id.to_bytes();
        // 1) Write canonical state
        let last = self.database.entities_tree.insert(id_bytes, binary_state.clone()).map_err(sled_error)?;
        let changed = if let Some(last_bytes) = last { last_bytes != binary_state } else { true };

        // 2) Write-time materialization into collection_{collection}
        let entity = TemporaryEntity::new(entity_id, collection, &sfrag.state)?;

        // Compact property IDs and materialized list
        let mut mat: Vec<(u32, ankurah_core::value::Value)> = Vec::new();
        for (name, opt_val) in entity.values().into_iter() {
            if let Some(val) = opt_val {
                mat.push((self.database.property_manager.get_property_id(&name)?, val));
            }
        }

        let mat_bytes = bincode::serialize(&mat)?;
        let old_mat_bytes = self.tree.insert(entity_id.to_bytes(), mat_bytes).map_err(|e| StorageError::BackendError(Box::new(e)))?;
        let old_mat = match old_mat_bytes {
            Some(ivec) => Some(bincode::deserialize::<Vec<(u32, ankurah_core::value::Value)>>(&ivec)?),
            None => None,
        };

        // 2c) Update indexes for this collection based on old/new mats
        self.database.index_manager.update_indexes_for_entity(self.collection_id.as_str(), &entity_id, old_mat.as_deref(), &mat)?;

        Ok(changed)
    }
    // I think this one is done - did it myself
    fn get_state_blocking(&self, id: EntityId) -> Result<Attested<EntityState>, StorageError> {
        match self.database.entities_tree.get(id.to_bytes()).map_err(sled_error)? {
            Some(ivec) => {
                let sfrag: StateFragment = bincode::deserialize(ivec.as_ref())?;
                let es = Attested::<EntityState>::from_parts(id, self.collection_id.clone(), sfrag);
                Ok(es)
            }
            None => Err(StorageError::EntityNotFound(id)),
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

    fn fetch_states_blocking(&self, selection: ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, StorageError> {
        // Type resolution (Literal -> Json for non-simple paths) is handled by TypeResolver
        // at the entry points (Context/Node). The selection here is already type-resolved.

        // Generate query plans and choose the first non-empty one
        let plans = Planner::new(PlannerConfig::full_support()).plan(&selection, "id");

        let plan = plans.into_iter().next().ok_or_else(|| StorageError::BackendError("No plan generated".into()))?;

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
        index_spec: KeySpec,
        bounds: KeyBounds,
        scan_direction: ScanDirection,
        remaining_predicate: Predicate,
        order_by_spill: OrderByComponents,
        limit: Option<u64>,
    ) -> Result<Vec<Attested<EntityState>>, StorageError> {
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
            return futures::executor::block_on(
                ids.limit(limit).entities(&self.database.entities_tree, &self.collection_id).collect_states(),
            );
        }

        // Values path: ids → materialized lookup → filter/sort/topk/limit → hydrate → collect
        let e_tree = &self.database.entities_tree;
        let needs_spill = !order_by_spill.spill.is_empty();
        let mats = SledCollectionLookup::new(&self.tree, &self.database.property_manager, ids);

        match remaining_predicate {
            Predicate::True => {
                match (needs_spill, limit) {
                    // order by + limit: use partition-aware TopK
                    (true, Some(limit)) => futures::executor::block_on(
                        mats.top_k(order_by_spill, limit as usize).entities(e_tree, &self.collection_id).collect_states(),
                    ),
                    // order by only: use partition-aware sort
                    (true, None) => {
                        futures::executor::block_on(mats.sort_by(order_by_spill).entities(e_tree, &self.collection_id).collect_states())
                    }
                    // limit only
                    (false, limit) => futures::executor::block_on(mats.limit(limit).entities(e_tree, &self.collection_id).collect_states()),
                }
            }
            _ => {
                let filtered = mats.filter_predicate(&remaining_predicate);
                match (needs_spill, limit) {
                    // filter + order by + limit: use partition-aware TopK
                    (true, Some(limit)) => futures::executor::block_on(
                        filtered.top_k(order_by_spill, limit as usize).entities(e_tree, &self.collection_id).collect_states(),
                    ),
                    // filter + order by: use partition-aware sort
                    (true, None) => {
                        futures::executor::block_on(filtered.sort_by(order_by_spill).entities(e_tree, &self.collection_id).collect_states())
                    }
                    // filter + limit
                    (false, limit) => {
                        futures::executor::block_on(filtered.limit(limit).entities(e_tree, &self.collection_id).collect_states())
                    }
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
    ) -> Result<Vec<Attested<EntityState>>, StorageError> {
        if remaining_predicate == Predicate::True && order_by_spill.is_satisfied() {
            let ids = SledCollectionKeyScanner::new(&self.tree, &bounds, scan_direction)?;
            let states = SledEntityLookup::new(&self.database.entities_tree, &self.collection_id, ids.limit(limit));
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
                    (true, Some(limit)) => futures::executor::block_on(
                        scanner.top_k(order_by_spill, limit as usize).entities(e_tree, &self.collection_id).collect_states(),
                    ),
                    // order by only: use partition-aware sort
                    (true, None) => {
                        futures::executor::block_on(scanner.sort_by(order_by_spill).entities(e_tree, &self.collection_id).collect_states())
                    }
                    // limit only
                    (false, limit) => {
                        futures::executor::block_on(scanner.limit(limit).entities(e_tree, &self.collection_id).collect_states())
                    }
                }
            }
            _ => {
                // Collect items from scanner first, then filter
                let collection_items: Vec<_> = futures::executor::block_on(scanner.collect());
                let filtered = futures::stream::iter(collection_items).filter_predicate(&remaining_predicate);
                match (needs_spill, limit) {
                    // filter + order by + limit: use partition-aware TopK
                    (true, Some(limit)) => futures::executor::block_on(
                        filtered.top_k(order_by_spill, limit as usize).entities(e_tree, &self.collection_id).collect_states(),
                    ),
                    // filter + order by: use partition-aware sort
                    (true, None) => {
                        futures::executor::block_on(filtered.sort_by(order_by_spill).entities(e_tree, &self.collection_id).collect_states())
                    }
                    // filter + limit
                    (false, limit) => {
                        futures::executor::block_on(filtered.limit(limit).entities(e_tree, &self.collection_id).collect_states())
                    }
                }
            }
        }
    }

    fn get_events_blocking(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, StorageError> {
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

    fn dump_entity_events_blocking(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, StorageError> {
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

    fn add_event_blocking(&self, event: &Attested<Event>) -> Result<bool, StorageError> {
        let binary_state = bincode::serialize(event)?;

        let last = self
            .database
            .events_tree
            .insert(event.payload.id().as_bytes(), binary_state.clone())
            .map_err(|err| StorageError::BackendError(Box::new(err)))?;

        if let Some(last_bytes) = last {
            Ok(last_bytes != binary_state)
        } else {
            Ok(true)
        }
    }
}
