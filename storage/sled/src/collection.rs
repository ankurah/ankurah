#[cfg(debug_assertions)]
use std::sync::{atomic::AtomicBool, Arc};

use ankql::ast::{OrderByItem, Predicate};
use ankurah_core::indexing::KeySpec;
use ankurah_core::value::cast_predicate::cast_predicate_types;
use ankurah_core::{
    entity::TemporaryEntity,
    error::{MutationError, RetrievalError},
    schema::CollectionSchema,
    storage::StorageCollection,
    EntityId,
};
use ankurah_proto::{Attested, CollectionId, EntityState, Event, EventId, StateFragment};
use ankurah_storage_common::{filtering::ValueSetStream, KeyBounds, Plan, Planner, PlannerConfig, ScanDirection};
use async_trait::async_trait;

use tokio::task;

use crate::entity::{SledEntityExt, SledEntityExtFromMats, SledEntityLookup};
// TODO: Will need bounds_to_sled_range and normalize when implementing scanner logic
use crate::scan_collection::SledMaterializeIter;
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

impl CollectionSchema for SledStorageCollectionInner {
    fn field_type(
        &self,
        identifier: &ankql::ast::Identifier,
    ) -> Result<ankurah_core::value::ValueType, ankurah_core::property::PropertyError> {
        use ankql::ast::Identifier;
        use ankurah_core::value::ValueType;

        match identifier {
            Identifier::Property(name) => {
                match name.as_str() {
                    "id" => Ok(ValueType::EntityId),
                    // TODO: Add proper schema-based type resolution here
                    // For now, we'll default to String for unknown fields
                    _ => Ok(ValueType::String),
                }
            }
            Identifier::CollectionProperty(_collection, property) => {
                match property.as_str() {
                    "id" => Ok(ValueType::EntityId),
                    // TODO: Add proper schema-based type resolution here
                    _ => Ok(ValueType::String),
                }
            }
        }
    }
}

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
    // I think this one is done - did it myself
    fn set_state_blocking(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let (entity_id, collection, sfrag) = state.to_parts();
        if self.collection_id != collection {
            return Err(MutationError::General(anyhow::anyhow!("Collection ID mismatch").into()));
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
                let es = Attested::<EntityState>::from_parts(id, self.collection_id.clone(), sfrag);
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
        // Cast all literals in the selection to their correct types based on field names
        let cast_predicate = cast_predicate_types(selection.predicate, self)?;
        let selection = ankql::ast::Selection { predicate: cast_predicate, order_by: selection.order_by, limit: selection.limit };

        // Generate query plans and choose the first non-empty one
        let plans = Planner::new(PlannerConfig::full_support()).plan(&selection, "id");

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
        index_spec: KeySpec,
        bounds: KeyBounds,
        scan_direction: ScanDirection,
        remaining_predicate: Predicate,
        order_by_spill: Vec<OrderByItem>,
        limit: Option<u64>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
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

        if remaining_predicate == Predicate::True && order_by_spill.is_empty() {
            return ids.limit(limit).entities(&self.database.entities_tree, &self.collection_id).collect_states();
        }

        // Values path: ids → materialized lookup → filter/sort/topk/limit → hydrate → collect
        let e_tree = &self.database.entities_tree;
        let sort: Option<Vec<OrderByItem>> = if order_by_spill.is_empty() { None } else { Some(order_by_spill) };
        let mats = SledMaterializeIter::new(&self.tree, &self.database.property_manager, ids);

        match remaining_predicate {
            Predicate::True => {
                match (sort, limit) {
                    // order by + limit
                    (Some(sort), Some(limit)) => {
                        mats.top_k(&sort, limit as usize) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
                    }
                    // order by only
                    (Some(sort), None) => {
                        mats.sort_by(&sort) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
                    }
                    // limit only
                    (None, limit) => {
                        mats.limit(limit) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
                    }
                }
            }
            _ => {
                let filtered = mats.filter_predicate(&remaining_predicate);
                match (sort, limit) {
                    // filter + order by + limit
                    (Some(sort), Some(limit)) => {
                        filtered
                            .top_k(&sort, limit as usize) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
                    }
                    // filter + order by
                    (Some(sort), None) => {
                        filtered
                            .sort_by(&sort) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
                    }
                    // filter + limit
                    (None, limit) => {
                        filtered
                            .limit(limit) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
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
        order_by_spill: Vec<OrderByItem>,
        limit: Option<u64>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        if remaining_predicate == Predicate::True && order_by_spill.is_empty() {
            let ids = SledCollectionKeyScanner::new(&self.tree, &bounds, scan_direction)?;
            let states = SledEntityLookup::new(&self.database.entities_tree, &self.collection_id, ids.limit(limit));
            return states.collect_states();
        }

        // Values path: kvs → decode mats → filter/sort/topk/limit → hydrate → collect
        let e_tree = &self.database.entities_tree;
        let sort: Option<Vec<OrderByItem>> = if order_by_spill.is_empty() { None } else { Some(order_by_spill) };
        let scanner = SledCollectionScanner::new(&self.tree, &bounds, scan_direction, &self.database.property_manager)?;

        match remaining_predicate {
            Predicate::True => {
                match (sort, limit) {
                    // order by + limit
                    (Some(sort), Some(limit)) => {
                        scanner
                            .top_k(&sort, limit as usize) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
                    }
                    // order by only
                    (Some(sort), None) => scanner
                        .sort_by(&sort) //
                        .entities(e_tree, &self.collection_id)
                        .collect_states(),
                    // limit only
                    (None, limit) => scanner
                        .limit(limit) //
                        .entities(e_tree, &self.collection_id)
                        .collect_states(),
                }
            }
            _ => {
                let collection_items: Vec<_> = scanner.collect();
                println!("collection_items: {:?}", collection_items);
                let filtered = collection_items.into_iter().filter_predicate(&remaining_predicate);
                match (sort, limit) {
                    // filter + order by + limit
                    (Some(sort), Some(limit)) => {
                        filtered
                            .top_k(&sort, limit as usize) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
                    }
                    // filter + order by
                    (Some(sort), None) => {
                        filtered
                            .sort_by(&sort) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
                    }
                    // filter + limit
                    (None, limit) => {
                        filtered
                            .limit(limit) //
                            .entities(e_tree, &self.collection_id)
                            .collect_states()
                    }
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
