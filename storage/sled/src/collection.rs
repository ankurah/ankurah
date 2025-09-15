#[cfg(debug_assertions)]
use std::sync::{atomic::AtomicBool, Arc};

use ankql::{
    ast::{ComparisonOperator, Expr, Identifier, Literal, OrderDirection, Predicate},
    selection::filter::evaluate_predicate,
};
use ankurah_core::{
    entity::TemporaryEntity,
    error::{MutationError, RetrievalError},
    storage::StorageCollection,
    EntityId,
};
use ankurah_proto::{Attested, CollectionId, EntityState, Event, EventId, StateFragment};
use ankurah_storage_common::{Plan, Planner, PlannerConfig};
use async_trait::async_trait;

use tokio::task;

use crate::planner_integration::{bounds_to_sled_range, normalize};
use crate::{
    database::Database,
    error::{sled_error, SledRetrievalError},
};

#[derive(Clone)]
pub struct SledStorageCollection {
    pub collection_id: CollectionId,
    pub database: Arc<Database>,
    pub tree: sled::Tree,
    #[cfg(debug_assertions)]
    pub prefix_guard_disabled: Arc<AtomicBool>,
}

#[async_trait]
impl StorageCollection for SledStorageCollection {
    // stub functions in the trait impl should all call out to their blocking counterparts
    // in order to keep this tidy
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let me = self.clone();
        // Use spawn_blocking since sled operations are not async
        Ok(task::spawn_blocking(move || me.set_state_blocking(state)).await??)
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let me = self.clone();
        Ok(task::spawn_blocking(move || me.get_state_blocking(id)).await??)
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let me = self.clone();
        let selection = selection.clone();
        Ok(task::spawn_blocking(move || me.fetch_states_blocking(selection)).await??)
    }

    async fn add_event(&self, event: &Attested<Event>) -> Result<bool, MutationError> {
        let binary_state = bincode::serialize(event)?;

        // TODO implement self.add_event_blocking
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

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        // TODO implement self.get_events_blocking
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

    async fn dump_entity_events(&self, entity_id: EntityId) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let mut events = Vec::new();

        // TODO implement self.dump_entity_events_blocking
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
}

impl SledStorageCollection {
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
        let mut mat: Vec<(u32, ankurah_core::property::PropertyValue)> = Vec::new();
        for (name, opt_val) in entity.values().into_iter() {
            if let Some(val) = opt_val {
                mat.push((self.database.property_manager.get_property_id(&name)?, val));
            }
        }

        let mat_bytes = bincode::serialize(&mat)?;
        self.tree.insert(entity_id.to_bytes(), mat_bytes).map_err(|e| MutationError::UpdateFailed(Box::new(e)))?;

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
        let plans = Planner::new(PlannerConfig::full_support()).plan(&selection, "id");

        // Choose first non-empty plan; if none, return empty for now (TODO: table scan fallback wiring)
        if let Some(plan) = plans.into_iter().find(|p| !matches!(p, Plan::EmptyScan)) {
            let prefix_guard_disabled = {
                #[cfg(debug_assertions)]
                {
                    use std::sync::atomic::Ordering;
                    self.prefix_guard_disabled.load(Ordering::Relaxed)
                }
                #[cfg(not(debug_assertions))]
                false
            };
            // return self.exec_index_plan(plan, selection.limit, prefix_guard_disabled);

            // Notional design
            // Each engine provides its own concrete types:
            // SledIndexScanner, SledCollectionScanner,SledCollectionLookup, SledStateLookup
            // TiKVIndexScanner, TiKVCollectionLookup, TiKVStateLookup
            let state_stream = match plan {
                Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill: _ } => {
                    let (index, index_match_type) = self.database.index_manager.assure_index_exists(
                        self.collection_id.as_str(),
                        &index_spec,
                        &self.database.db,
                        &self.database.property_manager,
                    )?;
                    let scan = SledIndexScanner::new(&index, index_match_type, &index_spec, &bounds, &scan_direction).scan();

                    if remaining_predicate.is_some() || order_by_spill.is_some() {
                        let materialized_stream = SledCollectionLookup = scan.to_materialized(&self.tree)?;
                        let materialized_stream = if let Some(remaining_predicate) = remaining_predicate {
                            let filtered_stream: <std::iter::Filter<_, _> as Try>::Output =
                                materialized_stream.filter(remaining_predicate)?;
                            materialized_stream = filtered_stream;
                        } else {
                            materialized_stream
                        };
                        let materialized_stream = if let Some(order_by_spill) = order_by_spill {
                            let ordered_stream = materialized_stream.order_by(order_by_spill)?;
                            materialized_stream = ordered_stream;
                        } else {
                            materialized_stream
                        };
                    } else {
                        scan.to_materialized(&self.tree)?
                    }
                    let entity_id_stream = if let Some(remaining_predicate) = remaining_predicate {
                        let filtered_stream: <std::iter::Filter<_, _> as Try>::Output = materialized_stream.filter(remaining_predicate)?;
                        let entity_id_stream = if let Some(order_by_spill) = order_by_spill {
                            let ordered_stream = filtered_stream.order_by(order_by_spill)?;
                            ordered_stream.to_entity_id_stream()
                        } else {
                            filtered_stream.to_entity_id_stream()
                        };
                        entity_id_stream
                    } else {
                        scan.to_entity_id_stream()
                    };

                    let state_stream: SledStateLookup = entity_id_stream.to_state_stream(&self.database.entities_tree)?;
                    state_stream
                }
                Plan::TableScan { bounds, scan_direction, remaining_predicate, order_by_spill: _ } => {
                    let scan = SledCollectionScanner::new(&self.tree, &bounds, &scan_direction).scan();
                    if let Some(remaining_predicate) = remaining_predicate {
                        let filtered_stream: <std::iter::Filter<_, _> as Try>::Output = scan.filter(remaining_predicate)?;
                        scan = filtered_stream;
                    }
                    if let Some(order_by_spill) = order_by_spill {
                        let ordered_stream = scan.order_by(order_by_spill)?;
                        scan = ordered_stream;
                    }
                    let state_stream: SledStateLookup = scan.to_state_stream(&self.database.entities_tree)?;
                    state_stream
                }

                Plan::EmptyScan => Ok(Vec::new()),
            };
            return state_stream.accumulate(selection.limit);
        }
        Ok(Vec::new())
    }
    pub fn exec_index_plan(
        &self,
        plan: Plan,
        limit: Option<u64>,
        prefix_guard_disabled: bool,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        match plan {
            Plan::EmptyScan => Ok(Vec::new()),
            Plan::Index { index_spec, bounds, scan_direction, remaining_predicate, order_by_spill: _ } => {
                // Ensure index exists and is built
                let (index, _match_type) = self.database.index_manager.assure_index_exists(
                    self.collection_id.as_str(),
                    &index_spec,
                    &self.database.db,
                    &self.database.property_manager,
                )?;

                // Normalize and convert bounds to sled range keys
                let (canonical, eq_prefix_len, eq_prefix_values) = normalize(&bounds);
                let (start_full, end_full_opt, upper_open_ended, eq_prefix_bytes) =
                    bounds_to_sled_range(&canonical, eq_prefix_len, &eq_prefix_values);

                let base_iter: Box<dyn Iterator<Item = Result<(sled::IVec, sled::IVec), sled::Error>>> = match &end_full_opt {
                    Some(end_full) => Box::new(index.tree().range(start_full.clone()..end_full.clone())),
                    None => Box::new(index.tree().range(start_full.clone()..)),
                };
                let iter: Box<dyn Iterator<Item = Result<(sled::IVec, sled::IVec), sled::Error>>> = match scan_direction {
                    ankurah_storage_common::ScanDirection::Forward => base_iter,
                    ankurah_storage_common::ScanDirection::Reverse => match &end_full_opt {
                        Some(end_full) => Box::new(index.tree().range(start_full.clone()..end_full.clone()).rev()),
                        None => Box::new(index.tree().range(start_full.clone()..).rev()),
                    },
                };

                let mut results: Vec<Attested<EntityState>> = Vec::new();
                let mut count: u64 = 0;

                let mut use_prefix_guard = upper_open_ended && eq_prefix_len > 0;
                #[cfg(debug_assertions)]
                if prefix_guard_disabled {
                    use_prefix_guard = false;
                }

                for kv in iter {
                    let (k, _v) = kv.map_err(SledRetrievalError::StorageError)?;
                    let key_bytes = k.as_ref();

                    // Equality-prefix guard for open-ended scans
                    if use_prefix_guard {
                        if key_bytes.len() < eq_prefix_bytes.len() || &key_bytes[..eq_prefix_bytes.len()] != eq_prefix_bytes.as_slice() {
                            break;
                        }
                    }

                    let eid = decode_entity_id_from_index_key(key_bytes)?;
                    if let Some(ivec) = self.database.entities_tree.get(eid.to_bytes()).map_err(sled_error)? {
                        let sfrag: StateFragment = bincode::deserialize(ivec.as_ref())?;
                        let es = Attested::<EntityState>::from_parts(eid, self.collection_id.clone(), sfrag);
                        let entity = TemporaryEntity::new(es.payload.entity_id, self.collection_id.clone(), &es.payload.state)?;
                        if evaluate_predicate(&entity, &remaining_predicate)? {
                            results.push(es);
                            if let Some(l) = limit {
                                count += 1;
                                if count >= l {
                                    break;
                                }
                            }
                        }
                    }
                }

                Ok(results)
            }
        }
    }

    pub(crate) fn exec_fallback_scan(
        &self,
        predicate: ankql::ast::Predicate,
        order_by: Option<Vec<ankql::ast::OrderByItem>>,
        limit: Option<u32>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        // TODO: Replace with new pipeline architecture
        // For now, use simple full scan - this will be replaced by the streaming pipeline
        let mut results: Vec<Attested<EntityState>> = Vec::new();
        let iter = self.tree.iter();
        for item in iter {
            let (key_bytes, value_bytes) = item.map_err(SledRetrievalError::StorageError)?;
            let id = EntityId::from_bytes(key_bytes.as_ref().try_into().map_err(RetrievalError::storage)?);
            let sfrag: StateFragment = bincode::deserialize(&value_bytes)?;
            let es = Attested::<EntityState>::from_parts(id, self.collection_id.clone(), sfrag);

            let entity = TemporaryEntity::new(id, self.collection_id.clone(), &es.payload.state)?;
            if evaluate_predicate(&entity, &predicate)? {
                results.push(es);
                // Apply limit during scan for efficiency
                if let Some(limit_val) = limit {
                    if results.len() >= limit_val as usize {
                        break;
                    }
                }
            }
        }

        // TODO: Replace with pipeline sorting logic
        // For now, always sort if ORDER BY is present
        if order_by.is_some() {
            ankurah_storage_common::sorting::apply_order_by_sort(&mut results, &order_by, &self.collection_id)?;
        }
        if let Some(limit_val) = limit {
            results.truncate(limit_val as usize);
        }
        Ok(results)
    }
}

/// Create the appropriate iterator based on the strategy

fn entity_id_successor_bytes(id: &EntityId) -> Option<[u8; 16]> {
    let mut bytes = id.to_bytes();
    for i in (0..bytes.len()).rev() {
        if bytes[i] != 0xFF {
            bytes[i] = bytes[i].saturating_add(1);
            for j in (i + 1)..bytes.len() {
                bytes[j] = 0x00;
            }
            return Some(bytes);
        }
    }
    None
}

fn decode_entity_id_from_index_key(key: &[u8]) -> Result<EntityId, RetrievalError> {
    if key.len() < 1 + 16 {
        return Err(RetrievalError::StorageError("index key too short".into()));
    }
    let eid_bytes: [u8; 16] =
        key[key.len() - 16..].try_into().map_err(|_| RetrievalError::StorageError("invalid entity id suffix".into()))?;
    Ok(EntityId::from_bytes(eid_bytes))
}
