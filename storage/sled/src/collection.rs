#[cfg(debug_assertions)]
use std::sync::{atomic::AtomicBool, Arc};

use ankql::{
    ast::{ComparisonOperator, Expr, Identifier, Literal, OrderDirection, Predicate},
    selection::filter::evaluate_predicate,
};
use ankurah_core::{
    collation::RangeBound,
    entity::TemporaryEntity,
    error::{MutationError, RetrievalError},
    storage::StorageCollection,
    EntityId,
};
use ankurah_proto::{Attested, CollectionId, EntityState, Event, EventId, StateFragment};
use ankurah_storage_common::{Plan, Planner, PlannerConfig};
use async_trait::async_trait;

use tokio::task;

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

        let last = self
            .events
            .insert(event.payload.id().as_bytes(), binary_state.clone())
            .map_err(|err| MutationError::UpdateFailed(Box::new(err)))?;

        if let Some(last_bytes) = last {
            Ok(last_bytes != binary_state)
        } else {
            Ok(true)
        }
    }

    async fn get_events(&self, event_ids: Vec<EventId>) -> Result<Vec<Attested<Event>>, RetrievalError> {
        let mut events = Vec::new();
        for event_id in event_ids {
            match self.events.get(event_id.as_bytes()).map_err(SledRetrievalError::StorageError)? {
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

        // TODO: this is a full table scan. If we actually need this for more than just tests, we should index the events by entity_id
        for event_data in self.events.iter() {
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
    fn set_state_blocking(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let (entity_id, collection, sfrag) = state.to_parts();
        if self.collection_id != collection {
            return Err(MutationError::General(anyhow::anyhow!("Collection ID mismatch").into()));
        }

        let binary_state = bincode::serialize(&sfrag)?;
        let id_bytes = entity_id.to_bytes();
        // 1) Write canonical state
        let last = self.tree.insert(id_bytes, binary_state.clone()).map_err(sled_error)?;
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
    fn fetch_states_blocking(&self, selection: ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let plans = Planner::new(PlannerConfig::full_support()).plan(&selection);

        // Fallback to naive full-scan path if no viable plan
        let Some(plan) = plans.into_iter().next() else {
            return self.exec_fallback_scan(&self.tree, &self.collection_id, &selection, selection.limit);
        };

        let prefix_guard_disabled = {
            #[cfg(debug_assertions)]
            {
                use std::sync::atomic::Ordering;
                self.prefix_guard_disabled.load(Ordering::Relaxed)
            }
            #[cfg(not(debug_assertions))]
            false
        };

        self.exec_index_plan(plan, selection.limit, prefix_guard_disabled)
    }
    pub fn exec_index_plan(
        &self,
        plan: Plan,
        limit: Option<u64>,
        prefix_guard_disabled: bool,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        unimplemented!()
    }

    pub(crate) fn exec_fallback_scan(
        &self,
        predicate: ankql::ast::Predicate,
        order_by: Option<Vec<ankql::ast::OrderByItem>>,
        limit: Option<u32>,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let strategy = determine_iteration_strategy(&predicate, &order_by);
        let skip_sorting = order_by_matches_iteration(&order_by, &strategy);

        let mut results: Vec<Attested<EntityState>> = Vec::new();
        let iter = create_tree_iterator(tree, strategy);
        for item in iter {
            let (key_bytes, value_bytes) = item.map_err(SledRetrievalError::StorageError)?;
            let id = EntityId::from_bytes(key_bytes.as_ref().try_into().map_err(RetrievalError::storage)?);
            let sfrag: StateFragment = bincode::deserialize(&value_bytes)?;
            let es = Attested::<EntityState>::from_parts(id, collection_id.clone(), sfrag);

            let entity = TemporaryEntity::new(id, collection_id.clone(), &es.payload.state)?;
            if evaluate_predicate(&entity, &predicate)? {
                results.push(es);
                if skip_sorting {
                    if let Some(limit_val) = limit {
                        if results.len() >= limit_val as usize {
                            break;
                        }
                    }
                }
            }
        }

        if !skip_sorting {
            apply_order_by_sort(&mut results, &order_by, &collection_id)?;
        }
        if let Some(limit_val) = limit {
            results.truncate(limit_val as usize);
        }
        Ok(results)
    }
}

/// Create the appropriate iterator based on the strategy
pub(crate) fn create_tree_iterator(
    tree: &sled::Tree,
    strategy: IterationStrategy,
) -> Box<dyn Iterator<Item = Result<(sled::IVec, sled::IVec), sled::Error>>> {
    match strategy {
        IterationStrategy::FullScan { reverse } => {
            if reverse {
                Box::new(tree.iter().rev())
            } else {
                Box::new(tree.iter())
            }
        }
        IterationStrategy::IdRange { range, reverse } => {
            let start_key = match &range.lower {
                RangeBound::Included(id) => Some(id.to_bytes()),
                RangeBound::Excluded(id) => id.successor_bytes().map(|b| b.try_into().unwrap_or_else(|_| id.to_bytes())),
                RangeBound::Unbounded => None,
            };

            let end_key = match &range.upper {
                RangeBound::Included(id) => id.successor_bytes().map(|b| b.try_into().unwrap_or_else(|_| id.to_bytes())),
                RangeBound::Excluded(id) => Some(id.to_bytes()),
                RangeBound::Unbounded => None,
            };

            match (start_key, end_key) {
                (Some(start), Some(end)) => {
                    if reverse {
                        Box::new(tree.range(start..end).rev())
                    } else {
                        Box::new(tree.range(start..end))
                    }
                }
                (Some(start), None) => {
                    if reverse {
                        Box::new(tree.range(start..).rev())
                    } else {
                        Box::new(tree.range(start..))
                    }
                }
                (None, Some(end)) => {
                    if reverse {
                        Box::new(tree.range(..end).rev())
                    } else {
                        Box::new(tree.range(..end))
                    }
                }
                (None, None) => {
                    if reverse {
                        Box::new(tree.iter().rev())
                    } else {
                        Box::new(tree.iter())
                    }
                }
            }
        }
    }
}

/// Extract EntityId from a comparison if it's an id field comparison
fn extract_id_from_comparison(left: &Expr, right: &Expr, operator: &ComparisonOperator) -> Option<(EntityId, IdRange)> {
    // Check if this is an id comparison and extract the value
    let (is_id_field, id_value) = match (left, right) {
        (Expr::Identifier(Identifier::Property(name)), Expr::Literal(Literal::String(s))) if name == "id" => {
            (true, EntityId::from_base64(s).ok()?)
        }
        (Expr::Literal(Literal::String(s)), Expr::Identifier(Identifier::Property(name))) if name == "id" => {
            (true, EntityId::from_base64(s).ok()?)
        }
        _ => return None,
    };

    if !is_id_field {
        return None;
    }

    // Convert comparison to range bounds
    let (lower, upper) = match operator {
        ComparisonOperator::Equal => (RangeBound::Included(id_value), RangeBound::Included(id_value)),
        ComparisonOperator::GreaterThan => (RangeBound::Excluded(id_value), RangeBound::Unbounded),
        ComparisonOperator::GreaterThanOrEqual => (RangeBound::Included(id_value), RangeBound::Unbounded),
        ComparisonOperator::LessThan => (RangeBound::Unbounded, RangeBound::Excluded(id_value)),
        ComparisonOperator::LessThanOrEqual => (RangeBound::Unbounded, RangeBound::Included(id_value)),
        _ => return None, // Other operators not supported for range optimization
    };

    Some((id_value, IdRange { lower, upper }))
}

/// Intersect two ID ranges to get the most restrictive bounds
fn intersect_ranges(left: &IdRange, right: &IdRange) -> IdRange {
    let lower = match (&left.lower, &right.lower) {
        (RangeBound::Unbounded, other) | (other, RangeBound::Unbounded) => other.clone(),
        (RangeBound::Included(a), RangeBound::Included(b)) => RangeBound::Included(std::cmp::max(*a, *b)),
        (RangeBound::Included(a), RangeBound::Excluded(b)) | (RangeBound::Excluded(b), RangeBound::Included(a)) => {
            if a >= b {
                RangeBound::Included(*a)
            } else {
                RangeBound::Excluded(*b)
            }
        }
        (RangeBound::Excluded(a), RangeBound::Excluded(b)) => RangeBound::Excluded(std::cmp::max(*a, *b)),
    };

    let upper = match (&left.upper, &right.upper) {
        (RangeBound::Unbounded, other) | (other, RangeBound::Unbounded) => other.clone(),
        (RangeBound::Included(a), RangeBound::Included(b)) => RangeBound::Included(std::cmp::min(*a, *b)),
        (RangeBound::Included(a), RangeBound::Excluded(b)) | (RangeBound::Excluded(b), RangeBound::Included(a)) => {
            if a <= b {
                RangeBound::Included(*a)
            } else {
                RangeBound::Excluded(*b)
            }
        }
        (RangeBound::Excluded(a), RangeBound::Excluded(b)) => RangeBound::Excluded(std::cmp::min(*a, *b)),
    };

    IdRange { lower, upper }
}

/// Determine the optimal iteration strategy based on predicate and ORDER BY
pub fn determine_iteration_strategy(predicate: &Predicate, order_by: &Option<Vec<ankql::ast::OrderByItem>>) -> IterationStrategy {
    // Check if ORDER BY is on id field
    let id_ordering = order_by.as_ref().and_then(|items| {
        if items.len() == 1 {
            if let Identifier::Property(name) = &items[0].identifier {
                if name == "id" {
                    return Some(items[0].direction.clone());
                }
            }
        }
        None
    });

    // Extract all id constraints from the predicate
    let mut visitor = |mut acc: Vec<IdRange>, pred: &Predicate| {
        if let Predicate::Comparison { left, operator, right } = pred {
            if let Some((_, range)) = extract_id_from_comparison(left, right, operator) {
                acc.push(range);
            }
        }
        acc
    };
    let id_ranges = predicate.walk(Vec::new(), &mut visitor);

    // If we have id constraints, use range optimization
    if !id_ranges.is_empty() {
        let combined_range =
            id_ranges.into_iter().reduce(|acc, range| intersect_ranges(&acc, &range)).expect("We know there's at least one range");

        return IterationStrategy::IdRange {
            range: combined_range,
            reverse: false, // Range queries don't benefit from reverse iteration
        };
    }

    // If ORDER BY id, use full scan with appropriate direction
    if let Some(direction) = id_ordering {
        return IterationStrategy::FullScan { reverse: matches!(direction, OrderDirection::Desc) };
    }

    // Default: full scan forward
    IterationStrategy::FullScan { reverse: false }
}

/// Check if ORDER BY matches the natural iteration order, making sorting unnecessary
pub fn order_by_matches_iteration(order_by: &Option<Vec<ankql::ast::OrderByItem>>, strategy: &IterationStrategy) -> bool {
    let Some(order_items) = order_by else { return true }; // No ORDER BY means any order is fine

    // Only single-field id ordering can match iteration order
    if order_items.len() != 1 {
        return false;
    }

    let Identifier::Property(field_name) = &order_items[0].identifier else { return false };
    if field_name != "id" {
        return false;
    }

    let expected_direction = &order_items[0].direction;

    match strategy {
        IterationStrategy::FullScan { reverse } => {
            let actual_direction = if *reverse { OrderDirection::Desc } else { OrderDirection::Asc };
            expected_direction == &actual_direction
        }
        IterationStrategy::IdRange { reverse, .. } => {
            let actual_direction = if *reverse { OrderDirection::Desc } else { OrderDirection::Asc };
            expected_direction == &actual_direction
        }
    }
}

/// Apply in-memory sorting to results based on ORDER BY clause
pub(crate) fn apply_order_by_sort(
    results: &mut Vec<Attested<EntityState>>,
    order_by: &Option<Vec<ankql::ast::OrderByItem>>,
    collection_id: &CollectionId,
) -> Result<(), RetrievalError> {
    let Some(order_items) = order_by else { return Ok(()) };

    results.sort_by(|a, b| {
        for order_item in order_items {
            if let Identifier::Property(field_name) = &order_item.identifier {
                // Create temporary entities for comparison
                let entity_a = TemporaryEntity::new(a.payload.entity_id, collection_id.clone(), &a.payload.state).ok();
                let entity_b = TemporaryEntity::new(b.payload.entity_id, collection_id.clone(), &b.payload.state).ok();

                use ankql::selection::filter::Filterable;
                if let (Some(ent_a), Some(ent_b)) = (entity_a, entity_b) {
                    let val_a = ent_a.value(field_name);
                    let val_b = ent_b.value(field_name);

                    let cmp = match (val_a, val_b) {
                        (Some(a_str), Some(b_str)) => {
                            // Try to parse as different types for proper collation
                            if let (Ok(a_int), Ok(b_int)) = (a_str.parse::<i64>(), b_str.parse::<i64>()) {
                                a_int.compare(&b_int)
                            } else if let (Ok(a_float), Ok(b_float)) = (a_str.parse::<f64>(), b_str.parse::<f64>()) {
                                a_float.compare(&b_float)
                            } else {
                                a_str.as_str().compare(&b_str.as_str())
                            }
                        }
                        (Some(_), None) => std::cmp::Ordering::Greater, // Non-null > null
                        (None, Some(_)) => std::cmp::Ordering::Less,    // null < non-null
                        (None, None) => std::cmp::Ordering::Equal,      // null == null
                    };

                    let final_cmp = match order_item.direction {
                        OrderDirection::Asc => cmp,
                        OrderDirection::Desc => cmp.reverse(),
                    };

                    if final_cmp != std::cmp::Ordering::Equal {
                        return final_cmp;
                    }
                }
            }
        }
        // Tie-breaker: sort by entity ID for deterministic results
        a.payload.entity_id.cmp(&b.payload.entity_id)
    });

    Ok(())
}

/// Represents a range constraint on EntityId for optimization
#[derive(Debug, Clone)]
pub struct IdRange {
    pub lower: RangeBound<EntityId>,
    pub upper: RangeBound<EntityId>,
}

/// Strategy for iterating through the sled tree
#[derive(Debug)]
pub enum IterationStrategy {
    FullScan { reverse: bool },
    IdRange { range: IdRange, reverse: bool },
}
