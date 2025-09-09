use ankurah_proto::{Attested, CollectionId, EntityId, EntityState, Event, EventId, StateFragment};
use anyhow::Result;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;

use ankurah_core::{
    collation::{Collatable, RangeBound},
    entity::TemporaryEntity,
    error::{MutationError, RetrievalError},
    storage::{StorageCollection, StorageEngine},
};

use ankql::{
    ast::{ComparisonOperator, Expr, Identifier, Literal, OrderDirection, Predicate},
    selection::filter::{evaluate_predicate, Filterable},
};
use sled::{Config, Db};
use tokio::task;

pub struct SledStorageEngine {
    pub db: Db,
}

impl SledStorageEngine {
    pub fn with_homedir_folder(folder_name: &str) -> anyhow::Result<Self> {
        let dir = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Failed to get home directory"))?.join(folder_name);

        Self::with_path(dir)
    }

    pub fn with_path(path: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&path)?;
        let dbpath = path.join("sled");
        let db = sled::open(&dbpath)?;
        Ok(Self { db })
    }

    // Open the storage engine without any specific column families
    pub fn new() -> anyhow::Result<Self> { Self::with_homedir_folder(".ankurah") }

    pub fn new_test() -> anyhow::Result<Self> {
        let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

        Ok(Self { db })
    }

    /// List all collections in the storage engine by looking for trees that end in _state
    pub fn list_collections(&self) -> Result<Vec<CollectionId>, RetrievalError> {
        let collections: Vec<CollectionId> = self
            .db
            .tree_names()
            .into_iter()
            .filter_map(|name| {
                // Convert &[u8] to String, skip if invalid UTF-8
                let name_str = String::from_utf8(name.to_vec()).ok()?;
                // Only include collections that end in _state
                if name_str.ends_with("_state") {
                    // Strip _state suffix and convert to CollectionId
                    Some(name_str.strip_suffix("_state")?.to_string().into())
                } else {
                    None
                }
            })
            .collect();
        Ok(collections)
    }
}

pub fn state_name(name: &str) -> String { format!("{}_state", name) }

pub fn event_name(name: &str) -> String { format!("{}_event", name) }

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
fn apply_order_by_sort(
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

/// Create the appropriate iterator based on the strategy
fn create_tree_iterator(
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

pub struct SledStorageCollection {
    pub collection_id: CollectionId,
    pub state: sled::Tree,
    pub events: sled::Tree,
}

#[async_trait]
impl StorageEngine for SledStorageEngine {
    type Value = Vec<u8>;
    async fn collection(&self, id: &CollectionId) -> Result<Arc<dyn StorageCollection>, RetrievalError> {
        // could this block for any meaningful period of time? We might consider spawn_blocking
        let state = self.db.open_tree(state_name(id.as_str())).map_err(SledRetrievalError::StorageError)?;
        let events = self.db.open_tree(event_name(id.as_str())).map_err(SledRetrievalError::StorageError)?;
        Ok(Arc::new(SledStorageCollection { collection_id: id.to_owned(), state, events }))
    }

    async fn delete_all_collections(&self) -> Result<bool, MutationError> {
        let mut any_deleted = false;

        // Get all tree names
        let tree_names = self.db.tree_names();

        // Drop each tree
        for name in tree_names {
            if name == "__sled__default" {
                continue;
            }
            match self.db.drop_tree(&name) {
                Ok(true) => any_deleted = true,
                Ok(false) => {}
                Err(err) => return Err(MutationError::General(Box::new(err))),
            }
        }

        Ok(any_deleted)
    }
}

#[async_trait]
impl StorageCollection for SledStorageCollection {
    async fn set_state(&self, state: Attested<EntityState>) -> Result<bool, MutationError> {
        let tree = self.state.clone();
        let (entity_id, _collection, sfrag) = state.to_parts();
        let binary_state = bincode::serialize(&sfrag)?;
        let id_bytes = entity_id.to_bytes();

        // Use spawn_blocking since sled operations are not async
        task::spawn_blocking(move || {
            let last = tree.insert(id_bytes, binary_state.clone()).map_err(|err| MutationError::UpdateFailed(Box::new(err)))?;
            if let Some(last_bytes) = last {
                Ok(last_bytes != binary_state)
            } else {
                Ok(true)
            }
        })
        .await
        .map_err(|e| MutationError::General(Box::new(e)))?
    }

    async fn get_state(&self, id: EntityId) -> Result<Attested<EntityState>, RetrievalError> {
        let tree = self.state.clone();
        let id_bytes = id.to_bytes();

        // Use spawn_blocking since sled operations are not async
        let result = task::spawn_blocking(move || -> Result<Option<sled::IVec>, sled::Error> { tree.get(id_bytes) })
            .await
            .map_err(|e| SledRetrievalError::Other(Box::new(e)))?;

        match result.map_err(SledRetrievalError::StorageError)? {
            Some(ivec) => {
                let sfrag: StateFragment = bincode::deserialize(&ivec)?;
                let es = Attested::<EntityState>::from_parts(id, self.collection_id.clone(), sfrag);
                Ok(es)
            }
            None => Err(SledRetrievalError::EntityNotFound(id).into()),
        }
    }

    async fn fetch_states(&self, selection: &ankql::ast::Selection) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let predicate = selection.predicate.clone();
        let order_by = selection.order_by.clone();
        let limit = selection.limit;
        let collection_id = self.collection_id.clone();
        let tree = self.state.clone();

        // Determine optimal iteration strategy
        let strategy = determine_iteration_strategy(&predicate, &order_by);
        let skip_sorting = order_by_matches_iteration(&order_by, &strategy);

        // Use spawn_blocking for the operation
        task::spawn_blocking(move || -> Result<Vec<Attested<EntityState>>, RetrievalError> {
            let mut results: Vec<Attested<EntityState>> = Vec::new();

            // Create iterator based on strategy
            let iter = create_tree_iterator(&tree, strategy);

            for item in iter {
                let (key_bytes, value_bytes) = item.map_err(SledRetrievalError::StorageError)?;
                let id = EntityId::from_bytes(key_bytes.as_ref().try_into().map_err(RetrievalError::storage)?);

                let sfrag: StateFragment = bincode::deserialize(&value_bytes)?;
                let es = Attested::<EntityState>::from_parts(id, collection_id.clone(), sfrag);

                // Create entity to evaluate predicate
                let entity = TemporaryEntity::new(id, collection_id.clone(), &es.payload.state)?;

                // Apply predicate filter
                if evaluate_predicate(&entity, &predicate)? {
                    results.push(es);

                    // Apply limit early if results are already in the correct order
                    if skip_sorting {
                        if let Some(limit_val) = limit {
                            if results.len() >= limit_val as usize {
                                break;
                            }
                        }
                    }
                }
            }

            // Apply ORDER BY sorting if needed
            if !skip_sorting {
                apply_order_by_sort(&mut results, &order_by, &collection_id)?;
            }

            // Apply LIMIT - always truncate, it's cheap and handles both cases
            if let Some(limit_val) = limit {
                results.truncate(limit_val as usize);
            }

            Ok(results)
        })
        .await
        .map_err(RetrievalError::future_join)?
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

pub enum SledRetrievalError {
    StorageError(sled::Error),
    EntityNotFound(EntityId),
    EventNotFound(EventId),
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<sled::Error> for SledRetrievalError {
    fn from(val: sled::Error) -> Self { SledRetrievalError::StorageError(val) }
}

impl From<SledRetrievalError> for RetrievalError {
    fn from(err: SledRetrievalError) -> Self {
        match err {
            SledRetrievalError::StorageError(e) => RetrievalError::StorageError(Box::new(e)),
            SledRetrievalError::EntityNotFound(id) => RetrievalError::EntityNotFound(id),
            SledRetrievalError::EventNotFound(id) => RetrievalError::EventNotFound(id),
            SledRetrievalError::Other(e) => RetrievalError::StorageError(e),
        }
    }
}
