use crate::reactor::candidate_changes::CandidateChanges;
use crate::reactor::comparison_index::ComparisonIndex;
use crate::reactor::{AbstractEntity, ChangeNotification, ExtractValue, ReactorSubscriptionId};
use ankurah_proto as proto;
use ankurah_proto::PropertyPath;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

pub struct WatcherSet {
    /// Each property path has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as change)
    index_watchers: HashMap<(crate::ModelId, PropertyPath), ComparisonIndex<(ReactorSubscriptionId, proto::QueryId)>>,
    /// The set of watchers who want to be notified of any changes to a given collection
    wildcard_watchers: HashMap<crate::ModelId, HashSet<(ReactorSubscriptionId, proto::QueryId)>>,
    /// Index of subscriptions that presently match each entity, either by predicate or by entity subscription.
    /// This is used to quickly find all subscriptions that need to be notified when an entity changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    entity_watchers: HashMap<ankurah_proto::EntityId, HashSet<EntityWatcherId>>,
    /// Used at watcher-index insertion/removal to cast comparison values to
    /// the registered property type. The AST may already be normalized, but
    /// the watcher index treats it as untrusted input and checks again.
    resolver: Option<std::sync::Weak<dyn crate::schema::CatalogResolver>>,
}

impl WatcherSet {
    pub fn new() -> Self {
        Self { index_watchers: HashMap::new(), wildcard_watchers: HashMap::new(), entity_watchers: HashMap::new(), resolver: None }
    }

    pub fn set_catalog_resolver(&mut self, resolver: std::sync::Weak<dyn crate::schema::CatalogResolver>) {
        self.resolver = Some(resolver);
    }
    /// Accumulate interested watchers for an entity change into CandidateChanges
    pub fn accumulate_interested_watchers<E: AbstractEntity, C: ChangeNotification<Entity = E>>(
        &self,
        entity: &E,
        offset: usize,
        changes_arc: &Arc<Vec<C>>,
        candidates_by_sub: &mut BTreeMap<ReactorSubscriptionId, CandidateChanges<C>>,
    ) {
        let entity_id = AbstractEntity::id(entity);

        // Find subscriptions interested based on index watchers
        for ((collection_id, property_path), index_ref) in &self.index_watchers {
            if *collection_id == AbstractEntity::collection(entity) {
                // Extract value at the property path (handles both simple fields and JSON paths)
                if let Some(value) = property_path.extract_value(entity) {
                    for (subscription_id, query_id) in index_ref.find_matching(value) {
                        candidates_by_sub
                            .entry(subscription_id)
                            .or_insert_with(|| CandidateChanges::new(changes_arc.clone()))
                            .add_query(query_id, offset);
                    }
                }
            }
        }

        // Check wildcard watchers for this collection
        if let Some(watchers) = self.wildcard_watchers.get(&AbstractEntity::collection(entity)) {
            for (subscription_id, query_id) in watchers.iter() {
                candidates_by_sub
                    .entry(*subscription_id)
                    .or_insert_with(|| CandidateChanges::new(changes_arc.clone()))
                    .add_query(*query_id, offset);
            }
        }

        // Check entity watchers
        if let Some(subscription_ids) = self.entity_watchers.get(entity_id) {
            let first_change_for_entity = changes_arc[..offset].iter().all(|prior| AbstractEntity::id(prior.entity()) != entity_id);
            for sub_id in subscription_ids.iter() {
                match sub_id {
                    EntityWatcherId::Predicate(subscription_id, query_id, model) if *model == AbstractEntity::collection(entity) => {
                        candidates_by_sub
                            .entry(*subscription_id)
                            .or_insert_with(|| CandidateChanges::new(changes_arc.clone()))
                            .add_query(*query_id, offset);
                    }
                    EntityWatcherId::Predicate(..) => {}
                    EntityWatcherId::Subscription(subscription_id) if first_change_for_entity => {
                        candidates_by_sub
                            .entry(*subscription_id)
                            .or_insert_with(|| CandidateChanges::new(changes_arc.clone()))
                            .add_entity(offset);
                    }
                    EntityWatcherId::Subscription(_) => {}
                }
            }
        }
    }

    /// Apply a watcher change
    pub fn apply_watcher_change(&mut self, change: WatcherChange) {
        match change {
            WatcherChange::Add { entity_id, model, subscription_id, query_id } => {
                self.entity_watchers.entry(entity_id).or_default().insert(EntityWatcherId::Predicate(subscription_id, query_id, model));
            }
            WatcherChange::Remove { entity_id, model, subscription_id, query_id } => {
                if let Some(watchers) = self.entity_watchers.get_mut(&entity_id) {
                    watchers.remove(&EntityWatcherId::Predicate(subscription_id, query_id, model));
                    if watchers.is_empty() {
                        self.entity_watchers.remove(&entity_id);
                    }
                }
            }
        }
    }

    /// Add entity subscription watcher for a subscription
    pub fn add_entity_subscription(&mut self, subscription_id: ReactorSubscriptionId, entity_id: proto::EntityId) {
        self.entity_watchers.entry(entity_id).or_default().insert(EntityWatcherId::Subscription(subscription_id));
    }

    /// Remove entity subscription watcher for a subscription
    pub fn remove_entity_subscription(&mut self, subscription_id: ReactorSubscriptionId, entity_id: proto::EntityId) {
        if let Some(watchers) = self.entity_watchers.get_mut(&entity_id) {
            watchers.remove(&EntityWatcherId::Subscription(subscription_id));
            if watchers.is_empty() {
                self.entity_watchers.remove(&entity_id);
            }
        }
    }

    /// Remove all entity subscription watchers for multiple entities
    pub fn remove_entity_subscriptions(
        &mut self,
        subscription_id: ReactorSubscriptionId,
        entity_ids: impl IntoIterator<Item = proto::EntityId>,
    ) {
        for entity_id in entity_ids {
            self.remove_entity_subscription(subscription_id, entity_id);
        }
    }

    /// Clear all entity watchers
    pub fn clear_entity_watchers(&mut self) { self.entity_watchers.clear(); }

    fn apply_wildcard_watcher(&mut self, model: crate::ModelId, watcher_id: (ReactorSubscriptionId, proto::QueryId), op: WatcherOp) {
        let wildcard = self.wildcard_watchers.entry(model).or_default();
        match op {
            WatcherOp::Add => {
                wildcard.insert(watcher_id);
            }
            WatcherOp::Remove => {
                wildcard.remove(&watcher_id);
            }
        }
    }

    /// Get references to internal data for debugging
    pub fn debug_data(
        &self,
    ) -> (
        &HashMap<(crate::ModelId, PropertyPath), ComparisonIndex<(ReactorSubscriptionId, proto::QueryId)>>,
        &HashMap<crate::ModelId, HashSet<(ReactorSubscriptionId, proto::QueryId)>>,
        &HashMap<ankurah_proto::EntityId, HashSet<EntityWatcherId>>,
    ) {
        (&self.index_watchers, &self.wildcard_watchers, &self.entity_watchers)
    }

    /// Add predicate entity watcher for multiple entities
    pub fn add_predicate_entity_watchers(
        &mut self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        model: crate::ModelId,
        entity_ids: impl IntoIterator<Item = proto::EntityId>,
    ) {
        for entity_id in entity_ids {
            self.entity_watchers.entry(entity_id).or_default().insert(EntityWatcherId::Predicate(subscription_id, query_id, model));
        }
    }

    /// Remove predicate entity watchers for entities that no longer match
    /// Returns list of entity_ids that should be removed from the entity cache
    pub fn cleanup_removed_predicate_watchers(
        &mut self,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
        model: crate::ModelId,
        removed_entities: &[proto::EntityId],
    ) {
        for entity_id in removed_entities {
            if let Some(entity_watcher) = self.entity_watchers.get_mut(entity_id) {
                entity_watcher.remove(&EntityWatcherId::Predicate(subscription_id, query_id, model));
            }
        }
    }
    pub fn recurse_predicate_watchers(
        &mut self,
        collection_id: &crate::ModelId,
        predicate: &ankql::ast::Predicate,
        watcher_id: (ReactorSubscriptionId, proto::QueryId), // Should this be a tuple of (subscription_id, query_id) or just subscription_id?
        op: WatcherOp,
    ) {
        use ankql::ast::{Expr, Predicate};
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                // Extract (property_path, literal) from either a Path or a
                // resolved PropertyPath on one side and a Literal on the other.
                // Identifier paths retain the stable property id so an active
                // watcher survives a catalog display-name change.
                let extracted = match (&**left, &**right) {
                    // A raw path watches by name: the system read IS "fetch by
                    // this name", so it lowers to a System reference (the same
                    // lowering the sort-key spec applies).
                    (Expr::Path(path), Expr::Literal(literal)) | (Expr::Literal(literal), Expr::Path(path)) => {
                        ankql::ast::SystemProperty::from_name(path.first())
                            .map(|property| (PropertyPath::system(property, path.steps[1..].to_vec()), literal))
                    }
                    (Expr::PropertyPath(identifier), Expr::Literal(literal)) | (Expr::Literal(literal), Expr::PropertyPath(identifier)) => {
                        Some((identifier.clone(), literal))
                    }
                    _ => None,
                };
                if let Some((property_path, literal)) = extracted {
                    // Use the full path for indexing.
                    // For simple paths like `name`, this is just "name".
                    // For JSON paths like `context.task_id`, this is "context.task_id".
                    // accumulate_interested_watchers will extract the value at this path.
                    // Re-cast at this execution boundary. Origin-time AST
                    // normalization is validation only; a wire/programmatic
                    // AST is not proof that the watcher threshold is in the
                    // same collation domain as entity values.
                    let target = if !property_path.subpath.is_empty() {
                        crate::value::ValueType::Json
                    } else {
                        match property_path.id() {
                            ankql::ast::PropertyId::Id => crate::value::ValueType::EntityId,
                            property => {
                                let Some(resolver) = self.resolver.as_ref().and_then(std::sync::Weak::upgrade) else {
                                    tracing::warn!(
                                        "catalog resolver unavailable for watcher property {}; falling back to wildcard evaluation",
                                        property_path
                                    );
                                    self.apply_wildcard_watcher(*collection_id, watcher_id, op);
                                    return;
                                };
                                match resolver.property_value_type(&property) {
                                    Ok(target) => target,
                                    Err(error) => {
                                        tracing::warn!(
                                            "could not resolve watcher type for {}: {}; falling back to wildcard evaluation",
                                            property_path,
                                            error
                                        );
                                        self.apply_wildcard_watcher(*collection_id, watcher_id, op);
                                        return;
                                    }
                                }
                            }
                        }
                    };
                    let value = match literal.cast_to(target) {
                        Ok(value) => value,
                        Err(error) => {
                            tracing::warn!(
                                "could not cast watcher threshold for {} to {:?}: {}; falling back to wildcard evaluation",
                                property_path,
                                target,
                                error
                            );
                            self.apply_wildcard_watcher(*collection_id, watcher_id, op);
                            return;
                        }
                    };
                    let index = self
                        .index_watchers
                        .entry((*collection_id, property_path))
                        .or_insert_with(|| ComparisonIndex::with_target_type(target));
                    match op {
                        WatcherOp::Add => {
                            index.add(value, operator.clone(), watcher_id);
                        }
                        WatcherOp::Remove => {
                            index.remove(value, operator.clone(), watcher_id);
                        }
                    }
                } else {
                    // warn!("Unsupported predicate: {:?}", predicate);
                }
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.recurse_predicate_watchers(collection_id, left, watcher_id, op);
                self.recurse_predicate_watchers(collection_id, right, watcher_id, op);
            }
            Predicate::Not(pred) => {
                self.recurse_predicate_watchers(collection_id, pred, watcher_id, op);
            }
            Predicate::IsNull(_) => {
                unimplemented!("Not sure how to implement this")
            }
            Predicate::True => {
                let set = self.wildcard_watchers.entry(collection_id.clone()).or_default();

                match op {
                    WatcherOp::Add => {
                        set.insert(watcher_id);
                    }
                    WatcherOp::Remove => {
                        set.remove(&watcher_id);
                    }
                }
            }
            Predicate::False => {
                // Matches nothing, ever: no index or wildcard watcher can make
                // an entity enter or leave the resultset, so Add installs
                // nothing and Remove has nothing to tear down. Predicate
                // algebra can legitimately produce False (fold rules,
                // assume_null, PolicyAgent::filter_predicate narrowing a
                // fully-denied principal), so this arm must be well-defined.
            }
            // Placeholder should be transformed before reaching this point
            Predicate::Placeholder => {
                // This should not happen in normal operation as Placeholder should be transformed
                // before being used in subscriptions
                unimplemented!("Placeholder should be transformed before reactor processing")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum EntityWatcherId {
    Predicate(ReactorSubscriptionId, proto::QueryId, crate::ModelId),
    Subscription(ReactorSubscriptionId),
}

impl EntityWatcherId {
    pub fn subscription_id(&self) -> ReactorSubscriptionId {
        match self {
            EntityWatcherId::Predicate(sub_id, ..) => *sub_id,
            EntityWatcherId::Subscription(sub_id) => *sub_id,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum WatcherOp {
    Add,
    Remove,
}

/// Represents a change to entity watchers that needs to be applied to the WatcherSet
#[derive(Debug, Clone)]
pub enum WatcherChange {
    /// Add an entity watcher (notify_change decides EntityWatcherId variant)
    Add { entity_id: proto::EntityId, model: crate::ModelId, subscription_id: ReactorSubscriptionId, query_id: proto::QueryId },
    /// Remove an entity watcher
    Remove { entity_id: proto::EntityId, model: crate::ModelId, subscription_id: ReactorSubscriptionId, query_id: proto::QueryId },
}

impl WatcherChange {
    /// Create a watcher change for adding an entity watcher
    pub fn add(
        entity_id: proto::EntityId,
        model: crate::ModelId,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
    ) -> Self {
        Self::Add { entity_id, model, subscription_id, query_id }
    }

    /// Create a watcher change for removing an entity watcher
    pub fn remove(
        entity_id: proto::EntityId,
        model: crate::ModelId,
        subscription_id: ReactorSubscriptionId,
        query_id: proto::QueryId,
    ) -> Self {
        Self::Remove { entity_id, model, subscription_id, query_id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reactor::ChangeNotification;

    #[derive(Clone, Debug)]
    struct TestEntity {
        id: proto::EntityId,
        model: crate::ModelId,
    }

    impl AbstractEntity for TestEntity {
        fn collection(&self) -> crate::ModelId { self.model }
        fn id(&self) -> &proto::EntityId { &self.id }
        fn value(&self, _field: &str) -> Option<crate::value::Value> { None }
    }

    #[derive(Clone, Debug)]
    struct TestChange(TestEntity);

    impl std::fmt::Display for TestChange {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0.id) }
    }

    impl ChangeNotification for TestChange {
        type Entity = TestEntity;
        type Event = ();

        fn into_parts(self) -> (Self::Entity, Vec<Self::Event>) { (self.0, Vec::new()) }
        fn entity(&self) -> &Self::Entity { &self.0 }
        fn events(&self) -> &[Self::Event] { &[] }
    }

    #[test]
    fn entity_watchers_are_model_scoped_and_direct_watchers_are_deduplicated() {
        let model_a = crate::ModelId::EntityId(proto::EntityId::from_bytes([0xA1; 16]));
        let model_b = crate::ModelId::EntityId(proto::EntityId::from_bytes([0xB2; 16]));
        let entity_id = proto::EntityId::from_bytes([0xE3; 16]);
        let subscription_a = ReactorSubscriptionId::new();
        let subscription_b = ReactorSubscriptionId::new();
        let direct_subscription = ReactorSubscriptionId::new();
        let query_a = proto::QueryId::new();
        let query_b = proto::QueryId::new();

        let mut watchers = WatcherSet::new();
        watchers.add_predicate_entity_watchers(subscription_a, query_a, model_a, [entity_id]);
        watchers.add_predicate_entity_watchers(subscription_b, query_b, model_b, [entity_id]);
        watchers.add_entity_subscription(direct_subscription, entity_id);

        let changes = Arc::new(vec![
            TestChange(TestEntity { id: entity_id, model: model_a }),
            TestChange(TestEntity { id: entity_id, model: model_b }),
        ]);
        let mut candidates = BTreeMap::new();
        for (offset, change) in changes.iter().enumerate() {
            watchers.accumulate_interested_watchers(change.entity(), offset, &changes, &mut candidates);
        }

        let offsets_for = |subscription, query| {
            candidates[&subscription]
                .query_iter()
                .find(|candidate| *candidate.query_id == query)
                .expect("query candidate")
                .iter_with_offsets()
                .map(|(offset, _)| offset)
                .collect::<Vec<_>>()
        };
        assert_eq!(offsets_for(subscription_a, query_a), vec![0]);
        assert_eq!(offsets_for(subscription_b, query_b), vec![1]);
        assert_eq!(candidates[&direct_subscription].entity_iter().count(), 1);
    }
}
