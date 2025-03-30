use super::comparison_index::ComparisonIndex;
use crate::changes::{ChangeSet, EntityChange, ItemChange};
use crate::collectionset::CollectionSet;
use crate::entity::Entity;
use crate::node::MatchArgs;
use crate::policy::PolicyAgent;
use crate::resultset::ResultSet;
use crate::storage::StorageEngine;
use crate::subscription::Subscription;
use crate::value::Value;
use ankql::ast;
use ankql::selection::filter::Filterable;
use dashmap::{DashMap, DashSet};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tracing::debug;

use ankurah_proto as proto;
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(String);

impl From<&str> for FieldId {
    fn from(val: &str) -> Self { FieldId(val.to_string()) }
}

#[derive(Default)]
pub struct WatcherSet {
    /// Subscriptions wanting to be notified of changes to a specific property
    property: std::sync::RwLock<HashMap<(proto::CollectionId, FieldId), ComparisonIndex>>,
    /// Subscriptions wanting to be notified of any change to a given collection
    collection: std::sync::RwLock<HashMap<proto::CollectionId, DashSet<proto::SubscriptionId>>>,
    /// Currently matching entities for each subscription
    entity: std::sync::RwLock<HashMap<ankurah_proto::ID, RwLock<HashSet<proto::SubscriptionId>>>>,
}

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of entities
pub struct Reactor<SE, PA> {
    subscriptions: DashMap<proto::SubscriptionId, Arc<Subscription<Entity>>>,
    watchers: WatcherSet,
    collectionset: CollectionSet<SE>,
    _policy_agent: PA,
}

#[derive(Debug, Copy, Clone)]
enum WatcherOp {
    Add,
    Remove,
}

impl<SE, PA> Reactor<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    pub fn new(collectionset: CollectionSet<SE>, policy_agent: PA) -> Arc<Self> {
        Arc::new(Self { subscriptions: DashMap::new(), watchers: Default::default(), collectionset, _policy_agent: policy_agent })
    }

    pub async fn subscribe(
        self: &Arc<Self>,
        sub_id: proto::SubscriptionId,
        collection_id: &proto::CollectionId,
        args: impl Into<MatchArgs>,
        callback: impl Fn(ChangeSet<Entity>) + Send + Sync + 'static,
    ) -> anyhow::Result<()> {
        let args = args.into();

        // Start watching the relevant indexes
        self.manage_watchers_recurse(collection_id, &args.predicate, sub_id, WatcherOp::Add);

        // Find initial matching entities
        let storage_collection = self.collectionset.get(collection_id).await?;
        let states = storage_collection.fetch_states(&args.predicate).await?;
        let mut matching_entities = Vec::new();

        // Convert states to Entity and filter by predicate
        for (id, state) in states {
            let entity = Entity::from_state(id, collection_id.to_owned(), &state)?;

            // Evaluate predicate for each entity
            if ankql::selection::filter::evaluate_predicate(&entity, &args.predicate).unwrap_or(false) {
                matching_entities.push(entity.clone());

                // Set up entity watchers
                self.watchers.entity.write().unwrap().entry(entity.id.clone()).or_default().write().unwrap().insert(sub_id);
            }
        }

        // Create subscription with initial matching entities
        let subscription = Arc::new(Subscription {
            id: sub_id,
            collection_id: collection_id.clone(),
            predicate: args.predicate,
            callback: Arc::new(Box::new(callback)),
            matching_entities: std::sync::RwLock::new(matching_entities.clone()),
        });

        // Store subscription
        self.subscriptions.insert(sub_id, subscription.clone());

        // Call callback with initial state
        (subscription.callback)(ChangeSet {
            changes: matching_entities.iter().map(|entity| ItemChange::Initial { item: entity.clone() }).collect(),
            resultset: ResultSet { loaded: true, items: matching_entities },
        });

        Ok(())
    }

    fn manage_watchers_recurse(
        &self,
        collection_id: &proto::CollectionId,
        predicate: &ast::Predicate,
        sub_id: proto::SubscriptionId,
        op: WatcherOp,
    ) {
        use ankql::ast::{Expr, Identifier, Predicate};
        match predicate {
            Predicate::Comparison { left, operator, right } => {
                if let (Expr::Identifier(field), Expr::Literal(literal)) | (Expr::Literal(literal), Expr::Identifier(field)) =
                    (&**left, &**right)
                {
                    let field_name = match field {
                        Identifier::Property(name) => name.clone(),
                        Identifier::CollectionProperty(_, name) => name.clone(),
                    };

                    let field_id = FieldId(field_name);
                    match op {
                        WatcherOp::Add => {
                            let entry = self.watchers.property.entry((collection_id.clone(), field_id));
                            entry.or_default().add((*literal).clone(), operator.clone(), sub_id);
                        }
                        WatcherOp::Remove => {
                            if let Some(mut index) = self.watchers.property.get_mut(&(collection_id.clone(), field_id)) {
                                index.remove((*literal).clone(), operator.clone(), sub_id);
                            }
                        }
                    }
                } else {
                    // warn!("Unsupported predicate: {:?}", predicate);
                }
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.manage_watchers_recurse(collection_id, left, sub_id, op);
                self.manage_watchers_recurse(collection_id, right, sub_id, op);
            }
            Predicate::Not(pred) => {
                self.manage_watchers_recurse(collection_id, pred, sub_id, op);
            }
            Predicate::IsNull(_) => {
                unimplemented!("Not sure how to implement this")
            }
            Predicate::True => {
                use dashmap::mapref::entry::Entry;
                match (op, self.watchers.collection.entry(collection_id.clone())) {
                    (WatcherOp::Remove, Entry::Occupied(e)) => {
                        e.get().remove(&sub_id);
                    }
                    (WatcherOp::Add, Entry::Vacant(e)) => {
                        e.insert(DashSet::new()).insert(sub_id);
                    }
                    _ => {}
                }
            }
            Predicate::False => {
                unimplemented!("Not sure how to implement this")
            }
        }
    }

    /// Remove a subscription and clean up its watchers
    pub(crate) async fn unsubscribe(&self, sub_id: proto::SubscriptionId) {
        if let Some((_, sub)) = self.subscriptions.remove(&sub_id) {
            // Remove from index watchers
            self.manage_watchers_recurse(&sub.collection_id, &sub.predicate, sub_id, WatcherOp::Remove);

            // Remove from entity watchers using subscription's matching_entities
            let matching = sub.matching_entities.read().unwrap();
            for entity in matching.iter() {
                if let Some(mut watchers) = self.watchers.entity.get_mut(&entity.id.clone()) {
                    watchers.retain(|&id| id != sub_id);
                }
            }
        }
    }

    /// Update entity watchers when an entity's matching status changes
    fn update_entity_watchers(&self, entity: &Entity, matching: bool, sub_id: proto::SubscriptionId) {
        if let Some(subscription) = self.subscriptions.get(&sub_id) {
            let mut entities = subscription.matching_entities.write().unwrap();
            let watchers = self.watchers.entity.entry(entity.id.clone()).or_default();

            // TODO - we can't just use the matching flag, because we need to know if the entity was in the set before
            // or after calling notify_change
            let did_match = entities.iter().any(|r| r.id == entity.id);
            match (did_match, matching) {
                (false, true) => {
                    entities.push(entity.clone());
                    watchers.insert(sub_id);
                }
                (true, false) => {
                    entities.retain(|r| r.id != entity.id);
                    watchers.retain(|&id| id != sub_id);
                }
                _ => {} // No change needed
            }
        }
    }

    /// Notify subscriptions about an entity change

    pub fn notify_change(&self, changes: Vec<EntityChange>) {
        debug!("Reactor.notify_change({:?})", changes);
        // Group changes by subscription
        let mut sub_changes: std::collections::HashMap<proto::SubscriptionId, Vec<ItemChange<Entity>>> = std::collections::HashMap::new();

        for change in &changes {
            let mut possibly_interested_subs = HashSet::new();

            debug!("Reactor - index watchers: {:?}", self.watchers.property);
            // Find subscriptions that might be interested based on index watchers
            for index_ref in self.watchers.property.iter() {
                // Get the field value from the entity
                let (collection_id, field_id) = index_ref.key();
                if collection_id == &(change.entity.collection) {
                    if let Some(field_value) = change.entity.value(&field_id.0) {
                        possibly_interested_subs.extend(index_ref.find_matching(Value::String(field_value)));
                    }
                }
            }

            debug!("wildcard watchers: {:?}", self.watchers.collection);
            // Also check wildcard watchers for this collection
            if let Some(watchers) = self.watchers.collection.get(&change.entity.collection) {
                for watcher in watchers.iter() {
                    possibly_interested_subs.insert(watcher.key().clone());
                }
            }

            // Check entity watchers
            if let Some(watchers) = self.watchers.entity.get(&change.entity.id) {
                for watcher in watchers.iter() {
                    possibly_interested_subs.insert(watcher.clone());
                }
            }

            debug!(" possibly_interested_subs: {possibly_interested_subs:?}");
            // Check each possibly interested subscription with full predicate evaluation
            for sub_id in possibly_interested_subs {
                if let Some(subscription) = self.subscriptions.get(&sub_id) {
                    let entity = &change.entity;
                    // Use evaluate_predicate directly on the entity instead of fetch_entities
                    debug!("\tnotify_change predicate: {} {:?}", sub_id, subscription.predicate);
                    let matches = ankql::selection::filter::evaluate_predicate::<Entity>(&entity, &subscription.predicate).unwrap_or(false);

                    let did_match = subscription.matching_entities.lock().unwrap().iter().any(|r| r.id == entity.id);
                    use ankql::selection::filter::Filterable;
                    debug!("\tnotify_change matches: {matches} did_match: {did_match} {}: {:?}", entity.id, entity.value("status"));

                    // Update entity watchers and notify subscription if needed
                    self.update_entity_watchers(entity, matches, sub_id);

                    // Determine the change type
                    let new_change: Option<ItemChange<Entity>> = if matches != did_match {
                        // Matching status changed
                        Some(if matches {
                            ItemChange::Add { item: entity.clone(), events: change.events.clone() }
                        } else {
                            ItemChange::Remove { item: entity.clone(), events: change.events.clone() }
                        })
                    } else if matches {
                        // Entity still matches but was updated
                        Some(ItemChange::Update { item: entity.clone(), events: change.events.clone() })
                    } else {
                        // Entity didn't match before and still doesn't match
                        None
                    };

                    // Add the change to the subscription's changes if there is one
                    if let Some(new_change) = new_change {
                        sub_changes.entry(sub_id).or_default().push(new_change);
                    }
                }
            }
        }

        // Send batched notifications
        for (sub_id, changes) in sub_changes {
            if let Some(subscription) = self.subscriptions.get(&sub_id) {
                let changeset = {
                    ChangeSet {
                        resultset: ResultSet { loaded: true, items: subscription.matching_entities.read().unwrap().clone() },
                        changes,
                    }
                };
                (subscription.callback)(changeset);
            }
        }
    }
}
