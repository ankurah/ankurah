use super::comparison_index::ComparisonIndex;
use crate::changes::{ChangeSet, EntityChange, ItemChange};
use crate::collectionset::CollectionSet;
use crate::entity::{Entity, WeakEntitySet};
use crate::node::MatchArgs;
use crate::policy::PolicyAgent;
use crate::resultset::ResultSet;
use crate::storage::StorageEngine;
use crate::subscription::Subscription;
use crate::util::safemap::SafeMap;
use crate::util::safeset::SafeSet;
use crate::value::Value;
use ankql::ast;
use ankql::selection::filter::Filterable;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::debug;
#[cfg(feature = "instrument")]
use tracing::instrument;

use ankurah_proto as proto;
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(String);

impl From<&str> for FieldId {
    fn from(val: &str) -> Self { FieldId(val.to_string()) }
}

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of entities
pub struct Reactor<SE, PA> {
    /// Current subscriptions
    subscriptions: SafeMap<proto::SubscriptionId, Arc<Subscription<Entity>>>,
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as changes)
    index_watchers: SafeMap<(proto::CollectionId, FieldId), Arc<std::sync::RwLock<ComparisonIndex>>>,
    /// The set of watchers who want to be notified of any changes to a given collection
    wildcard_watchers: SafeMap<proto::CollectionId, Arc<std::sync::RwLock<SafeSet<proto::SubscriptionId>>>>,
    /// Index of subscriptions that presently match each entity.
    /// This is used to quickly find all subscriptions that need to be notified when an entity changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    entity_watchers: SafeMap<ankurah_proto::ID, HashSet<proto::SubscriptionId>>,
    /// Reference to the storage engine
    collections: CollectionSet<SE>,

    entityset: WeakEntitySet,
    // Weak reference to the node
    // node: OnceCell<WeakNode<PA>>,
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
    pub fn new(collections: CollectionSet<SE>, entityset: WeakEntitySet, policy_agent: PA) -> Arc<Self> {
        Arc::new(Self {
            subscriptions: SafeMap::new(),
            index_watchers: SafeMap::new(),
            wildcard_watchers: SafeMap::new(),
            entity_watchers: SafeMap::new(),
            collections,
            entityset,
            _policy_agent: policy_agent,
            // node: OnceCell::new(),
        })
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
        Self::manage_watchers_recurse(self, collection_id, &args.predicate, sub_id, WatcherOp::Add);

        // Find initial matching entities
        let storage_collection = self.collections.get(collection_id).await?;
        let states = storage_collection.fetch_states(&args.predicate).await?;
        let mut matching_entities = Vec::new();

        // Convert states to Entity and filter by predicate
        for (id, state) in states {
            let entity = self.entityset.with_state(id, collection_id.to_owned(), state)?;

            // Evaluate predicate for each entity
            if ankql::selection::filter::evaluate_predicate(&entity, &args.predicate).unwrap_or(false) {
                matching_entities.push(entity.clone());

                // Set up entity watchers
                self.entity_watchers.set_insert(entity.id, sub_id);
            }
        }

        // Create subscription with initial matching entities
        let subscription = Arc::new(Subscription {
            id: sub_id,
            collection_id: collection_id.clone(),
            predicate: args.predicate,
            callback: Arc::new(Box::new(callback)),
            matching_entities: std::sync::Mutex::new(matching_entities.clone()),
        });

        // TODO - is this a memory leak?
        self.subscriptions.insert(sub_id, subscription.clone());

        // Call callback with initial state
        (subscription.callback)(ChangeSet {
            changes: matching_entities.iter().map(|entity| ItemChange::Initial { item: entity.clone() }).collect(),
            resultset: ResultSet { loaded: true, items: matching_entities.clone() },
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
                            let index = self.index_watchers.get_or_default((collection_id.clone(), field_id));
                            index.write().unwrap().add((*literal).clone(), operator.clone(), sub_id);
                        }
                        WatcherOp::Remove => {
                            let index = self.index_watchers.get_or_default((collection_id.clone(), field_id));
                            index.write().unwrap().remove((*literal).clone(), operator.clone(), sub_id);
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
                let set = self.wildcard_watchers.get_or_default(collection_id.clone());
                match op {
                    WatcherOp::Add => {
                        set.write().unwrap().insert(sub_id);
                    }
                    WatcherOp::Remove => {
                        set.write().unwrap().remove(&sub_id);
                    }
                }
            }
            Predicate::False => {
                unimplemented!("Not sure how to implement this")
            }
        }
    }

    /// Remove a subscription and clean up its watchers
    #[cfg_attr(feature = "instrument", instrument(skip_all, fields(sub_id = %sub_id)))]
    pub(crate) fn unsubscribe(&self, sub_id: proto::SubscriptionId) {
        if let Some(sub) = self.subscriptions.remove(&sub_id) {
            // Remove from index watchers
            self.manage_watchers_recurse(&sub.collection_id, &sub.predicate, sub_id, WatcherOp::Remove);

            // Remove from entity watchers using subscription's matching_entities
            let matching = sub.matching_entities.lock().unwrap();
            for entity in matching.iter() {
                self.entity_watchers.set_remove(&entity.id, &sub_id);
            }
        }
    }

    /// Update entity watchers when an entity's matching status changes
    fn update_entity_watchers(&self, entity: &Entity, matching: bool, sub_id: proto::SubscriptionId) {
        if let Some(subscription) = self.subscriptions.get(&sub_id) {
            let mut entities = subscription.matching_entities.lock().unwrap();
            // let mut watchers = self.entity_watchers.entry(entity.id.clone()).or_default();

            // TODO - we can't just use the matching flag, because we need to know if the entity was in the set before
            // or after calling notify_change
            let did_match = entities.iter().any(|e| e.id() == entity.id());
            match (did_match, matching) {
                (false, true) => {
                    entities.push(entity.clone());
                    self.entity_watchers.set_insert(entity.id.clone(), sub_id);
                }
                (true, false) => {
                    entities.retain(|r| r.id != entity.id);
                    self.entity_watchers.set_remove(&entity.id, &sub_id);
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

            // debug!("Reactor - index watchers: {:?}", self.index_watchers);
            // Find subscriptions that might be interested based on index watchers
            for ((collection_id, field_id), index_ref) in self.index_watchers.to_vec() {
                // Get the field value from the entity
                if collection_id == change.entity.collection {
                    if let Some(field_value) = change.entity.value(&field_id.0) {
                        possibly_interested_subs.extend(index_ref.read().unwrap().find_matching(Value::String(field_value)));
                    }
                }
            }

            // Also check wildcard watchers for this collection
            if let Some(watchers) = self.wildcard_watchers.get(&change.entity.collection) {
                for watcher in watchers.read().unwrap().to_vec() {
                    possibly_interested_subs.insert(watcher.clone());
                }
            }

            // Check entity watchers
            if let Some(watchers) = self.entity_watchers.get(&change.entity.id()) {
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

                    let did_match = subscription.matching_entities.lock().unwrap().iter().any(|r| r.id() == entity.id());
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
                (subscription.callback)(ChangeSet {
                    resultset: ResultSet { loaded: true, items: subscription.matching_entities.lock().unwrap().clone() },
                    changes,
                });
            }
        }
    }
}
