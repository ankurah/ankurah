use super::comparision_index::ComparisonIndex;
use crate::changes::{ChangeSet, EntityChange, ItemChange};
use crate::model::Entity;
use crate::resultset::ResultSet;
use crate::storage::StorageEngine;
use crate::subscription::{Subscription, SubscriptionHandle};
use crate::value::Value;
use ankql::ast;
use ankql::selection::filter::Filterable;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

use ankurah_proto as proto;
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(String);
impl From<&str> for FieldId {
    fn from(val: &str) -> Self { FieldId(val.to_string()) }
}

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of entities
pub struct Reactor {
    /// Current subscriptions
    subscriptions: DashMap<proto::SubscriptionId, Arc<Subscription<Arc<Entity>>>>,
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as changes)
    index_watchers: DashMap<FieldId, ComparisonIndex>,
    /// Index of subscriptions that presently match each entity.
    /// This is used to quickly find all subscriptions that need to be notified when an entity changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    entity_watchers: DashMap<ankurah_proto::ID, Vec<proto::SubscriptionId>>,
    /// Reference to the storage engine
    storage: Arc<dyn StorageEngine>,
}

impl Reactor {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Arc<Self> {
        Arc::new(Self { subscriptions: DashMap::new(), index_watchers: DashMap::new(), entity_watchers: DashMap::new(), storage })
    }

    pub async fn subscribe<F>(
        self: &Arc<Self>,
        collection: &str,
        predicate: ast::Predicate,
        callback: F,
    ) -> anyhow::Result<SubscriptionHandle>
    where
        F: Fn(ChangeSet<Arc<Entity>>) + Send + Sync + 'static,
    {
        let sub_id = proto::SubscriptionId::new();

        // Start watching the relevant indexes
        self.add_index_watchers(sub_id, &predicate);

        // Find initial matching entities
        let states = self.storage.fetch_states(collection.to_string(), &predicate).await?;
        let mut matching_entities = Vec::new();

        // Convert states to Entity and filter by predicate
        for (id, state) in states {
            let entity = crate::model::Entity::from_state(id, collection, &state)?;
            let entity = Arc::new(entity);

            // Evaluate predicate for each entity
            if ankql::selection::filter::evaluate_predicate(&*entity, &predicate).unwrap_or(false) {
                matching_entities.push(entity.clone());

                // Set up entity watchers
                self.entity_watchers.entry(entity.id()).or_default().push(sub_id);
            }
        }

        // Create subscription with initial matching entities
        let subscription = Arc::new(Subscription {
            id: sub_id,
            predicate,
            callback: Arc::new(Box::new(callback)),
            matching_entities: std::sync::Mutex::new(matching_entities.clone()),
        });

        // Store subscription
        self.subscriptions.insert(sub_id, subscription.clone());

        // Call callback with initial state
        if !matching_entities.is_empty() {
            (subscription.callback)(ChangeSet {
                changes: matching_entities.iter().map(|entity| ItemChange::Initial { item: entity.clone() }).collect(),
                resultset: ResultSet { items: matching_entities.clone() },
            });
        }

        Ok(SubscriptionHandle::new(self.clone(), sub_id))
    }

    fn recurse_predicate<F>(&self, predicate: &ast::Predicate, f: F)
    where F: Fn(dashmap::Entry<FieldId, ComparisonIndex>, FieldId, &ast::Literal, &ast::ComparisonOperator) + Copy {
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
                    let entry = self.index_watchers.entry(field_id.clone());
                    f(entry, field_id, literal, operator);
                } else {
                    // warn!("Unsupported predicate: {:?}", predicate);
                }
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.recurse_predicate(left, f);
                self.recurse_predicate(right, f);
            }
            Predicate::Not(pred) => {
                self.recurse_predicate(pred, f);
            }
            Predicate::IsNull(_) => {}
        }
    }

    fn add_index_watchers(&self, sub_id: proto::SubscriptionId, predicate: &ast::Predicate) {
        self.recurse_predicate(predicate, |entry, _field_id, literal, operator| {
            entry.or_default().add((*literal).clone(), operator.clone(), sub_id);
        });
    }

    fn remove_index_watchers(&self, sub_id: proto::SubscriptionId, predicate: &ast::Predicate) {
        self.recurse_predicate(
            predicate,
            |entry: dashmap::Entry<FieldId, ComparisonIndex>, _field_id, literal: &ast::Literal, operator| {
                if let dashmap::Entry::Occupied(mut index) = entry {
                    let literal = (*literal).clone();
                    index.get_mut().remove(literal, operator.clone(), sub_id);
                }
            },
        );
    }

    /// Remove a subscription and clean up its watchers
    pub(crate) fn unsubscribe(&self, sub_id: proto::SubscriptionId) {
        if let Some((_, subscription)) = self.subscriptions.remove(&sub_id) {
            // Remove from index watchers
            self.remove_index_watchers(sub_id, &subscription.predicate);

            // Remove from entity watchers using subscription's matching_entities
            let matching = subscription.matching_entities.lock().unwrap();
            for entity in matching.iter() {
                if let Some(mut watchers) = self.entity_watchers.get_mut(&entity.id()) {
                    watchers.retain(|&id| id != sub_id);
                }
            }
        }
    }

    /// Update entity watchers when an entity's matching status changes
    fn update_entity_watchers(&self, entity: &Arc<crate::model::Entity>, matching: bool, sub_id: proto::SubscriptionId) {
        if let Some(subscription) = self.subscriptions.get(&sub_id) {
            let mut entities = subscription.matching_entities.lock().unwrap();
            let mut watchers = self.entity_watchers.entry(entity.id()).or_default();

            // TODO - we can't just use the matching flag, because we need to know if the entity was in the set before
            // or after calling notify_change
            let did_match = entities.iter().any(|r| r.id() == entity.id());
            match (did_match, matching) {
                (false, true) => {
                    entities.push(entity.clone());
                    watchers.push(sub_id);
                }
                (true, false) => {
                    entities.retain(|r| r.id() != entity.id());
                    watchers.retain(|&id| id != sub_id);
                }
                _ => {} // No change needed
            }
        }
    }

    /// Notify subscriptions about an entity change
    pub fn notify_change(&self, changes: Vec<EntityChange>) {
        // Group changes by subscription
        let mut sub_changes: std::collections::HashMap<proto::SubscriptionId, Vec<ItemChange<Arc<Entity>>>> =
            std::collections::HashMap::new();

        for change in &changes {
            let mut possibly_interested_subs = HashSet::new();

            // Find subscriptions that might be interested based on index watchers
            for index_ref in self.index_watchers.iter() {
                // Get the field value from the entity
                if let Some(field_value) = change.entity.value(&index_ref.key().0) {
                    possibly_interested_subs.extend(index_ref.find_matching(Value::String(field_value)));
                }
            }

            // Check each possibly interested subscription with full predicate evaluation
            for sub_id in possibly_interested_subs {
                if let Some(subscription) = self.subscriptions.get(&sub_id) {
                    let entity = &change.entity;
                    // Use evaluate_predicate directly on the entity instead of fetch_entities
                    let matches = ankql::selection::filter::evaluate_predicate(&**entity, &subscription.predicate).unwrap_or(false);

                    let did_match = subscription.matching_entities.lock().unwrap().iter().any(|r| r.id() == entity.id());

                    // Update entity watchers and notify subscription if needed
                    self.update_entity_watchers(entity, matches, sub_id);

                    // info!(
                    //     "NOTIFY CHANGE: {} {matches} {did_match} {change:?}",
                    //     entity.id(),
                    //     change = change.clone()
                    // );
                    // Determine the change type
                    let new_change: Option<ItemChange<Arc<Entity>>> = if matches != did_match {
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
                    resultset: ResultSet { items: subscription.matching_entities.lock().unwrap().clone() },
                    changes,
                });
            }
        }
    }
}
