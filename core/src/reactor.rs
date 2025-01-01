use super::comparision_index::ComparisonIndex;
use crate::changes::{ChangeSet, RecordChange, RecordChangeKind};
use crate::storage::StorageEngine;
use crate::subscription::{Subscription, SubscriptionHandle, SubscriptionId};
use crate::value::Value;
use ankql::ast;
use ankql::selection::filter::Filterable;
use anyhow::Result;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(String);
impl From<&str> for FieldId {
    fn from(val: &str) -> Self {
        FieldId(val.to_string())
    }
}

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of records
pub struct Reactor {
    /// Current subscriptions
    subscriptions: DashMap<SubscriptionId, Arc<Subscription>>,
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as changes)
    index_watchers: DashMap<FieldId, ComparisonIndex>,
    /// Index of subscriptions that presently match each record.
    /// This is used to quickly find all subscriptions that need to be notified when a record changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    record_watchers: DashMap<ankurah_proto::ID, Vec<SubscriptionId>>,
    /// Next subscription id
    next_sub_id: AtomicUsize,
    /// Reference to the storage engine
    storage: Arc<dyn StorageEngine>,
}

impl Reactor {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Arc<Self> {
        Arc::new(Self {
            subscriptions: DashMap::new(),
            index_watchers: DashMap::new(),
            record_watchers: DashMap::new(),
            next_sub_id: AtomicUsize::new(0),
            storage,
        })
    }

    pub async fn subscribe<F>(
        self: &Arc<Self>,
        bucket_name: &str,
        predicate: &ast::Predicate,
        callback: F,
    ) -> Result<SubscriptionHandle>
    where
        F: Fn(ChangeSet) + Send + Sync + 'static,
    {
        let sub_id: SubscriptionId = self
            .next_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .into();

        // Start watching the relevant indexes
        self.add_index_watchers(sub_id, predicate);

        // Find initial matching records
        let states = self
            .storage
            .fetch_states(bucket_name.to_string(), predicate)
            .await?;
        let mut matching_records = Vec::new();

        // Convert states to RecordInner
        for (id, state) in states {
            let record = crate::model::RecordInner::from_record_state(id, bucket_name, &state)?;
            let record = Arc::new(record);
            matching_records.push(record.clone());

            // Set up record watchers
            self.record_watchers
                .entry(record.id())
                .or_default()
                .push(sub_id);
        }

        // Create subscription with initial matching records
        let subscription = Arc::new(Subscription {
            id: sub_id,
            predicate: predicate.clone(),
            callback: Arc::new(Box::new(callback)),
            matching_records: std::sync::Mutex::new(matching_records.clone()),
        });

        // Store subscription
        self.subscriptions.insert(sub_id, subscription.clone());

        // Call callback with initial state
        (subscription.callback)(ChangeSet {
            changes: matching_records
                .into_iter()
                .map(|r| RecordChange {
                    record: r,
                    updates: vec![],
                    kind: RecordChangeKind::Add,
                })
                .collect(),
        });

        Ok(SubscriptionHandle {
            id: sub_id,
            reactor: self.clone(),
        })
    }

    fn recurse_predicate<F>(&self, predicate: &ast::Predicate, f: F)
    where
        F: Fn(
                dashmap::Entry<FieldId, ComparisonIndex>,
                FieldId,
                &ast::Literal,
                &ast::ComparisonOperator,
            ) + Copy,
    {
        use ankql::ast::{Expr, Identifier, Predicate};
        match predicate {
            Predicate::Comparison {
                left,
                operator,
                right,
            } => {
                if let (Expr::Identifier(field), Expr::Literal(literal))
                | (Expr::Literal(literal), Expr::Identifier(field)) = (&**left, &**right)
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

    fn add_index_watchers(&self, sub_id: SubscriptionId, predicate: &ast::Predicate) {
        self.recurse_predicate(predicate, |entry, _field_id, literal, operator| {
            entry
                .or_default()
                .add((*literal).clone(), operator.clone(), sub_id);
        });
    }

    fn remove_index_watchers(&self, sub_id: SubscriptionId, predicate: &ast::Predicate) {
        self.recurse_predicate(
            predicate,
            |entry: dashmap::Entry<FieldId, ComparisonIndex>,
             _field_id,
             literal: &ast::Literal,
             operator| {
                if let dashmap::Entry::Occupied(mut index) = entry {
                    let literal = (*literal).clone();
                    index.get_mut().remove(literal, operator.clone(), sub_id);
                }
            },
        );
    }

    /// Remove a subscription and clean up its watchers
    pub(crate) fn unsubscribe(&self, sub_id: SubscriptionId) {
        if let Some((_, subscription)) = self.subscriptions.remove(&sub_id) {
            // Remove from index watchers
            self.remove_index_watchers(sub_id, &subscription.predicate);

            // Remove from record watchers using subscription's matching_records
            let matching = subscription.matching_records.lock().unwrap();
            for record in matching.iter() {
                if let Some(mut watchers) = self.record_watchers.get_mut(&record.id()) {
                    watchers.retain(|&id| id != sub_id);
                }
            }
        }
    }

    /// Update record watchers when a record's matching status changes
    fn update_record_watchers(
        &self,
        record: &Arc<crate::model::RecordInner>,
        matching: bool,
        sub_id: SubscriptionId,
    ) {
        if let Some(subscription) = self.subscriptions.get(&sub_id) {
            let mut records = subscription.matching_records.lock().unwrap();
            let mut watchers = self.record_watchers.entry(record.id()).or_default();

            // TODO - we can't just use the matching flag, because we need to know if the record was in the set before
            // or after calling notify_change
            let did_match = records.iter().any(|r| r.id() == record.id());
            match (did_match, matching) {
                (false, true) => {
                    records.push(record.clone());
                    watchers.push(sub_id);
                }
                (true, false) => {
                    records.retain(|r| r.id() != record.id());
                    watchers.retain(|&id| id != sub_id);
                }
                _ => {} // No change needed
            }
        }
    }

    /// Notify subscriptions about a record change
    pub fn notify_change(&self, changeset: ChangeSet) {
        for change in &changeset.changes {
            let mut possibly_interested_subs = HashSet::new();

            // Find subscriptions that might be interested based on index watchers
            for index_ref in self.index_watchers.iter() {
                // Get the field value from the record
                if let Some(field_value) = change.record.value(&index_ref.key().0) {
                    possibly_interested_subs
                        .extend(index_ref.find_matching(Value::String(field_value)));
                }
            }

            // TODO NOW - we should be creating one changeset per subscription, and populating it with the relevant changes from the input changeset
            // Check each possibly interested subscription with full predicate evaluation
            for sub_id in possibly_interested_subs {
                if let Some(subscription) = self.subscriptions.get(&sub_id) {
                    // Use evaluate_predicate directly on the record instead of fetch_records
                    let matches = ankql::selection::filter::evaluate_predicate(
                        &*change.record,
                        &subscription.predicate,
                    )
                    .unwrap_or(false);

                    let did_match = subscription
                        .matching_records
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|r| r.id() == change.record.id());

                    // Only update watchers and notify if matching status changed
                    if matches != did_match {
                        self.update_record_watchers(&change.record, matches, sub_id);

                        (subscription.callback)(ChangeSet {
                            changes: vec![RecordChange {
                                record: change.record.clone(),
                                updates: change.updates.clone(),
                                kind: if matches {
                                    RecordChangeKind::Add
                                } else {
                                    RecordChangeKind::Remove
                                },
                            }],
                        });
                    } else if matches && change.kind == RecordChangeKind::Edit {
                        // Record still matches but was edited
                        (subscription.callback)(ChangeSet {
                            changes: vec![RecordChange {
                                record: change.record.clone(),
                                updates: change.updates.clone(),
                                kind: RecordChangeKind::Edit,
                            }],
                        });
                    }
                }
            }
        }
    }
}
