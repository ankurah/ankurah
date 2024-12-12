use super::comparision_index::ComparisonIndex;
use ankql::ast::Predicate;
use anyhow::Result;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, Weak};
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FieldId(String);

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of records
pub struct Reactor<S, R>
where
    R: Record,
    S: StorageEngine<R>,
{
    /// Current subscriptions
    subscriptions: DashMap<usize, Arc<Subscription<R, S::Update>>>,
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as changes)
    index_watchers: DashMap<FieldId, ComparisonIndex>,
    /// Index of subscriptions that presently match each record.
    /// This is used to quickly find all subscriptions that need to be notified when a record changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    record_watchers: DashMap<R::Id, Vec<SubscriptionId>>,
    /// Next subscription id
    next_sub_id: AtomicUsize,
    /// Weak reference to the server, which we need to perform the initial subscription setup
    storage: Arc<S>,
}

pub(crate) trait StorageEngine<R>
where
    R: Record,
{
    type Id: std::hash::Hash + std::cmp::Eq;
    type Update;
    fn fetch_records(&self, predicate: &Predicate) -> Vec<R>;
}
pub(crate) trait Record {
    type Id: std::hash::Hash + std::cmp::Eq + Copy + Clone;
    fn id(&self) -> Self::Id;
}

impl<S, R> Reactor<S, R>
where
    R: Record,
    S: StorageEngine<R>,
{
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            subscriptions: DashMap::new(),
            index_watchers: DashMap::new(),
            record_watchers: DashMap::new(),
            next_sub_id: AtomicUsize::new(0),
            storage,
        }
    }
    pub fn subscribe<F>(
        self: &Arc<Self>,
        predicate: &Predicate,
        callback: F,
    ) -> Result<SubscriptionHandle<S, R>>
    where
        F: Fn(ChangeSet<R, S::Update>) + Send + Sync + 'static,
    {
        let sub_id = self
            .next_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let subscription = Arc::new(Subscription {
            id: sub_id,
            predicate: predicate.clone(),
            callback: Arc::new(Box::new(callback)),
        });
        // Start watching the relevant indexes
        self.add_index_watchers(sub_id, predicate);
        // Store subscription
        self.subscriptions.insert(sub_id, subscription.clone());

        // Find initial matching records
        let mut matching_records = self.storage.fetch_records(predicate);

        // iterate over the records and set the record watchers to reflect the currently matching records
        for record in matching_records.iter() {
            self.record_watchers
                .entry(record.id())
                .or_default()
                .push(sub_id.into());
        }
        // call the callback with the initial state
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
            id: sub_id.into(),
            reactor: self.clone(),
        })
    }

    fn recurse_predicate<F>(&self, sub_id: usize, predicate: &Predicate, f: F)
    where
        F: Fn(
                &mut dashmap::Entry<FieldId, ComparisonIndex>,
                &str,
                &ankql::ast::ComparisonOperator,
                SubscriptionId,
            ) + Copy,
    {
        use ankql::ast::{Expr, Identifier, Literal, Predicate};
        match predicate {
            Predicate::Comparison {
                left,
                operator,
                right,
            } => {
                // TODO handle reversed order
                if let (Expr::Identifier(field), Expr::Literal(literal)) = (&**left, &**right) {
                    let field_name = match field {
                        Identifier::Property(name) => name.clone(),
                        Identifier::CollectionProperty(_, name) => name.clone(),
                    };

                    // Convert literal to string representation
                    let value = match literal {
                        Literal::String(s) => s.clone(),
                        Literal::Integer(i) => i.to_string(),
                        Literal::Float(f) => f.to_string(),
                        Literal::Boolean(b) => b.to_string(),
                    };

                    // For add_index_watchers, this will create if not exists
                    // For remove_index_watchers, this will only get if exists
                    let mut entry = self.index_watchers.entry(FieldId(field_name));
                    f(&mut entry, &value, operator, &sub_id);
                }
            }
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                self.recurse_predicate(sub_id, left, f);
                self.recurse_predicate(sub_id, right, f);
            }
            Predicate::Not(pred) => {
                self.recurse_predicate(sub_id, pred, f);
            }
            Predicate::IsNull(_) => {}
        }
    }

    fn add_index_watchers(&self, sub_id: usize, predicate: &Predicate) {
        // First get the ComparisonIndex for the field
        self.recurse_predicate(sub_id, predicate, |entry, value, operator, sub_id| {
            // This gets called once for each comparison in the predicate, recursively

            match entry {
                dashmap::Entry::Occupied(entry) => {
                    // push the subscription id on to the vec
                    entry.get_mut().add_entry(value, operator.clone(), sub_id);
                }
                dashmap::Entry::Vacant(&mut entry) => {
                    entry
                        .insert(ComparisonIndex::new())
                        .add_entry(value, operator.clone(), sub_id);
                }
            };
        });
    }

    fn remove_index_watchers(&self, sub_id: usize, predicate: &Predicate) {
        self.recurse_predicate(sub_id, predicate, |entry, value, operator, sub_id| {
            if let dashmap::Entry::Occupied(entry) = entry {
                entry
                    .get_mut()
                    .remove_entry(value, operator.clone(), sub_id);
            }
        });
    }
}

#[derive(Debug)]
pub enum RecordChangeKind {
    Add,
    Remove,
    Edit,
}
/// Represents a change in the record set
pub struct RecordChange<R, U> {
    record: R,
    updates: Vec<U>,
    kind: RecordChangeKind,
}

/// A set of changes to the record set
pub struct ChangeSet<R, U> {
    changes: Vec<RecordChange<R, U>>,
    // other stuff later
}

/// A callback function that receives subscription updates
type Callback<R, U> = Box<dyn Fn(ChangeSet<R, U>) + Send + Sync + 'static>;

/// A subscription that can be shared between indexes
pub struct Subscription<R, U> {
    id: usize,
    predicate: ankql::ast::Predicate,
    callback: Arc<Callback<R, U>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(usize);
impl Deref for SubscriptionId {
    type Target = usize;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl PartialEq<usize> for SubscriptionId {
    fn eq(&self, other: &usize) -> bool {
        self.0 == *other
    }
}
impl From<usize> for SubscriptionId {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

/// A handle to a subscription that can be used to register callbacks
pub struct SubscriptionHandle<S, R>
where
    S: StorageEngine<R>,
{
    id: SubscriptionId,
    reactor: Arc<Reactor<S, R>>,
}

impl<S, R> SubscriptionHandle<S, R> where S: StorageEngine<R> {}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql;
    struct DummyEngine {}
    impl StorageEngine<()> for DummyEngine {
        type Id = usize;
        type Update = ();
        fn fetch_records(&self, _predicate: &Predicate) -> Vec<()> {
            vec![]
        }
    }
    pub fn test_watch_index() {
        let server = Arc::new(DummyEngine {});
        let mut reactor = Reactor::new(server);
        let predicate =
            ankql::parser::parse_selection("name = 'Alice' and age > 25 OR lattitude < 80")
                .unwrap();

        reactor.add_index_watchers(0, &predicate);

        println!("{:?}", reactor.index_watchers);

        // TODO
        // check to make sure that:
        // name has an eq watcher
        // age has a gt watcher
        // lattitude has a lt watcher
    }
}
