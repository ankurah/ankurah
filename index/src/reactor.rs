use super::comparision_index::ComparisonIndex;
use super::operation::Operation;
use ankql::ast::Predicate;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, Weak};

pub struct FieldId(String);

/// A Reactor is a collection of subscriptions, which are to be notified of changes to a set of records
pub struct Reactor<S, R>
where
    S: StorageEngine<R>,
{
    /// Current subscriptions
    subscriptions: HashMap<usize, Subscription<R, S::Update>>,
    /// Each field has a ComparisonIndex so we can quickly find all subscriptions that care if a given value CHANGES (creation and deletion also count as changes)
    index_watchers: HashMap<FieldId, ComparisonIndex>,
    /// Index of subscriptions that presently match each record.
    /// This is used to quickly find all subscriptions that need to be notified when a record changes.
    /// We have to maintain this to add and remove subscriptions when their matching state changes.
    record_watchers: HashMap<S::Id, Vec<SubscriptionId>>,
    /// Next subscription id
    next_sub_id: AtomicUsize,
    /// Weak reference to the server, which we need to perform the initial subscription setup
    storage: Arc<S>,
}

pub(crate) trait StorageEngine<R> {
    type Id: std::hash::Hash + std::cmp::Eq;
    type Update;
    fn fetch_records(&self, predicate: &Predicate) -> Vec<R>;
}
pub(crate) trait Record<R> {
    type Id: std::hash::Hash + std::cmp::Eq + Copy + Clone;
    fn id(&self) -> Self::Id;
}

impl<S, R> Reactor<S, R>
where
    S: StorageEngine<R>,
{
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            subscriptions: HashMap::new(),
            index_watchers: HashMap::new(),
            record_watchers: HashMap::new(),
            next_sub_id: AtomicUsize::new(0),
            storage,
        }
    }
    pub fn subscribe<F>(
        &self,
        predicate: &Predicate,
        callback: F,
    ) -> Result<SubscriptionHandle<S, R>>
    where
        F: Fn(Operation<R, S::Update>, Vec<R>) + Send + Sync + 'static,
    {
        let sub_id = self
            .next_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        self.add_index_watchers(sub_id, predicate);
        // LEFT OFF HERE

        // Store subscription
        self.subscriptions.insert(sub_id, Subscription {
            id: sub_id,
            predicate: predicate.clone(),
            callbacks: Arc::new(Box::new(callback)),
        });

        // Find initial matching records
        let mut matching_records = self.storage.fetch_records(predicate);
        // Send initial state
        let initial_records: Vec<_> = matching_records
            .iter()
            .filter_map(|id| self.storage.fetch_records(predicate).get(id))
            .cloned()
            .collect();

        if !initial_records.is_empty() {
            for callback in subscription.callbacks.iter() {
                callback(
                    Op::Add {
                        id: *matching_records.iter().next().unwrap(),
                        record: initial_records[0].clone(),
                        updates: vec![],
                    },
                    initial_records.clone(),
                );
            }
        }

        self
    }

    fn recurse_predicate<F>(&mut self, sub_id: usize, predicate: &Predicate, f: F)
    where
        F: Fn(&mut ComparisonIndex, &str, &ankql::ast::ComparisonOperator, SubscriptionId) + Copy,
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
                    if let Some(index) = self.index_watchers.get_mut(&FieldId(field_name)) {
                        f(index, &value, operator, sub_id.into());
                    }
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

    fn add_index_watchers(&mut self, sub_id: usize, predicate: &Predicate) {
        self.recurse_predicate(sub_id, predicate, |index, value, operator, sub_id| {
            index.add_entry(value, operator.clone(), sub_id);
        });
    }

    fn remove_index_watchers(&mut self, sub_id: usize, predicate: &Predicate) {
        self.recurse_predicate(sub_id, predicate, |index, value, operator, sub_id| {
            index.remove_entry(value, operator.clone(), sub_id);
        });
    }
}

/// A callback function that receives subscription updates
type Callback<R, U> = Box<dyn Fn(Operation<R, U>, Vec<R>) + Send + Sync + 'static>;

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

impl<S, R> SubscriptionHandle<S, R>
where
    S: StorageEngine<R>,
{
    pub fn new(reactor: Weak<Reactor<S, R>>) -> Self {
        Self {
            id: SubscriptionId(0),
            reactor,
        }
    }
}
