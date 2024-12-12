use super::comparision_index::ComparisonIndex;
use super::operation::Operation;
use ankql::ast::Predicate;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, Weak};

pub struct FieldId(String);

pub struct Reactor<S, R>
where
    S: Server<R>,
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
    server: Weak<S>,
}

pub(crate) trait Server<R> {
    type Id: std::hash::Hash + std::cmp::Eq;
    type Update;
    fn fetch_records(&self, predicate: &Predicate) -> Vec<(usize, R)>;
}

impl<S, R> Reactor<S, R>
where
    S: Server<R>,
{
    pub fn new(server: &Arc<S>) -> Self {
        Self {
            subscriptions: HashMap::new(),
            index_watchers: HashMap::new(),
            record_watchers: HashMap::new(),
            next_sub_id: AtomicUsize::new(0),
            server: Arc::downgrade(server),
        }
    }
    pub fn subscribe<F>(&self, predicate: &Predicate, callback: F) -> Result<SubscriptionHandle<S, R>, Error>
    where
        F: Fn(Operation<R, S::Update>, Vec<R>) + Send + Sync + 'static,
    {
        let sub_id = self
            .next_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);


        // Store subscription
        self.subscriptions.insert(sub_id, Subscription {
            id: sub_id,
            predicate: predicate.clone(),
            callbacks: Arc::new(Box::new(callback)),
        });

        // Find initial matching records
        let mut matching_records = self.server.fetch_records(predicate);

        
        
        subscription.matching_records.lock().unwrap();
        for (id, record) in &self.server.records {
            if FilterIterator::new(vec![record.clone()].into_iter(), self.predicate.clone())
                .next()
                .map(|r| matches!(r, FilterResult::Pass(_)))
                .unwrap_or(false)
            {
                matching_records.insert(*id);
            }
        }

        // Send initial state
        let initial_records: Vec<_> = matching_records
            .iter()
            .filter_map(|id| self.server.records.get(id))
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
}

/// A callback function that receives subscription updates
type Callback<R, U> = Box<dyn Fn(Operation<R, U>, Vec<R>) + Send + Sync + 'static>;

/// A subscription that can be shared between indexes
pub struct Subscription<R, U> {
    id: usize,
    predicate: ankql::ast::Predicate,
    callbacks: Arc<Callback<R, U>>>,
    // fields: HashSet<String>, // Fields this subscription depends ony
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
    S: Server<R>,
{
    id: SubscriptionId,
    // Need a weak reference to the reactor so that we can remove the subscription when it is dropped
    reactor: Weak<Reactor<S, R>>,
}

impl<S, R> SubscriptionHandle<S, R>
where
    S: Server<R>,
{
    pub fn new(reactor: Weak<Reactor<S, R>>) -> Self {
        Self { id: SubscriptionId(0), reactor }
    }
}
