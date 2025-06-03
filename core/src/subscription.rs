use crate::util::onetimeflag::OneTimeFlag;
use crate::{changes::ChangeSet, entity::Entity, error::RetrievalError, node::TNodeErased};
use ankurah_proto as proto;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::debug;

use crate::changes::ItemChange;
use crate::resultset::ResultSet;

/// A callback function that receives subscription updates
pub type Callback<R> = Box<dyn Fn(ChangeSet<R>) + Send + Sync + 'static>;

pub struct SubscriptionInner<T> {
    pub id: proto::SubscriptionId,
    pub collection_id: proto::CollectionId,
    pub predicate: ankql::ast::Predicate,
    pub on_change: Arc<Box<dyn Fn(ChangeSet<T>) + Send + Sync>>,
    /// Entities that presently match the subscription
    /// TODO - consider making this a ResultSet so we can clone it cheaply
    pub matching_entities: std::sync::Mutex<Vec<T>>,
    /// True if the initial data has been sent to the callback
    pub initial_data_sent: AtomicBool,
    /// Set when initial loading has completed (successfully or with error)
    pub loaded: OneTimeFlag,
    /// Set if there was an error during loading
    pub load_error: std::sync::Mutex<Option<RetrievalError>>,
}

/// A subscription to a collection of entities
#[derive(Clone)]
pub struct Subscription<T>(Arc<SubscriptionInner<T>>);

impl<T> std::ops::Deref for Subscription<T> {
    type Target = Arc<SubscriptionInner<T>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl<T: Clone> Subscription<T> {
    pub fn new(
        id: proto::SubscriptionId,
        collection_id: proto::CollectionId,
        predicate: ankql::ast::Predicate,
        on_change: Arc<Box<dyn Fn(ChangeSet<T>) + Send + Sync>>,
    ) -> Self {
        Self(Arc::new(SubscriptionInner {
            id,
            collection_id,
            predicate,
            on_change,
            matching_entities: std::sync::Mutex::new(Vec::new()),
            initial_data_sent: AtomicBool::new(false),
            loaded: OneTimeFlag::new(),
            load_error: std::sync::Mutex::new(None),
        }))
    }

    /// Load initial data for the subscription using the provided retriever
    pub async fn load(&self, retriever: impl crate::retrieve::Fetch<T>) -> Result<(), crate::error::RetrievalError> {
        debug!("Loading subscription {} for collection {}", self.id, self.collection_id);

        // Retrieve initial entities using the retriever
        let entities = retriever.fetch(&self.collection_id, &self.predicate).await?;

        debug!("Subscription {} loaded {} initial entities", self.id, entities.len());

        // Convert to ItemChange::Initial for each entity
        let initial_changes: Vec<ItemChange<T>> = entities.into_iter().map(|entity| ItemChange::Initial { item: entity }).collect();

        // Store the entities in matching_entities
        {
            let initial_entities: Vec<T> = initial_changes
                .iter()
                .map(|change| match change {
                    ItemChange::Initial { item } => item.clone(),
                    _ => unreachable!("All changes should be Initial at this point"),
                })
                .collect();
            *self.matching_entities.lock().unwrap() = initial_entities.clone();
        }

        // Send initial notification
        (self.on_change)(ChangeSet {
            resultset: ResultSet { loaded: true, items: self.matching_entities.lock().unwrap().clone() },
            changes: initial_changes,
            initial: true,
        });

        // Mark that initial data has been sent
        self.initial_data_sent.store(true, std::sync::atomic::Ordering::SeqCst);

        debug!("Subscription {} initial load complete", self.id);
        Ok(())
    }

    /// Mark initialization as completed
    pub fn initialization_completed(&self) {
        self.loaded.set();
        self.check_and_send_initial();
    }

    /// Check if we should send initial data and do so if conditions are met
    fn check_and_send_initial(&self) {
        let should_send = self.loaded.is_set();

        if should_send && !self.initial_data_sent.load(Ordering::SeqCst) {
            // Mark as sent first to prevent duplicate sends
            if self.initial_data_sent.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                self.send_initial_data();
            }
        }
    }

    fn send_initial_data(&self) {
        let matching_entities = self.matching_entities.lock().unwrap().clone();

        (self.on_change)(ChangeSet {
            changes: matching_entities.iter().map(|entity| ItemChange::Initial { item: entity.clone() }).collect(),
            resultset: ResultSet { loaded: true, items: matching_entities },
            initial: true,
        });
    }
}

impl<T> std::fmt::Debug for Subscription<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscription {{ id: {}, collection_id: {}, predicate: {} }}", self.id, self.collection_id, self.predicate)
    }
}

/// A handle to a subscription that can be used to register callbacks
pub struct SubscriptionHandle {
    pub(crate) id: proto::SubscriptionId,
    pub(crate) node: Box<dyn TNodeErased>,
    pub(crate) peers: Vec<proto::EntityId>,
}

impl SubscriptionHandle {
    pub fn new(node: Box<dyn TNodeErased>, id: proto::SubscriptionId) -> Self { Self { id, node, peers: Vec::new() } }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        debug!("Dropping SubscriptionHandle {}", self.id);
        self.node.unsubscribe(self);
    }
}

impl std::fmt::Debug for SubscriptionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "SubscriptionHandle({:?})", self.id) }
}
