use crate::{changes::ChangeSet, entity::Entity, node::TNodeErased};
use ankurah_proto as proto;
use std::sync::{Arc, Mutex};
use tracing::debug;
/// A callback function that receives subscription updates
pub type Callback<R> = Box<dyn Fn(ChangeSet<R>) + Send + Sync + 'static>;

/// A subscription that can be shared between indexes
pub struct Subscription<R: Clone> {
    #[allow(unused)]
    pub(crate) id: proto::SubscriptionId,
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) predicate: ankql::ast::Predicate,
    pub(crate) callback: Arc<Callback<R>>,
    // Track which entities currently match this subscription
    // TODO make this a ResultSet so we can clone it cheaply
    pub(crate) matching_entities: Mutex<Vec<Entity>>,
    /// Whether this subscription has been initialized with its initial state
    pub(crate) initialized: std::sync::atomic::AtomicBool,
}

impl<R: Clone> std::fmt::Debug for Subscription<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subscription {{ id: {:?}, collection_id: {:?}, predicate: {:?} }}", self.id, self.collection_id, self.predicate)
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
