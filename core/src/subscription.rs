use crate::{changes::ChangeSet, model::Entity};
use ankurah_proto as proto;
use std::sync::{Arc, Mutex};

/// A callback function that receives subscription updates
pub type Callback<R> = Box<dyn Fn(ChangeSet<R>) + Send + Sync + 'static>;

/// A subscription that can be shared between indexes
pub struct Subscription<R: Clone, SE, PA> {
    #[allow(unused)]
    pub(crate) id: proto::SubscriptionId,
    pub(crate) collection_id: proto::CollectionId,
    pub(crate) predicate: ankql::ast::Predicate,
    pub(crate) callback: Arc<Callback<R>>,
    // Track which entities currently match this subscription
    // TODO make this a ResultSet so we can clone it cheaply
    pub(crate) matching_entities: Mutex<Vec<Arc<Entity<SE, PA>>>>,
}

/// A handle to a subscription that can be used to register callbacks
pub struct SubscriptionHandle {
    pub(crate) id: proto::SubscriptionId,
    pub(crate) reactor: Arc<crate::reactor::Reactor>,
}

impl SubscriptionHandle {
    pub fn new(reactor: Arc<crate::reactor::Reactor>, id: proto::SubscriptionId) -> Self { Self { id, reactor } }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) { self.reactor.unsubscribe(self.id); }
}
