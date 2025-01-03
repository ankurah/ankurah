use crate::{changes::ChangeSet, model::RecordInner};
use ankurah_proto as proto;
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

/// A callback function that receives subscription updates
pub type Callback = Box<dyn Fn(ChangeSet) + Send + Sync + 'static>;

/// A subscription that can be shared between indexes
pub struct Subscription {
    pub(crate) id: proto::SubscriptionId,
    pub(crate) predicate: ankql::ast::Predicate,
    pub(crate) callback: Arc<Callback>,
    // Track which records currently match this subscription
    pub(crate) matching_records: Mutex<Vec<Arc<RecordInner>>>,
}

/// A handle to a subscription that can be used to register callbacks
pub struct SubscriptionHandle {
    pub(crate) id: proto::SubscriptionId,
    pub(crate) reactor: Arc<crate::reactor::Reactor>,
}

impl SubscriptionHandle {
    pub fn new(reactor: Arc<crate::reactor::Reactor>, id: proto::SubscriptionId) -> Self {
        Self { id, reactor }
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        self.reactor.unsubscribe(self.id);
    }
}
