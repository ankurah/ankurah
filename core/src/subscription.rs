use crate::{changes::ChangeSet, model::RecordInner};
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

/// A callback function that receives subscription updates
pub type Callback = Box<dyn Fn(ChangeSet) + Send + Sync + 'static>;

/// A subscription that can be shared between indexes
pub struct Subscription {
    pub(crate) id: SubscriptionId,
    pub(crate) predicate: ankql::ast::Predicate,
    pub(crate) callback: Arc<Callback>,
    // Track which records currently match this subscription
    pub(crate) matching_records: Mutex<Vec<Arc<RecordInner>>>,
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
pub struct SubscriptionHandle {
    pub(crate) id: SubscriptionId,
    pub(crate) reactor: Arc<crate::reactor::Reactor>,
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        self.reactor.unsubscribe(self.id);
    }
}
