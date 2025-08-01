use std::sync::{Arc, RwLock, Weak};

use crate::{
    CurrentContext, SubscriptionGuard,
    subscription::Subscriber,
    traits::{Notify, Observer},
};

/// A CallbackObserver is an observer that wraps a callback which is called
/// whenver the observed signals notify the observer of a change.
pub struct CallbackObserver(Arc<CallbackObserverInner>);
pub struct CallbackObserverInner {
    // The callback to call when the observed signals notify the observer of a change
    callback: Box<dyn Fn() + Send + Sync>,
    // The subscription handles to keep the subscriptions alive
    subscription_handles: RwLock<Vec<SubscriptionGuard>>,
}

impl CallbackObserver {
    /// Create a new callback observer
    pub fn new<F: Fn() + Send + Sync + 'static>(callback: Arc<F>) -> Self {
        Self(Arc::new(CallbackObserverInner { callback: Box::new(move || callback()), subscription_handles: RwLock::new(vec![]) }))
    }
}

impl std::ops::Deref for CallbackObserver {
    type Target = Arc<CallbackObserverInner>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Clone for CallbackObserver {
    fn clone(&self) -> Self { CallbackObserver(Arc::clone(&self.0)) }
}

impl CallbackObserverInner {
    /// Trigger the callback using this observer's context
    pub fn trigger(self: &Arc<Self>) { self.with_context(&self.callback); }

    /// Execute a function with this observer as the current context
    pub fn with_context<F: Fn()>(self: &Arc<Self>, f: &F) {
        CurrentContext::set(CallbackObserver(Arc::clone(&self)));
        f();
        CurrentContext::unset();
    }

    pub fn subscriber<T>(self: &Arc<Self>) -> Subscriber<T> { Subscriber::Notify(Box::new(Arc::downgrade(self))) }
    pub fn clear(self: &Arc<Self>) { self.subscription_handles.write().unwrap().clear(); }
    pub fn store_handle(self: &Arc<Self>, handle: SubscriptionGuard) { self.subscription_handles.write().unwrap().push(handle); }
    pub fn clone(self: &Arc<Self>) -> CallbackObserver { CallbackObserver(Arc::clone(&self)) }
}

impl Notify for Weak<CallbackObserverInner> {
    fn notify(&self) {
        if let Some(inner) = self.upgrade() {
            inner.trigger();
        }
    }
}

// SignalObserver implementation
impl Observer for CallbackObserver {
    fn get_notifier(&self) -> Box<dyn Notify + Send + Sync> { Box::new(Arc::downgrade(&self.0)) }

    fn store_handle(&self, handle: SubscriptionGuard) { self.0.store_handle(handle); }
}
