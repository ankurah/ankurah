use std::sync::Arc;

use crate::{
    broadcast::Broadcast,
    context::CurrentObserver,
    porcelain::{Subscribe, SubscriptionGuard, subscribe::IntoSubscribeListener},
    signal::{Get, GetReadCell, Signal, With, read::Read},
    value::{ReadValueCell, ValueCell},
};

#[derive(Clone)]
pub struct Mut<T> {
    value: ValueCell<T>,
    broadcast: Broadcast<()>,
}

impl<T: 'static> Mut<T> {
    pub fn new(value: T) -> Self {
        let broadcast = Broadcast::new();
        Self { value: ValueCell::new(value), broadcast }
    }

    pub fn set(&self, value: T) {
        self.value.set(value);
        // Notify all listeners
        self.broadcast.send(());
    }

    /// Calls a closure with a borrow of the current value
    /// not tracked by the current context
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R { self.value.with(f) }

    /// Returns a read-only version of this signal  
    pub fn read(&self) -> Read<T> { Read { value: self.value.clone(), broadcast: self.broadcast.clone() } }
}

// impl<T: 'static> Signal<T> for Mut<T> {
//     fn subscribe<S: Into<Subscriber<T>>>(&self, subscriber: S) -> SubscriptionGuard { self.subscribers.subscribe(subscriber) }
// }

impl<T> Mut<T>
where T: Clone
{
    /// Returns a clone of the current value - not tracked by the current context
    pub fn value(&self) -> T { self.value.value() }
}

impl<T: Clone + 'static> Get<T> for Mut<T> {
    fn get(&self) -> T { self.value.value() }
}

impl<T: 'static> With<T> for Mut<T> {
    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        CurrentObserver::track(self);
        self.value.with(f)
    }
}

impl<T: 'static> GetReadCell<T> for Mut<T> {
    fn get_readcell(&self) -> ReadValueCell<T> { self.value.readvalue() }
}

impl<T> Signal for Mut<T> {
    fn listen(&self, listener: crate::broadcast::Listener<()>) -> crate::broadcast::ListenerGuard<()> {
        self.broadcast.reference().listen(listener)
    }

    fn broadcast_id(&self) -> crate::broadcast::BroadcastId { self.broadcast.id() }
}

impl<T> Subscribe<T> for Mut<T>
where T: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<T> {
        let listener = listener.into_subscribe_listener();
        let ro_value = self.get_readcell(); // Get read-only value handle
        let subscription = self.listen(Arc::new(move |_| {
            // Get current value when the broadcast fires
            let current_value = ro_value.value();
            listener(current_value);
        }));
        SubscriptionGuard::new(subscription)
    }
}
