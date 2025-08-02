use crate::{Read, Signal, core::Value};

#[derive(Clone)]
pub struct Mut<T> {
    value: Value<T>,
    broadcast: tokio::sync::broadcast::Sender<()>,
}

impl<T: 'static> Mut<T> {
    pub fn new(value: T) -> Self {
        let (broadcast, _) = tokio::sync::broadcast::channel(1);
        Self { value: Value::new(value), broadcast }
    }

    pub fn set(&self, value: T) {
        self.value.set(value);
        // Ignore send errors - just means no receivers, which is fine
        let _ = self.broadcast.send(());
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
