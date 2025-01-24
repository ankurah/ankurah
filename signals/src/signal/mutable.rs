use crate::{Read, core::Value, subscription::SubscriberSet};

/// Mutable (stateful) signal. We intentionally do not implement Subscribe for this signal type
pub struct Mut<T> {
    value: Value<T>,
    subscribers: SubscriberSet<T>,
}

impl<T> Mut<T> {
    pub fn new(value: T) -> Self { Self { value: Value::new(value), subscribers: SubscriberSet::new() } }

    pub fn set(&self, value: T) {
        println!("DEBUG: Setting new value");
        self.value.set_with(value, |value| {
            println!("DEBUG: Notifying subscribers of new value");
            self.subscribers.notify(value)
        })
    }

    /// Calls a closure with a borrow of the current value
    /// not tracked by the current context
    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R { self.value.with(f) }

    /// Readonly signal downstream of this mutable signal
    pub fn read(&self) -> Read<T> {
        let value = self.value.clone();
        let subscribers = self.subscribers.clone();
        Read { value, subscribers }
    }
}

impl<T> Mut<T>
where T: Clone
{
    /// Returns a clone of the current value - not tracked by the current context
    fn value(&self) -> T { self.value.value() }
}
