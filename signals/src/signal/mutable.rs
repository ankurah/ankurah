use std::sync::{Arc, RwLock};

use crate::{Read, core::Value, subscription::SubscriberSet, traits::Signal};

/// Mutable (stateful) signal. We intentionally do not implement Subscribe for this signal type
pub struct Mut<T> {
    value: Value<T>,
    subscribers: SubscriberSet<T>,
}

impl<T: Send> Mut<T> {
    pub fn new(value: T) -> Self {
        // nothing else to initialize
        Self { value: Arc::new(RwLock::new(value)), subscribers: SubscriberSet::new() }
    }

    pub fn set(&self, value: T) {
        {
            let mut current = self.value.write().unwrap();
            *current = value;
        }

        let guard = self.value.read().unwrap();
        self.subscribers.notify(&*guard);
    }

    /// Readonly signal downstream of this mutable signal
    pub fn read(&self) -> Read<T> { Read { value: self.value.clone(), subscribers: self.subscribers.clone() } }

    /// Calls a closure with a borrow of the current value
    /// not tracked by the current context
    fn with_value<R>(&self, f: impl FnOnce(&T) -> R) -> R { self.value.with(f) }
}

impl<T> Mut<T>
where T: Clone
{
    /// Returns a clone of the current value - not tracked by the current context
    fn value(&self) -> T { self.value.read().unwrap().clone() }
}
