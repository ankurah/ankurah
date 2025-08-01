use crate::{Read, core::Value, subscription::SubscriberSet};

/// Mutable (stateful) signal. We intentionally do not implement Subscribe for this signal type
#[derive(Clone)]
pub struct Mut<T> {
    value: Value<T>,
    subscribers: SubscriberSet<T>,
}

impl<T: 'static> Mut<T> {
    pub fn new(value: T) -> Self { Self { value: Value::new(value), subscribers: SubscriberSet::new() } }

    pub fn set(&self, value: T) {
        println!("Mut::set, pre set_with");
        self.value.set_with(value, |value| {
            println!("Mut::set, inside set_with closure");
            self.subscribers.notify(value)
        })
    }

    /// Calls a closure with a borrow of the current value
    /// not tracked by the current context
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R { self.value.with(f) }

    /// Returns a read-only version of this signal  
    pub fn read(&self) -> Read<T> { Read { value: self.value.clone(), subscribers: self.subscribers.clone() } }
}

impl<T> Mut<T>
where T: Clone
{
    /// Returns a clone of the current value - not tracked by the current context
    pub fn value(&self) -> T { self.value.value() }
}
