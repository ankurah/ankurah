use crate::{
    Read, Stateful, Value, WithValue,
    observer::{ObserverSet, ValueObserverSet},
};
use std::sync::{Arc, RwLock};

/// Mutable (stateful) signal. We intentionally do not implement Subscribe for this signal type
pub struct Mut<T> {
    value: Arc<RwLock<T>>,
    observers: ObserverSet,
    value_observers: ValueObserverSet<T>,
}

impl<T: Send> Mut<T> {
    pub fn new(value: T) -> Self {
        //
        Self { value: Arc::new(RwLock::new(value)), observers: ObserverSet::new(), value_observers: ValueObserverSet::new() }
    }

    pub fn set(&self, value: T) {
        {
            let mut current = self.value.write().unwrap();
            *current = value;
        }

        let guard = self.value.read().unwrap();
        self.value_observers.notify(&*guard);
        self.observers.notify();
    }

    pub fn read(&self) -> Read<T> { Read::new(self) }
}

impl<T> Stateful<T> for Mut<T> {
    fn state(&self) -> Arc<RwLock<T>> { self.value.clone() }
}

// Should Mut actually implement Value? We want business logic in the writer scope to be able to read it
// but we don't want it to be observable
impl<T> Value<T> for Mut<T>
where T: Clone
{
    fn value(&self) -> T { self.value.read().unwrap().clone() }
}
impl<T> WithValue<T> for Mut<T> {
    fn with_value<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let guard = self.value.read().unwrap();
        f(&*guard)
    }
}
