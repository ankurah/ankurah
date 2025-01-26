use std::sync::{Arc, RwLock};

use crate::{Read, subscription::SubscriberSet};

/// Mutable (stateful) signal. We intentionally do not implement Subscribe for this signal type
pub struct Mut<T> {
    value: Arc<RwLock<T>>,
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

    pub fn read(&self) -> Read<T> { Read { value: self.value.clone(), subscribers: self.subscribers.clone() } }
}

// impl<T> Stateful<T> for Mut<T> {
//     fn state(&self) -> Arc<RwLock<T>> { self.value.clone() }
// }

// Should Mut actually implement Value? We want business logic in the writer scope to be able to read it
// but we don't want it to be observable
// impl<T> Value<T> for Mut<T>
// where T: Clone
// {
//     fn value(&self) -> T { self.value.read().unwrap().clone() }
// }
// impl<T> WithValue<T> for Mut<T> {
//     fn with_value<R>(&self, f: impl FnOnce(&T) -> R) -> R {
//         let guard = self.value.read().unwrap();
//         f(&*guard)
//     }
// }
