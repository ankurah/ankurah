use std::sync::{Arc, RwLock};

use crate::{
    core::{CurrentContext, Value},
    subscription::{Subscriber, SubscriberSet, SubscriptionHandle},
    traits::{Get, Signal},
};

/// Read-only signal
pub struct Read<T> {
    pub(crate) value: Value<T>,
    pub(crate) subscribers: SubscriberSet<T>,
}

impl<T> Read<T> {
    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        CurrentContext::track(self);
        self.value.with(f)
    }
}

impl<T> Clone for Read<T> {
    fn clone(&self) -> Self { Self { value: self.value.clone(), subscribers: self.subscribers.clone() } }
}

impl<T: Clone> Get<T> for Read<T> {
    fn get(&self) -> T {
        CurrentContext::track(self);
        self.value.value()
    }
}

impl<T> Signal<T> for Read<T> {
    fn subscribe<S: Into<Subscriber<T>>>(&self, subscriber: S) -> SubscriptionHandle { self.subscribers.subscribe(subscriber) }
}

impl<T: std::fmt::Display> std::fmt::Display for Read<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.with(|v| write!(f, "{}", v)) }
}
