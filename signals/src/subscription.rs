use std::collections::BTreeMap;
use std::sync::{Arc, RwLock, Weak};

use crate::traits::{Notify, NotifyValue};

pub struct SubscriptionHandle {
    id: SubscriptionId,
}

impl std::ops::Drop for SubscriptionHandle {
    fn drop(&mut self) {
        // unsubscribe
    }
}

pub struct SubscriptionId(usize);
#[derive(Default, Clone)]
pub struct SubscriberSet<T>(Arc<RwLock<BTreeMap<SubscriptionId, Subscriber<T>>>>);

/// A value observer is an observer that wants to be notified of changes to a value with a borrow of the value
pub enum Subscriber<T> {
    Callback(Box<dyn Fn(&T)>),
    Notify(Weak<dyn Notify>),
    Value(Weak<dyn NotifyValue<T>>),
    // So we can unsubscribe a nested subscriber set when the child is dropped
    // Nested(SubscriberSet<T>),
}

impl<T> Into<Subscriber<T>> for Box<dyn Fn(&T)> {
    fn into(self) -> Subscriber<T> { Subscriber::Callback(self) }
}

// any type that implements Notify can be used as a subscriber
impl<T> Into<Subscriber<T>> for Box<dyn Notify> {
    fn into(self) -> Subscriber<T> { Subscriber::Notify(Arc::downgrade(&self)) }
}

impl<T> PartialEq for Subscriber<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Subscriber::Callback(_), Subscriber::Callback(_)) => true,
            (Subscriber::Notify(a), Subscriber::Notify(b)) => Weak::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl<T> SubscriberSet<T> {
    pub(crate) fn new() -> Self { Self(Arc::new(RwLock::new(Vec::new()))) }
    pub fn subscribe(&self, subscriber: Subscriber<T>) {
        let mut subscribers = self.0.write().unwrap();
        if !subscribers.contains(&subscriber) {
            subscribers.push(subscriber);
        }
    }
    // pub fn unsubscribe(&self, subscriber: &Subscriber<T>) {
    // not sure how we want to handle unsusubscribing a closure. Maybe take a subscriber handle instead of a Subscriber<T> ?
    //     let mut subscribers = self.0.write().unwrap();
    //     subscribers.remove(subscriber);
    // }
    pub(crate) fn notify(&self, value: &T) {
        let observers = self.0.read().unwrap();
        for observer in observers.iter() {
            match observer {
                Subscriber::Callback(f) => f(value),
                Subscriber::Notify(w) => {
                    if let Some(n) = w.upgrade() {
                        n.notify();
                    }
                }
                Subscriber::Nested(nested) => nested.notify(value),
            }
        }
    }
}
