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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubscriptionId(usize);

#[derive(Default)]
pub struct SubscriberSet<T>(Arc<RwLock<BTreeMap<SubscriptionId, Subscriber<T>>>>);

impl<T> Clone for SubscriberSet<T> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

/// A value observer is an observer that wants to be notified of changes to a value with a borrow of the value
pub enum Subscriber<T> {
    Callback(Box<dyn Fn(&T) + Send + Sync>),
    Notify(Weak<dyn Notify>),
    Value(Weak<dyn NotifyValue<T>>),
}

impl<T> Into<Subscriber<T>> for Box<dyn Fn(&T) + Send + Sync> {
    fn into(self) -> Subscriber<T> { Subscriber::Callback(self) }
}

impl<T> Into<Subscriber<T>> for Box<dyn Notify> {
    fn into(self) -> Subscriber<T> { Subscriber::Notify(Arc::downgrade(&Arc::from(self))) }
}

impl<T> Into<Subscriber<T>> for Arc<dyn Notify> {
    fn into(self) -> Subscriber<T> { Subscriber::Notify(Arc::downgrade(&self)) }
}

impl<T> From<&Arc<dyn Notify>> for Subscriber<T> {
    fn from(notify: &Arc<dyn Notify>) -> Self { Subscriber::Notify(Arc::downgrade(notify)) }
}

impl<T> PartialEq for Subscriber<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Subscriber::Callback(_), Subscriber::Callback(_)) => true,
            (Subscriber::Notify(a), Subscriber::Notify(b)) => Weak::ptr_eq(a, b),
            (Subscriber::Value(a), Subscriber::Value(b)) => Weak::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl<T> SubscriberSet<T> {
    pub fn new() -> Self { Self(Arc::new(RwLock::new(BTreeMap::new()))) }

    pub fn subscribe<S: Into<Subscriber<T>>>(&self, subscriber: S) -> SubscriptionHandle {
        let mut subscribers = self.0.write().unwrap();
        let id = SubscriptionId(subscribers.len());
        println!("DEBUG: Adding subscription with id {}", id.0);
        subscribers.insert(id, subscriber.into());
        SubscriptionHandle { id }
    }

    pub fn notify(&self, value: &T) {
        println!("DEBUG: Notifying subscribers");
        let subscribers = self.0.read().unwrap();
        for (id, subscriber) in subscribers.iter() {
            println!("DEBUG: Notifying subscriber {}", id.0);
            match subscriber {
                Subscriber::Callback(callback) => callback(value),
                Subscriber::Notify(notify) => {
                    println!("DEBUG: Attempting to upgrade weak reference for subscriber {}", id.0);
                    if let Some(notify) = notify.upgrade() {
                        println!("DEBUG: Successfully upgraded weak reference for subscriber {}", id.0);
                        notify.notify();
                    } else {
                        println!("DEBUG: Failed to upgrade weak reference for subscriber {}", id.0);
                    }
                }
                Subscriber::Value(notify) => {
                    if let Some(notify) = notify.upgrade() {
                        notify.notify(value);
                    }
                }
            }
        }
    }
}
