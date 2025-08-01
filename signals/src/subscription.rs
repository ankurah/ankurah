use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::traits::{Notify, NotifyValue};

pub struct SubscriptionGuard {
    unsubscribe_fn: Box<dyn Fn() + Send + Sync>,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) { (self.unsubscribe_fn)(); }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(usize);

pub struct SubscriberSet<T>(Arc<SubscriberSetInner<T>>);

pub struct SubscriberSetInner<T> {
    active: std::sync::Mutex<HashMap<SubscriptionId, Subscriber<T>>>,
    pending: std::sync::Mutex<Vec<(SubscriptionId, Subscriber<T>)>>,
    next_id: AtomicUsize,
}

impl<T> Clone for SubscriberSet<T> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

/// A value observer is an observer that wants to be notified of changes to a value with a borrow of the value
/// TODO - can we think of a way to remove the Send + Sync bounds?
pub enum Subscriber<T> {
    Callback(Box<dyn Fn(&T) + Send + Sync>),
    Notify(Box<dyn Notify + Send + Sync>),
    Value(Box<dyn NotifyValue<T> + Send + Sync>),
}

impl<T, F> From<F> for Subscriber<T>
where
    F: Fn(&T) + Send + Sync + 'static,
    T: 'static,
{
    fn from(f: F) -> Self { Subscriber::Callback(Box::new(f)) }
}

impl<T> Into<Subscriber<T>> for Box<dyn Notify + Send + Sync> {
    fn into(self) -> Subscriber<T> { Subscriber::Notify(self) }
}

impl<T> From<tokio::sync::mpsc::UnboundedSender<()>> for Subscriber<T> {
    fn from(sender: tokio::sync::mpsc::UnboundedSender<()>) -> Self { Subscriber::Notify(Box::new(sender)) }
}

impl<T: 'static> SubscriberSet<T> {
    pub fn new() -> Self {
        Self(Arc::new(SubscriberSetInner {
            active: std::sync::Mutex::new(HashMap::new()),
            pending: std::sync::Mutex::new(Vec::new()),
            next_id: AtomicUsize::new(0),
        }))
    }
}

impl<T> std::ops::Deref for SubscriberSet<T> {
    type Target = Arc<SubscriberSetInner<T>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl<T: 'static> SubscriberSetInner<T> {
    pub fn subscribe<S: Into<Subscriber<T>>>(self: &Arc<Self>, subscriber: S) -> SubscriptionGuard {
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        match self.active.try_lock() {
            Ok(mut active) => {
                active.insert(id, subscriber.into());
            }
            Err(_) => {
                // TODO - document why the pending set is needed. I faintly recall that
                // it had to do with subscribing during a notification and avoiding a deadlock
                // but it's been a while and the approach needs to be revalidated.
                self.pending.lock().unwrap().push((id, subscriber.into()));
            }
        }

        // Create closure that captures weak reference and ID
        let weak_set = Arc::downgrade(self);
        let unsubscribe_closure = move || {
            if let Some(set) = weak_set.upgrade() {
                set.active.lock().unwrap().remove(&id);
            }
        };

        SubscriptionGuard { unsubscribe_fn: Box::new(unsubscribe_closure) }
    }

    pub fn notify(self: &Arc<Self>, value: &T) {
        let mut active = self.active.lock().unwrap();

        // Notify all current subscribers
        for (_id, subscriber) in active.iter() {
            match subscriber {
                Subscriber::Callback(callback) => callback(value),
                Subscriber::Notify(notify) => notify.notify(),
                Subscriber::Value(notify) => notify.notify(value),
            }
        }

        // Merge in any pending subscribers - no need for read check
        active.extend(self.pending.lock().unwrap().drain(..));
    }
}
