use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::traits::{Notify, NotifyValue};

trait Unsubscriber<'a>: Send + Sync {
    fn unsubscribe(&self);
}

struct SetUnsubscriber<T> {
    set: Weak<SubscriberSetInner<T>>,
    id: SubscriptionId,
}

impl<'a, T> Unsubscriber<'a> for SetUnsubscriber<T> {
    fn unsubscribe(&self) {
        if let Some(set) = self.set.upgrade() {
            set.unsubscribe(self.id);
        }
    }
}

pub struct SubscriptionHandle<'a> {
    unsubscriber: Box<dyn Unsubscriber<'a> + 'a>,
}

impl<'a> Drop for SubscriptionHandle<'a> {
    fn drop(&mut self) { self.unsubscriber.unsubscribe(); }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubscriptionId(usize);

pub struct SubscriberSet<T>(Arc<SubscriberSetInner<T>>);

pub struct SubscriberSetInner<T> {
    active: Mutex<Vec<(SubscriptionId, Subscriber<T>)>>,
    pending: Mutex<Vec<(SubscriptionId, Subscriber<T>)>>,
    next_id: AtomicUsize,
}

impl<T> Clone for SubscriberSet<T> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

/// A value observer is an observer that wants to be notified of changes to a value with a borrow of the value
pub enum Subscriber<T> {
    Callback(Box<dyn Fn(&T) + Send + Sync>),
    Notify(Box<dyn Notify>),
    Value(Box<dyn NotifyValue<T>>),
}

impl<T, F> From<F> for Subscriber<T>
where F: Fn(&T) + Send + Sync + 'static
{
    fn from(f: F) -> Self { Subscriber::Callback(Box::new(f)) }
}

impl<T> Into<Subscriber<T>> for Box<dyn Notify> {
    fn into(self) -> Subscriber<T> { Subscriber::Notify(self) }
}

impl<T> SubscriberSet<T> {
    pub fn new() -> Self {
        Self(Arc::new(SubscriberSetInner { active: Mutex::new(Vec::new()), pending: Mutex::new(Vec::new()), next_id: AtomicUsize::new(0) }))
    }
}

impl<T> std::ops::Deref for SubscriberSet<T> {
    type Target = Arc<SubscriberSetInner<T>>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl<T> SubscriberSetInner<T> {
    pub fn subscribe<'a, S: Into<Subscriber<T>>>(self: &'a Arc<Self>, subscriber: S) -> SubscriptionHandle<'a> {
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        match self.active.try_lock() {
            Ok(mut active) => {
                active.push((id, subscriber.into()));
            }
            Err(_) => {
                self.pending.lock().unwrap().push((id, subscriber.into()));
            }
        }
        SubscriptionHandle { unsubscriber: Box::new(SetUnsubscriber { set: Arc::downgrade(self), id }) }
    }
    pub fn unsubscribe(self: &Arc<Self>, id: SubscriptionId) {
        let mut active = self.active.lock().unwrap();
        active.retain(|(id, _)| id != id);
    }

    pub fn notify(self: &Arc<Self>, value: &T) {
        let mut active = self.active.lock().unwrap();

        // Notify all current subscribers
        for (id, subscriber) in active.iter() {
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
