//! Watcher utilities for tracking subscription notifications during benchmarks.

use ankurah::signals::broadcast::{BroadcastListener, IntoBroadcastListener};
use ankurah::signals::subscribe::IntoSubscribeListener;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

/// Lightweight watcher for benchmarking subscription notifications.
#[derive(Clone)]
pub struct BenchWatcher<T> {
    notification_count: Arc<AtomicUsize>,
    changes: Arc<Mutex<Vec<T>>>,
    notify: Arc<Notify>,
}

impl<T> BenchWatcher<T> {
    pub fn new() -> Self {
        Self { notification_count: Arc::new(AtomicUsize::new(0)), changes: Arc::new(Mutex::new(Vec::new())), notify: Arc::new(Notify::new()) }
    }

    /// Returns the total number of notifications received.
    pub fn notification_count(&self) -> usize { self.notification_count.load(Ordering::SeqCst) }

    /// Waits for at least `count` notifications to be received.
    pub async fn wait_for(&self, count: usize) {
        while self.notification_count() < count {
            self.notify.notified().await;
        }
    }

    /// Resets the watcher state.
    pub fn reset(&self) {
        self.notification_count.store(0, Ordering::SeqCst);
        self.changes.lock().unwrap().clear();
    }
}

impl<T: Send + 'static> IntoBroadcastListener<T> for &BenchWatcher<T> {
    fn into_broadcast_listener(self) -> BroadcastListener<T> {
        let notification_count = self.notification_count.clone();
        let changes = self.changes.clone();
        let notify = self.notify.clone();
        BroadcastListener::Payload(Arc::new(move |item: T| {
            notification_count.fetch_add(1, Ordering::SeqCst);
            changes.lock().unwrap().push(item);
            notify.notify_waiters();
        }))
    }
}

impl<T: Send + 'static> IntoSubscribeListener<T> for &BenchWatcher<T> {
    fn into_subscribe_listener(self) -> Box<dyn Fn(T) + Send + Sync> {
        let notification_count = self.notification_count.clone();
        let changes = self.changes.clone();
        let notify = self.notify.clone();
        Box::new(move |item: T| {
            notification_count.fetch_add(1, Ordering::SeqCst);
            changes.lock().unwrap().push(item);
            notify.notify_waiters();
        })
    }
}

impl<T> Default for BenchWatcher<T> {
    fn default() -> Self { Self::new() }
}

