use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Weak};

/// A listener that can be called when broadcast notifications are sent.
pub type Listener = Arc<dyn Fn() + Send + Sync + 'static>;

/// Trait for types that can be converted into broadcast listeners.
pub trait IntoListener {
    /// Convert this type into a listener function that can be called on notifications.
    fn into_listener(self) -> Listener;
}

/// A broadcast sender that notifies multiple subscribers without payload data.
/// Uses synchronous function callbacks for immediate notification.
#[derive(Clone)]
pub struct Broadcast(Arc<Inner>);
struct Inner {
    listeners: std::sync::RwLock<HashMap<usize, Listener>>,
    next_id: AtomicUsize,
}

/// A listen-only reference to a broadcast
pub struct Ref<'a>(&'a Broadcast);

/// A subscription handle that can be used to unsubscribe from notifications.
pub struct ListenerGuard {
    inner: Weak<Inner>,
    id: usize,
}

impl Broadcast {
    /// Creates a new Broadcast struct
    pub fn new() -> Self { Self(Arc::new(Inner { listeners: std::sync::RwLock::new(HashMap::new()), next_id: AtomicUsize::new(0) })) }

    /// Sends a notification to all active listeners
    pub fn send(&self) {
        // Clone the listeners to avoid holding the lock during callback execution
        let subscribers = {
            // maybe someday we can avoid the alloc here using a thread-local buffer?
            let listeners = self.0.listeners.read().unwrap();
            listeners.values().cloned().collect::<Vec<_>>()
        };

        // Call all listeners without holding any locks
        for callback in subscribers {
            callback();
        }
    }

    /// Get a read-only reference to this sender that can only subscribe to notifications.
    /// This avoids cloning the sender while still forbidding the user from sending notifications.
    pub fn reference(&self) -> Ref { Ref(&self) }
}

impl<'a> Ref<'a> {
    /// Subscribe to notifications from the associated sender.
    pub fn listen<L>(&self, listener: L) -> ListenerGuard
    where L: IntoListener {
        let id = self.0.0.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.0.0.listeners.write().unwrap().insert(id, listener.into_listener());
        ListenerGuard { inner: Arc::downgrade(&self.0.0), id }
    }

    /// Get a unique identifier for this broadcast (for deduplication purposes)
    pub fn unique_id(&self) -> usize { Arc::as_ptr(&self.0.0) as usize }
}

impl Drop for ListenerGuard {
    /// Automatically unsubscribes when the subscription handle is dropped.
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.listeners.write().unwrap().remove(&self.id);
        }
    }
}

// IntoListener implementations for various types

// Implementation for function types - multi-threaded
impl<F> IntoListener for F
where F: Fn() + Send + Sync + 'static
{
    fn into_listener(self) -> Listener { Arc::new(self) }
}

#[cfg(feature = "tokio")]
impl IntoListener for tokio::sync::mpsc::UnboundedSender<()> {
    fn into_listener(self) -> Listener {
        Arc::new(move || {
            let _ = self.send(()); // Ignore send errors
        })
    }
}

impl IntoListener for std::sync::mpsc::Sender<()> {
    fn into_listener(self) -> Listener {
        Arc::new(move || {
            let _ = self.send(()); // Ignore send errors
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_multiple_subscribers() {
        let sender = Broadcast::new();

        let counter = Arc::new(Mutex::new(0));

        // Subscribe two callbacks
        let _sub1 = {
            let counter = counter.clone();
            sender.reference().listen(move || *counter.lock().unwrap() += 1)
        };

        let sub2 = {
            let counter = counter.clone();
            sender.reference().listen(move || *counter.lock().unwrap() += 10)
        };

        // Send notification - both callbacks should be called
        sender.send();
        assert_eq!(*counter.lock().unwrap(), 11); // 1 + 10

        // Drop one subscription
        drop(sub2);

        // Send again - only first callback should be called
        sender.send();
        assert_eq!(*counter.lock().unwrap(), 12); // 11 + 1 (only sub1)
    }

    #[test]
    #[cfg(feature = "tokio")]
    fn test_channel_sender_subscriber() {
        let sender = Broadcast::new();

        // Create a channel to receive notifications
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        // Subscribe the channel sender - it will send () when notified
        let _sub = sender.reference().listen(tx);

        // Send notification
        sender.send();

        // The channel should have received the notification
        assert!(rx.try_recv().is_ok());

        // Send another notification
        sender.send();
        assert!(rx.try_recv().is_ok());

        // No more messages should be in the channel
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_subscribe_trait() {
        use crate::porcelain::Subscribe;
        use crate::signal::mutable::Mut;

        let signal = Mut::new(42);
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let _subscription = signal.subscribe(move |_| {
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });

        signal.set(100);

        // Should have been called once
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn test_reentrant_subscription_during_send() {
        let sender = Broadcast::new();
        let counter = Arc::new(Mutex::new(0));

        // Create a listener that will try to create new subscriptions during the callback
        // This tests that our Arc-based approach handles re-entrancy without deadlocks
        let sender_clone = sender.clone();
        let counter_clone = counter.clone();
        let _sub = sender.reference().listen(move || {
            *counter_clone.lock().unwrap() += 1;

            // Try to add a new subscription during the callback - should work without deadlock
            let _temp_sub = sender_clone.reference().listen(|| {
                // This callback doesn't matter for the test
            });
            // temp_sub will be dropped here, which should also work without deadlock
        });

        // Send notification - this should work without deadlocks
        sender.send();

        // Verify the callback was called
        assert_eq!(*counter.lock().unwrap(), 1);

        // Send again to verify the system is still working
        sender.send();
        assert_eq!(*counter.lock().unwrap(), 2);
    }
}
