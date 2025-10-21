use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Weak};

/// A unique identifier for a broadcast that cannot be forged or extracted.
/// Can only be created by a Broadcast and used for deduplication/comparison.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BroadcastId(usize);

impl Into<usize> for BroadcastId {
    fn into(self) -> usize { self.0 }
}
impl std::fmt::Display for BroadcastId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

/// A listener that can be called when broadcast notifications are sent.
/// Supports both full listeners (receive value) and unit listeners (notification only).
#[derive(Clone)]
pub enum BroadcastListener<T = ()> {
    /// Full listener receives the broadcast value
    Payload(Arc<dyn Fn(T) + Send + Sync + 'static>),
    /// Unit listener only receives notification, ignores the value
    NotifyOnly(Arc<dyn Fn() + Send + Sync + 'static>),
}

/// Trait for types that can be converted into broadcast listeners.
pub trait IntoBroadcastListener<T> {
    /// Convert this type into a listener function that can be called on notifications.
    fn into_broadcast_listener(self) -> BroadcastListener<T>;
}

/// A broadcast sender that notifies multiple subscribers without payload data.
/// Uses synchronous function callbacks for immediate notification.
#[derive(Clone)]
pub struct Broadcast<T = ()>(Arc<Inner<T>>);

struct Inner<T> {
    listeners: std::sync::RwLock<HashMap<usize, BroadcastListener<T>>>,
    next_id: AtomicUsize,
}

impl<T> std::fmt::Debug for Broadcast<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broadcast").field("listeners", &self.0.listeners.read().unwrap().len()).finish()
    }
}

/// A listen-only reference to a broadcast
pub struct Ref<'a, T>(&'a Broadcast<T>);

/// Trait for abstractly representing any ListenerGuard<T>
pub trait TListenerGuard {
    fn broadcast_id(&self) -> BroadcastId;
}

/// A subscription handle that can be used to unsubscribe from notifications.
pub struct ListenerGuard<T = ()> {
    inner: Weak<Inner<T>>,
    id: usize,
}

impl<T> ListenerGuard<T> {
    /// Get the broadcast ID that this guard is subscribed to
    pub fn broadcast_id(&self) -> BroadcastId {
        // A ListenerGuard does not keep the broadcast alive
        // but the address is reserved until all Arc/Weak references are dropped
        // Given that we are using the address as the ID, this is safe.
        // We don't actually care if the broadcast is alive. The point is to
        // provide a unqique id for removing the correct listener.
        BroadcastId(self.inner.as_ptr() as usize)
    }
}

impl<T> TListenerGuard for ListenerGuard<T> {
    fn broadcast_id(&self) -> BroadcastId { self.broadcast_id() }
}

impl<T> Default for Broadcast<T>
where T: Clone
{
    fn default() -> Self { Self::new() }
}

impl<T> Broadcast<T>
where T: Clone
{
    /// Creates a new Broadcast struct
    pub fn new() -> Self { Self(Arc::new(Inner { listeners: std::sync::RwLock::new(HashMap::new()), next_id: AtomicUsize::new(0) })) }

    /// Get the unique identifier for this broadcast
    pub fn id(&self) -> BroadcastId { BroadcastId(Arc::as_ptr(&self.0) as usize) }

    /// Sends a notification to all active listeners
    pub fn send(&self, value: T) {
        // Clone the listeners to avoid holding the lock during callback execution
        let subscribers = {
            // maybe someday we can avoid the alloc here using a thread-local buffer?
            let listeners = self.0.listeners.read().unwrap();
            listeners.values().cloned().collect::<Vec<_>>()
        };

        // Call all listeners without holding any locks
        // clone the value for each subscriber except the last one
        if let Some((last, rest)) = subscribers.split_last() {
            for callback in rest {
                match callback {
                    BroadcastListener::Payload(callback) => callback(value.clone()),
                    BroadcastListener::NotifyOnly(callback) => callback(),
                }
            }
            match last {
                BroadcastListener::Payload(callback) => callback(value),
                BroadcastListener::NotifyOnly(callback) => callback(),
            }
        }
    }

    /// Get a read-only reference to this sender that can only subscribe to notifications.
    /// This avoids cloning the sender while still forbidding the user from sending notifications.
    pub fn reference(&self) -> Ref<'_, T> { Ref(self) }
}

impl<'a, T> Ref<'a, T> {
    /// Subscribe to notifications from the associated sender.
    pub fn listen<L>(&self, listener: L) -> ListenerGuard<T>
    where L: IntoBroadcastListener<T> {
        let id = self.0.0.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.0.0.listeners.write().unwrap().insert(id, listener.into_broadcast_listener());
        ListenerGuard { inner: Arc::downgrade(&self.0.0), id }
    }

    /// Get a unique identifier for this broadcast (for deduplication purposes)
    pub fn broadcast_id(&self) -> BroadcastId { BroadcastId(Arc::as_ptr(&self.0.0) as usize) }
}

impl<T> Drop for ListenerGuard<T> {
    /// Automatically unsubscribes when the subscription handle is dropped.
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner.listeners.write().unwrap().remove(&self.id);
        }
    }
}

// IntoListener implementations for various types

// Implementation for function types - multi-threaded
impl<F, T> IntoBroadcastListener<T> for F
where F: Fn(T) + Send + Sync + 'static
{
    fn into_broadcast_listener(self) -> BroadcastListener<T> { BroadcastListener::Payload(Arc::new(self)) }
}

// Implementation for Listener itself
impl<T> IntoBroadcastListener<T> for BroadcastListener<T> {
    fn into_broadcast_listener(self) -> BroadcastListener<T> { self }
}

// Implementation for Arc<dyn Fn(T)> - wrap in Full variant
impl<T> IntoBroadcastListener<T> for Arc<dyn Fn(T) + Send + Sync + 'static> {
    fn into_broadcast_listener(self) -> BroadcastListener<T> { BroadcastListener::Payload(self) }
}

// Implementation for Arc<dyn Fn()> - wrap in Unit variant for any T
// This allows unit-type listeners (e.g., from Signal trait) to work with any broadcast type
impl<T> IntoBroadcastListener<T> for Arc<dyn Fn() + Send + Sync + 'static> {
    fn into_broadcast_listener(self) -> BroadcastListener<T> { BroadcastListener::NotifyOnly(self) }
}

#[cfg(feature = "tokio")]
impl<T> IntoBroadcastListener<T> for tokio::sync::mpsc::UnboundedSender<T>
where T: Send + Sync + 'static
{
    fn into_broadcast_listener(self) -> BroadcastListener<T> {
        BroadcastListener::Payload(Arc::new(move |value| {
            let _ = self.send(value); // Ignore send errors
        }))
    }
}

impl<T> IntoBroadcastListener<T> for std::sync::mpsc::Sender<T>
where T: Send + Sync + 'static
{
    fn into_broadcast_listener(self) -> BroadcastListener<T> {
        BroadcastListener::Payload(Arc::new(move |value| {
            let _ = self.send(value); // Ignore send errors
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_multiple_subscribers() {
        let sender = Broadcast::<()>::new();

        let counter = Arc::new(Mutex::new(0));

        // Subscribe two callbacks
        let _sub1 = {
            let counter = counter.clone();
            sender.reference().listen(move |_| *counter.lock().unwrap() += 1)
        };

        let sub2 = {
            let counter = counter.clone();
            sender.reference().listen(move |_| *counter.lock().unwrap() += 10)
        };

        // Send notification - both callbacks should be called
        sender.send(());
        assert_eq!(*counter.lock().unwrap(), 11); // 1 + 10

        // Drop one subscription
        drop(sub2);

        // Send again - only first callback should be called
        sender.send(());
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
        sender.send(());

        // The channel should have received the notification
        assert!(rx.try_recv().is_ok());

        // Send another notification
        sender.send(());
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
        let sender = Broadcast::<()>::new();
        let counter = Arc::new(Mutex::new(0));

        // Create a listener that will try to create new subscriptions during the callback
        // This tests that our Arc-based approach handles re-entrancy without deadlocks
        let sender_clone = sender.clone();
        let counter_clone = counter.clone();
        let _sub = sender.reference().listen(move |_| {
            *counter_clone.lock().unwrap() += 1;

            // Try to add a new subscription during the callback - should work without deadlock
            let _temp_sub = sender_clone.reference().listen(|_| {
                // This callback doesn't matter for the test
            });
            // temp_sub will be dropped here, which should also work without deadlock
        });

        // Send notification - this should work without deadlocks
        sender.send(());

        // Verify the callback was called
        assert_eq!(*counter.lock().unwrap(), 1);

        // Send again to verify the system is still working
        sender.send(());
        assert_eq!(*counter.lock().unwrap(), 2);
    }
}
