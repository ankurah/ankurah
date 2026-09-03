use std::sync::Arc;

use crate::{
    Peek,
    broadcast::Broadcast,
    context::CurrentObserver,
    porcelain::{Subscribe, SubscriptionGuard, subscribe::IntoSubscribeListener},
    signal::{Get, GetReadCell, Listener, ListenerGuard, Signal, With, read::Read},
    value::{ReadValueCell, ValueCell},
};

#[derive(Clone)]
pub struct Mut<T> {
    value: ValueCell<T>,
    broadcast: Broadcast<()>,
}

impl<T: 'static> Mut<T> {
    pub fn new(value: T) -> Self {
        let broadcast = Broadcast::new();
        Self { value: ValueCell::new(value), broadcast }
    }

    pub fn set(&self, value: T) {
        self.value.set(value);
        self.broadcast.send(());
    }

    /// Sets the value, runs `before_notify`, then notifies listeners.
    /// This lets callers release coordination locks before callbacks run.
    pub fn set_before_notify<R>(&self, value: T, before_notify: impl FnOnce() -> R) -> R {
        self.value.set(value);
        let result = before_notify();
        self.broadcast.send(());
        result
    }

    /// Mutates the value in place and notifies listeners.
    ///
    /// The closure runs under the value's write lock, and the lock is released
    /// before listeners are notified, so a listener that immediately re-reads
    /// never executes under the writer's lock. Listeners are notified even if
    /// the closure leaves the value unchanged.
    pub fn update<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let result = self.value.with_mut(f);
        self.broadcast.send(());
        result
    }

    /// Calls a closure with a borrow of the current value
    /// not tracked by the current context
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R { self.value.with(f) }

    /// Returns a read-only version of this signal  
    pub fn read(&self) -> Read<T> { Read { value: self.value.clone(), broadcast: self.broadcast.clone() } }
}

impl<T> Mut<T>
where T: Clone
{
    /// Returns a clone of the current value - not tracked by the current context
    pub fn value(&self) -> T { self.value.value() }
}

impl<T: Clone + 'static> Get<T> for Mut<T> {
    fn get(&self) -> T {
        CurrentObserver::track(self);
        self.value.value()
    }
}

impl<T: Clone + 'static> Peek<T> for Mut<T> {
    fn peek(&self) -> T { self.value.value() }
}

impl<T: 'static> With<T> for Mut<T> {
    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        CurrentObserver::track(self);
        self.value.with(f)
    }
}

impl<T: 'static> GetReadCell<T> for Mut<T> {
    fn get_readcell(&self) -> ReadValueCell<T> { self.value.readvalue() }
}

impl<T> Signal for Mut<T> {
    fn listen(&self, listener: Listener) -> ListenerGuard { ListenerGuard::new(self.broadcast.reference().listen(listener)) }

    fn broadcast_id(&self) -> crate::broadcast::BroadcastId { self.broadcast.id() }
}

impl<T> Subscribe<T> for Mut<T>
where T: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<T> {
        let listener = listener.into_subscribe_listener();
        let ro_value = self.get_readcell(); // Get read-only value handle
        let subscription = self.listen(Arc::new(move |_| {
            // Get current value when the broadcast fires
            let current_value = ro_value.value();
            listener(current_value);
        }));
        SubscriptionGuard::new(subscription)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn test_update_mutates_in_place_and_notifies() {
        let signal = Mut::new(vec![1, 2]);

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = seen.clone();
        let cell = signal.get_readcell();
        // Reading during the callback would deadlock if update still held the
        // write lock while notifying
        let _guard = signal.listen(Arc::new(move |_| {
            seen_clone.lock().unwrap().push(cell.value());
        }));

        let len = signal.update(|v| {
            v.push(3);
            v.len()
        });

        assert_eq!(len, 3);
        assert_eq!(signal.value(), vec![1, 2, 3]);
        // Fired exactly once, and the listener observed the updated value
        assert_eq!(*seen.lock().unwrap(), vec![vec![1, 2, 3]]);
    }

    #[test]
    fn set_before_notify_runs_hook_before_listener() {
        let signal = Mut::new(1);
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let listener_ready = ready.clone();
        let _guard = signal.listen(Arc::new(move |_| {
            assert!(listener_ready.load(std::sync::atomic::Ordering::Acquire));
        }));

        signal.set_before_notify(2, || ready.store(true, std::sync::atomic::Ordering::Release));
        assert_eq!(signal.value(), 2);
    }
}
