use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    Observer, Peek,
    broadcast::{Broadcast, BroadcastId},
    context::CurrentObserver,
    porcelain::{Subscribe, SubscriptionGuard, subscribe::IntoSubscribeListener},
    signal::{Get, GetReadCell, Listener, ListenerGuard, Signal, With},
    value::{ReadValueCell, ValueCell},
};

struct SubscriptionEntry {
    _guard: ListenerGuard,
    marked_for_removal: bool,
}

struct Inner<T> {
    /// The compute function
    compute: Box<dyn Fn() -> T + Send + Sync>,
    /// Cached computed value
    value: ValueCell<Option<T>>,
    /// Broadcast for notifying downstream observers (fires AFTER context cleanup)
    broadcast: Broadcast<()>,
    /// Subscriptions to upstream signals, mapped by broadcast ID for mark-and-sweep
    entries: std::sync::RwLock<HashMap<BroadcastId, SubscriptionEntry>>,
}

/// A calculated/derived signal that computes its value from other signals.
///
/// Automatically tracks which signals are accessed during computation.
/// When any upstream signal changes, the computed value is recalculated
/// and downstream observers are notified.
///
/// The compute function can close over arbitrary state, allowing for stateful
/// computations (e.g., maintaining display order stability).
///
/// # Example
/// ```ignore
/// let a = Mut::new(1);
/// let b = Mut::new(2);
///
/// let sum = {
///     let a = a.read();
///     let b = b.read();
///
///     Calculated::new(move || a.get() + b.get())
/// };
///
/// assert_eq!(sum.get(), 3);
/// a.set(10);
/// assert_eq!(sum.get(), 12);
/// ```
/// Cloning a `Calculated` shares the same underlying computed value and observer.
pub struct Calculated<T>(Arc<Inner<T>>);

impl<T> Clone for Calculated<T> {
    fn clone(&self) -> Self { Self(Arc::clone(&self.0)) }
}

impl<T: Send + Sync + 'static> Calculated<T> {
    /// Create a new calculated signal from a compute function.
    ///
    /// The compute function will be called immediately to get the initial value,
    /// and will be called again whenever any signal accessed during computation
    /// changes.
    pub fn new<F>(compute: F) -> Self
    where F: Fn() -> T + Send + Sync + 'static {
        let inner = Arc::new(Inner {
            compute: Box::new(compute),
            value: ValueCell::new(None),
            broadcast: Broadcast::new(),
            entries: std::sync::RwLock::new(HashMap::new()),
        });

        // Trigger initial computation to establish subscriptions and compute initial value
        trigger(&inner);

        Self(inner)
    }
}

/// Trigger recomputation with dependency tracking, then notify downstream
fn trigger<T: Send + Sync + 'static>(inner: &Arc<Inner<T>>) {
    // Mark-and-sweep: mark all existing subscriptions for removal
    {
        let mut entries = inner.entries.write().expect("entries lock poisoned");
        for entry in entries.values_mut() {
            entry.marked_for_removal = true;
        }
    }

    // Set ourselves as the current observer and run compute
    CurrentObserver::set(Arc::clone(inner));
    let new_value = (inner.compute)();
    inner.value.set(Some(new_value));
    CurrentObserver::remove(&*inner);

    // Sweep away any subscriptions that weren't accessed during compute
    {
        let mut entries = inner.entries.write().expect("entries lock poisoned");
        entries.retain(|_, entry| !entry.marked_for_removal);
    }

    // NOW it's safe to broadcast - no observer context is active
    inner.broadcast.send(());
}

impl<T: Clone + 'static> Get<T> for Calculated<T> {
    fn get(&self) -> T {
        CurrentObserver::track(self);
        self.0.value.with(|opt| opt.as_ref().expect("Calculated value not initialized").clone())
    }
}

impl<T: Clone + 'static> Peek<T> for Calculated<T> {
    fn peek(&self) -> T { self.0.value.with(|opt| opt.as_ref().expect("Calculated value not initialized").clone()) }
}

impl<T: 'static> With<T> for Calculated<T> {
    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        CurrentObserver::track(self);
        self.0.value.with(|opt| f(opt.as_ref().expect("Calculated value not initialized")))
    }
}

impl<T: 'static> GetReadCell<Option<T>> for Calculated<T> {
    fn get_readcell(&self) -> ReadValueCell<Option<T>> { self.0.value.readvalue() }
}

impl<T> Signal for Calculated<T> {
    fn listen(&self, listener: Listener) -> ListenerGuard { ListenerGuard::new(self.0.broadcast.reference().listen(listener)) }

    fn broadcast_id(&self) -> BroadcastId { self.0.broadcast.id() }
}

impl<T: Send + Sync + 'static> Observer for Arc<Inner<T>> {
    fn observe(&self, signal: &dyn Signal) {
        let broadcast_id = signal.broadcast_id();

        // Check if we already have a subscription for this signal
        {
            let mut entries = self.entries.write().expect("entries lock poisoned");
            if let Some(entry) = entries.get_mut(&broadcast_id) {
                entry.marked_for_removal = false;
                return;
            }
        }
        // Lock released before calling listen() to avoid recursive lock

        // Create new subscription - when upstream changes, trigger recomputation
        let weak = Arc::downgrade(self);
        let guard = signal.listen(Arc::new(move |_| {
            if let Some(inner) = weak.upgrade() {
                trigger(&inner);
            }
        }));

        // Re-acquire lock to insert
        let mut entries = self.entries.write().expect("entries lock poisoned");
        entries.insert(broadcast_id, SubscriptionEntry { _guard: guard, marked_for_removal: false });
    }

    fn observer_id(&self) -> usize { Arc::as_ptr(self) as usize }

    fn as_any(&self) -> &dyn std::any::Any { self }
}

impl<T> Subscribe<T> for Calculated<T>
where T: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<T> {
        let listener = listener.into_subscribe_listener();
        let ro_value = self.0.value.readvalue();
        let subscription = self.listen(Arc::new(move |_| {
            let current = ro_value.with(|opt| opt.as_ref().expect("Calculated value not initialized").clone());
            listener(current);
        }));
        SubscriptionGuard::new(subscription)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::mutable::Mut;

    #[test]
    fn test_basic_calculated() {
        let a = Mut::new(1);
        let b = Mut::new(2);

        let sum = Calculated::new({
            let a = a.read();
            let b = b.read();
            move || a.get() + b.get()
        });

        assert_eq!(sum.get(), 3);

        a.set(10);
        assert_eq!(sum.get(), 12);

        b.set(5);
        assert_eq!(sum.get(), 15);
    }

    #[test]
    fn test_two_independent_inputs() {
        // Tests that a calculated signal properly tracks two independent input signals
        // and recomputes when either one changes
        let first_name = Mut::new("Alice".to_string());
        let last_name = Mut::new("Smith".to_string());

        let full_name = {
            let first = first_name.read();
            let last = last_name.read();
            Calculated::new(move || format!("{} {}", first.get(), last.get()))
        };

        assert_eq!(full_name.get(), "Alice Smith");

        // Change first name only
        first_name.set("Bob".to_string());
        assert_eq!(full_name.get(), "Bob Smith");

        // Change last name only
        last_name.set("Jones".to_string());
        assert_eq!(full_name.get(), "Bob Jones");

        // Change both
        first_name.set("Carol".to_string());
        last_name.set("Williams".to_string());
        assert_eq!(full_name.get(), "Carol Williams");
    }

    #[test]
    fn test_calculated_with_closed_over_state() {
        let trigger = Mut::new(0);

        let counter = Calculated::new({
            let trigger = trigger.read();
            let count = Arc::new(std::sync::atomic::AtomicUsize::new(0)); // closed-over mutable state
            move || {
                let _ = trigger.get(); // track the trigger
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
            }
        });

        assert_eq!(counter.get(), 1);

        trigger.set(1);
        assert_eq!(counter.get(), 2);

        trigger.set(2);
        assert_eq!(counter.get(), 3);
    }

    #[test]
    fn test_calculated_downstream_subscription() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let source = Mut::new(5);
        let doubled = Calculated::new({
            let source = source.read();
            move || source.get() * 2
        });

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_ref = call_count.clone();

        let _sub = doubled.subscribe(move |value| {
            assert_eq!(value, 20); // Should be 10 * 2
            call_count_ref.fetch_add(1, Ordering::SeqCst);
        });

        source.set(10);

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_chained_calculated() {
        let base = Mut::new(2);

        let doubled = Calculated::new({
            let base = base.read();
            move || base.get() * 2
        });

        let quadrupled = Calculated::new(move || doubled.get() * 2);

        assert_eq!(quadrupled.get(), 8);

        base.set(5);
        assert_eq!(quadrupled.get(), 20);
    }

    /// Test that downstream listeners don't pollute the calculated signal's dependencies.
    ///
    /// This validates the fix for the issue where `broadcast.send()` was called inside
    /// the observer context. If a listener accessed other signals, those would incorrectly
    /// be tracked as dependencies of the calculated signal.
    #[test]
    fn test_listener_does_not_pollute_dependencies() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let source = Mut::new(1);
        let unrelated = Mut::new(100);

        let compute_count = Arc::new(AtomicUsize::new(0));
        let compute_count_ref = compute_count.clone();

        let doubled = Calculated::new({
            let source = source.read();
            move || {
                compute_count_ref.fetch_add(1, Ordering::SeqCst);
                source.get() * 2
            }
        });

        assert_eq!(doubled.get(), 2);
        assert_eq!(compute_count.load(Ordering::SeqCst), 1);

        // Subscribe a listener that reads an unrelated signal
        let unrelated_read = unrelated.read();
        let _sub = doubled.subscribe(move |_value| {
            // This reads `unrelated` during the notification callback.
            // With the old buggy implementation, this would cause `unrelated`
            // to be tracked as a dependency of `doubled`.
            let _ = unrelated_read.get();
        });

        // Change source - should trigger recompute
        source.set(2);
        assert_eq!(doubled.get(), 4);
        assert_eq!(compute_count.load(Ordering::SeqCst), 2);

        // Change the unrelated signal - should NOT trigger recompute
        // With the old buggy implementation, this would have caused a recompute
        unrelated.set(200);
        assert_eq!(doubled.get(), 4);
        assert_eq!(compute_count.load(Ordering::SeqCst), 2); // Still 2, no extra recompute
    }
}
