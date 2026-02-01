use std::sync::Arc;

use crate::{
    CallbackObserver, Peek,
    broadcast::Broadcast,
    context::CurrentObserver,
    porcelain::{Subscribe, SubscriptionGuard, subscribe::IntoSubscribeListener},
    signal::{Get, GetReadCell, Listener, ListenerGuard, Signal, With},
    value::{ReadValueCell, ValueCell},
};

/// A calculated/derived signal that computes its value from other signals.
///
/// Uses `CallbackObserver` internally to automatically track which signals are
/// accessed during computation. When any upstream signal changes, the computed
/// value is recalculated and downstream observers are notified.
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
#[derive(Clone)]
pub struct Calculated<T> {
    /// Cached computed value
    value: ValueCell<Option<T>>,
    /// Broadcast for notifying downstream observers
    broadcast: Broadcast<()>,
    /// The observer that tracks upstream dependencies - must be kept alive
    _observer: CallbackObserver,
}

impl<T> Calculated<T>
where T: Send + Sync + 'static
{
    /// Create a new calculated signal from a compute function.
    ///
    /// The compute function will be called immediately to get the initial value,
    /// and will be called again whenever any signal accessed during computation
    /// changes.
    pub fn new<F>(compute: F) -> Self
    where F: Fn() -> T + Send + Sync + 'static {
        // Start with None, will be populated by trigger()
        let value = ValueCell::new(None);
        let broadcast = Broadcast::new();

        // Create the callback observer that will recompute when upstream changes
        let value_ref = value.clone();
        let broadcast_ref = broadcast.clone();
        let observer = CallbackObserver::new(Arc::new(move || {
            let new_value = compute();
            value_ref.set(Some(new_value));
            broadcast_ref.send(());
        }));

        // Trigger initial observation to establish subscriptions and compute initial value
        // The callback observer will use mark-and-sweep to track which signals
        // are accessed during this call
        observer.trigger();

        Self { value, broadcast, _observer: observer }
    }
}

impl<T: Clone + 'static> Get<T> for Calculated<T> {
    fn get(&self) -> T {
        CurrentObserver::track(self);
        self.value.with(|opt| opt.as_ref().expect("Calculated value not initialized").clone())
    }
}

impl<T: Clone + 'static> Peek<T> for Calculated<T> {
    fn peek(&self) -> T { self.value.with(|opt| opt.as_ref().expect("Calculated value not initialized").clone()) }
}

impl<T: 'static> With<T> for Calculated<T> {
    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        CurrentObserver::track(self);
        self.value.with(|opt| f(opt.as_ref().expect("Calculated value not initialized")))
    }
}

impl<T: 'static> GetReadCell<Option<T>> for Calculated<T> {
    fn get_readcell(&self) -> ReadValueCell<Option<T>> { self.value.readvalue() }
}

impl<T> Signal for Calculated<T> {
    fn listen(&self, listener: Listener) -> ListenerGuard { ListenerGuard::new(self.broadcast.reference().listen(listener)) }

    fn broadcast_id(&self) -> crate::broadcast::BroadcastId { self.broadcast.id() }
}

impl<T> Subscribe<T> for Calculated<T>
where T: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<T> {
        let listener = listener.into_subscribe_listener();
        let ro_value = self.value.readvalue();
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

        let quadrupled = Calculated::new({ move || doubled.get() * 2 });

        assert_eq!(quadrupled.get(), 8);

        base.set(5);
        assert_eq!(quadrupled.get(), 20);
    }
}
