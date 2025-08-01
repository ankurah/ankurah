use crate::subscription::{Subscriber, SubscriptionGuard};

/// Signal is Generic over T because some Subscribers want a borrow or clone of T
pub trait Signal<T: 'static> {
    fn subscribe<S: Into<Subscriber<T>>>(&self, subscriber: S) -> SubscriptionGuard;
}

/// Trait for things that can be observed (signals), but minus the type parameter
/// We need this to be dyn, because CONTEXT_STACK is a thread_local and we could have
/// different types of observers.
pub trait Observable {
    /// Subscribe to this observable with a notification callback
    fn subscribe(&self, subscriber: Box<dyn Notify>) -> SubscriptionGuard;
}

/// Core trait for signal observers
pub trait Observer {
    /// Get a subscriber that will be notified when signals change
    fn get_notifier(&self) -> Box<dyn Notify + Send + Sync>;

    /// Store a subscription handle to keep the subscription alive
    fn store_handle(&self, handle: SubscriptionGuard);
}
pub trait Get<T: 'static> {
    fn get(&self) -> T;
}

pub trait Notify {
    fn notify(&self);
}

pub trait NotifyValue<T: 'static> {
    fn notify(&self, value: &T);
}

/// Helper trait for `wait_for` to allow flexible predicate return types.
///
/// ## Semantics
/// - `result()` returns `Some(output)` to stop waiting and return `output`
/// - `result()` returns `None` to continue waiting for the next signal update
pub trait WaitResult {
    type Output;
    /// Returns Some(output) if we should stop waiting, None if we should continue
    fn result(self) -> Option<Self::Output>;
}

// Blanket impl for bool: true = stop with (), false = continue waiting
impl WaitResult for bool {
    type Output = ();
    fn result(self) -> Option<Self::Output> { if self { Some(()) } else { None } }
}

// Blanket impl for Option<T>: Some(value) = stop with value, None = continue waiting
impl<T> WaitResult for Option<T> {
    type Output = T;
    fn result(self) -> Option<Self::Output> { self }
}

/// Simple Notify implementation for tokio unbounded sender
impl Notify for tokio::sync::mpsc::UnboundedSender<()> {
    fn notify(&self) { let _ = self.send(()); }
}
