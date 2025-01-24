use std::sync::{Arc, RwLock};

use crate::subscription::{Subscriber, SubscriptionHandle};
pub trait Signal<T> {
    fn subscribe<S: Into<Subscriber<T>>>(&self, subscriber: S) -> SubscriptionHandle;
}
pub trait Get<T> {
    fn get(&self) -> T;
}

pub trait Notify: Send + Sync {
    fn notify(&self);
}

pub trait NotifyValue<T>: Send + Sync {
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
