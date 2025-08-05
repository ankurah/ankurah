use crate::Signal;
mod callback_observer;
pub use callback_observer::*;

/// An Observer is a struct that can observe multiple signals
pub trait Observer {
    /// Observe a signal reader - implement this method to handle subscriptions
    fn observe(&self, signal: &dyn Signal);

    /// Get a unique identifier for this observer (for equality comparison)
    fn observer_id(&self) -> usize;
}
