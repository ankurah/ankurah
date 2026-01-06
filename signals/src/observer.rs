use crate::Signal;
mod callback_observer;
pub use callback_observer::*;

// Conditional bounds for Observer trait
// When `multithread` is enabled, observers must be Send + Sync for cross-thread tracking
#[cfg(feature = "multithread")]
pub trait ObserverBounds: Send + Sync {}
#[cfg(feature = "multithread")]
impl<T: Send + Sync> ObserverBounds for T {}

#[cfg(not(feature = "multithread"))]
pub trait ObserverBounds {}
#[cfg(not(feature = "multithread"))]
impl<T> ObserverBounds for T {}

/// An Observer is a struct that can observe multiple signals
///
/// When the `multithread` feature is enabled, observers must be `Send + Sync`
/// to support cross-thread tracking (required for UniFFI/React Native).
pub trait Observer: ObserverBounds {
    /// Observe a signal reader - implement this method to handle subscriptions
    fn observe(&self, signal: &dyn Signal);

    /// Get a unique identifier for this observer (for equality comparison)
    fn observer_id(&self) -> usize;

    /// Downcast support for trait objects (used internally for diagnostics)
    #[doc(hidden)]
    fn as_any(&self) -> &dyn std::any::Any;
}
