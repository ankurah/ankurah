pub mod calculated;
pub mod map;
pub mod memo;
pub mod mutable;
pub mod read;

pub use calculated::*;
pub use map::*;
pub use memo::*;
pub use mutable::*;
pub use read::*;

pub type Listener = std::sync::Arc<dyn Fn(()) + Send + Sync + 'static>;

/// Wraps any broadcast::ListenerGuard<T> in a type-erased container
pub struct ListenerGuard(Box<dyn crate::broadcast::TListenerGuard + Send + Sync + 'static>);

impl ListenerGuard {
    /// Wrap any broadcast::ListenerGuard<T>
    pub fn new<T: 'static>(guard: crate::broadcast::ListenerGuard<T>) -> Self { Self(Box::new(guard)) }

    /// Get the broadcast ID that this guard is subscribed to
    pub fn broadcast_id(&self) -> crate::broadcast::BroadcastId { self.0.broadcast_id() }
}

impl<T: 'static> From<crate::broadcast::ListenerGuard<T>> for ListenerGuard {
    fn from(guard: crate::broadcast::ListenerGuard<T>) -> Self { Self(Box::new(guard)) }
}

/// Core trait for signals - provides observation capability without regard to a payload value
/// The sole purpose of this trait is to provide a way to listen to changes to a signal.
///
/// Note: Multiple signals may share the same broadcast (and thus the same broadcast_id).
/// This is intentional and allows observers to deduplicate subscriptions efficiently.
pub trait Signal {
    /// Listen to changes to this signal with a listener function
    /// Takes an Arc-wrapped closure for efficiency - wrap your closure with Arc::new(|| { ... })
    fn listen(&self, listener: Listener) -> ListenerGuard;

    /// Get the broadcast identifier for this signal.
    /// Multiple signals may return the same broadcast_id if they share a broadcast.
    /// The broadcast_id remains valid as a deduplication key as long as any ListenerGuard for that broadcast exists.
    fn broadcast_id(&self) -> crate::broadcast::BroadcastId;
}

/// Trait for getting the current value of a signal in a way that will be tracked by the current context
pub trait Get<T: 'static>: Signal {
    fn get(&self) -> T;
}

/// Trait for accessing the current value of a signal with a closure in a way that will be tracked by the current context
pub trait With<T: 'static> {
    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R;
}

/// Trait for getting the current value of a signal in a way that will not be tracked by the current context
pub trait Peek<T: 'static> {
    fn peek(&self) -> T;
}

/// Trait for getting a read-only cell containing a present value
pub trait GetReadCell<T: 'static> {
    fn get_readcell(&self) -> crate::value::ReadValueCell<T>;
}

impl<T> Signal for &T
where T: Signal
{
    fn listen(&self, listener: Listener) -> ListenerGuard { Signal::listen(*self, listener) }

    fn broadcast_id(&self) -> crate::broadcast::BroadcastId { Signal::broadcast_id(*self) }
}
