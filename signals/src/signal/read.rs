use std::sync::Arc;

use crate::{
    Peek,
    broadcast::Broadcast,
    context::CurrentObserver,
    porcelain::{Subscribe, SubscriptionGuard, subscribe::IntoSubscribeListener},
    signal::{Get, GetReadCell, Signal, With, map::Map},
    value::{ReadValueCell, ValueCell},
};

/// Read-only signal
pub struct Read<T> {
    pub(crate) value: ValueCell<T>,
    pub(crate) broadcast: Broadcast,
}

impl<T> Read<T>
where T: Clone
{
    pub fn value(&self) -> T { self.value.value() }
}

impl<T> Read<T> {
    /// Create a mapped signal that transforms this signal's values on-demand
    ///
    /// Note: The Send + Sync bounds on Transform are only required if you want to subscribe
    /// to the mapped signal. For just using .with(), they're not needed.
    pub fn map<Output, Transform>(&self, transform: Transform) -> Map<Self, T, Output, Transform>
    where
        T: 'static,
        Transform: Fn(&T) -> Output,
        Output: 'static,
    {
        Map::new(self.clone(), transform)
    }
}

impl<T> Clone for Read<T> {
    fn clone(&self) -> Self { Self { value: self.value.clone(), broadcast: self.broadcast.clone() } }
}

impl<T: Clone + 'static> Get<T> for Read<T> {
    fn get(&self) -> T {
        CurrentObserver::track(self);
        self.value.value()
    }
}

impl<T: Clone + 'static> Peek<T> for Read<T> {
    fn peek(&self) -> T { self.value.value() }
}

impl<T: 'static> With<T> for Read<T> {
    fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        CurrentObserver::track(self);
        self.value.with(f)
    }
}

impl<T: 'static> GetReadCell<T> for Read<T> {
    fn get_readcell(&self) -> ReadValueCell<T> { self.value.readvalue() }
}

impl<T> Signal for Read<T> {
    fn listen(&self, listener: crate::broadcast::Listener) -> crate::broadcast::ListenerGuard {
        self.broadcast.reference().listen(listener)
    }

    fn broadcast_id(&self) -> crate::broadcast::BroadcastId { self.broadcast.id() }
}

/// foo == bar will automatically track the signals used in the comparison against the current observer
impl<T: PartialEq + 'static> PartialEq for Read<T> {
    fn eq(&self, other: &Self) -> bool {
        // Short-circuit if comparing to self to avoid deadlock from nested with calls
        if std::ptr::eq(self, other) {
            return true;
        }
        self.with(|self_val| other.with(|other_val| self_val == other_val))
    }
}

impl<T: Eq + 'static> Eq for Read<T> {}

impl<T: std::fmt::Display + Send + Sync + 'static> std::fmt::Display for Read<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.with(|v| write!(f, "{}", v)) }
}

impl<T> Subscribe<T> for Read<T>
where T: Clone + Send + Sync + 'static
{
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<T> {
        let listener = listener.into_subscribe_listener();
        let ro_value = self.get_readcell(); // Get read-only value handle
        let subscription = self.listen(Arc::new(move || {
            // Get current value when the broadcast fires
            let current_value = ro_value.value();
            listener(current_value);
        }));
        SubscriptionGuard::new(subscription)
    }
}
