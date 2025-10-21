#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

use crate::{Get, Peek, signal::ListenerGuard};

/// Type alias for subscribe listeners with conditional thread safety bounds
pub type SubscribeListener<T> = Box<dyn Fn(T) + Send + Sync + 'static>;

/// Trait for types that can be converted into subscribe listeners
pub trait IntoSubscribeListener<T> {
    fn into_subscribe_listener(self) -> SubscribeListener<T>;
}

/// Trait for subscribing to changes - provides the subscribe method
pub trait Subscribe<T: 'static> {
    /// Subscribe to changes with a listener that receives the new value
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<T>;
}

pub trait DynSubscribe<T: 'static> {
    fn dyn_subscribe(&self, listener: Box<dyn Fn(T) + Send + Sync + 'static>) -> SubscriptionGuard;
}

impl<S, T: 'static> DynSubscribe<T> for S
where S: Subscribe<T>
{
    fn dyn_subscribe(&self, listener: Box<dyn Fn(T) + Send + Sync + 'static>) -> SubscriptionGuard { Subscribe::subscribe(self, listener) }
}

pub trait GetAndDynSubscribe<T: 'static>: Get<T> + Peek<T> + DynSubscribe<T> {}
impl<T: 'static, S> GetAndDynSubscribe<T> for S where S: Get<T> + Peek<T> + DynSubscribe<T> {}

/// A guard for a subscription to a signal
#[cfg_attr(feature = "wasm", wasm_bindgen)]
pub struct SubscriptionGuard {
    _listenerguard: Box<dyn std::any::Any + Send + Sync>,
}

impl SubscriptionGuard {
    pub fn new(lguard: ListenerGuard) -> Self { Self { _listenerguard: Box::new(lguard) } }
}

// IntoSubscribeListener implementation for std::sync::mpsc channels
impl<T: Send + 'static> IntoSubscribeListener<T> for std::sync::mpsc::Sender<T> {
    fn into_subscribe_listener(self) -> SubscribeListener<T> {
        Box::new(move |value| {
            let _ = self.send(value);
        })
    }
}

// IntoSubscribeListener implementation for tokio channels
#[cfg(feature = "tokio")]
impl<T: Send + 'static> IntoSubscribeListener<T> for tokio::sync::mpsc::UnboundedSender<T> {
    fn into_subscribe_listener(self) -> SubscribeListener<T> {
        Box::new(move |value| {
            let _ = self.send(value);
        })
    }
}

// Implementations for converting closures to SubscribeListener<T>
impl<F, T> IntoSubscribeListener<T> for F
where F: Fn(T) + Send + Sync + 'static
{
    fn into_subscribe_listener(self) -> SubscribeListener<T> { Box::new(self) }
}
