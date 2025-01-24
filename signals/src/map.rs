use crate::{
    Notify, Stateful,
    observer::{ObserverSet, ValueObserverSet},
};
use std::marker::PhantomData;
use std::sync::{Arc, Weak};

/// A signal that is a map of another signal
/// Stateless. Does not keep upstream signal alive
pub struct MapSignal<I, O: 'static, F: Fn(&I) -> O> {
    _upstream: PhantomData<I>,
    map_function: Arc<F>,
    observers: ObserverSet,
    value_observers: ValueObserverSet<O>,
}

impl<I, O: 'static, F> MapSignal<I, O, F>
where F: Fn(&I) -> O + Send + Sync + 'static
{
    pub(crate) fn new<S>(upstream: &S, f: F) -> Self
    where S: Stateful<I> {
        Self { _upstream: PhantomData, map_function: Arc::new(f), observers: ObserverSet::new(), value_observers: ValueObserverSet::new() }
    }

    fn track_observer(&self) {
        if let Some(observer) = GLOBAL_OBSERVER.read().unwrap().as_ref() {
            let mut observers = self.observers.write().unwrap();
            let observer_weak = Arc::downgrade(&observer.0) as Weak<dyn Notify>;

            if !observers.iter().any(|entry| match entry {
                ObserverKind::Notify(weak) => weak.ptr_eq(&observer_weak),
                _ => false,
            }) {
                observers.push(ObserverKind::Notify(observer_weak));
            }
        }
    }

    fn with_value<R>(&self, f: impl FnOnce(&O) -> R) -> R {
        self.track_observer();
        self.input.with_value(|input| {
            let mapped = (self.map_function)(input);
            f(&mapped)
        })
    }

    pub fn subscribe<S>(&self, subscriber: S)
    where S: Fn(&O) + Send + Sync + 'static {
        self.with_value(|value| subscriber(value));
        self.observers.write().unwrap().push(ObserverKind::Callback(Box::new(subscriber)));
    }
}

impl<I, O: Clone + 'static, F> Stateful<O> for MapSignal<I, O, F>
where F: Fn(&I) -> O + Send + Sync + 'static
{
    fn value(&self) -> O { self.with_value(|v| v.clone()) }
}

impl<I, O: std::fmt::Display + 'static, F> std::fmt::Display for MapSignal<I, O, F>
where F: Fn(&I) -> O + Send + Sync + 'static
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.with_value(|v| write!(f, "{}", v)) }
}
