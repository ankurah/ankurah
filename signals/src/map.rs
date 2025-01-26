use crate::observer::ObserverSet;
use crate::traits::Stateful;
use std::marker::PhantomData;
use std::sync::{Arc, Weak};

/// A signal that is a map of another signal
/// Stateless.
pub struct Map<I, O: 'static, F: Fn(&I) -> O> {
    map_function: Arc<F>,
    observers: ObserverSet,
}

impl<I, O: 'static, F> Map<I, O, F>
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

impl<I, O: Clone + 'static, F> Stateful<O> for Map<I, O, F>
where F: Fn(&I) -> O + Send + Sync + 'static
{
    fn value(&self) -> O { self.with_value(|v| v.clone()) }
}

impl<I, O: std::fmt::Display + 'static, F> std::fmt::Display for Map<I, O, F>
where F: Fn(&I) -> O + Send + Sync + 'static
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.with_value(|v| write!(f, "{}", v)) }
}
