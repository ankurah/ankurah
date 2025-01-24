use crate::{
    Stateful, Value, WithValue,
    observer::{ObserverSet, ValueObserverSet},
};
use std::sync::{Arc, RwLock};

/// Read-only signal
pub struct Read<T> {
    value: Arc<RwLock<T>>,
    observers: ObserverSet,
    value_observers: ValueObserverSet<T>,
}

impl<T> Read<T> {
    pub(crate) fn new<S>(upstream: &S) -> Self
    where S: Stateful<T> + Observable<T> {
        // clone the state and become an observer of upstream
        let state = upstream.state();
        let observers = ObserverSet::new();
        // ???
        Self { state: upstream.state().clone(), observers: ObserverSet::new(), value_observers: ValueObserverSet::new() }
        unimplemented!()
    }

    // fn track_observer(&self) {
    //     if let Some(observer) = GLOBAL_OBSERVER.read().unwrap().as_ref() {
    //         let mut observers = self.observers.write().unwrap();
    //         let observer_weak = Arc::downgrade(&observer.0) as Weak<dyn Notify>;

    //         if !observers.iter().any(|entry| match entry {
    //             ObserverKind::Notify(weak) => weak.ptr_eq(&observer_weak),
    //             _ => false,
    //         }) {
    //             observers.push(ObserverKind::Notify(observer_weak));
    //         }
    //     }
    // }

    // fn with_value<R>(&self, f: impl FnOnce(&T) -> R) -> R {
    //     let guard = self.value.read().unwrap();
    //     self.track_observer();

    //     f(&*guard)
    // }

    // pub fn subscribe<F>(&self, f: F)
    // where F: Fn(&T) + Send + Sync + 'static {
    //     self.with_value(|value| f(value));
    //     self.observers.write().unwrap().push(ObserverKind::Callback(Box::new(f)));
    // }
}

impl<T: Clone> Stateful<T> for Read<T> {
    fn state(&self) -> Arc<RwLock<T>> { self.value.clone() }
}

impl<T: std::fmt::Display> std::fmt::Display for Read<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.with_value(|v| write!(f, "{}", v)) }
}
