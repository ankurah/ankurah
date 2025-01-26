use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

/// Similar to a Map Signal, but it is stateful.
/// and it will not keep the upstream signal alive.
pub struct Memo<I, O: 'static, F: Fn(&I) -> O> {
    value: Arc<RwLock<O>>,
    observers: ObserverSet,
    value_observers: ValueObserverSet<O>,
}

impl<I, O: 'static, F: Fn(&I) -> O> Memo<I, O, F> {
    pub fn new(upstream: &dyn Stateful<I>, f: F) -> Self {
        Self { value: Arc::new(RwLock::new(f(&upstream.value()))), observers: ObserverSet::new(), value_observers: ValueObserverSet::new() }
    }
}

impl Stateful<I> for Memo<I, O, F> {
    fn value(&self) -> O { self.value.read().unwrap() }
}

impl Value<O> for Memo<I, O, F> {
    fn value(&self) -> O { self.value.read().unwrap() }
}

impl WithValue<I, O> for Memo<I, O, F> {
    fn with_value(&self, f: impl Fn(&I) -> O) -> O { f(&self.value.read().unwrap()) }
}
