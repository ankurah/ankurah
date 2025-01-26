use crate::{Renderer, Subscriber, observer::Observer, traits::Signal};
use std::{
    ops::Deref,
    sync::{Arc, RwLock, RwLockReadGuard},
};

pub(crate) struct Value<T>(Arc<RwLock<T>>);

impl<T> Clone for Value<T> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<T> Value<T> {
    pub fn new(value: T) -> Self { Self(Arc::new(RwLock::new(value))) }

    pub fn set(&self, value: T) {
        let mut current = self.0.write().unwrap();
        *current = value;
    }
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let guard = self.0.read().unwrap();
        f(&*guard)
    }
    pub fn set_with<R>(&self, value: T, f: impl Fn(&T) -> R) -> R {
        let mut current = self.0.write().unwrap();
        *current = value;
        f(&*current)
    }
}

impl<T: Clone> Value<T> {
    pub fn value(&self) -> T { self.0.read().unwrap().clone() }
}

// impl<T> Into<T> for Value<T>
// where
//     T: Clone,
// {
//     fn into(self) -> T { self.0.read().unwrap().clone() }
// }

static CURRENT_CONTEXT: RwLock<Option<ObserverContext>> = RwLock::new(None);
pub struct CurrentContext {}

impl CurrentContext {
    /// Subscribes the current context to a signal
    pub fn track<S: Signal<T>, T>(signal: &S) {
        if let Some(observer) = CURRENT_CONTEXT.read().unwrap().as_ref() {
            signal.subscribe(observer.subscriber());
        }
    }

    /// Sets an observer as the current context
    pub fn set(current: impl Into<ObserverContext>) {
        let current: ObserverContext = current.into();
        current.clear();
        *CURRENT_CONTEXT.write().unwrap() = Some(current);
    }

    /// Resets the current context to no observer
    pub fn unset() { *CURRENT_CONTEXT.write().unwrap() = None; }
}

pub enum ObserverContext {
    Renderer(Renderer),
    Observer(Observer),
}

impl ObserverContext {
    pub fn clear(&self) {
        match self {
            ObserverContext::Renderer(renderer) => renderer.clear(),
            ObserverContext::Observer(observer) => observer.clear(),
        }
    }
    pub fn subscriber<T>(&self) -> Subscriber<T> {
        match self {
            ObserverContext::Renderer(renderer) => renderer.subscriber(),
            ObserverContext::Observer(observer) => observer.subscriber(),
        }
    }
}

impl Into<ObserverContext> for Renderer {
    fn into(self) -> ObserverContext { ObserverContext::Renderer(self) }
}
impl Into<ObserverContext> for Observer {
    fn into(self) -> ObserverContext { ObserverContext::Observer(self) }
}
