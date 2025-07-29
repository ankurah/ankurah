use crate::{Renderer, Subscriber, SubscriptionHandle, observer::Observer, traits::Signal};
use std::{
    cell::RefCell,
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

// Thread-local stack for nested observer contexts (e.g., React component trees)
thread_local! {
    static OBSERVER_STACK: RefCell<Vec<ObserverContext>> = RefCell::new(Vec::new());
}

pub struct CurrentContext {}

impl CurrentContext {
    /// Subscribes the current context to a signal
    pub fn track<S: Signal<T>, T>(signal: &S) {
        OBSERVER_STACK.with(|stack| {
            if let Some(observer) = stack.borrow().last() {
                let subscriber = observer.subscriber();
                let _handle = signal.subscribe(subscriber);
                // TODO: Properly manage subscription handle lifecycle
                // For now, let it drop - this means subscriptions are temporary
                // This is a known limitation that needs to be addressed
            }
        });
    }

    /// Sets an observer as the current context, pushing it onto the stack
    pub fn set(current: impl Into<ObserverContext>) {
        let current: ObserverContext = current.into();
        current.clear();
        OBSERVER_STACK.with(|stack| {
            stack.borrow_mut().push(current);
        });
    }

    /// Removes the current observer from the stack, restoring the previous one
    pub fn unset() {
        OBSERVER_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }

    /// Get a copy of the current observer context (for testing/debugging)
    pub fn current() -> Option<ObserverContext> { OBSERVER_STACK.with(|stack| stack.borrow().last().cloned()) }
}

pub enum ObserverContext {
    Renderer(Renderer),
    Observer(Observer),
}

impl Clone for ObserverContext {
    fn clone(&self) -> Self {
        match self {
            ObserverContext::Renderer(renderer) => ObserverContext::Renderer(renderer.clone()),
            ObserverContext::Observer(observer) => ObserverContext::Observer(observer.clone()),
        }
    }
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
    pub fn store_handle(&self, handle: SubscriptionHandle<'static>) {
        match self {
            ObserverContext::Renderer(renderer) => renderer.store_handle(handle),
            ObserverContext::Observer(observer) => observer.store_handle(handle),
        }
    }
}

impl Into<ObserverContext> for Renderer {
    fn into(self) -> ObserverContext { ObserverContext::Renderer(self) }
}
impl Into<ObserverContext> for Observer {
    fn into(self) -> ObserverContext { ObserverContext::Observer(self) }
}
