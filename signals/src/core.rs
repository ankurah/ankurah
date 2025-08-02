use crate::{Signal, traits::Observer};
use std::{
    cell::RefCell,
    sync::{Arc, RwLock},
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
        println!("Value::set_with pre-lock");
        let mut current = self.0.write().unwrap();
        println!("Value::set_with post-lock");
        *current = value;
        f(&*current)
    }
}

impl<T: Clone> Value<T> {
    pub fn value(&self) -> T { self.0.read().unwrap().clone() }
}

// Thread-local stack for nested observer contexts (e.g., React component trees)
thread_local! {
    static OBSERVER_STACK: RefCell<Vec<Arc<dyn Observer>>> = RefCell::new(Vec::new());
}

pub struct CurrentContext {}

impl CurrentContext {
    /// Subscribes the current context to a signal
    pub fn track<S, T>(signal: &S)
    where
        S: Signal<T>,
        T: 'static,
    {
        OBSERVER_STACK.with(|stack| {
            if let Some(observer) = stack.borrow().last() {
                observer.observe_signal_receiver(signal.observe());
            }
        });
    }

    /// Sets an observer as the current context, pushing it onto the stack  
    pub fn set<O: Observer + 'static>(observer: O) {
        OBSERVER_STACK.with(|stack| {
            stack.borrow_mut().push(Arc::new(observer));
        });
    }

    /// Removes the current observer from the stack, restoring the previous one
    pub fn pop() {
        OBSERVER_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }

    /// Removes a specific observer from the stack
    pub fn remove(observer: &dyn Observer) {
        let target_id = observer.observer_id();
        OBSERVER_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            // Check if the observer we want to remove is the last one
            if let Some(last) = stack.last() {
                if last.observer_id() == target_id {
                    stack.pop();
                    return;
                }
            }
            // If not the last one, search and remove it
            stack.retain(|o| o.observer_id() != target_id);
        });
    }

    /// Get a copy of the current observer context (for testing/debugging)
    pub fn current() -> Option<Arc<dyn Observer>> { OBSERVER_STACK.with(|stack| stack.borrow().last().cloned()) }
}
