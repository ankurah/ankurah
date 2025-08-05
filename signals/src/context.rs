use crate::{Observer, Signal};
use std::{cell::RefCell, sync::Arc};

// Thread-local stack for nested observer contexts (e.g., React component trees)
thread_local! {
    static OBSERVER_STACK: RefCell<Vec<Arc<dyn Observer>>> = RefCell::new(Vec::new());
}

/// Manages the current observer stack
/// and provides a way to subscribe the current observer to a given signal
pub struct CurrentObserver {}

impl CurrentObserver {
    /// Subscribes the current context to a signal
    pub fn track<S>(signal: &S)
    where S: Signal {
        OBSERVER_STACK.with(|stack| {
            if let Some(observer) = stack.borrow().last() {
                observer.observe(signal);
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
