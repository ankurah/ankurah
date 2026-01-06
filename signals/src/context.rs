use crate::{Observer, Signal};
use std::sync::Arc;

// ============================================================================
// Thread-local stack (default) - for single-threaded environments like WASM
// ============================================================================
#[cfg(not(feature = "multithread"))]
mod stack {
    use super::*;
    use std::cell::RefCell;

    thread_local! {
        static OBSERVER_STACK: RefCell<Vec<Arc<dyn Observer>>> = RefCell::new(Vec::new());
    }

    pub fn track<S: Signal>(signal: &S) {
        OBSERVER_STACK.with(|stack| {
            if let Some(observer) = stack.borrow().last() {
                observer.observe(signal);
            }
        });
    }

    pub fn set<O: Observer + 'static>(observer: O) {
        OBSERVER_STACK.with(|stack| {
            stack.borrow_mut().push(Arc::new(observer));
        });
    }

    pub fn pop() {
        OBSERVER_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }

    pub fn remove(observer: &dyn Observer) {
        let target_id = observer.observer_id();
        OBSERVER_STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            if let Some(last) = stack.last()
                && last.observer_id() == target_id
            {
                stack.pop();
                return;
            }
            stack.retain(|o| o.observer_id() != target_id);
        });
    }

    pub fn current() -> Option<Arc<dyn Observer>> {
        OBSERVER_STACK.with(|stack| stack.borrow().last().cloned())
    }
}

// ============================================================================
// Global stack (multithread) - for cross-thread environments like React Native
// ============================================================================
#[cfg(feature = "multithread")]
mod stack {
    use super::*;
    use std::sync::RwLock;

    static OBSERVER_STACK: RwLock<Vec<Arc<dyn Observer>>> = RwLock::new(Vec::new());

    pub fn track<S: Signal>(signal: &S) {
        if let Ok(stack) = OBSERVER_STACK.read() {
            if let Some(observer) = stack.last() {
                observer.observe(signal);
            }
        }
    }

    pub fn set<O: Observer + 'static>(observer: O) {
        if let Ok(mut stack) = OBSERVER_STACK.write() {
            stack.push(Arc::new(observer));
        }
    }

    pub fn pop() {
        if let Ok(mut stack) = OBSERVER_STACK.write() {
            stack.pop();
        }
    }

    pub fn remove(observer: &dyn Observer) {
        let target_id = observer.observer_id();
        if let Ok(mut stack) = OBSERVER_STACK.write() {
            if let Some(last) = stack.last() {
                if last.observer_id() == target_id {
                    stack.pop();
                    return;
                }
            }
            stack.retain(|o| o.observer_id() != target_id);
        }
    }

    pub fn current() -> Option<Arc<dyn Observer>> {
        OBSERVER_STACK.read().ok().and_then(|stack| stack.last().cloned())
    }
}

/// Manages the current observer stack
/// and provides a way to subscribe the current observer to a given signal
pub struct CurrentObserver {}

impl CurrentObserver {
    /// Subscribes the current context to a signal
    pub fn track<S>(signal: &S)
    where S: Signal {
        stack::track(signal);
    }

    /// Sets an observer as the current context, pushing it onto the stack
    pub fn set<O: Observer + 'static>(observer: O) {
        stack::set(observer);
    }

    /// Removes the current observer from the stack, restoring the previous one
    pub fn pop() {
        stack::pop();
    }

    /// Removes a specific observer from the stack
    pub fn remove(observer: &dyn Observer) {
        stack::remove(observer);
    }

    /// Get a copy of the current observer context (for testing/debugging)
    pub fn current() -> Option<Arc<dyn Observer>> {
        stack::current()
    }
}
