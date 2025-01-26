use std::{
    ops::Deref,
    sync::{Arc, RwLock},
};

use crate::traits::{Notify, Signal};

pub(crate) struct Value<T>(Arc<RwLock<T>>);

impl<T> Value<T> {
    pub fn new(value: T) -> Self { Self(Arc::new(RwLock::new(value))) }

    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let guard = self.0.read().unwrap();
        f(&*guard)
    }
}

// impl<T> Into<T> for Value<T>
// where
//     T: Clone,
// {
//     fn into(self) -> T { self.0.read().unwrap().clone() }
// }

static CURRENT_CONTEXT: RwLock<Option<Box<dyn Notify>>> = RwLock::new(None);
pub struct CurrentContext {}

impl CurrentContext {
    /// Subscribes the current context to a signal
    pub fn track<S: Signal<T>, T>(signal: &S) {
        if let Some(observer) = CURRENT_CONTEXT.read().unwrap().as_ref() {
            signal.subscribe(observer);
        }
    }

    /// Sets an observer as the current context
    pub fn set<T: Notify>(current: &T) { *CURRENT_CONTEXT.write().unwrap() = Some(Box::new(current)); }

    /// Resets the current context to no observer
    pub fn unset() { *CURRENT_CONTEXT.write().unwrap() = None; }
}
