use std::sync::{Arc, RwLock};

use crate::{
    CurrentContext,
    traits::Observer,
    util::task::{self, TaskHandle},
};

/// A CallbackObserver is an observer that wraps a callback which is called
/// whenver the observed signals notify the observer of a change.
pub struct CallbackObserver(Arc<CallbackObserverInner>);
pub struct CallbackObserverInner {
    // The callback to call when the observed signals notify the observer of a change
    callback: Box<dyn Fn() + Send + Sync>,
    // Task handles for managing signal observation tasks
    task_handles: RwLock<Vec<TaskHandle>>,
}

impl CallbackObserver {
    /// Create a new callback observer
    pub fn new<F: Fn() + Send + Sync + 'static>(callback: Arc<F>) -> Self {
        Self(Arc::new(CallbackObserverInner { callback: Box::new(move || callback()), task_handles: RwLock::new(vec![]) }))
    }
}

impl std::ops::Deref for CallbackObserver {
    type Target = Arc<CallbackObserverInner>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Clone for CallbackObserver {
    fn clone(&self) -> Self { CallbackObserver(Arc::clone(&self.0)) }
}

impl CallbackObserverInner {
    /// Trigger the callback using this observer's context
    pub fn trigger(self: &Arc<Self>) { self.with_context(&self.callback); }

    /// Execute a function with this observer as the current context
    pub fn with_context<F: Fn()>(self: &Arc<Self>, f: &F) {
        let observer = CallbackObserver(Arc::clone(&self));
        CurrentContext::set(observer.clone());
        f();
        CurrentContext::remove(&observer);
    }

    pub fn clear(self: &Arc<Self>) {
        // Abort all tasks - they'll be dropped automatically
        self.task_handles.write().unwrap().clear();
    }

    pub fn clone(self: &Arc<Self>) -> CallbackObserver { CallbackObserver(Arc::clone(&self)) }
}

// Observer trait implementation - dyn safe
impl Observer for CallbackObserver {
    fn observe_signal_receiver(&self, mut receiver: tokio::sync::broadcast::Receiver<()>) {
        let observer_weak = Arc::downgrade(self);

        let handle = task::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match receiver.recv().await {
                    Ok(_) | Err(RecvError::Lagged(_)) => {
                        if let Some(observer) = observer_weak.upgrade() {
                            observer.trigger();
                        } else {
                            break; // Observer dropped
                        }
                    }
                    Err(RecvError::Closed) => {
                        // Signal dropped, end task
                        break;
                    }
                }
            }
        });

        self.task_handles.write().unwrap().push(handle);
    }

    fn observer_id(&self) -> usize { Arc::as_ptr(&self.0) as *const _ as usize }
}
