use super::Observer;
use crate::{CurrentObserver, Signal, broadcast::BroadcastId, signal::ListenerGuard};
use std::collections::HashMap;
use std::sync::{Arc, Weak};

/// A CallbackObserver is an observer that wraps a callback which is called
/// whenver the observed signals notify the observer of a change.
#[derive(Clone)]
pub struct CallbackObserver(Arc<Inner>);
struct SubscriptionEntry {
    _guard: ListenerGuard,
    marked_for_removal: bool,
}

struct Inner {
    // The callback to call when the observed signals notify the observer of a change
    callback: Box<dyn Fn() + Send + Sync>,
    // Subscriptions mapped by broadcast ID for mark-and-sweep
    entries: std::sync::RwLock<HashMap<BroadcastId, SubscriptionEntry>>,
}
struct WeakCallbackObserver(Weak<Inner>);

impl WeakCallbackObserver {
    fn upgrade(&self) -> Option<CallbackObserver> { self.0.upgrade().map(CallbackObserver) }
}

impl CallbackObserver {
    /// Create a new callback observer
    pub fn new<F: Fn() + Send + Sync + 'static>(callback: Arc<F>) -> Self {
        Self(Arc::new(Inner { callback: Box::new(move || callback()), entries: std::sync::RwLock::new(HashMap::new()) }))
    }

    /// Trigger the callback using this observer's context
    pub fn trigger(&self) { self.with_context(&self.0.callback); }

    /// Execute a function with this observer as the current context
    pub fn with_context<F: Fn()>(&self, f: &F) {
        // Mark all existing listeners for removal
        self.mark_all_for_removal();

        CurrentObserver::set(self.clone());
        f();
        CurrentObserver::remove(self);

        // Sweep away any listeners that weren't preserved during the callback
        self.sweep_marked_listeners();
    }

    pub fn clear(&self) {
        // Clear all listeners - they'll be dropped automatically
        self.0.entries.write().expect("entries lock is poisoned").clear();
    }

    /// Mark all existing listeners for removal (mark phase of mark-and-sweep)
    fn mark_all_for_removal(&self) {
        // keep the for loop
        let mut entries = self.0.entries.write().expect("entries lock is poisoned");
        for entry in entries.values_mut() {
            entry.marked_for_removal = true;
        }
    }

    /// Remove all listeners that are still marked for removal (sweep phase)
    fn sweep_marked_listeners(&self) {
        let mut entries = self.0.entries.write().expect("entries lock is poisoned");
        entries.retain(|_, entry| !entry.marked_for_removal);
    }
}

// Observer trait implementation - dyn safe
impl Observer for CallbackObserver {
    fn observe(&self, signal: &dyn Signal) {
        // Use the signal's broadcast ID for identification
        let broadcast_id = signal.broadcast_id();

        // Check if we already have a listener for this broadcast
        let mut entries = self.0.entries.write().expect("entries lock is poisoned");

        if let Some(entry) = entries.get_mut(&broadcast_id) {
            // We already have a Listener/ListenerGuard for this broadcast, just unmark it for removal
            entry.marked_for_removal = false;
            return;
        }

        // Create new listener
        let weak = WeakCallbackObserver(Arc::downgrade(&self.0));
        entries.insert(
            broadcast_id,
            SubscriptionEntry {
                _guard: signal.listen(Arc::new(move |_| {
                    if let Some(observer) = weak.upgrade() {
                        observer.trigger();
                    }
                })),
                marked_for_removal: false,
            },
        );
    }

    fn observer_id(&self) -> usize { Arc::as_ptr(&self.0) as *const _ as usize }

    #[doc(hidden)]
    fn as_any(&self) -> &dyn std::any::Any { self }
}
