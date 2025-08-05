use super::Observer;
use crate::{CurrentObserver, Signal, broadcast::ListenerGuard};
use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

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
    // Subscriptions mapped by signal pointer for mark-and-sweep
    subscriptions: RwLock<HashMap<usize, SubscriptionEntry>>,
}
struct WeakCallbackObserver(Weak<Inner>);

impl WeakCallbackObserver {
    fn upgrade(&self) -> Option<CallbackObserver> { self.0.upgrade().map(|inner| CallbackObserver(inner)) }
}

impl CallbackObserver {
    /// Create a new callback observer
    pub fn new<F: Fn() + Send + Sync + 'static>(callback: Arc<F>) -> Self {
        Self(Arc::new(Inner { callback: Box::new(move || callback()), subscriptions: RwLock::new(HashMap::new()) }))
    }

    /// Trigger the callback using this observer's context
    pub fn trigger(&self) { self.with_context(&self.0.callback); }

    /// Execute a function with this observer as the current context
    pub fn with_context<F: Fn()>(&self, f: &F) {
        // Mark all existing subscriptions for removal
        self.mark_all_for_removal();

        CurrentObserver::set(self.clone());
        f();
        CurrentObserver::remove(self);

        // Sweep away any subscriptions that weren't preserved during the callback
        self.sweep_marked_subscriptions();
    }

    pub fn clear(&self) {
        // Clear all subscriptions - they'll be dropped automatically
        match self.0.subscriptions.try_write() {
            Ok(mut subscriptions) => {
                subscriptions.clear();
            }
            Err(_) => {
                tracing::warn!("Possible recursion detected in clear - subscriptions lock is held");
                // Try with blocking write just in case we're not in a recursive situation
                self.0.subscriptions.write().unwrap().clear();
            }
        }
    }

    /// Mark all existing subscriptions for removal (mark phase of mark-and-sweep)
    fn mark_all_for_removal(&self) {
        if let Ok(mut subscriptions) = self.0.subscriptions.write() {
            for entry in subscriptions.values_mut() {
                entry.marked_for_removal = true;
            }
        }
    }

    /// Remove all subscriptions that are still marked for removal (sweep phase)
    fn sweep_marked_subscriptions(&self) {
        if let Ok(mut subscriptions) = self.0.subscriptions.write() {
            subscriptions.retain(|_, entry| !entry.marked_for_removal);
        }
    }
}

// Observer trait implementation - dyn safe
impl Observer for CallbackObserver {
    fn observe(&self, signal: &dyn Signal) {
        // Use the broadcast inner pointer as a unique identifier for the signal
        let broadcast_ref = signal.broadcast();
        let signal_id = broadcast_ref.unique_id();

        // Check if we already have a subscription to this signal
        if let Ok(mut subscriptions) = self.0.subscriptions.write() {
            if let Some(entry) = subscriptions.get_mut(&signal_id) {
                // We already have a subscription, just unmark it for removal
                entry.marked_for_removal = false;
                return;
            }

            // Create new subscription
            let weak = WeakCallbackObserver(Arc::downgrade(&self.0));

            let subscription = broadcast_ref.listen(move || {
                if let Some(observer) = weak.upgrade() {
                    observer.trigger();
                }
            });

            let entry = SubscriptionEntry { _guard: subscription, marked_for_removal: false };

            subscriptions.insert(signal_id, entry);
        }
    }

    fn observer_id(&self) -> usize { Arc::as_ptr(&self.0) as *const _ as usize }
}
