//! React Native integration for ankurah-signals via UniFFI
//!
//! This module provides the `useObserve` pattern for React Native components to
//! automatically re-render when signals they access are updated.
//!
//! Unlike the WASM version which can directly pass closures to React's useSyncExternalStore,
//! UniFFI requires a callback interface pattern where JS implements the callback trait.

use std::collections::HashMap;
use std::sync::{
    Arc, Mutex, RwLock, Weak,
    atomic::{AtomicUsize, Ordering},
};

use crate::{CurrentObserver, Signal, broadcast::BroadcastId, observer::Observer, signal::ListenerGuard};

/// Callback interface for notifying React of store changes
/// JS implements this trait and passes it to subscribe()
#[uniffi::export(callback_interface)]
pub trait StoreChangeCallback: Send + Sync {
    /// Called when any observed signal changes
    fn on_change(&self);
}

struct ListenerEntry {
    _guard: ListenerGuard,
    marked_for_removal: bool,
}

/// Inner state shared between ReactObserver and its weak references
struct Inner {
    /// Signal subscriptions keyed by broadcast ID
    entries: RwLock<HashMap<BroadcastId, ListenerEntry>>,
    /// Version counter for React's useSyncExternalStore snapshot
    version: AtomicUsize,
    /// The callback to trigger React re-renders
    /// Uses Arc so we can clone it out of the lock before calling (avoids deadlock)
    trigger_render: Mutex<Option<Arc<dyn StoreChangeCallback>>>,
}

/// Weak reference to ReactObserver for use in signal listeners
struct ReactObserverWeak(Weak<Inner>);

impl ReactObserverWeak {
    fn upgrade(&self) -> Option<ReactObserver> { self.0.upgrade().map(ReactObserver) }
}

/// A React Native observer handle that manages signal subscriptions for a component
///
/// This is the UniFFI equivalent of the WASM ReactObserver. It integrates with
/// React's useSyncExternalStore via callback interfaces.
///
/// # Usage from TypeScript
/// ```typescript
/// function useObserve() {
///   const observerRef = useRef<ReactObserver | null>(null);
///   if (!observerRef.current) {
///     observerRef.current = new ReactObserver();
///   }
///   const observer = observerRef.current;
///   
///   const subscribe = useCallback((onStoreChange: () => void) => {
///     const callback: StoreChangeCallback = { onChange: onStoreChange };
///     observer.subscribe(callback);
///     return () => observer.unsubscribe();
///   }, [observer]);
///   
///   const getSnapshot = useCallback(() => observer.getSnapshot(), [observer]);
///   
///   useSyncExternalStore(subscribe, getSnapshot);
///   
///   observer.beginTracking();
///   // ... component renders, accessing signals ...
///   // In finally block: observer.finish();
/// }
/// ```
#[derive(uniffi::Object, Clone)]
pub struct ReactObserver(Arc<Inner>);

#[uniffi::export]
impl ReactObserver {
    /// Create a new ReactObserver
    #[uniffi::constructor]
    pub fn new() -> Self {
        Self(Arc::new(Inner { entries: RwLock::new(HashMap::new()), version: AtomicUsize::new(0), trigger_render: Mutex::new(None) }))
    }

    /// Subscribe to store changes
    ///
    /// The callback will be called whenever any observed signal changes.
    /// This is designed to be used with React's useSyncExternalStore.
    pub fn subscribe(&self, callback: Box<dyn StoreChangeCallback>) {
        let mut trigger = self.0.trigger_render.lock().expect("trigger_render lock poisoned");
        *trigger = Some(Arc::from(callback));
    }

    /// Unsubscribe from store changes
    pub fn unsubscribe(&self) {
        let mut trigger = self.0.trigger_render.lock().expect("trigger_render lock poisoned");
        *trigger = None;
    }

    /// Get the current snapshot version
    ///
    /// This returns a monotonically increasing number that changes whenever
    /// any observed signal changes. Used by React's useSyncExternalStore.
    pub fn get_snapshot(&self) -> u64 { self.0.version.load(Ordering::Relaxed) as u64 }

    /// Begin tracking signal accesses for this render
    ///
    /// Call this at the start of your component render. Any signals accessed
    /// after this call will be automatically subscribed to.
    pub fn begin_tracking(&self) {
        // Mark all existing listeners for removal (mark phase of mark-and-sweep)
        let mut entries = self.0.entries.write().expect("entries lock poisoned");
        for entry in entries.values_mut() {
            entry.marked_for_removal = true;
        }
        drop(entries);

        // Set as current observer context
        CurrentObserver::set(self.clone());
    }

    /// Finish tracking and clean up unused subscriptions
    ///
    /// Call this in a finally block after component rendering completes.
    /// This removes subscriptions to signals that weren't accessed during this render.
    pub fn finish(&self) {
        // Sweep away any listeners that weren't preserved during render
        let mut entries = self.0.entries.write().expect("entries lock poisoned");
        entries.retain(|_, entry| !entry.marked_for_removal);
        drop(entries);

        // Remove from current observer context
        CurrentObserver::remove(self as &dyn Observer);
    }

    /// Get the number of signals currently being observed
    pub fn signal_count(&self) -> u32 {
        self.0.entries.read().expect("entries lock poisoned").values().filter(|e| !e.marked_for_removal).count() as u32
    }
}

impl ReactObserver {
    fn weak(&self) -> ReactObserverWeak { ReactObserverWeak(Arc::downgrade(&self.0)) }
}

/// Observer implementation for ReactObserver
///
/// This is called automatically when signals are accessed during a render
/// while this observer is the current context.
impl Observer for ReactObserver {
    fn observe(&self, signal: &dyn Signal) {
        let broadcast_id = signal.broadcast_id();

        let mut entries = self.0.entries.write().expect("entries lock poisoned");

        // Check if we already have a listener for this broadcast
        if let Some(entry) = entries.get_mut(&broadcast_id) {
            // Already subscribed, just unmark for removal
            entry.marked_for_removal = false;
            return;
        }

        // Create new listener using weak reference to prevent circular reference
        let weak = self.weak();
        entries.insert(
            broadcast_id,
            ListenerEntry {
                _guard: signal.listen(Arc::new(move |_| {
                    if let Some(observer) = weak.upgrade() {
                        // Increment version to trigger React re-render
                        observer.0.version.fetch_add(1, Ordering::Relaxed);

                        // Clone callback out of the lock BEFORE calling to avoid deadlock.
                        // If we held the lock while calling on_change(), and the JS callback
                        // triggered subscribe/unsubscribe, we'd deadlock.
                        let callback = observer.0.trigger_render.lock().ok().and_then(|guard| guard.clone());
                        if let Some(cb) = callback {
                            cb.on_change();
                        }
                    }
                })),
                marked_for_removal: false,
            },
        );
    }

    fn observer_id(&self) -> usize { Arc::as_ptr(&self.0) as *const _ as usize }

    fn as_any(&self) -> &dyn std::any::Any { self }
}

// ============================================================================
// JsSignal - A signal that can be created and controlled from JavaScript
// ============================================================================

/// A signal that can be created and controlled from JavaScript/TypeScript.
///
/// This allows TypeScript code to create signals that participate in the
/// Ankurah signal graph. The value is stored in JS - Rust only handles
/// the observation and notification mechanism.
///
/// # Usage from TypeScript
/// ```typescript
/// function useSignal<T>(initialValue: T) {
///   const [signal] = useState(() => ({
///     rust: new JsSignal(),
///     value: initialValue,
///   }));
///
///   return {
///     get: () => {
///       signal.rust.track(); // Register with current observer
///       return signal.value;
///     },
///     set: (newValue: T) => {
///       signal.value = newValue;
///       signal.rust.notify(); // Trigger re-renders
///     },
///   };
/// }
/// ```
#[derive(uniffi::Object)]
pub struct JsSignal {
    broadcast: crate::broadcast::Broadcast<()>,
}

#[uniffi::export]
impl JsSignal {
    /// Create a new JsSignal
    #[uniffi::constructor]
    pub fn new() -> Self { Self { broadcast: crate::broadcast::Broadcast::new() } }

    /// Call this when reading the signal value.
    /// Registers this signal with the current observer (if any).
    pub fn track(&self) { CurrentObserver::track(self); }

    /// Call this when the signal value changes.
    /// Notifies all observers that this signal has changed.
    pub fn notify(&self) { self.broadcast.send(()); }
}

impl crate::signal::Signal for JsSignal {
    fn listen(&self, listener: crate::signal::Listener) -> crate::signal::ListenerGuard {
        crate::signal::ListenerGuard::new(self.broadcast.reference().listen(listener))
    }

    fn broadcast_id(&self) -> crate::broadcast::BroadcastId { self.broadcast.id() }
}
