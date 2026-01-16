//! React integration for ankurah-signals
//!
//! This module provides the `useObserve` hook that allows React components to
//! automatically re-render when signals they access are updated.

use send_wrapper::SendWrapper;
use wasm_bindgen::prelude::*;

use std::collections::HashMap;
#[cfg(debug_assertions)]
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{CurrentObserver, Signal, broadcast::BroadcastId, observer::Observer, signal::ListenerGuard};

#[wasm_bindgen(module = "react")]
extern "C" {
    #[wasm_bindgen(catch)]
    fn useRef() -> Result<JsValue, JsValue>;
    #[wasm_bindgen(catch)]
    fn useSyncExternalStore(
        subscribe: &Closure<dyn Fn(js_sys::Function) -> JsValue>,
        get_snapshot: &Closure<dyn Fn() -> JsValue>,
        get_server_snapshot: &Closure<dyn Fn() -> JsValue>,
    ) -> Result<JsValue, JsValue>;
}

struct ListenerEntry {
    _guard: ListenerGuard,
    marked_for_removal: bool,
}

/// A React observer handle that manages signal subscriptions for a component
#[wasm_bindgen]
#[derive(Clone)]
pub struct ReactObserver(Arc<Inner>);

struct ReactObserverWeak(Weak<Inner>);

pub struct Inner {
    #[cfg(debug_assertions)]
    name: OnceLock<Option<String>>,
    entries: std::sync::RwLock<HashMap<BroadcastId, ListenerEntry>>,
    /// This function gets called when a change is observed.
    /// Using Mutex<Option<...>> to support subscribe/unsubscribe pattern.
    trigger_render: Arc<std::sync::Mutex<Option<SendWrapper<js_sys::Function>>>>,

    /// This function gets called by react on the initial render with the notify_fn as an argument
    /// we have to store it to prevent it from being dropped.
    /// Only used by the legacy useObserve() hook.
    subscribe_fn: SendWrapper<Closure<dyn Fn(js_sys::Function) -> JsValue>>,
    /// get_snapshot is called by react for every render to get the "snapshot" - which in our case is just the version number
    /// it's a goofy way to get react to re-render the component. We're using this instead of the typical
    /// 'let [_,forceUpdate] = useReducer((a) => a + 1, 0)' pattern because it's what preact-signals uses.
    /// I haven't yet analyzed the react dispatch cycle internals sufficiently well to know the difference, but
    /// the preact folks probably didn't choose useSyncExternalStore by throwing a dart at the dartboard,
    /// so I'm inclined to start there.
    /// Only used by the legacy useObserve() hook.
    get_snapshot: SendWrapper<Closure<dyn Fn() -> JsValue>>,
    version: Arc<AtomicUsize>,
}

impl ReactObserverWeak {
    fn upgrade(&self) -> Option<ReactObserver> { self.0.upgrade().map(ReactObserver) }
}

impl ReactObserver {
    fn new() -> Self {
        // Version counter for React's useSyncExternalStore
        let version = Arc::new(AtomicUsize::new(0));

        // Storage for React's on_store_change callback
        let trigger_render = Arc::new(std::sync::Mutex::new(None));

        // Create subscription function for useSyncExternalStore (legacy useObserve hook)
        let subscribe_fn = {
            let trigger_render = trigger_render.clone();
            Closure::wrap(Box::new(move |callback: js_sys::Function| {
                // Store the callback for later use when signals change
                if let Ok(mut guard) = trigger_render.lock() {
                    *guard = Some(SendWrapper::new(callback));
                }
                JsValue::UNDEFINED
            }) as Box<dyn Fn(js_sys::Function) -> JsValue>)
        };

        // Create snapshot function for useSyncExternalStore (legacy useObserve hook)
        let get_snapshot = {
            let version = version.clone();
            Closure::wrap(Box::new(move || JsValue::from(version.load(Ordering::Relaxed))) as Box<dyn Fn() -> JsValue>)
        };

        Self(Arc::new(Inner {
            #[cfg(debug_assertions)]
            name: OnceLock::new(),
            version,
            entries: std::sync::RwLock::new(HashMap::new()),
            trigger_render,
            subscribe_fn: SendWrapper::new(subscribe_fn),
            get_snapshot: SendWrapper::new(get_snapshot),
        }))
    }
    fn weak(&self) -> ReactObserverWeak { ReactObserverWeak(Arc::downgrade(&self.0)) }

    /// Mark all existing listeners for removal (mark phase of mark-and-sweep)
    fn mark_all_for_removal(&self) {
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

    /// Get the number of signals currently being observed (excluding those marked for removal)
    #[cfg(debug_assertions)]
    pub fn signal_count(&self) -> usize {
        self.0.entries.read().expect("entries lock is poisoned").values().filter(|e| !e.marked_for_removal).count()
    }

    /// Get the observer name (only available in debug builds)
    #[cfg(debug_assertions)]
    pub fn name(&self) -> Option<String> { self.0.name.get().and_then(|opt| opt.clone()) }

    /// Set the observer name (debug builds only, can only be set once)
    /// Throws an error if the name has already been set to a different value
    #[cfg(debug_assertions)]
    pub fn set_name(&self, name: String) -> Result<(), JsValue> {
        // Check if a name is already set
        if let Some(existing) = self.0.name.get() {
            // If it's the same name, that's fine
            if existing.as_ref() == Some(&name) {
                return Ok(());
            }
            // Different name - error
            return Err(JsValue::from_str(&format!(
                "Observer name already set to '{}', cannot change to '{}'",
                existing.as_ref().unwrap_or(&"<none>".to_string()),
                name
            )));
        }
        // No name set yet, set it
        self.0.name.set(Some(name)).map_err(|_| JsValue::from_str("Failed to set observer name"))
    }
}

#[wasm_bindgen]
impl ReactObserver {
    /// Create a new ReactObserver
    ///
    /// This constructor is exported for use with @ankurah/react-hooks.
    /// The hooks package manages React integration (useRef, useSyncExternalStore)
    /// in JavaScript, and just needs the Rust observer for signal tracking.
    #[wasm_bindgen(constructor)]
    pub fn js_new() -> Self { Self::new() }

    /// Begin tracking signal accesses for this render
    ///
    /// Call this at the start of your component render. Any signals accessed
    /// after this call will be automatically subscribed to.
    #[wasm_bindgen(js_name = beginTracking)]
    pub fn begin_tracking(&self) {
        // Mark all existing listeners for removal (mark phase of mark-and-sweep)
        self.mark_all_for_removal();
        // Set as current observer context
        CurrentObserver::set(self.clone());
    }

    /// Finish using this observer and restore the previous context
    /// This should be called in a finally block after component rendering
    #[wasm_bindgen]
    pub fn finish(&self) {
        // Sweep away any listeners that weren't preserved during render
        self.sweep_marked_listeners();
        CurrentObserver::remove(self as &dyn Observer);
    }

    /// Subscribe to store changes
    ///
    /// The callback object should have an `onChange()` method that will be called
    /// whenever any observed signal changes. Used with React's useSyncExternalStore.
    #[wasm_bindgen]
    pub fn subscribe(&self, callback: JsValue) {
        // Extract onChange function from callback object
        if let Ok(on_change) = js_sys::Reflect::get(&callback, &JsValue::from_str("onChange")) {
            if let Some(func) = on_change.dyn_ref::<js_sys::Function>() {
                if let Ok(mut guard) = self.0.trigger_render.lock() {
                    *guard = Some(SendWrapper::new(func.clone()));
                }
            }
        }
    }

    /// Unsubscribe from store changes
    #[wasm_bindgen]
    pub fn unsubscribe(&self) {
        if let Ok(mut guard) = self.0.trigger_render.lock() {
            *guard = None;
        }
    }

    /// Get the current snapshot version
    ///
    /// Returns a monotonically increasing number that changes whenever
    /// any observed signal changes. Used by React's useSyncExternalStore.
    #[wasm_bindgen(js_name = getSnapshot)]
    pub fn get_snapshot(&self) -> js_sys::BigInt { js_sys::BigInt::from(self.0.version.load(Ordering::Relaxed) as u64) }

    /// Get the number of signals currently being observed by this observer
    #[wasm_bindgen(js_name = signalCount)]
    pub fn js_signal_count(&self) -> usize {
        self.0.entries.read().expect("entries lock is poisoned").values().filter(|e| !e.marked_for_removal).count()
    }

    /// Get the observer name (debug builds only)
    #[cfg(debug_assertions)]
    #[wasm_bindgen(js_name = name, getter)]
    pub fn js_name(&self) -> Option<String> { self.name() }
}

/// React hook for observing signals within a component
///
/// This is the new useObserve hook that replaces the old withSignals pattern.
/// Instead of taking a closure, it returns an observer object that you call .finish() on.
///
/// The hook must be used with a try-finally pattern to ensure proper cleanup.
/// Call .get() on signals within the try block to register subscriptions.
/// Always call observer.finish() in the finally block.
///
/// # Example
/// ```javascript
/// const observer = useObserve();
/// try {
///   const items = query.get();
///   return <div>...</div>;
/// } finally {
///   observer.finish();
/// }
/// ```
///
/// For diagnostics, use `useObserveDiag` instead to set a name and access stats.
#[wasm_bindgen(js_name = useObserve)]
pub fn use_observe() -> Result<ReactObserver, JsValue> {
    let ref_value = useRef()?;

    let mut observer_ref = js_sys::Reflect::get(&ref_value, &"current".into()).unwrap();
    if observer_ref.is_undefined() {
        // Create new observer
        let react_observer = ReactObserver::new();

        // Set up React integration
        useSyncExternalStore(&react_observer.0.subscribe_fn, &react_observer.0.get_snapshot, &react_observer.0.get_snapshot)?;

        // Store in React ref for reuse
        observer_ref = JsValue::from(react_observer.clone());
        js_sys::Reflect::set(&ref_value, &"current".into(), &observer_ref).unwrap();

        // Set as current context
        CurrentObserver::set(react_observer.clone());

        Ok(react_observer)
    } else {
        // Reuse existing observer
        let ptr = js_sys::Reflect::get(&observer_ref, &JsValue::from_str("__wbg_ptr")).unwrap();
        let react_observer = {
            // Workaround for lack of downcasting in wasm-bindgen
            let ptr_u32: u32 = ptr.as_f64().unwrap() as u32;
            use wasm_bindgen::convert::RefFromWasmAbi;
            unsafe { ReactObserver::ref_from_abi(ptr_u32) }
        };

        // Set up React integration
        useSyncExternalStore(&react_observer.0.subscribe_fn, &react_observer.0.get_snapshot, &react_observer.0.get_snapshot)?;

        // Set as current context
        CurrentObserver::set(react_observer.clone());

        react_observer.mark_all_for_removal();

        Ok((*react_observer).clone())
    }
}

/// React hook for diagnostics: gets the current observer and sets its name
///
/// This hook retrieves the current ReactObserver (created by `useObserve`)
/// and sets its name for debugging purposes. Does NOT create a new observer
/// or affect the observer stack.
///
/// **Only available in debug builds** - will not be exported in release builds.
///
/// # Example
/// ```javascript
/// function MyComponent() {
///   const observer = useObserve();
///   try {
///     const diag = useObserveDiag("MyComponent"); // Sets name for profiling
///     const items = query.get();
///
///     console.log(`Observing ${diag.signalCount()} signals`);
///     return <div>...</div>;
///   } finally {
///     observer.finish();
///   }
/// }
/// ```
#[cfg(debug_assertions)]
#[wasm_bindgen(js_name = useObserveDiag)]
pub fn use_observe_diag(name: String) -> Result<ReactObserver, JsValue> {
    // Get the current observer from the context (set by useObserve)
    let current = CurrentObserver::current().ok_or_else(|| JsValue::from_str("No active observer"))?;

    // Downcast to ReactObserver
    let react_observer =
        current.as_any().downcast_ref::<ReactObserver>().ok_or_else(|| JsValue::from_str("Current observer is not a ReactObserver"))?;

    // Set the name (OnceLock ensures it can only be set once)
    react_observer.set_name(name)?;

    Ok((*react_observer).clone())
}
// Observer implementation for ReactObserver
impl Observer for ReactObserver {
    fn observe(&self, signal: &dyn Signal) {
        let broadcast_id = signal.broadcast_id();

        // Check if we already have a listener for this broadcast
        let mut entries = self.0.entries.write().expect("entries lock is poisoned");
        if let Some(entry) = entries.get_mut(&broadcast_id) {
            // We already have a listener for this broadcast, just unmark it for removal
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

                        // Call React's callback if it's been set
                        if let Ok(guard) = observer.0.trigger_render.lock() {
                            if let Some(callback) = guard.as_ref() {
                                let _ = callback.call0(&JsValue::NULL);
                            }
                        }
                    }
                })),
                marked_for_removal: false,
            },
        );
    }

    fn observer_id(&self) -> usize {
        // Use the pointer address of one of the Arc fields as a unique identifier
        Arc::as_ptr(&self.0) as *const _ as usize
    }

    #[doc(hidden)]
    fn as_any(&self) -> &dyn std::any::Any { self }
}
