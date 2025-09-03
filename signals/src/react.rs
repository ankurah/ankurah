//! React integration for ankurah-signals
//!
//! This module provides the `useObserve` hook that allows React components to
//! automatically re-render when signals they access are updated.

use send_wrapper::SendWrapper;
use wasm_bindgen::prelude::*;

use std::cell::OnceCell;
use std::collections::HashMap;
use std::sync::Weak;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{
    CurrentObserver, Signal,
    broadcast::{BroadcastId, ListenerGuard},
    observer::Observer,
};

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
    guard: ListenerGuard<()>,
    marked_for_removal: bool,
}

/// A React observer handle that manages signal subscriptions for a component
#[wasm_bindgen]
#[derive(Clone)]
pub struct ReactObserver(Arc<Inner>);

#[allow(unused)]
struct ReactObserverWeak(Weak<Inner>);

pub struct Inner {
    _human_readable_id: usize,
    entries: std::sync::RwLock<HashMap<BroadcastId, ListenerEntry>>,
    /// This function gets called when a change is observed
    trigger_render: Arc<SendWrapper<OnceCell<js_sys::Function>>>,

    /// This function gets called by react on the initial render with the notify_fn as an argument
    /// we have to store it to prevent it from being dropped.
    subscribe_fn: Closure<dyn Fn(js_sys::Function) -> JsValue>,
    /// get_snapshot is called by react for every render to get the "snapshot" - which in our case is just the version number
    /// it's a goofy way to get react to re-render the component. We're using this instead of the typical
    /// 'let [_,forceUpdate] = useReducer((a) => a + 1, 0)' pattern because it's what preact-signals uses.
    /// I haven't yet analyzed the react dispatch cycle internals sufficiently well to know the difference, but
    /// the preact folks probably didn't choose useSyncExternalStore by throwing a dart at the dartboard,
    /// so I'm inclined to start there.
    get_snapshot: Closure<dyn Fn() -> JsValue>,
    version: Arc<AtomicUsize>,
}

static HUMAN_READABLE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

impl ReactObserverWeak {
    fn upgrade(&self) -> Option<ReactObserver> { self.0.upgrade().map(ReactObserver) }
}

impl ReactObserver {
    fn new() -> Self {
        let react_observer_id = HUMAN_READABLE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Version counter for React's useSyncExternalStore
        let version = Arc::new(AtomicUsize::new(0));

        // Storage for React's on_store_change callback - wrap in Arc immediately
        let trigger_render = Arc::new(SendWrapper::new(OnceCell::new()));

        // Create subscription function for useSyncExternalStore
        let subscribe_fn = {
            let trigger_render = trigger_render.clone();
            Closure::wrap(Box::new(move |callback: js_sys::Function| {
                // Store the callback for later use when signals change
                let _ = trigger_render.set(callback);
                JsValue::UNDEFINED
            }) as Box<dyn Fn(js_sys::Function) -> JsValue>)
        };

        // Create snapshot function for useSyncExternalStore
        let get_snapshot = {
            let version = version.clone();
            Closure::wrap(Box::new(move || {
                let version = version.load(Ordering::Relaxed);
                JsValue::from(version)
            }) as Box<dyn Fn() -> JsValue>)
        };

        Self(Arc::new(Inner {
            _human_readable_id: react_observer_id,
            version,
            entries: std::sync::RwLock::new(HashMap::new()),
            trigger_render,
            subscribe_fn,
            get_snapshot,
        }))
    }
    #[allow(unused)]
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
}

#[allow(unused)]
#[wasm_bindgen]
impl ReactObserver {
    /// Finish using this observer and restore the previous context
    /// This should be called in a finally block after component rendering
    #[wasm_bindgen]
    pub fn finish(&self) {
        // Sweep away any listeners that weren't preserved during render
        self.sweep_marked_listeners();
        CurrentObserver::remove(self as &dyn Observer);
    }
}

/// React hook for observing signals within a component
///
/// This is the new useObserve hook that replaces the old withSignals pattern.
/// Instead of taking a closure, it returns an observer object that you call .finish() on.
///
/// The hook must be used with a try-finally pattern to ensure proper cleanup.
/// Call .get() on signals within the try block to register subscriptions.
/// Always call observer.finish() in the finally block.
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

        // Create new listener
        let version = self.0.version.clone();
        let trigger_render = self.0.trigger_render.clone();
        entries.insert(
            broadcast_id,
            ListenerEntry {
                guard: signal.listen(Arc::new(move |_| {
                    // Increment version to trigger React re-render
                    version.fetch_add(1, Ordering::Relaxed);

                    // Call React's callback if it's been set
                    if let Some(callback) = trigger_render.get() {
                        let _ = callback.call0(&JsValue::NULL);
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
}
