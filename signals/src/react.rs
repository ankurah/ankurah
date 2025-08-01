//! React integration for ankurah-signals
//!
//! This module provides the `useObserve` hook that allows React components to
//! automatically re-render when signals they access are updated.

use send_wrapper::SendWrapper;
use wasm_bindgen::prelude::*;

use std::cell::OnceCell;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{
    CurrentContext, SubscriptionGuard,
    traits::{Notify, Observer},
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

/// A React observer handle that manages signal subscriptions for a component
#[wasm_bindgen]
#[derive(Clone)]
pub struct ReactObserver {
    subscription_handles: Arc<std::sync::RwLock<Vec<SubscriptionGuard>>>,
    /// This function gets called when a change is observed
    notify_fn: Arc<OnceCell<js_sys::Function>>,
    /// This function gets called by react on the initial render with the notify_fn as an argument
    /// we have to store it to prevent it from being dropped.
    subscribe_fn: Arc<Closure<dyn Fn(js_sys::Function) -> JsValue>>,
    /// get_snapshot is called by react for every render to get the "snapshot" - which in our case is just the version number
    /// it's a goofy way to get react to re-render the component. We're using this instead of the typical
    /// 'let [_,forceUpdate] = useReducer((a) => a + 1, 0)' pattern because it's what preact-signals uses.
    /// I haven't yet analyzed the react dispatch cycle internals sufficiently well to know the difference, but
    /// the preact folks probably didn't choose useSyncExternalStore by throwing a dart at the dartboard,
    /// so I'm inclined to start there.
    get_snapshot: Arc<Closure<dyn Fn() -> JsValue>>,
    version: Arc<AtomicU64>,
}

impl ReactObserver {
    fn new() -> Self {
        // Version counter for React's useSyncExternalStore
        let version = Arc::new(AtomicU64::new(0));

        // Storage for React's on_store_change callback
        let trigger_render = Arc::new(OnceCell::new());

        // Create subscription function for useSyncExternalStore
        let subscribe_fn = {
            let trigger_render = trigger_render.clone();
            Closure::wrap(Box::new(move |callback: js_sys::Function| {
                // Store the callback for later use when signals change
                let _ = trigger_render.set(callback);
                tracing::info!("LOOK: subscribe_fn called, callback stored");
                JsValue::UNDEFINED
            }) as Box<dyn Fn(js_sys::Function) -> JsValue>)
        };

        // Create snapshot function for useSyncExternalStore
        let get_snapshot = {
            let version = version.clone();
            Closure::wrap(Box::new(move || {
                let version = version.load(Ordering::Relaxed);
                tracing::info!("LOOK: Getting snapshot. Version: {}", version);
                JsValue::from(version)
            }) as Box<dyn Fn() -> JsValue>)
        };

        Self {
            version,
            subscription_handles: Arc::new(std::sync::RwLock::new(vec![])),
            notify_fn: trigger_render,
            subscribe_fn: Arc::new(subscribe_fn),
            get_snapshot: Arc::new(get_snapshot),
        }
    }

    /// Clear all subscription handles
    pub fn clear(&self) { self.subscription_handles.write().unwrap().clear(); }
}

#[wasm_bindgen]
impl ReactObserver {
    /// Finish using this observer and restore the previous context
    /// This should be called in a finally block after component rendering
    #[wasm_bindgen]
    pub fn finish(&self) { CurrentContext::unset(); }
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
        useSyncExternalStore(&*react_observer.subscribe_fn, &*react_observer.get_snapshot, &*react_observer.get_snapshot)?;

        // Store in React ref for reuse
        observer_ref = JsValue::from(react_observer.clone());
        js_sys::Reflect::set(&ref_value, &"current".into(), &observer_ref).unwrap();

        // Set as current context
        CurrentContext::set(react_observer.clone());

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
        useSyncExternalStore(&*react_observer.subscribe_fn, &*react_observer.get_snapshot, &*react_observer.get_snapshot)?;

        // Set as current context
        CurrentContext::set(react_observer.clone());

        // Clear subscriptions from previous render. Those signals which are .get()'d
        // during this render will be re-subscribed, and the rest (which must be in a non-executed
        // logic branch) will not be subscribed.

        react_observer.clear();

        Ok((*react_observer).clone())
    }
}

// SignalObserver implementation for ReactObserver
impl Observer for ReactObserver {
    fn get_notifier(&self) -> Box<dyn Notify + Send + Sync> {
        Box::new(SendWrapper::new(ReactNotifier { version: self.version.clone(), notify_fn: self.notify_fn.clone() }))
    }

    fn store_handle(&self, handle: SubscriptionGuard) { self.subscription_handles.write().unwrap().push(handle); }
}

// Helper struct to implement Notify for React updates
struct ReactNotifier {
    version: Arc<AtomicU64>,
    notify_fn: Arc<OnceCell<js_sys::Function>>,
}

// Implementation for SendWrapper<ReactNotifier> to satisfy Send + Sync requirements
// while keeping ReactNotifier single-threaded for WASM
impl Notify for SendWrapper<ReactNotifier> {
    fn notify(&self) {
        tracing::info!("LOOK: Notifying React");
        self.version.fetch_add(1, Ordering::Relaxed);

        // Call React's callback if it's been set
        if let Some(notify_fn) = self.notify_fn.get() {
            notify_fn.call0(&JsValue::NULL).unwrap_or(JsValue::UNDEFINED);
        }
    }
}
