//! React integration for ankurah-signals
//!
//! This module provides the `useObserve` hook that allows React components to
//! automatically re-render when signals they access are updated.

use wasm_bindgen::prelude::*;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{CurrentContext, Observer};

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
    observer: Observer,
    subscribe_fn: Arc<Closure<dyn Fn(js_sys::Function) -> JsValue>>,
    get_snapshot: Arc<Closure<dyn Fn() -> JsValue>>,
}

impl ReactObserver {
    fn new() -> Self {
        // Version counter for React's useSyncExternalStore
        let version = Arc::new(AtomicU64::new(0));

        // Create observer that increments version when notified
        let observer = {
            let version = version.clone();
            Observer::new(Arc::new(move || {
                version.fetch_add(1, Ordering::Relaxed);
            }))
        };

        // Create subscription function for useSyncExternalStore
        let subscribe_fn = {
            let version = version.clone();
            Closure::wrap(Box::new(move |on_store_change: js_sys::Function| {
                // Trigger React re-render by calling the callback
                // In a more complete implementation, we would set up a proper subscription
                version.fetch_add(1, Ordering::Relaxed);
                on_store_change.call0(&JsValue::NULL).unwrap_or(JsValue::UNDEFINED);
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

        Self { observer, subscribe_fn: Arc::new(subscribe_fn), get_snapshot: Arc::new(get_snapshot) }
    }
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
        CurrentContext::set(react_observer.observer.clone());

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
        CurrentContext::set(react_observer.observer.clone());

        Ok((*react_observer).clone())
    }
}
