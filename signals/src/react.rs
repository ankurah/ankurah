use std::{
    cell::RefCell,
    sync::{Arc, RwLock, atomic::AtomicU64},
};

use crate::Observer;
use futures::StreamExt;
use or_poisoned::OrPoisoned;
use reactive_graph::graph::{AnySource, AnySubscriber, ReactiveNode, Subscriber};
use std::sync::Weak;

use wasm_bindgen::prelude::*;

#[wasm_bindgen(module = "react")]
extern "C" {
    fn useRef() -> JsValue;
    fn useSyncExternalStore(
        subscribe: &Closure<dyn Fn(js_sys::Function) -> JsValue>,
        get_snapshot: &Closure<dyn Fn() -> JsValue>,
        get_server_snapshot: &Closure<dyn Fn() -> JsValue>,
    ) -> JsValue;
}

/// Creates a subscription to track signal usage within a React component.
///
/// This hook enables automatic re-rendering of React components when signals they access are updated.
/// It returns a controller object that must be used with a try-finally pattern to properly clean up
/// signal subscriptions.
///
/// # Usage
///
/// ```typescript
/// import { withSignals } from 'example-wasm-bindings';
///
/// function MyComponent() {
///     // withSignals is a temporary hack. Signal management will change soon
///     let s = useSignals();
///     try {
///         // Your component logic here
///         return <div>{my_signal.value}</div>;
///     } finally {
///         s.finish();
///     }
/// }
/// ```
///
/// # Note
/// This implementation follows the pattern used by Preact Signals, adapted for
/// WebAssembly-based signal integration with React
#[wasm_bindgen(js_name = useSignals)]
pub fn use_signals(f: &js_sys::Function) -> JsValue {
    let ref_value = useRef();

    let mut store = js_sys::Reflect::get(&ref_value, &"current".into()).unwrap();
    if store.is_undefined() {
        let new_store = Observer::new(f);
        useSyncExternalStore(&new_store.0.subscribe_fn, &new_store.0.get_snapshot, &new_store.0.get_snapshot);
        // TODO: Check to see if this sets up the finalizer in JS land
        store = JsValue::from(new_store.clone());
        js_sys::Reflect::set(&ref_value, &"current".into(), &store).unwrap();
        new_store.with_observer(f)
    } else {
        let ptr = js_sys::Reflect::get(&store, &JsValue::from_str("__wbg_ptr")).unwrap();
        let store = {
            // workaround for lack of downcasting in wasm-bindgen
            // https://github.com/rustwasm/wasm-bindgen/pull/3088
            let ptr_u32: u32 = ptr.as_f64().unwrap() as u32;
            use wasm_bindgen::convert::RefFromWasmAbi;
            unsafe { EffectStore::ref_from_abi(ptr_u32) }
        };
        useSyncExternalStore(&store.0.subscribe_fn, &store.0.get_snapshot, &store.0.get_snapshot);
        store.with_observer(f)
    }
}

#[derive(Clone)]
#[wasm_bindgen]
pub struct EffectStore(Arc<Inner>);

struct Inner {
    // The function which gets called by useSyncExternalStore to subscribe the react component to changes to the "store"
    subscribe_fn: Closure<dyn Fn(js_sys::Function) -> JsValue>,
    // The function which gets called by useSyncExternalStore to get the current value of the "store"
    get_snapshot: Closure<dyn Fn() -> JsValue>,

    // Tracks the sources which the "store" depends on
    tracker: Tracker,
}

// Thread local for currently active SignalContext
thread_local! {
    pub static CURRENT_STORE: RefCell<Option<EffectStore>> = const { RefCell::new(None) };
}

impl Default for EffectStore {
    fn default() -> Self { Self::new() }
}

impl EffectStore {
    pub fn new() -> Self {
        // Necessary for hooking into the react render cycle
        let version = Arc::new(AtomicU64::new(0));
        let (sender, rx) = crate::effect::channel::channel();
        let tracker = Tracker(Arc::new(TrackerInner(RwLock::new(TrackerState {
            dirty: false,
            notifier: sender,
            sources: Vec::new(),
            version: version.clone(),
        }))));
        let rx = RefCell::new(Some(rx));
        let subscribe_fn = {
            let tracker = tracker.clone();
            let version = version.clone();
            Closure::wrap(Box::new(move |on_store_change: js_sys::Function| {
                let Some(mut rx) = rx.borrow_mut().take() else {
                    return JsValue::UNDEFINED;
                };

                wasm_bindgen_futures::spawn_local({
                    let subscriber = tracker.to_any_subscriber();

                    async move {
                        while rx.next().await.is_some() {
                            // Do we need to call with_observer here? I can't see why
                            // if subscriber.with_observer(|| subscriber.update_if_necessary())
                            if subscriber.update_if_necessary() {
                                subscriber.clear_sources(&subscriber);
                                on_store_change.call0(&JsValue::NULL).unwrap();
                            }
                        }
                    }
                });

                version.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // TODO: return unsubscribe closure
                JsValue::UNDEFINED
            }) as Box<dyn Fn(js_sys::Function) -> JsValue>)
        };
        let get_snapshot = {
            let version = version.clone();

            Closure::wrap(Box::new(move || {
                let version = version.load(std::sync::atomic::Ordering::Relaxed);
                JsValue::from(version)
            }) as Box<dyn Fn() -> JsValue>)
        };

        Self(Arc::new(Inner { subscribe_fn, get_snapshot, tracker }))
    }
}

#[wasm_bindgen]
impl EffectStore {
    // pub fn start(&self) { reactive_graph::graph::Observer::set(Some(self.0.tracker.to_any_subscriber())); }
    // pub fn finish(&self) { reactive_graph::graph::Observer::set(None); }

    pub fn with_observer(&self, f: &js_sys::Function) -> JsValue {
        use reactive_graph::graph::WithObserver;
        self.0.tracker.to_any_subscriber().with_observer(|| f.call0(&JsValue::NULL).unwrap_or(JsValue::UNDEFINED))
    }
}
#[derive(Debug, Clone)]
pub struct Tracker(Arc<TrackerInner>);

#[derive(Debug)]
pub struct TrackerInner(RwLock<TrackerState>);

#[derive(Debug)]
struct TrackerState {
    dirty: bool,
    notifier: crate::effect::channel::Sender,
    sources: Vec<AnySource>,
    version: Arc<AtomicU64>,
}

impl ReactiveNode for TrackerInner {
    fn mark_subscribers_check(&self) {}

    fn update_if_necessary(&self) -> bool {
        let mut guard = self.0.write().or_poisoned();
        let (is_dirty, sources) = (guard.dirty, (!guard.dirty).then(|| guard.sources.clone()));

        if is_dirty {
            guard.dirty = false;
            return true;
        }

        drop(guard);
        for source in sources.into_iter().flatten() {
            if source.update_if_necessary() {
                return true;
            }
        }
        false
    }

    fn mark_check(&self) { self.0.write().or_poisoned().notifier.notify(); }

    fn mark_dirty(&self) {
        let mut lock = self.0.write().or_poisoned();
        lock.dirty = true;
        lock.version.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        lock.notifier.notify();
    }
}

impl Subscriber for TrackerInner {
    fn add_source(&self, source: AnySource) { self.0.write().or_poisoned().sources.push(source); }

    fn clear_sources(&self, subscriber: &AnySubscriber) { self.0.write().or_poisoned().sources.clear(); }
}

impl ToAnySubscriber for Tracker {
    fn to_any_subscriber(&self) -> AnySubscriber {
        AnySubscriber(Arc::as_ptr(&self.0) as usize, Arc::downgrade(&self.0) as Weak<dyn Subscriber + Send + Sync>)
    }
}

pub trait ToAnySubscriber {
    /// Converts this type to its type-erased equivalent.
    fn to_any_subscriber(&self) -> AnySubscriber;
}
