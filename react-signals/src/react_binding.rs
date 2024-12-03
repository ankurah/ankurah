use std::{
    cell::RefCell,
    sync::{atomic::AtomicU64, Arc, RwLock},
};

use futures::StreamExt;
use or_poisoned::OrPoisoned;
use reactive_graph::graph::{AnySource, AnySubscriber, ReactiveNode, SourceSet, Subscriber};
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

// Substantially an homage to https://github.com/preactjs/signals/blob/main/packages/react/runtime/src/auto.ts
// TODO need to get fancier with this, probably by hooking into the react internals, similar technique to preact-signals
// For now it works, but the render cycle start/finish is thrashy:
// INFO react-signals/src/react_binding.rs:65 effectstore new
// example_wasm_bindings_bg.wasm:0x78592 INFO react-signals/src/react_binding.rs:91 effectstore start
// example_wasm_bindings_bg.wasm:0x78592 INFO react-signals/src/react_binding.rs:91 effectstore start
// example_wasm_bindings_bg.wasm:0x78592 INFO react-signals/src/react_binding.rs:95 effectstore finish
// example_wasm_bindings_bg.wasm:0x78592 INFO react-signals/src/react_binding.rs:91 effectstore start
// example_wasm_bindings_bg.wasm:0x78592 INFO react-signals/src/react_binding.rs:95 effectstore finish
// example_wasm_bindings_bg.wasm:0x78592 INFO react-signals/src/react_binding.rs:91 effectstore start
// example_wasm_bindings_bg.wasm:0x78592 INFO react-signals/src/react_binding.rs:95 effectstore finish

/// Creates a subscription to track signal usage within a React component.
///
/// This hook enables automatic re-rendering of React components when signals they access are updated.
/// It returns a controller object that must be used with a try-finally pattern to properly clean up
/// signal subscriptions.
///
/// # Usage
///
/// ```typescript
/// import { useSignals } from 'example-wasm-bindings';
///
/// function MyComponent() {
///     const controller = useSignals();
///     
///     try {
///         // Your component logic here
///         return <div>{my_signal.value}</div>;
///     } finally {
///         controller.finish();
///     }
/// }
/// ```
///
/// # Note
/// This implementation follows the pattern used by Preact Signals, adapted for
/// WebAssembly-based signal integration with React
#[wasm_bindgen(js_name = useSignals)]
pub fn use_signals() -> JsValue {
    log::info!("useSignals");
    let ref_value = useRef();

    let mut store = js_sys::Reflect::get(&ref_value, &"current".into()).unwrap();
    if store.is_undefined() {
        let new_store = EffectStore::new();
        new_store.start();
        useSyncExternalStore(
            &new_store.0.subscribe_fn,
            &new_store.0.get_snapshot,
            &new_store.0.get_snapshot,
        );
        // TODO: Check to see if this sets up the finalizer in JS land
        store = JsValue::from(new_store);
        js_sys::Reflect::set(&ref_value, &"current".into(), &store).unwrap();
    } else {
        let ptr = js_sys::Reflect::get(&store, &JsValue::from_str("__wbg_ptr")).unwrap();
        log::info!("ptr: {:?}", store);
        let store = {
            // workaround for lack of downcasting in wasm-bindgen
            // https://github.com/rustwasm/wasm-bindgen/pull/3088
            let ptr_u32: u32 = ptr.as_f64().unwrap() as u32;
            use wasm_bindgen::convert::RefFromWasmAbi;
            unsafe { EffectStore::ref_from_abi(ptr_u32) }
        };
        useSyncExternalStore(
            &store.0.subscribe_fn,
            &store.0.get_snapshot,
            &store.0.get_snapshot,
        );
        store.start();
    };

    store
}

// DO NOT CHANGE ANYTHING ABOVE THIS LINE (except to add imports)

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
    pub static CURRENT_STORE: RefCell<Option<EffectStore>> = RefCell::new(None);
}

impl EffectStore {
    pub fn new() -> Self {
        log::info!("effectstore new");

        // Necessary for hooking into the react render cycle
        let version = Arc::new(AtomicU64::new(0));
        let (sender, rx) = crate::effect::channel::channel();
        let tracker = Tracker(Arc::new(TrackerInner(RwLock::new(TrackerState {
            dirty: false,
            notifier: sender,
            sources: SourceSet::new(),
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
                use reactive_graph::graph::WithObserver;
                any_spawner::Executor::spawn_local({
                    //     // let value = Arc::clone(&value);
                    let subscriber = tracker.to_any_subscriber();

                    async move {
                        while rx.next().await.is_some() {
                            log::info!("effectstore rx next");
                            if subscriber.with_observer(|| subscriber.update_if_necessary()) {
                                subscriber.clear_sources(&subscriber);
                                log::info!("effectstore rx clear_sources");
                                on_store_change.call0(&JsValue::NULL).unwrap();
                                // let old_value = mem::take(
                                //     &mut *value.write().or_poisoned(),
                                // );
                                // let new_value = owner.with_cleanup(|| {
                                //     subscriber.with_observer(|| fun(old_value))
                                // });
                                // *value.write().or_poisoned() = Some(new_value);
                            }
                        }
                    }
                });

                log::info!("effectstore on_change_notify");
                version.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // TODO: return unsubscribe closure
                JsValue::UNDEFINED
            }) as Box<dyn Fn(js_sys::Function) -> JsValue>)
        };
        let get_snapshot = {
            let version = version.clone();

            Closure::wrap(Box::new(move || {
                let version = version.load(std::sync::atomic::Ordering::Relaxed);
                log::info!("effectstore get_snapshot {}", version);
                JsValue::from(version)
            }) as Box<dyn Fn() -> JsValue>)
        };

        let me = Self(Arc::new(Inner {
            subscribe_fn,
            get_snapshot,
            tracker,
        }));
        me.start();
        me
    }
}

#[wasm_bindgen]
impl EffectStore {
    pub fn start(&self) {
        log::info!("effectstore start");
        // self.0.tracker.update_if_necessary();
        reactive_graph::graph::Observer::set(Some(self.0.tracker.to_any_subscriber()));
    }
    pub fn finish(&self) {
        log::info!("effectstore finish");
        reactive_graph::graph::Observer::set(None);
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
    sources: SourceSet,
    version: Arc<AtomicU64>,
}

impl ReactiveNode for TrackerInner {
    fn mark_subscribers_check(&self) {
        log::info!("effectstore mark_subscribers_check");
    }

    fn update_if_necessary(&self) -> bool {
        log::info!("effectstore update_if_necessary");
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

    fn mark_check(&self) {
        log::info!("effectstore mark_check");
        self.0.write().or_poisoned().notifier.notify();
    }

    fn mark_dirty(&self) {
        log::info!("effectstore mark_dirty");
        let mut lock = self.0.write().or_poisoned();
        lock.dirty = true;
        lock.version
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        lock.notifier.notify();
    }
}

impl Subscriber for TrackerInner {
    fn add_source(&self, source: AnySource) {
        log::info!("effectstore add_source");
        self.0.write().or_poisoned().sources.insert(source);
    }

    fn clear_sources(&self, subscriber: &AnySubscriber) {
        log::info!("effectstore clear_sources");
        self.0
            .write()
            .or_poisoned()
            .sources
            .clear_sources(subscriber);
    }
}

impl ToAnySubscriber for Tracker {
    fn to_any_subscriber(&self) -> AnySubscriber {
        AnySubscriber(
            Arc::as_ptr(&self.0) as usize,
            Arc::downgrade(&self.0) as Weak<dyn Subscriber + Send + Sync>,
        )
    }
}

pub trait ToAnySubscriber {
    /// Converts this type to its type-erased equivalent.
    fn to_any_subscriber(&self) -> AnySubscriber;
}
