use crate::porcelain::Subscribe;
use crate::{Calculated, Get, Mut, Peek, Read, porcelain::subscribe::DynSubscribe};
use send_wrapper::SendWrapper;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(skip_typescript)]
pub struct JsValueMut(Mut<SendWrapper<JsValue>>);

#[wasm_bindgen(skip_typescript)]
pub struct JsValueRead(Arc<dyn JsValueReadSignal>);

trait JsValueReadSignal: Get<SendWrapper<JsValue>> + Peek<SendWrapper<JsValue>> + DynSubscribe<SendWrapper<JsValue>> + Send + Sync {}

#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND: &'static str = r#"
export class JsValueMut<T = any> {
  free(): void;
  constructor(value: T);
  static newPair<T>(value: T): [JsValueMut<T>, JsValueRead<T>];
  set(value: T): void;
  get(): T;
  peek(): T;
  read(): JsValueRead<T>;
  get value(): T;
  set value(value: T);
}

export class JsValueRead<T = any> {
  free(): void;
  get(): T;
  peek(): T;
  get value(): T;
}
"#;

#[wasm_bindgen]
impl JsValueMut {
    #[wasm_bindgen(constructor)]
    pub fn new(value: JsValue) -> Self { Self(Mut::new(SendWrapper::new(value))) }

    #[wasm_bindgen(skip_typescript, js_name = "newPair")]
    pub fn new_pair(value: JsValue) -> Vec<JsValue> {
        let mut_val = Self::new(value);
        let read_val = mut_val.read();
        vec![wasm_bindgen::JsValue::from(mut_val), wasm_bindgen::JsValue::from(read_val)]
    }

    #[wasm_bindgen(skip_typescript)]
    pub fn set(&self, value: JsValue) { self.0.set(SendWrapper::new(value)); }

    #[wasm_bindgen(skip_typescript)]
    pub fn get(&self) -> JsValue {
        use crate::Get;
        self.0.get().take()
    }

    #[wasm_bindgen(skip_typescript)]
    pub fn peek(&self) -> JsValue { self.0.value().take() }

    #[wasm_bindgen(skip_typescript)]
    pub fn read(&self) -> JsValueRead { JsValueRead(Arc::new(self.0.read())) }

    #[wasm_bindgen(skip_typescript)]
    pub fn subscribe(&self, listener: js_sys::Function) -> JsValue {
        use crate::Subscribe;
        let listener = SendWrapper::new(listener);
        let guard = self.0.subscribe(move |value: SendWrapper<JsValue>| {
            let _ = listener.call1(&JsValue::NULL, &value.take());
        });
        // Return the guard so it can be dropped to unsubscribe
        JsValue::from(Box::into_raw(Box::new(guard)) as u32)
    }

    #[wasm_bindgen(skip_typescript, getter)]
    pub fn value(&self) -> JsValue { self.get() }

    #[wasm_bindgen(skip_typescript, setter, js_name = "value")]
    pub fn set_value(&self, value: JsValue) { self.set(value); }
}

#[wasm_bindgen]
impl JsValueRead {
    #[wasm_bindgen(skip_typescript)]
    pub fn get(&self) -> JsValue { Get::<SendWrapper<JsValue>>::get(&*self.0).take() }

    #[wasm_bindgen(skip_typescript)]
    pub fn peek(&self) -> JsValue { Peek::<SendWrapper<JsValue>>::peek(&*self.0).take() }

    #[wasm_bindgen(skip_typescript)]
    pub fn subscribe(&self, listener: js_sys::Function) -> JsValue {
        let listener = SendWrapper::new(listener);
        let guard = self.0.dyn_subscribe(Box::new(move |value: SendWrapper<JsValue>| {
            let _ = listener.call1(&JsValue::NULL, &value);
        }));
        JsValue::from(Box::into_raw(Box::new(guard)) as u32)
    }

    #[wasm_bindgen(skip_typescript, getter)]
    pub fn value(&self) -> JsValue { self.get() }
}

// Blanket impl for anything that implements the required traits
impl<S> JsValueReadSignal for S where S: Get<SendWrapper<JsValue>> + Peek<SendWrapper<JsValue>> + DynSubscribe<SendWrapper<JsValue>> + Send + Sync
{}

impl<T> From<Read<T>> for JsValueRead
where T: Clone + Into<JsValue> + Send + Sync + 'static
{
    fn from(read: Read<T>) -> Self {
        // Use memo instead of map - caches the JsValue until upstream changes.
        // This ensures the same JsValue reference is returned on repeated reads,
        // which is important for React dependency tracking (useMemo, useEffect, etc.)
        let memoized = read.memo(|value: &T| SendWrapper::new(value.clone().into()));
        JsValueRead(Arc::new(memoized))
    }
}

impl Clone for JsValueRead {
    fn clone(&self) -> Self { JsValueRead(self.0.clone()) }
}

/// A calculated signal for JavaScript that computes its value from a JS closure.
///
/// The closure can access other signals (via their .get() methods), and the
/// calculated signal will automatically recompute when any accessed signal changes.
#[wasm_bindgen(skip_typescript)]
pub struct JsValueCalculated(Calculated<SendWrapper<JsValue>>);

#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CALCULATED: &'static str = r#"
export class JsValueCalculated<T = any> {
  free(): void;
  constructor(compute: () => T);
  get(): T;
  peek(): T;
  read(): JsValueRead<T>;
  subscribe(listener: (value: T) => void): number;
  get value(): T;
}
"#;

#[wasm_bindgen]
impl JsValueCalculated {
    /// Create a new calculated signal from a JavaScript function.
    ///
    /// The function will be called immediately to compute the initial value,
    /// and will be called again whenever any signal accessed during computation changes.
    #[wasm_bindgen(constructor)]
    pub fn new(compute: js_sys::Function) -> Self {
        let compute = SendWrapper::new(compute);
        Self(Calculated::new(move || {
            let result = compute.call0(&JsValue::NULL).unwrap_or_else(|e| {
                let msg = e.as_string().unwrap_or_else(|| {
                    // Try to get the stack property from Error objects
                    js_sys::Reflect::get(&e, &JsValue::from_str("stack"))
                        .ok()
                        .and_then(|s| s.as_string())
                        .unwrap_or_else(|| format!("{:?}", e))
                });
                tracing::error!("JsValueCalculated compute function threw: {}", msg);
                JsValue::UNDEFINED
            });
            SendWrapper::new(result)
        }))
    }

    /// Get the current value, tracking this signal for the current observer.
    #[wasm_bindgen(skip_typescript)]
    pub fn get(&self) -> JsValue { Get::<SendWrapper<JsValue>>::get(&self.0).take() }

    /// Get the current value without tracking.
    #[wasm_bindgen(skip_typescript)]
    pub fn peek(&self) -> JsValue { Peek::<SendWrapper<JsValue>>::peek(&self.0).take() }

    /// Get a read-only signal reference.
    #[wasm_bindgen(skip_typescript)]
    pub fn read(&self) -> JsValueRead { JsValueRead(Arc::new(self.0.clone())) }

    /// Subscribe to changes.
    #[wasm_bindgen(skip_typescript)]
    pub fn subscribe(&self, listener: js_sys::Function) -> JsValue {
        let listener = SendWrapper::new(listener);
        let guard = self.0.subscribe(move |value: SendWrapper<JsValue>| {
            let _ = listener.call1(&JsValue::NULL, &value.take());
        });
        JsValue::from(Box::into_raw(Box::new(guard)) as u32)
    }

    #[wasm_bindgen(skip_typescript, getter)]
    pub fn value(&self) -> JsValue { self.get() }
}
