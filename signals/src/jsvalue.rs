use crate::{Mut, Read};
use send_wrapper::SendWrapper;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(skip_typescript)]
pub struct JsValueMut(Mut<SendWrapper<JsValue>>);

#[wasm_bindgen(skip_typescript)]
pub struct JsValueRead(Read<SendWrapper<JsValue>>);

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
}

export class JsValueRead<T = any> {
  free(): void;
  get(): T;
  peek(): T;
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
    pub fn read(&self) -> JsValueRead { JsValueRead(self.0.read()) }

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
}

#[wasm_bindgen]
impl JsValueRead {
    #[wasm_bindgen(skip_typescript)]
    pub fn get(&self) -> JsValue {
        use crate::Get;
        self.0.get().take()
    }

    #[wasm_bindgen(skip_typescript)]
    pub fn peek(&self) -> JsValue {
        use crate::Peek;
        self.0.peek().take()
    }

    #[wasm_bindgen(skip_typescript)]
    pub fn subscribe(&self, listener: js_sys::Function) -> JsValue {
        use crate::Subscribe;
        let listener = SendWrapper::new(listener);
        let guard = self.0.subscribe(move |value: SendWrapper<JsValue>| {
            let _ = listener.call1(&JsValue::NULL, &value);
        });
        // Return the guard so it can be dropped to unsubscribe
        JsValue::from(Box::into_raw(Box::new(guard)) as u32)
    }
}
