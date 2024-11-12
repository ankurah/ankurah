// Re-export the ankurah-derive WasmSignal derive macro
pub use ankurah_derive::WasmSignal;

use std::rc::Rc;

use futures::future::AbortHandle;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
pub struct Subscription {
    abort_handle: AbortHandle,
}

impl Subscription {
    pub fn new(abort_handle: AbortHandle) -> Self {
        Self { abort_handle }
    }
}

#[wasm_bindgen]
impl Subscription {
    // Method to unsubscribe (cancel the future)
    #[wasm_bindgen]
    pub fn unsubscribe(self) {
        self.abort_handle.abort();
    }
}
