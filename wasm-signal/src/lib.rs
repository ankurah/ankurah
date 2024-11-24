// Re-export the ankurah-derive WasmSignal derive macro
pub use ankurah_derive::WasmSignal;
use reactive_graph::{effect::Effect, owner::LocalStorage, traits::Dispose};

use std::rc::Rc;

use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
pub struct Subscription {
    effect: Option<Effect<LocalStorage>>,
}

impl Subscription {
    pub fn new(effect: Effect<LocalStorage>) -> Self {
        Self {
            effect: Some(effect),
        }
    }
}

#[wasm_bindgen]
impl Subscription {
    #[wasm_bindgen]
    pub fn unsubscribe(&mut self) {
        self.effect.take().unwrap().dispose();
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.effect.take().unwrap().dispose();
    }
}
