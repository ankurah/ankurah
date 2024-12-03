pub mod react_binding;

// Re-export the ankurah-derive WasmSignal derive macro
pub use ankurah_derive::WasmSignal;
use reactive_graph::{effect::Effect, owner::LocalStorage, traits::Dispose};

use std::sync::Mutex;
use std::{cell::RefCell, rc::Rc, sync::Arc};

use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::prelude::*;

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

// fn add_dependency<T>(signal: &reactive_graph::signal::ReadSignal<T>) {
//     let foo = react_binding::CURRENT_STORE.with(|cell| cell.borrow());

//     if let Some(store) = &*foo {
//         signal.subscribe(store.notifier.clone());
//     }
// }

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
