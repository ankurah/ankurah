mod model;
mod wasm_signal;

use proc_macro::TokenStream;

#[proc_macro_derive(Model, attributes(serde, active_value))]
pub fn derive_model(input: TokenStream) -> TokenStream { model::derive_model_impl(input) }

#[proc_macro_derive(WasmSignal)]
pub fn derive_wasm_signal(input: TokenStream) -> TokenStream { wasm_signal::derive_wasm_signal_impl(input) }
