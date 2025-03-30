mod model;
mod property;
#[cfg(feature = "wasm")]
mod tsify;
#[cfg(feature = "wasm")]
mod wasm_signal;

use proc_macro::TokenStream;

#[proc_macro_derive(Model, attributes(active_type, ephemeral, model))]
pub fn derive_model(input: TokenStream) -> TokenStream { model::derive_model_impl(input) }

#[cfg(feature = "wasm")]
#[proc_macro_derive(WasmSignal)]
pub fn derive_wasm_signal(input: TokenStream) -> TokenStream { wasm_signal::derive_wasm_signal_impl(input) }

#[proc_macro_derive(Property)]
pub fn derive_property(input: TokenStream) -> TokenStream { property::derive_property_impl(input) }
