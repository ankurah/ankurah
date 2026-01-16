pub(crate) mod backend;
pub(crate) mod backend_registry;
pub(crate) mod description;
pub(crate) mod model;
pub(crate) mod mutable;
#[cfg(all(feature = "uniffi", not(feature = "wasm")))]
pub(crate) mod uniffi;
pub(crate) mod view;
#[cfg(feature = "wasm")]
pub(crate) mod wasm;

use proc_macro2::TokenStream;

/// FFI-specific attributes for View struct generation.
pub(crate) struct ViewAttributes {
    /// Attribute on the struct definition (e.g., `#[wasm_bindgen]`, `#[derive(uniffi::Object)]`)
    pub struct_attr: TokenStream,
    /// Attribute on the impl block containing id/track methods
    pub impl_attr: TokenStream,
    /// Attribute on the id() method specifically (e.g., `#[wasm_bindgen(getter)]`)
    pub id_method_attr: TokenStream,
    /// Extra FFI-specific impl blocks (e.g., edit_wasm, subscribe_wasm)
    pub extra_impl: TokenStream,
}
