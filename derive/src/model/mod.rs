pub(crate) mod backend;
pub(crate) mod backend_registry;
pub(crate) mod description;
pub(crate) mod model;
pub(crate) mod mutable;
pub(crate) mod schema;
#[cfg(all(feature = "uniffi", not(feature = "wasm")))]
pub(crate) mod uniffi;
pub(crate) mod view;
#[cfg(feature = "wasm")]
pub(crate) mod wasm;

use proc_macro2::TokenStream;

/// The label prefix reserved for Ankurah's built-in collections. A user model
/// may not claim it (checked in [`schema::validate_schema_attrs`]); a
/// `#[model(system = "...")]` model is built FROM it. Core states the same
/// constant for the runtime side (`ankurah_core::schema::RESERVED_COLLECTION_PREFIX`).
pub(crate) const RESERVED_COLLECTION_PREFIX: &str = "_ankurah_";

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
