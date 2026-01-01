//! UniFFI wrapper generation for Model types.
//!
//! Similar to wasm.rs, UniFFI doesn't support generics, so we generate concrete wrapper types:
//! - `SessionRef` - wraps `Ref<Session>` with `get(ctx)`, `id`, `from(view)` methods
//! - `SessionResultSet`, `SessionLiveQuery`, etc.
//! - `SessionOps` - singleton with static-like methods (get, fetch, query, create)
//! - `SessionLiveQueryCallback` - callback trait for subscription updates

use proc_macro2::TokenStream;
use quote::quote;

use super::ViewAttributes;

/// Generate UniFFI-specific attributes for Mutable struct.
/// Returns (struct_attributes, field_attributes)
pub fn mutable_attributes() -> (TokenStream, TokenStream) {
    (
        quote! { #[derive(::ankurah::derive_deps::uniffi::Object)] },
        quote! {}, // No field skip needed for UniFFI
    )
}

/// Generate UniFFI-specific attributes for View struct.
pub fn view_attributes() -> ViewAttributes {
    ViewAttributes {
        struct_attr: quote! { #[derive(::ankurah::derive_deps::uniffi::Object)] },
        impl_attr: quote! { #[::ankurah::derive_deps::uniffi::export] },
        id_method_attr: quote! {},
        extra_impl: quote! {},
    }
}

/// Main UniFFI implementation generator for a Model.
pub fn uniffi_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let _name = model.name();
    let _view_name = model.view_name();

    // TODO: Implement UniFFI wrappers
    // Phase 3c: uniffi_ref_wrapper
    // Phase 3d: uniffi_ops_wrapper (singleton with get, fetch, query, create)
    // Phase 3e: uniffi_resultset_wrapper, uniffi_changeset_wrapper, uniffi_livequery_wrapper
    // Phase 3e: uniffi_livequery_callback_trait

    quote! {
        // UniFFI wrappers will be generated here
        // See ankurah/specs/uniffi-derive-integration.md for implementation plan
    }
}

