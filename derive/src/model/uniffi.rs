//! UniFFI wrapper generation for Model types.
//!
//! Similar to wasm.rs, UniFFI doesn't support generics, so we generate concrete wrapper types:
//! - `SessionRef` - wraps `Ref<Session>` with `get(ctx)`, `id`, `from(view)` methods
//! - `SessionResultSet`, `SessionLiveQuery`, etc.
//! - `SessionOps` - singleton with static-like methods (get, fetch, query, create)
//! - `SessionLiveQueryCallback` - callback trait for subscription updates

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

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
    let name = model.name();
    let view_name = model.view_name();
    let ref_name = model.ref_name();

    // Phase 3c: Ref wrapper
    let ref_wrapper = uniffi_ref_wrapper(&ref_name, &name, &view_name);

    // TODO: Implement remaining UniFFI wrappers
    // Phase 3d: uniffi_ops_wrapper (singleton with get, fetch, query, create)
    // Phase 3e: uniffi_resultset_wrapper, uniffi_changeset_wrapper, uniffi_livequery_wrapper
    // Phase 3e: uniffi_livequery_callback_trait

    quote! {
        #ref_wrapper
    }
}

/// MessageRef(Ref<Message>) - Typed entity reference wrapper for UniFFI
///
/// Wraps `Ref<Model>` with methods for TypeScript/native consumption:
/// - `id()` - Get the EntityId
/// - `from_view(view)` - Create from a View (constructor-style)
///
/// Note: `get(ctx)` is not yet implemented because Context needs to be
/// a UniFFI Object. This will be added when we implement the Ops singleton.
pub fn uniffi_ref_wrapper(ref_name: &Ident, model_name: &Ident, view_name: &Ident) -> TokenStream {
    quote! {
        #[derive(::ankurah::derive_deps::uniffi::Object)]
        pub struct #ref_name(::ankurah::property::Ref<#model_name>);

        #[::ankurah::derive_deps::uniffi::export]
        impl #ref_name {
            /// Create a reference from a View
            #[uniffi::constructor]
            pub fn from_view(view: &#view_name) -> Self {
                #ref_name(view.r())
            }

            /// Get the EntityId
            pub fn id(&self) -> ::ankurah::proto::EntityId {
                self.0.id()
            }

            // TODO: get(ctx) method requires Context to be a UniFFI Object
            // This will be implemented in Phase 3d with the Ops singleton
        }

        // Allow constructing RefModel from Ref<Model>
        impl From<::ankurah::property::Ref<#model_name>> for #ref_name {
            fn from(r: ::ankurah::property::Ref<#model_name>) -> Self {
                #ref_name(r)
            }
        }

        // Allow extracting Ref<Model> from RefModel
        impl From<#ref_name> for ::ankurah::property::Ref<#model_name> {
            fn from(r: #ref_name) -> Self {
                r.0
            }
        }
    }
}
