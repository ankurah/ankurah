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
/// NOTE: We use ::uniffi directly for the same reason as view_attributes.
pub fn mutable_attributes() -> (TokenStream, TokenStream) {
    (quote! { #[cfg_attr(feature = "uniffi", derive(::uniffi::Object))] }, quote! {})
}

/// Generate UniFFI-specific attributes for View struct.
/// NOTE: We use ::uniffi directly, not ::ankurah::derive_deps::uniffi, because
/// the derive macro needs to resolve crate::UniFfiTag in the USER's crate context.
pub fn view_attributes() -> ViewAttributes {
    ViewAttributes {
        struct_attr: quote! { #[cfg_attr(feature = "uniffi", derive(::uniffi::Object))] },
        impl_attr: quote! { #[cfg_attr(feature = "uniffi", ::uniffi::export)] },
        id_method_attr: quote! {},
        extra_impl: quote! {},
    }
}

/// Main UniFFI implementation generator for a Model.
/// Generates Ref wrapper and Ops singleton for UniFFI consumption.
/// These are generated in the same hygiene module as the View/Mutable types so that
/// Vec<View> works (generic containers only work within the same crate/module context).
pub fn uniffi_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let name = model.name();
    let view_name = model.view_name();
    let ref_name = model.ref_name();
    let ops_name = quote::format_ident!("{}Ops", name);

    let ref_wrapper = uniffi_ref_wrapper(&ref_name, &name, &view_name);
    let ops_wrapper = uniffi_ops_wrapper(&ops_name, &name, &view_name);

    // Wrapped in cfg - these are in the same hygiene module as View/Mutable
    quote! {
        #[cfg(feature = "uniffi")]
        #ref_wrapper

        #[cfg(feature = "uniffi")]
        #ops_wrapper
    }
}

/// MessageRef(Ref<Message>) - Typed entity reference wrapper for UniFFI
///
/// Wraps `Ref<Model>` with methods for TypeScript/native consumption:
/// - `id()` - Get the EntityId
/// - `get(ctx)` - Fetch the referenced entity
/// - `from_view(view)` - Create from a View (constructor-style)
fn uniffi_ref_wrapper(ref_name: &Ident, model_name: &Ident, view_name: &Ident) -> TokenStream {
    quote! {
        #[derive(::uniffi::Object)]
        pub struct #ref_name(::ankurah::property::Ref<#model_name>);

        #[::uniffi::export]
        impl #ref_name {
            /// Create a reference from a View
            #[uniffi::constructor]
            pub fn from_view(view: &#view_name) -> Self {
                #ref_name(view.r())
            }

            /// Get the EntityId (returns owned - works cross-crate)
            pub fn id(&self) -> ::ankurah::proto::EntityId {
                self.0.id()
            }

            /// Fetch the referenced entity
            /// Note: uses &Context (borrowed) because owned args don't work cross-crate
            pub async fn get(&self, ctx: &::ankurah::core::context::Context) -> Result<#view_name, ::ankurah::core::error::RetrievalError> {
                self.0.get(ctx).await
            }
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

/// MessageOps - Singleton with static-like methods for UniFFI
///
/// Since UniFFI doesn't support true static methods, we use a singleton pattern:
/// - `MessageOps.new()` - Get the singleton instance
/// - `ops.get(ctx, id)` - Fetch entity by ID
/// - `ops.fetch(ctx, selection)` - Fetch entities matching selection
///
/// Note: All args use borrowed references (&T) because owned args don't work cross-crate.
/// Functions returning Vec<T> are defined here (same crate as T) because generic
/// containers don't work cross-crate.
fn uniffi_ops_wrapper(ops_name: &Ident, model_name: &Ident, view_name: &Ident) -> TokenStream {
    quote! {
        /// Singleton providing static-like operations for this model type.
        /// Use `new()` to get an instance, then call methods like `get()` and `fetch()`.
        #[derive(::uniffi::Object)]
        pub struct #ops_name;

        #[::uniffi::export]
        impl #ops_name {
            /// Get the singleton instance
            #[uniffi::constructor]
            pub fn new() -> Self {
                Self
            }

            /// Get a single entity by ID
            /// Note: uses &Context and &EntityId (borrowed) because owned args don't work cross-crate
            pub async fn get(
                &self,
                ctx: &::ankurah::core::context::Context,
                id: &::ankurah::proto::EntityId,
            ) -> Result<#view_name, ::ankurah::core::error::RetrievalError> {
                ctx.get::<#view_name>(id.clone()).await
            }

            /// Fetch all entities matching the selection predicate
            /// Returns Vec<Arc<View>> because UniFFI Objects must be wrapped in Arc for collections
            pub async fn fetch(
                &self,
                ctx: &::ankurah::core::context::Context,
                selection: String,
            ) -> Result<Vec<std::sync::Arc<#view_name>>, ::ankurah::core::error::RetrievalError> {
                let selection = ::ankurah::ankql::parser::parse_selection(&selection)?;
                let results = ctx.fetch::<#view_name>(selection).await?;
                Ok(results.into_iter().map(std::sync::Arc::new).collect())
            }

            // TODO Phase 3e: query() returning LiveQuery wrapper
            // TODO Phase 3e: create() and create_one() methods
        }
    }
}
