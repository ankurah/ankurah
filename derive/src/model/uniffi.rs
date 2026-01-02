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
/// Generates Ref wrapper, Ops singleton, and ResultSet wrapper for UniFFI consumption.
/// These are generated in the same hygiene module as the View/Mutable types so that
/// Vec<Arc<View>> works (generic containers only work within the same crate/module context).
pub fn uniffi_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let name = model.name();
    let view_name = model.view_name();
    let ref_name = model.ref_name();
    let ops_name = quote::format_ident!("{}Ops", name);
    let resultset_name = model.resultset_name();
    let changeset_name = model.changeset_name();
    let livequery_name = model.livequery_name();

    let ref_wrapper = uniffi_ref_wrapper(&ref_name, &name, &view_name);
    let ops_wrapper = uniffi_ops_wrapper(&ops_name, &name, &view_name, &livequery_name);
    let resultset_wrapper = uniffi_resultset_wrapper(&resultset_name, &view_name);
    let changeset_wrapper = uniffi_changeset_wrapper(&changeset_name, &view_name, &resultset_name);
    let livequery_wrapper = uniffi_livequery_wrapper(&livequery_name, &view_name, &resultset_name);

    // Wrapped in cfg - these are in the same hygiene module as View/Mutable
    quote! {
        #[cfg(feature = "uniffi")]
        #ref_wrapper

        #[cfg(feature = "uniffi")]
        #ops_wrapper

        #[cfg(feature = "uniffi")]
        #resultset_wrapper

        #[cfg(feature = "uniffi")]
        #changeset_wrapper

        #[cfg(feature = "uniffi")]
        #livequery_wrapper
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
fn uniffi_ops_wrapper(ops_name: &Ident, _model_name: &Ident, view_name: &Ident, livequery_name: &Ident) -> TokenStream {
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

            /// Create a live query that tracks changes matching the selection
            pub fn query(
                &self,
                ctx: &::ankurah::core::context::Context,
                selection: String,
            ) -> Result<#livequery_name, ::ankurah::core::error::RetrievalError> {
                let selection = ::ankurah::ankql::parser::parse_selection(&selection)?;
                let lq = ctx.query::<#view_name>(selection)?;
                Ok(#livequery_name::from(lq))
            }

            // TODO: create() and create_one() methods
        }
    }
}

/// MessageResultSet - Wrapper around ResultSet<MessageView> for UniFFI
///
/// Provides access to query results with proper Arc wrapping for UniFFI Objects.
/// Methods:
/// - `items()` - Get all items as Vec<Arc<View>>
/// - `get(index)` - Get item at index as Option<Arc<View>>
/// - `by_id(id)` - Get item by EntityId as Option<Arc<View>>
/// - `len()` - Get count of items
/// - `is_loaded()` - Check if initial load is complete
fn uniffi_resultset_wrapper(resultset_name: &Ident, view_name: &Ident) -> TokenStream {
    quote! {
        #[derive(::uniffi::Object)]
        pub struct #resultset_name(::ankurah::core::resultset::ResultSet<#view_name>);

        #[::uniffi::export]
        impl #resultset_name {
            /// Get all items in the result set
            pub fn items(&self) -> Vec<std::sync::Arc<#view_name>> {
                use ::ankurah::signals::Get;
                self.0.get().into_iter().map(std::sync::Arc::new).collect()
            }

            /// Get item at index
            pub fn get(&self, index: u32) -> Option<std::sync::Arc<#view_name>> {
                use ::ankurah::signals::Get;
                self.0.get().into_iter().nth(index as usize).map(std::sync::Arc::new)
            }

            /// Get item by EntityId
            pub fn by_id(&self, id: &::ankurah::proto::EntityId) -> Option<std::sync::Arc<#view_name>> {
                self.0.by_id(id).map(std::sync::Arc::new)
            }

            /// Get the number of items
            pub fn len(&self) -> u32 {
                use ::ankurah::signals::Get;
                self.0.get().len() as u32
            }

            /// Check if initial load is complete
            pub fn is_loaded(&self) -> bool {
                self.0.is_loaded()
            }
        }

        // Allow constructing from inner type
        impl From<::ankurah::core::resultset::ResultSet<#view_name>> for #resultset_name {
            fn from(rs: ::ankurah::core::resultset::ResultSet<#view_name>) -> Self {
                #resultset_name(rs)
            }
        }
    }
}

/// MessageChangeSet - Wrapper around ChangeSet<MessageView> for UniFFI
///
/// Provides access to subscription change deltas with proper Arc wrapping.
/// Methods:
/// - `resultset()` - Get the current ResultSet
/// - `initial()` - Items from initial query load
/// - `added()` - Newly added items (after subscription started)
/// - `appeared()` - All items that appeared (initial + added)
/// - `removed()` - Items that no longer match the query
/// - `updated()` - Items that were updated but still match
fn uniffi_changeset_wrapper(changeset_name: &Ident, view_name: &Ident, resultset_name: &Ident) -> TokenStream {
    quote! {
        #[derive(::uniffi::Object)]
        pub struct #changeset_name(::ankurah::core::changes::ChangeSet<#view_name>);

        #[::uniffi::export]
        impl #changeset_name {
            /// Get the current result set
            pub fn resultset(&self) -> #resultset_name {
                #resultset_name(self.0.resultset.wrap())
            }

            /// Items from the initial query load (before subscription was active)
            pub fn initial(&self) -> Vec<std::sync::Arc<#view_name>> {
                self.0.initial().into_iter().map(std::sync::Arc::new).collect()
            }

            /// Genuinely new items (added after subscription started)
            pub fn added(&self) -> Vec<std::sync::Arc<#view_name>> {
                self.0.added().into_iter().map(std::sync::Arc::new).collect()
            }

            /// All items that appeared in the result set (initial load + newly added)
            pub fn appeared(&self) -> Vec<std::sync::Arc<#view_name>> {
                self.0.appeared().into_iter().map(std::sync::Arc::new).collect()
            }

            /// Items that no longer match the query
            pub fn removed(&self) -> Vec<std::sync::Arc<#view_name>> {
                self.0.removed().into_iter().map(std::sync::Arc::new).collect()
            }

            /// Items that were updated but still match
            pub fn updated(&self) -> Vec<std::sync::Arc<#view_name>> {
                self.0.updated().into_iter().map(std::sync::Arc::new).collect()
            }
        }

        // Allow constructing from inner type
        impl From<::ankurah::core::changes::ChangeSet<#view_name>> for #changeset_name {
            fn from(cs: ::ankurah::core::changes::ChangeSet<#view_name>) -> Self {
                #changeset_name(cs)
            }
        }
    }
}

/// MessageLiveQuery - Wrapper around LiveQuery<MessageView> for UniFFI
///
/// Provides reactive query subscription with callback-based updates.
/// Methods:
/// - `items()` - Get current items
/// - `resultset()` - Get the ResultSet wrapper
/// - `loaded()` - Check if initial load is complete
/// - `subscribe(callback)` - Subscribe to changes, returns guard that unsubscribes on drop
/// - `update_selection(selection)` - Update the query predicate
/// MessageLiveQuery - Wrapper around LiveQuery<MessageView> for UniFFI
///
/// Provides reactive query with polling-based access to results.
/// Note: Callback-based subscriptions are not yet supported due to UniFFI
/// callback interface limitations with hygiene modules. Use polling for now.
///
/// Methods:
/// - `items()` - Get current items
/// - `resultset()` - Get the ResultSet wrapper  
/// - `is_loaded()` - Check if initial load is complete
/// - `len()` - Get count of items
/// - `error()` - Get any error message
/// - `update_selection(selection)` - Update the query predicate
fn uniffi_livequery_wrapper(livequery_name: &Ident, view_name: &Ident, resultset_name: &Ident) -> TokenStream {
    quote! {
        #[derive(::uniffi::Object)]
        pub struct #livequery_name(::ankurah::LiveQuery<#view_name>);

        #[::uniffi::export]
        impl #livequery_name {
            /// Get all current items
            pub fn items(&self) -> Vec<std::sync::Arc<#view_name>> {
                use ::ankurah::signals::Get;
                self.0.get().into_iter().map(std::sync::Arc::new).collect()
            }

            /// Get the current result set
            pub fn resultset(&self) -> #resultset_name {
                #resultset_name(self.0.resultset())
            }

            /// Check if initial load is complete
            pub fn is_loaded(&self) -> bool {
                self.0.loaded()
            }

            /// Get the number of items
            pub fn len(&self) -> u32 {
                use ::ankurah::signals::Get;
                self.0.get().len() as u32
            }

            /// Get any error from the query
            pub fn error(&self) -> Option<String> {
                use ::ankurah::signals::Get;
                self.0.error().map(|e| e.as_ref().map(|e| e.to_string())).get()
            }

            /// Update the query selection predicate
            pub async fn update_selection(&self, new_selection: String) -> Result<(), ::ankurah::core::error::RetrievalError> {
                let selection = ::ankurah::ankql::parser::parse_selection(&new_selection)?;
                self.0.update_selection_wait(selection).await?;
                Ok(())
            }

            // TODO: Add callback-based subscribe() once UniFFI callback interfaces
            // work correctly with hygiene modules. For now, use polling.
        }

        // Allow constructing from inner type
        impl From<::ankurah::LiveQuery<#view_name>> for #livequery_name {
            fn from(lq: ::ankurah::LiveQuery<#view_name>) -> Self {
                #livequery_name(lq)
            }
        }
    }
}
