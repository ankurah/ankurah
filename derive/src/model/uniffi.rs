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
///
/// Note: extra_impl for UniFFI (edit method) is generated in uniffi_impl() because
/// it needs access to model names that aren't available here.
pub fn view_attributes() -> ViewAttributes {
    ViewAttributes {
        struct_attr: quote! { #[cfg_attr(feature = "uniffi", derive(::uniffi::Object))] },
        impl_attr: quote! { #[cfg_attr(feature = "uniffi", ::uniffi::export)] },
        id_method_attr: quote! {},
        extra_impl: quote! {},
    }
}

/// Generate UniFFI edit() method for View struct.
/// This is separate from view_attributes() because it needs model/mutable names.
/// Uses `name = "edit"` so it's exposed as `edit` in foreign bindings while
/// not conflicting with the Rust `edit()` method that returns MutableBorrow.
pub fn uniffi_view_edit_impl(view_name: &Ident, model_name: &Ident, mutable_name: &Ident) -> TokenStream {
    quote! {
        #[::uniffi::export]
        impl #view_name {
            /// Edit this entity in a transaction (UniFFI version - returns owned Mutable)
            /// Note: The returned Mutable is not lifetime-bound to the transaction,
            /// so the caller must ensure the transaction outlives the Mutable.
            #[uniffi::method(name = "edit")]
            pub fn uniffi_edit(&self, trx: &::ankurah::transaction::Transaction) -> Result<#mutable_name, ::ankurah::core::error::MutationError> {
                use ::ankurah::model::View;
                match trx.edit::<#model_name>(&self.entity) {
                    Ok(mutable_borrow) => Ok(mutable_borrow.into_core()),
                    Err(e) => Err(::ankurah::core::error::MutationError::AccessDenied(e))
                }
            }
        }
    }
}

/// Generate UniFFI getter methods for Mutable struct that return wrapper types.
///
/// For each active field, generates a getter that returns the UniFFI wrapper type
/// (e.g., `LWWString`, `YrsStringString`). The wrapper types have the methods
/// defined in the RON config (get/set for LWW, value/replace for YrsString, etc.).
///
/// This mirrors the WASM approach where wrapper types expose the active type methods.
/// Uses `uniffi_fieldname` as Rust name, exposed as `fieldname` in foreign bindings.
pub fn uniffi_mutable_field_methods(model: &crate::model::description::ModelDescription) -> TokenStream {
    let mutable_name = model.mutable_name();
    let model_name_str = model.name().to_string();

    let methods: Vec<TokenStream> = model
        .active_fields()
        .iter()
        .filter_map(|field| {
            let field_name = field.ident.as_ref()?;
            let uniffi_method_name = quote::format_ident!("uniffi_{}", field_name);
            let field_name_str = field_name.to_string();

            // Get the backend description for this field
            let backend_desc = model.backend_registry.resolve_active_type(field)?;

            // Get the wrapper type path (fully qualified for provided types, local for custom)
            let wrapper_type_str = if backend_desc.is_provided_type() {
                // Provided types: use full path from ankurah-core
                backend_desc.wrapper_type_path("local")
            } else {
                // Custom types: generated in same hygiene module, use local name
                backend_desc.wrapper_type_name_for_model(&model_name_str)
            };
            let wrapper_type: syn::Type = syn::parse_str(&wrapper_type_str).expect("Failed to parse wrapper type path");

            Some(quote! {
                /// Get the active property wrapper for this field
                #[uniffi::method(name = #field_name_str)]
                pub fn #uniffi_method_name(&self) -> #wrapper_type {
                    #wrapper_type(self.#field_name())
                }
            })
        })
        .collect();

    if methods.is_empty() {
        return quote! {};
    }

    quote! {
        #[::uniffi::export]
        impl #mutable_name {
            #(#methods)*
        }
    }
}

/// Generate UniFFI wrapper types for active types.
///
/// For each unique active type used in the model, generates a wrapper struct
/// with the methods defined in the RON config. These are similar to WASM wrappers
/// but use UniFFI attributes.
pub fn uniffi_custom_wrappers(model: &crate::model::description::ModelDescription) -> TokenStream {
    let model_name_str = model.name().to_string();

    let wrappers: Vec<TokenStream> = model
        .active_fields()
        .iter()
        .filter_map(|field| {
            let backend_desc = model.backend_registry.resolve_active_type(field)?;

            // Only generate wrappers for custom types (non-provided)
            // Provided types are generated globally elsewhere
            if backend_desc.is_provided_type() {
                return None;
            }

            Some(backend_desc.uniffi_wrapper("external", Some(&model_name_str)))
        })
        .collect();

    quote! {
        #(#wrappers)*
    }
}

/// Check if a type is Ref<T> and extract the inner type name
fn is_ref_type(ty: &syn::Type) -> Option<Ident> {
    if let syn::Type::Path(type_path) = ty {
        let segment = type_path.path.segments.last()?;
        if segment.ident == "Ref" {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(syn::Type::Path(inner_path))) = args.args.first() {
                    return Some(inner_path.path.segments.last()?.ident.clone());
                }
            }
        }
    }
    None
}

/// Main UniFFI implementation generator for a Model.
/// Generates Ref wrapper, Ops singleton, and ResultSet wrapper for UniFFI consumption.
/// These are generated in the same hygiene module as the View/Mutable types so that
/// Vec<Arc<View>> works (generic containers only work within the same crate/module context).
pub fn uniffi_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let name = model.name();
    let view_name = model.view_name();
    let mutable_name = model.mutable_name();
    let ref_name = model.ref_name();
    let ops_name = quote::format_ident!("{}Ops", name);
    let input_name = quote::format_ident!("{}Input", name);
    let resultset_name = model.resultset_name();
    let changeset_name = model.changeset_name();
    let livequery_name = model.livequery_name();

    let view_edit_impl = uniffi_view_edit_impl(&view_name, &name, &mutable_name);
    let mutable_field_methods = uniffi_mutable_field_methods(model);
    let ref_wrapper = uniffi_ref_wrapper(&ref_name, &name, &view_name);
    let input_record = uniffi_input_record(model, &input_name, &name);
    let ops_wrapper = uniffi_ops_wrapper(&ops_name, &name, &view_name, &livequery_name, &input_name);
    let resultset_wrapper = uniffi_resultset_wrapper(&resultset_name, &view_name);
    let changeset_wrapper = uniffi_changeset_wrapper(&changeset_name, &view_name, &resultset_name);
    let livequery_wrapper = uniffi_livequery_wrapper(&name, &livequery_name, &view_name, &resultset_name, &changeset_name);
    let custom_wrappers = uniffi_custom_wrappers(model);

    quote! {
        #view_edit_impl
        #mutable_field_methods
        #ref_wrapper
        #input_record
        #ops_wrapper
        #resultset_wrapper
        #changeset_wrapper
        #livequery_wrapper
        #custom_wrappers
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

/// MessageInput - UniFFI Record for creating new entities
///
/// This is a value type that can be passed across FFI boundaries.
/// Ref<T> fields become String (base64 EntityId) since:
/// 1. UniFFI doesn't support generics
/// 2. EntityId is an Object, not a Record, so can't be in a Record directly
fn uniffi_input_record(model: &crate::model::description::ModelDescription, input_name: &Ident, model_name: &Ident) -> TokenStream {
    let fields = model.active_fields();

    // Generate Input Record fields - Ref<T> becomes String (base64 EntityId)
    let input_fields: Vec<_> = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let field_vis = &field.vis;

            if is_ref_type(&field.ty).is_some() {
                quote! { #field_vis #field_name: String }
            } else {
                let field_ty = &field.ty;
                quote! { #field_vis #field_name: #field_ty }
            }
        })
        .collect();

    // Generate conversion from Input to Model - Ref<T> fields parse base64 to EntityId
    // Uses TryFrom to properly propagate parsing errors instead of panicking
    let field_conversions: Vec<_> = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();

            if is_ref_type(&field.ty).is_some() {
                // Parse base64 string to EntityId, propagating errors
                quote! {
                    #field_name: ::ankurah::property::Ref::from(
                        ::ankurah::proto::EntityId::from_base64(&input.#field_name)?
                    )
                }
            } else {
                quote! { #field_name: input.#field_name }
            }
        })
        .collect();

    quote! {
        /// Input record for creating new entities.
        /// Ref<T> fields are represented as String (base64 EntityId) since:
        /// - UniFFI doesn't support generics
        /// - EntityId is an Object, not a Record
        #[derive(::uniffi::Record)]
        pub struct #input_name {
            #(#input_fields),*
        }

        impl TryFrom<#input_name> for #model_name {
            type Error = ::ankurah::proto::IdParseError;

            fn try_from(input: #input_name) -> Result<Self, Self::Error> {
                Ok(#model_name {
                    #(#field_conversions),*
                })
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
fn uniffi_ops_wrapper(ops_name: &Ident, model_name: &Ident, view_name: &Ident, livequery_name: &Ident, input_name: &Ident) -> TokenStream {
    quote! {
        /// Singleton providing static-like operations for this model type.
        /// Use `new()` to get an instance, then call methods like `get()` and `fetch()`.
        #[derive(::uniffi::Object)]
        pub struct #ops_name;

        #[::uniffi::export(async_runtime = "tokio")]
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
            /// Use `values` to fill in `?` placeholders in the selection string
            pub async fn fetch(
                &self,
                ctx: &::ankurah::core::context::Context,
                selection: String,
                values: Vec<::ankurah::core::QueryValue>,
            ) -> Result<Vec<std::sync::Arc<#view_name>>, ::ankurah::core::error::RetrievalError> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(&selection)?;
                selection.predicate = selection.predicate.populate(values)?;
                let results = ctx.fetch::<#view_name>(selection).await?;
                Ok(results.into_iter().map(std::sync::Arc::new).collect())
            }

            /// Create a live query that tracks changes matching the selection
            /// Use `values` to fill in `?` placeholders in the selection string
            pub async fn query(
                &self,
                ctx: &::ankurah::core::context::Context,
                selection: String,
                values: Vec<::ankurah::core::QueryValue>,
            ) -> Result<#livequery_name, ::ankurah::core::error::RetrievalError> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(&selection)?;
                selection.predicate = selection.predicate.populate(values)?;
                let lq = ctx.query::<#view_name>(selection)?;
                Ok(#livequery_name::from(lq))
            }

            /// Create a live query that waits for remote subscription before initializing
            /// Ensures existing items appear as `initial` in the first changeset
            /// Use `values` to fill in `?` placeholders in the selection string
            pub async fn query_nocache(
                &self,
                ctx: &::ankurah::core::context::Context,
                selection: String,
                values: Vec<::ankurah::core::QueryValue>,
            ) -> Result<#livequery_name, ::ankurah::core::error::RetrievalError> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(&selection)?;
                selection.predicate = selection.predicate.populate(values)?;
                let args = ::ankurah::MatchArgs { selection, cached: false };
                let lq = ctx.query::<#view_name>(args)?;
                Ok(#livequery_name::from(lq))
            }

            /// Create a new entity within a transaction
            /// Use the Input record type which has EntityId fields for Ref<T> types
            pub async fn create(
                &self,
                trx: &::ankurah::transaction::Transaction,
                input: #input_name,
            ) -> Result<#view_name, ::ankurah::core::error::MutationError> {
                use ::ankurah::Mutable;
                use std::convert::TryInto;
                let model: #model_name = input.try_into()
                    .map_err(|e: ::ankurah::proto::IdParseError| ::ankurah::core::error::MutationError::General(Box::new(e)))?;
                let mutable = trx.create(&model).await?;
                Ok(mutable.read())
            }

            /// Create a new entity with an auto-committed transaction
            /// Use the Input record type which has EntityId fields for Ref<T> types
            pub async fn create_one(
                &self,
                ctx: &::ankurah::core::context::Context,
                input: #input_name,
            ) -> Result<#view_name, ::ankurah::core::error::MutationError> {
                use ::ankurah::Mutable;
                use std::convert::TryInto;
                let model: #model_name = input.try_into()
                    .map_err(|e: ::ankurah::proto::IdParseError| ::ankurah::core::error::MutationError::General(Box::new(e)))?;
                let tx = ctx.begin();
                let mutable = tx.create(&model).await?;
                let view = mutable.read();
                tx.commit().await?;
                Ok(view)
            }
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
/// - `is_loaded()` - Check if initial load is complete
/// - `len()` - Get count of items
/// - `error()` - Get any error message
/// - `subscribe(callback)` - Subscribe to changes, returns guard that unsubscribes on drop
/// - `update_selection(selection)` - Update the query predicate
fn uniffi_livequery_wrapper(
    model_name: &Ident,
    livequery_name: &Ident,
    view_name: &Ident,
    resultset_name: &Ident,
    changeset_name: &Ident,
) -> TokenStream {
    let callback_name = quote::format_ident!("{}LiveQueryCallback", model_name);

    quote! {
        /// Callback interface for LiveQuery subscription updates.
        /// JS implements this trait and passes it to LiveQuery.subscribe()
        #[::uniffi::export(callback_interface)]
        pub trait #callback_name: Send + Sync {
            /// Called when the LiveQuery resultset changes
            fn on_change(&self, changeset: std::sync::Arc<#changeset_name>);
        }

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
            /// Use `values` to fill in `?` placeholders in the selection string
            pub async fn update_selection(&self, new_selection: String, values: Vec<::ankurah::core::QueryValue>) -> Result<(), ::ankurah::core::error::RetrievalError> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(&new_selection)?;
                selection.predicate = selection.predicate.populate(values)?;
                self.0.update_selection_wait(selection).await?;
                Ok(())
            }

            /// Get the current selection predicate as a string
            pub fn current_selection(&self) -> String {
                use ::ankurah::signals::With;
                self.0.selection().with(|(sel, _version)| sel.to_string())
            }

            /// Subscribe to changes in the LiveQuery resultset
            /// The callback will be called whenever the resultset changes.
            /// Returns a guard that cancels the subscription when dropped.
            pub fn subscribe(&self, callback: Box<dyn #callback_name>) -> std::sync::Arc<::ankurah::signals::SubscriptionGuard> {
                use ::ankurah::signals::Subscribe;
                let guard = self.0.subscribe(move |changeset| {
                    callback.on_change(std::sync::Arc::new(#changeset_name::from(changeset)));
                });
                std::sync::Arc::new(guard)
            }
        }

        // Allow constructing from inner type
        impl From<::ankurah::LiveQuery<#view_name>> for #livequery_name {
            fn from(lq: ::ankurah::LiveQuery<#view_name>) -> Self {
                #livequery_name(lq)
            }
        }
    }
}
