//! UniFFI wrapper generation for Model types.
//!
//! Similar to wasm.rs, UniFFI doesn't support generics, so we generate concrete wrapper types:
//! - `SessionRef` - wraps `Ref<Session>`, the form in which every `Ref<Session>` crosses the
//!   bindings, with `get(ctx)`, `id`, `from_view(view)` and `from_id(id)` methods
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
                    Err(e) => Err(::ankurah::core::error::MutationError::from(e))
                }
            }
        }
    }
}

/// Generate the UniFFI methods for the Mutable struct: `id`, `read`, and a getter per field.
///
/// For each active field, generates a getter that returns the UniFFI wrapper type
/// (e.g., `LWWString`, `YrsStringString`). The wrapper types have the methods
/// defined in the RON config (get/set for LWW, value/replace for YrsString, etc.).
///
/// This mirrors the WASM approach where wrapper types expose the active type methods.
/// Uses `uniffi_fieldname` as Rust name, exposed as `fieldname` in foreign bindings.
pub fn uniffi_mutable_field_methods(model: &crate::model::description::ModelDescription) -> TokenStream {
    let mutable_name = model.mutable_name();
    let view_name = model.view_name();
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
                pub fn #uniffi_method_name(&self) -> Result<#wrapper_type, ::ankurah::property::PropertyError> {
                    Ok(#wrapper_type(self.#field_name()?))
                }
            })
        })
        .collect();

    quote! {
        #[::uniffi::export]
        impl #mutable_name {
            #[uniffi::method(name = "id")]
            pub fn uniffi_id(&self) -> ::ankurah::proto::EntityId { self.entity.id() }

            /// A View of this entity, which stays readable after commit.
            #[uniffi::method(name = "read")]
            pub fn uniffi_read(&self) -> Result<#view_name, ::ankurah::core::error::RetrievalError> { ::ankurah::model::Mutable::read(self) }

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
    let mut seen = std::collections::HashSet::new();

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

            // Deduplicate: skip if we've already generated this wrapper for this model
            if !seen.insert(backend_desc.wrapper_type_name_for_model(&model_name_str)) {
                return None;
            }

            Some(backend_desc.uniffi_wrapper("external", Some(&model_name_str)))
        })
        .collect();

    quote! {
        #(#wrappers)*
    }
}

/// How the bindings carry an entity id a model holds, so that a foreign caller handles every id as
/// an object and never as a string: a plain `EntityId` crosses as the EntityId Object and a `Ref<T>`
/// as T's generated Ref object, each behind the `Arc` that UniFFI needs to put an Object in a
/// Record, an `Option` or an owned argument; an `Option` of either crosses as an `Option` of that.
pub(crate) struct IdBinding {
    /// `EntityId`, or `<T as UniffiModel>::RefObject` for a `Ref<T>`
    object: TokenStream,
    is_ref: bool,
    optional: bool,
}

impl IdBinding {
    /// How the bindings carry a value of type `ty`, or None when `ty` is not an entity id
    pub(crate) fn of(ty: &syn::Type) -> Option<Self> {
        match generic_argument(ty, "Option") {
            Some(inner) => Self::required(inner).map(|binding| Self { optional: true, ..binding }),
            None => Self::required(ty),
        }
    }

    fn required(ty: &syn::Type) -> Option<Self> {
        if let Some(model) = generic_argument(ty, "Ref") {
            return Some(Self { object: quote! { <#model as ::ankurah::model::UniffiModel>::RefObject }, is_ref: true, optional: false });
        }
        let syn::Type::Path(path) = ty else { return None };
        let segment = path.path.segments.last()?;
        (segment.ident == "EntityId" && segment.arguments.is_none()).then(|| Self {
            object: quote! { ::ankurah::proto::EntityId },
            is_ref: false,
            optional: false,
        })
    }

    /// The type the bindings carry the value as
    pub(crate) fn carried_type(&self) -> TokenStream {
        let object = &self.object;
        if self.optional {
            quote! { Option<::std::sync::Arc<#object>> }
        } else {
            quote! { ::std::sync::Arc<#object> }
        }
    }

    /// Convert `value`, of the carried type, to the model's value
    pub(crate) fn to_model(&self, value: TokenStream) -> TokenStream {
        self.each(value, |id| {
            if self.is_ref {
                quote! { ::ankurah::property::Ref::new(#id.id()) }
            } else {
                quote! { *#id }
            }
        })
    }

    /// Convert `value`, of the model's type, to the carried value
    pub(crate) fn to_carried(&self, value: TokenStream) -> TokenStream {
        let object = &self.object;
        self.each(value, |id| {
            if self.is_ref {
                quote! { ::std::sync::Arc::new(#object::from(#id)) }
            } else {
                quote! { ::std::sync::Arc::new(#id) }
            }
        })
    }

    /// Apply `convert` to `value`, or to the id inside it when the id is optional
    fn each(&self, value: TokenStream, convert: impl Fn(TokenStream) -> TokenStream) -> TokenStream {
        if self.optional {
            let converted = convert(quote! { id });
            quote! { #value.map(|id| #converted) }
        } else {
            convert(value)
        }
    }
}

/// The type argument of `ty` when `ty` is the generic type `name`, such as `T` in `Option<T>`
fn generic_argument<'a>(ty: &'a syn::Type, name: &str) -> Option<&'a syn::Type> {
    let syn::Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    match &segment.arguments {
        syn::PathArguments::AngleBracketed(args) if segment.ident == name => match args.args.first() {
            Some(syn::GenericArgument::Type(argument)) => Some(argument),
            _ => None,
        },
        _ => None,
    }
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
    let ops_wrapper = uniffi_ops_wrapper(&ops_name, &name, &view_name, &mutable_name, &livequery_name, &input_name);
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
/// Wraps `Ref<Model>` with methods for TypeScript/native consumption, and carries every `Ref<Model>`
/// a model holds across the bindings:
/// - `id()` - Get the EntityId
/// - `get(ctx)` - Fetch the referenced entity
/// - `from_view(view)` - Create from a View (constructor-style)
/// - `from_id(id)` - Create from an EntityId (constructor-style)
/// - equality and hashing by id
fn uniffi_ref_wrapper(ref_name: &Ident, model_name: &Ident, view_name: &Ident) -> TokenStream {
    quote! {
        #[derive(::uniffi::Object)]
        #[::uniffi::export(Eq, Hash)]
        pub struct #ref_name(::ankurah::property::Ref<#model_name>);

        impl ::ankurah::model::UniffiModel for #model_name {
            type RefObject = #ref_name;
        }

        // A Ref names an entity, so two Refs are equal when they name the same one
        impl PartialEq for #ref_name {
            fn eq(&self, other: &Self) -> bool {
                self.0.id_ref() == other.0.id_ref()
            }
        }

        impl Eq for #ref_name {}

        impl ::std::hash::Hash for #ref_name {
            fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
                ::std::hash::Hash::hash(self.0.id_ref(), state)
            }
        }

        #[::uniffi::export(async_runtime = "tokio")]
        impl #ref_name {
            /// Create a reference from a View
            #[uniffi::constructor]
            pub fn from_view(view: &#view_name) -> Self {
                #ref_name(view.r())
            }

            /// Create a reference to the entity with this id
            #[uniffi::constructor]
            pub fn from_id(id: &::ankurah::proto::EntityId) -> Self {
                #ref_name(::ankurah::property::Ref::new(*id))
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
/// Entity id fields carry objects, as `IdBinding` describes: `Arc<EntityId>` for an EntityId and
/// T's Ref object for a Ref<T>.
fn uniffi_input_record(model: &crate::model::description::ModelDescription, input_name: &Ident, model_name: &Ident) -> TokenStream {
    let fields = model.active_fields();

    let input_fields: Vec<_> = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let field_vis = &field.vis;
            let field_ty = &field.ty;
            let field_ty = IdBinding::of(field_ty).map_or_else(|| quote! { #field_ty }, |id| id.carried_type());
            quote! { #field_vis #field_name: #field_ty }
        })
        .collect();

    let field_conversions: Vec<_> = fields
        .iter()
        .map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let value = quote! { input.#field_name };
            let value = match IdBinding::of(&field.ty) {
                Some(id) => id.to_model(value),
                None => value,
            };
            quote! { #field_name: #value }
        })
        .collect();

    quote! {
        /// Input record for creating new entities.
        /// Its entity id fields carry EntityId and Ref objects, so an id is never a string.
        #[derive(::uniffi::Record)]
        pub struct #input_name {
            #(#input_fields),*
        }

        impl From<#input_name> for #model_name {
            fn from(input: #input_name) -> Self {
                #model_name {
                    #(#field_conversions),*
                }
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
fn uniffi_ops_wrapper(
    ops_name: &Ident,
    model_name: &Ident,
    view_name: &Ident,
    mutable_name: &Ident,
    livequery_name: &Ident,
    input_name: &Ident,
) -> TokenStream {
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
                let args = ::ankurah::MatchArgs { selection, cache_policy: ::ankurah::CachePolicy::Durable };
                let lq = ctx.query::<#view_name>(args)?;
                Ok(#livequery_name::from(lq))
            }

            /// Create a new entity within a transaction, returning its Mutable
            /// Use the Input record type, whose id fields take EntityId and Ref objects
            pub async fn create(
                &self,
                trx: &::ankurah::transaction::Transaction,
                input: #input_name,
            ) -> Result<#mutable_name, ::ankurah::core::error::MutationError> {
                let model = #model_name::from(input);
                Ok(trx.create(&model).await?.into_core())
            }

            /// Create a new entity with an auto-committed transaction
            /// Use the Input record type, whose id fields take EntityId and Ref objects
            pub async fn create_one(
                &self,
                ctx: &::ankurah::core::context::Context,
                input: #input_name,
            ) -> Result<#view_name, ::ankurah::core::error::MutationError> {
                use ::ankurah::Mutable;
                let model = #model_name::from(input);
                let tx = ctx.begin();
                let mutable = tx.create(&model).await?;
                let view = mutable.read()?;
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
/// - `is_empty()` - Check if there are no items
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

            /// Check if there are no items
            pub fn is_empty(&self) -> bool {
                use ::ankurah::signals::Get;
                self.0.get().is_empty()
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
                #resultset_name(self.0.resultset.clone())
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
/// - `is_empty()` - Check if there are no items
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

            /// Check if there are no items
            pub fn is_empty(&self) -> bool {
                use ::ankurah::signals::Get;
                self.0.get().is_empty()
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

            /// The installed selection as a string, or empty before initial resolution.
            pub fn current_selection(&self) -> String {
                use ::ankurah::signals::With;
                self.0.selection().with(|resolved| resolved.as_ref().map(|(sel, _version)| sel.to_string()).unwrap_or_default())
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
