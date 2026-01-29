//! WASM wrapper generation for Model types.
//!
//! wasm-bindgen doesn't support generics, so we generate concrete wrapper types:
//! - `SessionRef` - wraps `Ref<Session>` with `get(ctx)`, `id`, `from(view)` methods
//! - `SessionResultSet`, `SessionLiveQuery`, etc.
//! - Static methods via `NSSession` namespace class

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident};

use super::ViewAttributes;

/// Generate WASM-specific attributes for Mutable struct.
/// Returns (struct_attributes, field_attributes)
pub fn mutable_attributes() -> (TokenStream, TokenStream) { (quote! { #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)] }, quote! { #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, skip)] }) }

/// Generate WASM-specific attributes for View struct.
pub fn view_attributes(view_name: &Ident, mutable_name: &Ident, model_name: &Ident) -> ViewAttributes {
    let subscribe_ts = format!(
        r#"export interface {view_name} {{
    subscribe(callback: () => void): SubscriptionGuard;
}}"#
    );

    ViewAttributes {
        struct_attr: quote! { #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)] },
        impl_attr: quote! { #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)] },
        id_method_attr: quote! { #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)] },
        extra_impl: quote! {
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, typescript_custom_section)]
            const TS_VIEW_SUBSCRIBE: &'static str = #subscribe_ts;

            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
            impl #view_name {
                /// Edit this entity in a transaction (WASM version - returns owned Mutable)
                #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, js_name = "edit")]
                pub fn edit_wasm(&self, trx: &ankurah::transaction::Transaction) -> Result<#mutable_name, ::ankurah::derive_deps::wasm_bindgen::JsValue> {
                    use ::ankurah::model::View;
                    match trx.edit::<#model_name>(&self.entity) {
                        Ok(mutable_borrow) => {
                            // Extract the core mutable from the borrow wrapper
                            Ok(mutable_borrow.into_core())
                        }
                        Err(e) => Err(::ankurah::derive_deps::wasm_bindgen::JsValue::from(e.to_string()))
                    }
                }

                #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, skip_typescript, js_name = "subscribe")]
                pub fn subscribe_wasm(&self, callback: ::ankurah::derive_deps::js_sys::Function) -> ::ankurah::signals::SubscriptionGuard {

                    let callback = ::ankurah::derive_deps::send_wrapper::SendWrapper::new(callback);
                    ::ankurah::core::model::view_subscribe_no_clone(self, move |_| {
                        let _ = callback.call0(
                            &::ankurah::derive_deps::wasm_bindgen::JsValue::NULL,
                        );
                    })
                }
            }
        },
    }
}

/// Main WASM implementation generator for a Model.
pub fn wasm_impl(input: &syn::DeriveInput, model: &crate::model::description::ModelDescription) -> TokenStream {
    // Generate the namespace struct name (NSEntry for Entry)
    let namespace_struct = format_ident!("NS{}", model.name());

    // Generate the POJO interface name (IEntry for Entry)
    let pojo_interface = format_ident!("{}", model.name());

    // Generate the TypeScript interface using tsify internals
    let tsify_impl = expand_ts_model_type(input, pojo_interface.to_string()).unwrap_or_else(syn::Error::into_compile_error);

    // Generate WASM wrapper code
    let namespace_class = wasm_model_namespace(&namespace_struct, model, &model.view_name(), &model.livequery_name(), &pojo_interface);
    let resultset_wrapper = wasm_resultset_wrapper(&model.resultset_name(), &model.view_name());
    let changeset_wrapper = wasm_changeset_wrapper(&model.changeset_name(), &model.view_name(), &model.resultset_name());
    let livequery_wrapper =
        wasm_livequery_wrapper(&model.livequery_name(), &model.view_name(), &model.resultset_name(), &model.changeset_name());
    let ref_wrapper = wasm_ref_wrapper(&model.ref_name(), model.name(), &model.view_name());

    quote! {
        #tsify_impl

        // RefModel wrapper at module level (accessible for trait associated type).
        // Uses short wasm_bindgen path from module-level import in lib.rs
        #ref_wrapper

        // Generate ResultSet wrapper (at module level for re-export)
        #resultset_wrapper

        // Generate ChangeSet wrapper (at module level for re-export)
        #changeset_wrapper

        // Generate LiveQuery wrapper (at module level for re-export)
        #livequery_wrapper

        const _: () = {
            use ::ankurah::derive_deps::{tracing::error,wasm_bindgen::prelude::*, wasm_bindgen_futures};

            // Generate namespace struct with static methods
            #namespace_class
        };
    }
}

/// FooResultSet(ResultSet<FooView>)
pub fn wasm_resultset_wrapper(resultset_name: &Ident, view_name: &Ident) -> TokenStream {
    quote! {
        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
        #[derive(Clone, Default)]
        pub struct #resultset_name(::ankurah::core::resultset::ResultSet<#view_name>);

        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
        impl #resultset_name {
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn items(&self) -> Vec<#view_name> {
                use ::ankurah::signals::Get;
                self.0.get()
            }
            pub fn by_id(&self, id: ::ankurah::proto::EntityId) -> Option<#view_name> {
                ::ankurah::signals::CurrentObserver::track(&self);
                self.0.by_id(&id)
            }
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn loaded(&self) -> bool {
                ::ankurah::signals::CurrentObserver::track(&self);
                self.0.is_loaded()
            }
            /// Call the provided callback on each item in the resultset, returning a new array of the results
            pub fn map(&self, callback: ::ankurah::derive_deps::js_sys::Function) -> ::ankurah::derive_deps::js_sys::Array {
                ::ankurah::core::model::js_resultset_map(&self.0, &callback)
            }
        }

        // not sure if we actually need this
        impl ankurah::signals::Signal for #resultset_name {
            fn listen(&self, listener: ::ankurah::signals::signal::Listener) -> ::ankurah::signals::signal::ListenerGuard {
                self.0.listen(listener).into()
            }
            fn broadcast_id(&self) -> ::ankurah::signals::broadcast::BroadcastId {
                use ::ankurah::signals::Signal;
                self.0.broadcast_id()
            }
        }
    }
}

/// FooChangeSet(ChangeSet<FooView>)
pub fn wasm_changeset_wrapper(changeset_name: &Ident, view_name: &Ident, resultset_name: &Ident) -> TokenStream {
    quote! {
        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
        pub struct #changeset_name(::ankurah::core::changes::ChangeSet<#view_name>);

        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
        impl #changeset_name {
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn resultset(&self) -> #resultset_name {
                #resultset_name(self.0.resultset.wrap())
            }

            /// Items from the initial query load (before subscription was active)
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn initial(&self) -> Vec<#view_name> {
                self.0.initial()
            }

            /// Genuinely new items (added after subscription started)
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn added(&self) -> Vec<#view_name> {
                self.0.added()
            }

            /// All items that appeared in the result set (initial load + newly added)
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn appeared(&self) -> Vec<#view_name> {
                self.0.appeared()
            }

            /// Items that no longer match the query
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn removed(&self) -> Vec<#view_name> {
                self.0.removed()
            }

            /// Items that were updated but still match
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn updated(&self) -> Vec<#view_name> {
                self.0.updated()
            }

            // Deprecated methods for backwards compatibility
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            #[deprecated(since = "0.7.10", note = "Use appeared, initial, or added instead")]
            pub fn adds(&self) -> Vec<#view_name> {
                #[allow(deprecated)]
                self.0.adds()
            }

            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            #[deprecated(since = "0.7.10", note = "Use removed instead")]
            pub fn removes(&self) -> Vec<#view_name> {
                #[allow(deprecated)]
                self.0.removes()
            }

            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            #[deprecated(since = "0.7.10", note = "Use updated instead")]
            pub fn updates(&self) -> Vec<#view_name> {
                #[allow(deprecated)]
                self.0.updates()
            }
        }
    }
}

/// FooLiveQuery(LiveQuery<FooView>)
pub fn wasm_livequery_wrapper(livequery_name: &Ident, view_name: &Ident, resultset_name: &Ident, changeset_name: &Ident) -> TokenStream {
    // Generate custom TypeScript for the subscribe method with proper callback typing
    let subscribe_ts = format!(
        r#"export interface {livequery_name} {{
    subscribe(callback: (changeset: {changeset_name}) => void, immediate?: boolean): SubscriptionGuard;
    updateSelection(selection: string, ...substitution_values: any): Promise<void>;
}}"#
    );

    quote! {
        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, typescript_custom_section)]
        const TS_APPEND_CONTENT: &'static str = #subscribe_ts;

        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
        pub struct #livequery_name(::ankurah::LiveQuery<#view_name>);

        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
        impl #livequery_name {
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn items(&self) -> Vec<#view_name> {
                use ::ankurah::signals::Get;
                self.0.get()
            }
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn signal_id(&self) -> usize {
                use ::ankurah::signals::Signal;
                self.0.broadcast_id().into()
            }
            pub fn map(&self, callback: ::ankurah::derive_deps::js_sys::Function) -> ::ankurah::derive_deps::js_sys::Array {
                ::ankurah::core::model::js_resultset_map(&self.0.resultset(), &callback)
            }

            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn peek(&self) -> Vec<#view_name> {
                use ::ankurah::signals::Peek;
                self.0.peek()
            }

            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn loaded(&self) -> bool { self.0.loaded() }

            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn resultset(&self) -> #resultset_name {
                #resultset_name(self.0.resultset())
            }
            /// DEPREDCATED - use resultset() instead
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn value(&self) -> #resultset_name {
                #resultset_name(self.0.resultset())
            }

            /// reading this will track the error signal, so updates will cause the (React)Observer to be notified
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn error(&self) -> Option<String> {
                use ::ankurah::signals::Get;
                // TODO maybe don't make a new map signal each time this is called?
                // doing this for now because we need the signal broadcast to be tracked by the current observer
                self.0.error().map(|e| e.as_ref().map(|e| e.to_string())).get()

            }

            /// Get the current selection as a string, tracked by the observer
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter, js_name = currentSelection)]
            pub fn current_selection(&self) -> String {
                use ::ankurah::signals::With;
                self.0.selection().with(|(sel, _version)| sel.to_string())
            }

            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, skip_typescript)]
            pub fn subscribe(&self, callback: ::ankurah::derive_deps::js_sys::Function, immediate: Option<bool>) -> ::ankurah::signals::SubscriptionGuard {
                ::ankurah::core::model::js_livequery_subscribe(
                    &self.0,
                    callback,
                    immediate.unwrap_or(true),
                    #changeset_name
                )
            }

            /// Update the predicate for this query and return a promise that resolves when complete
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, js_name = updateSelection, variadic, skip_typescript)]
            pub async fn update_selection(&self, new_selection: String, substitution_values: &::ankurah::derive_deps::wasm_bindgen::JsValue) -> Result<(), ::ankurah::derive_deps::wasm_bindgen::JsValue> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(new_selection.as_str())?;

                // Convert the variadic JsValue (which is an array) and pass directly to populate
                let args_array: ::ankurah::derive_deps::js_sys::Array = substitution_values.clone().try_into()
                    .map_err(|_| ::ankurah::derive_deps::wasm_bindgen::JsValue::from_str("Invalid arguments array"))?;
                selection.predicate = selection.predicate.populate(args_array)?;

                self.0.update_selection_wait(selection)
                    .await
                    .map_err(|e| ::ankurah::derive_deps::wasm_bindgen::JsValue::from(e.to_string()))
            }
        }
    }
}

/// SessionRef(Ref<Session>) - Typed entity reference wrapper
///
/// Wraps `Ref<Model>` with methods for TypeScript consumption:
/// - `get(ctx)` - Fetch the referenced entity, returns Promise<ModelView>
/// - `id` getter - Get the raw EntityId (use `.id().to_base64()` for string)
/// - `from(view)` - Create from a View (static method)
///
/// Uses short wasm_bindgen path from module-level import in lib.rs
pub fn wasm_ref_wrapper(ref_name: &Ident, model_name: &Ident, view_name: &Ident) -> TokenStream {
    quote! {
        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
        pub struct #ref_name(::ankurah::property::Ref<#model_name>);

        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
        impl #ref_name {
            /// Fetch the referenced entity
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen)]
            pub async fn get(&self, ctx: &::ankurah::core::context::Context) -> Result<#view_name, ::ankurah::derive_deps::wasm_bindgen::JsValue> {
                use ::ankurah::model::Model;
                self.0.get(ctx).await.map_err(|e| ::ankurah::derive_deps::wasm_bindgen::JsValue::from(e.to_string()))
            }

            /// Get the raw EntityId
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter)]
            pub fn id(&self) -> ::ankurah::proto::EntityId {
                self.0.id()
            }

            /// Create a reference from a View
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, js_name = "from")]
            pub fn from_view(view: &#view_name) -> Self {
                #ref_name(view.r())
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

/// Static methods namespace struct for the model (e.g., NSEntry for Entry model)
///
/// Uses `skip_typescript` because we need to avoid conflicts between:
/// 1. The tsify-generated interface for the POJO (Entry interface with fields)  
/// 2. The class methods we generate here
/// TypeScript interface merging rules don't allow multiple interfaces to merge
/// with the same class, so we skip wasm-bindgen's auto-generation and provide
/// our own complete TypeScript definition.
pub fn wasm_model_namespace(
    namespace_struct: &Ident,
    model: &crate::model::description::ModelDescription,
    view_name: &Ident,
    livequery_name: &Ident,
    pojo_interface: &Ident,
) -> TokenStream {
    let name = model.name();
    let ref_field_names: Vec<String> = model.ref_fields().iter().map(|(field, _)| field.ident.as_ref().unwrap().to_string()).collect();
    let preprocess_calls: Vec<TokenStream> = ref_field_names
        .iter()
        .map(|field_name| {
            quote! {
                ::ankurah::core::model::js_preprocess_ref_field(&js_value, #field_name)?;
            }
        })
        .collect();
    // Generate the custom TypeScript for the static methods
    let static_methods_ts = format!(
        r#"export class {name} {{
        /** Get a single {name} by ID  */
        static get(context: Context, id: EntityId): Promise<{view_name}>;
        /** Fetch all {name}s that match the predicate */
        static fetch(context: Context, selection: string, ...substitution_values: any): Promise<{view_name}[]>;
        /** Subscribe to the set of {name}s that match the predicate */
        static query(context: Context, selection: string, ...substitution_values: any): {livequery_name};
        /** Waits for remote subscription establishment - existing items are Initial, items added after are Add */
        static query_nocache(context: Context, selection: string, ...substitution_values: any): {livequery_name};
        /** Create a new {name} */
        static create(transaction: Transaction, me: {pojo_interface}): Promise<{view_name}>;
        /** Create a new {name} within an automatically created and committed transaction. */
        static create_one(context: Context, me: {pojo_interface}): Promise<{view_name}>;
}}"#
    );

    quote! {
        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, typescript_custom_section)]
        const TS_APPEND_CONTENT: &'static str = #static_methods_ts;

        // These methods are only available via wasm bindgen, so it's ok that we're inside a const block
        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, js_name = #name, skip_typescript)]
        pub struct #namespace_struct {}

        fn js_to_model(me: JsValue) -> Result<#name, JsValue> {
            let js_value = me;
            #(#preprocess_calls)*
            <#name as ::ankurah::core::model::tsify::Tsify>::from_js(js_value)
                .map_err(|e| JsValue::from_str(&e.to_string()))
        }

        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, js_class = #name)]
        impl #namespace_struct {
            pub async fn get (context: &::ankurah::core::context::Context, id: ::ankurah::EntityId) -> Result<#view_name, JsValue> {
                context.get(id).await.map_err(|e| JsValue::from(e.to_string()))
            }

            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, variadic)]
            pub async fn fetch (
                context: &::ankurah::core::context::Context,
                selection: String,
                substitution_values: &JsValue
            ) -> Result<Vec<#view_name>, JsValue> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(selection.as_str())?;

                // Convert the variadic JsValue (which is an array) and pass directly to populate
                let args_array: ::ankurah::derive_deps::js_sys::Array = substitution_values.clone().try_into()
                    .map_err(|_| JsValue::from_str("Invalid arguments array"))?;
                selection.predicate = selection.predicate.populate(args_array)?;

                let items = context
                    .fetch::<#view_name>(selection)
                    .await
                    .map_err(|e| JsValue::from(e.to_string()))?;
                Ok(items)
            }

            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, variadic)]
            pub fn query (context: &ankurah::core::context::Context, selection: String, substitution_values: &JsValue) -> Result<#livequery_name, JsValue> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(selection.as_str())?;

                // Convert the variadic JsValue (which is an array) and pass directly to populate
                let args_array: ::ankurah::derive_deps::js_sys::Array = substitution_values.clone().try_into()
                    .map_err(|_| JsValue::from_str("Invalid arguments array"))?;
                selection.predicate = selection.predicate.populate(args_array)?;

                let livequery = context.query::<#view_name>(selection)
                    .map_err(|e| JsValue::from(e.to_string()))?;
                Ok(#livequery_name(livequery))
            }

            /// Query that waits for remote subscription establishment before initializing, ensuring existing items
            /// appear as `Initial` in the first changeset. Items added after subscription is initialized
            /// will be reported as `Add`.
            /// TODO: May be replaced with a NOCACHE pragma in the selection string when MatchArgs parsing is implemented.
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, variadic)]
            pub fn query_nocache (context: &ankurah::core::context::Context, selection: String, substitution_values: &JsValue) -> Result<#livequery_name, JsValue> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(selection.as_str())?;

                // Convert the variadic JsValue (which is an array) and pass directly to populate
                let args_array: ::ankurah::derive_deps::js_sys::Array = substitution_values.clone().try_into()
                    .map_err(|_| JsValue::from_str("Invalid arguments array"))?;
                selection.predicate = selection.predicate.populate(args_array)?;

                let args = ::ankurah::MatchArgs { selection, cached: false };
                let livequery = context.query::<#view_name>(args)
                    .map_err(|e| JsValue::from(e.to_string()))?;
                Ok(#livequery_name(livequery))
            }

            pub async fn create(transaction: &::ankurah::transaction::Transaction, me: JsValue) -> Result<#view_name, JsValue> {
                use ankurah::Mutable;
                let model = js_to_model(me)?;
                let mutable_entity = transaction.create(&model).await?;
                Ok(mutable_entity.read())
            }

            pub async fn create_one(context: &::ankurah::core::context::Context, me: JsValue) -> Result<#view_name, JsValue> {
                use ankurah::Mutable;
                let tx = context.begin();
                let model = js_to_model(me)?;
                let mutable_entity = tx.create(&model).await?;
                let read = mutable_entity.read();
                tx.commit().await.map_err(|e| JsValue::from(e.to_string()))?;
                Ok(read)
            }
        }
    }
}

/// We have incorporated the code from the `tsify` crate into this crate, with the intention to modify it to better fit the needs of the Ankurah project.
/// The original license files are preserved in the `tsify` directory.
pub fn expand_ts_model_type(input: &DeriveInput, interface_name: String) -> syn::Result<proc_macro2::TokenStream> {
    let cont = crate::tsify::container::Container::from_derive_input(input)?;

    let parser = crate::tsify::parser::Parser::new(&cont);
    let mut decl = parser.parse();

    decl.set_id(interface_name);

    let tokens = crate::tsify::wasm_bindgen::expand(&cont, decl);
    cont.check()?;

    Ok(tokens)
}
