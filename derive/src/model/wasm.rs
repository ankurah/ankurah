use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident};

/// we need to generate wrappers for any generic types we want to expose to typescript, because wasm-bindgen doesn't support types with generics

pub fn wasm_impl(input: &syn::DeriveInput, model: &crate::model::description::ModelDescription) -> TokenStream {
    // Generate the namespace struct name (NSEntry for Entry)
    let namespace_struct = format_ident!("NS{}", model.name());

    // Generate the POJO interface name (IEntry for Entry)
    let pojo_interface = format_ident!("{}", model.name());

    // Generate the TypeScript interface using tsify internals
    let tsify_impl = expand_ts_model_type(input, pojo_interface.to_string()).unwrap_or_else(syn::Error::into_compile_error);

    // Generate WASM wrapper code
    let namespace_class =
        wasm_model_namespace(&namespace_struct, model.name(), &model.view_name(), &model.livequery_name(), &pojo_interface);
    let resultset_wrapper = wasm_resultset_wrapper(&model.resultset_name(), &model.view_name());
    let changeset_wrapper = wasm_changeset_wrapper(&model.changeset_name(), &model.view_name(), &model.resultset_name());
    let livequery_wrapper =
        wasm_livequery_wrapper(&model.livequery_name(), &model.view_name(), &model.resultset_name(), &model.changeset_name());

    quote! {
        #tsify_impl

        const _: () = {
            use ::ankurah::derive_deps::{tracing::error,wasm_bindgen::prelude::*, wasm_bindgen_futures};

            // Generate namespace struct with static methods
            #namespace_class

            // Generate ResultSet wrapper
            #resultset_wrapper

            // Generate ChangeSet wrapper
            #changeset_wrapper

            // Generate LiveQuery wrapper
            #livequery_wrapper
        };
    }
}

/// FooResultSet(ResultSet<FooView>)
pub fn wasm_resultset_wrapper(resultset_name: &Ident, view_name: &Ident) -> TokenStream {
    quote! {
        #[wasm_bindgen]
        #[derive(Clone, Default)]
        pub struct #resultset_name(::ankurah::core::resultset::ResultSet<#view_name>);

        #[wasm_bindgen]
        impl #resultset_name {
            #[wasm_bindgen(getter)]
            pub fn items(&self) -> Vec<#view_name> {
                use ::ankurah::signals::Get;
                self.0.get()
            }
            pub fn by_id(&self, id: ::ankurah::proto::EntityId) -> Option<#view_name> {
                ::ankurah::signals::CurrentObserver::track(&self);
                self.0.by_id(&id)
            }
            #[wasm_bindgen(getter)]
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
        #[wasm_bindgen]
        pub struct #changeset_name(::ankurah::core::changes::ChangeSet<#view_name>);

        #[wasm_bindgen]
        impl #changeset_name {
            #[wasm_bindgen(getter)]
            pub fn resultset(&self) -> #resultset_name {
                #resultset_name(self.0.resultset.wrap())
            }

            #[wasm_bindgen(getter)]
            pub fn adds(&self) -> Vec<#view_name> {
                self.0.adds()
            }

            #[wasm_bindgen(getter)]
            pub fn removes(&self) -> Vec<#view_name> {
                self.0.removes()
            }

            #[wasm_bindgen(getter)]
            pub fn updates(&self) -> Vec<#view_name> {
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
}}"#
    );

    quote! {
        #[wasm_bindgen(typescript_custom_section)]
        const TS_APPEND_CONTENT: &'static str = #subscribe_ts;

        #[wasm_bindgen]
        pub struct #livequery_name(::ankurah::LiveQuery<#view_name>);

        #[wasm_bindgen]
        impl #livequery_name {
            #[wasm_bindgen(getter)]
            pub fn items(&self) -> Vec<#view_name> {
                use ::ankurah::signals::Get;
                self.0.get()
            }
            #[wasm_bindgen(getter)]
            pub fn signal_id(&self) -> usize {
                use ::ankurah::signals::Signal;
                self.0.broadcast_id().into()
            }
            pub fn map(&self, callback: ::ankurah::derive_deps::js_sys::Function) -> ::ankurah::derive_deps::js_sys::Array {
                ::ankurah::core::model::js_resultset_map(&self.0.resultset(), &callback)
            }

            #[wasm_bindgen(getter)]
            pub fn peek(&self) -> Vec<#view_name> {
                use ::ankurah::signals::Peek;
                self.0.peek()
            }

            #[wasm_bindgen(getter)]
            pub fn loaded(&self) -> bool { self.0.loaded() }

            #[wasm_bindgen(getter)]
            pub fn resultset(&self) -> #resultset_name {
                #resultset_name(self.0.resultset())
            }
            /// DEPREDCATED - use resultset() instead
            #[wasm_bindgen(getter)]
            pub fn value(&self) -> #resultset_name {
                #resultset_name(self.0.resultset())
            }

            /// reading this will track the error signal, so updates will cause the (React)Observer to be notified
            #[wasm_bindgen(getter)]
            pub fn error(&self) -> Option<String> {
                use ::ankurah::signals::Get;
                // TODO maybe don't make a new map signal each time this is called?
                // doing this for now because we need the signal broadcast to be tracked by the current observer
                self.0.error().map(|e| e.as_ref().map(|e| e.to_string())).get()

            }

            #[wasm_bindgen(skip_typescript)]
            pub fn subscribe(&self, callback: ::ankurah::derive_deps::js_sys::Function, immediate: Option<bool>) -> ::ankurah::signals::SubscriptionGuard {
                ::ankurah::core::model::js_livequery_subscribe(
                    &self.0,
                    callback,
                    immediate.unwrap_or(true),
                    #changeset_name
                )
            }

            /// Update the predicate for this query and return a promise that resolves when complete
            pub async fn update_selection(&self, new_selection: &str) -> Result<(), ::wasm_bindgen::JsValue> {
                self.0.update_selection_wait(new_selection)
                    .await
                    .map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))
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
    name: &Ident,
    view_name: &Ident,
    livequery_name: &Ident,
    pojo_interface: &Ident,
) -> TokenStream {
    // Generate the custom TypeScript for the static methods
    let static_methods_ts = format!(
        r#"export class {name} {{
        /** Get a single {name} by ID  */
        static get(context: Context, id: EntityId): Promise<{view_name}>;
        /** Fetch all {name}s that match the predicate */
        static fetch(context: Context, selection: string, ...substitution_values: any): Promise<{view_name}[]>;
        /** Subscribe to the set of {name}s that match the predicate */
        static query(context: Context, selection: string, ...substitution_values: any): {livequery_name};
        /** Create a new {name} */
        static create(transaction: Transaction, me: {pojo_interface}): Promise<{view_name}>;
        /** Create a new {name} within an automatically created and committed transaction. */
        static create_one(context: Context, me: {pojo_interface}): Promise<{view_name}>;
}}"#
    );

    quote! {
        #[wasm_bindgen(typescript_custom_section)]
        const TS_APPEND_CONTENT: &'static str = #static_methods_ts;

        // These methods are only available via wasm bindgen, so it's ok that we're inside a const block
        #[wasm_bindgen(js_name = #name, skip_typescript)]
        pub struct #namespace_struct {}

        #[wasm_bindgen(js_class = #name)]
        impl #namespace_struct {
            pub async fn get (context: &::ankurah::core::context::Context, id: ::ankurah::EntityId) -> Result<#view_name, ::wasm_bindgen::JsValue> {
                context.get(id).await.map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))
            }

            #[wasm_bindgen(variadic)]
            pub async fn fetch (
                context: &::ankurah::core::context::Context,
                selection: String,
                substitution_values: &JsValue
            ) -> Result<Vec<#view_name>, ::wasm_bindgen::JsValue> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(selection.as_str())?;

                // Convert the variadic JsValue (which is an array) and pass directly to populate
                let args_array: ::ankurah::derive_deps::js_sys::Array = substitution_values.clone().try_into()
                    .map_err(|_| ::wasm_bindgen::JsValue::from_str("Invalid arguments array"))?;
                selection.predicate = selection.predicate.populate(args_array)?;

                let items = context
                    .fetch::<#view_name>(selection)
                    .await
                    .map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))?;
                Ok(items)
            }

            #[wasm_bindgen(variadic)]
            pub fn query (context: &ankurah::core::context::Context, selection: String, substitution_values: &JsValue) -> Result<#livequery_name, ::wasm_bindgen::JsValue> {
                let mut selection = ::ankurah::ankql::parser::parse_selection(selection.as_str())?;

                // Convert the variadic JsValue (which is an array) and pass directly to populate
                let args_array: ::ankurah::derive_deps::js_sys::Array = substitution_values.clone().try_into()
                    .map_err(|_| ::wasm_bindgen::JsValue::from_str("Invalid arguments array"))?;
                selection.predicate = selection.predicate.populate(args_array)?;

                let livequery = context.query::<#view_name>(selection)
                    .map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))?;
                Ok(#livequery_name(livequery))
            }

            pub async fn create(transaction: &::ankurah::transaction::Transaction, me: #name) -> Result<#view_name, ::wasm_bindgen::JsValue> {
                use ankurah::Mutable;
                let mutable_entity = transaction.create(&me).await?;
                Ok(mutable_entity.read())
            }

            pub async fn create_one(context: &::ankurah::core::context::Context, me: #name) -> Result<#view_name, ::wasm_bindgen::JsValue> {
                use ankurah::Mutable;
                let tx = context.begin();
                let mutable_entity = tx.create(&me).await?;
                let read = mutable_entity.read();
                tx.commit().await.map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))?;
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
