use quote::{format_ident, quote};

pub fn wasm_impl(
    name: &syn::Ident,
    view_name: &syn::Ident,
    resultset_name: &syn::Ident,
    resultset_signal_name: &syn::Ident,
) -> proc_macro2::TokenStream {
    eprintln!("WASM IMPL");
    let namespace_struct = format_ident!("NS{}", name);
    let pojo_interface = format_ident!("{}", name);

    // We have copied the internals of the `tsify` crate into the `tsify` directory.
    // The purpose of which is to generate the Wasm ABI for the model struct so the Pojo version of the struct can be used to call methods which accept the Model struct
    // and the pojo will be automatically converted to the Model struct.
    // This has been modified to use a different name for the typescript interface,
    // so as not to conflict with the namespace that matches the model struct.
    // Over time, this code will diverge from the original code sufficiently that
    // the distinction between tsify and the rest of the ankurah code will be indistinguishable.
    // We thank the authors of the `tsify` crate for their work. Please see the license files in the `tsify` directory for more information
    // let tsify_impl = expand_ts_model_type(&input, pojo_interface.to_string()).unwrap_or_else(syn::Error::into_compile_error);

    // We have to generate the typescript interface for the static methods because the name of the pojo interface is different from the name of the model class
    // Maybe there's a more elegant way to do this later, but this gets the job done.
    let static_methods_ts = get_static_methods_ts(&name, &view_name, &resultset_signal_name, &pojo_interface);

    quote! {

        // #tsify_impl

        const _: () = {
            use ::ankurah::derive_deps::{tracing::error,wasm_bindgen::prelude::*, wasm_bindgen_futures};

            // These methods are only available via wasm bindgen, so it's ok that we're inside a const block
            #[wasm_bindgen(js_name = #name, skip_typescript)]
            pub struct #namespace_struct {}

            #[wasm_bindgen(js_class = #name)]
            impl #namespace_struct {
                pub async fn get (context: &::ankurah::core::context::Context, id: ::ankurah::derive_deps::ankurah_proto::EntityId) -> Result<#view_name, ::wasm_bindgen::JsValue> {
                    context.get(id).await.map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))
                }

                pub async fn fetch (context: &::ankurah::core::context::Context, predicate: &str) -> Result<#resultset_name, ::wasm_bindgen::JsValue> {
                    let resultset = context.fetch(predicate).await.map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))?;
                    Ok(#resultset_name(::std::sync::Arc::new(resultset)))
                }

                pub fn subscribe (context: &ankurah::core::context::Context, predicate: String) -> Result<#resultset_signal_name, ::wasm_bindgen::JsValue> {
                    let handle = ::std::sync::Arc::new(::std::sync::OnceLock::new());
                    let (signal, rwsignal) = ::ankurah::derive_deps::reactive_graph::signal::RwSignal::new(#resultset_name::default()).split();

                    let context2 = (*context).clone();
                    let handle2 = handle.clone();
                    let future = Box::pin(async move {
                        use ::ankurah::derive_deps::reactive_graph::traits::Set;
                        let handle = context2
                            .subscribe(predicate.as_str(), move |changeset: ::ankurah::core::changes::ChangeSet<#view_name>| {
                                rwsignal.set(#resultset_name(::std::sync::Arc::new(changeset.resultset)));
                            })
                            .await;
                        match handle {
                            Ok(h) => {
                                handle2.set(h).unwrap();
                            }
                            Err(e) => {
                                error!("Failed to subscribe to changes: {} for predicate: {}", e, predicate);
                            }
                        }
                    });
                    wasm_bindgen_futures::spawn_local(future);

                    Ok(#resultset_signal_name{
                        sig: Box::new(signal),
                        handle: Box::new(handle)
                    })
                }

                // pub async fn create(transaction: &::ankurah::transaction::Transaction, model: JsValue) -> Result<#view_name, ::wasm_bindgen::JsValue> {
                //     let model = model.into_serde::<#name>().map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))?;
                //     use ankurah::Mutable;
                //     let mutable_entity = transaction.create(&model).await?;
                //     Ok(mutable_entity.read())
                // }

                // pub async fn create_one(context: &::ankurah::core::context::Context, model: JsValue) -> Result<#view_name, ::wasm_bindgen::JsValue> {
                //     use ankurah::Mutable;
                //     let model = model.into_serde::<#name>().map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))?;
                //     let tx = context.begin();
                //     let mutable_entity = tx.create(&model).await?;
                //     let read = mutable_entity.read();
                //     tx.commit().await.map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))?;
                //     Ok(read)
                // }
            }

            #[wasm_bindgen(typescript_custom_section)]
            const TS_APPEND_CONTENT: &'static str = #static_methods_ts;

            #[wasm_bindgen]
            #[derive(ankurah::WasmSignal, Clone, Default)]
            pub struct #resultset_name(::std::sync::Arc<::ankurah::core::resultset::ResultSet<#view_name>>);

            #[wasm_bindgen]
            impl #resultset_name {
                #[wasm_bindgen(getter)]
                pub fn items(&self) -> Vec<#view_name> {
                    self.0.items.to_vec()
                }
                pub fn by_id(&self, id: ::ankurah::derive_deps::ankurah_proto::EntityId) -> Option<#view_name> {
                    self.0.items.iter().find(|item| item.id() == id).map(|item| item.clone())
                    // todo generate a map on demand if there are more than a certain number of items (benchmark this)
                }
                #[wasm_bindgen(getter)]
                pub fn loaded(&self) -> bool {
                    self.0.loaded
                }
            }
        };
    }
}

fn get_static_methods_ts(
    name: &syn::Ident,
    view_name: &syn::Ident,
    resultset_signal_name: &syn::Ident,
    pojo_interface: &syn::Ident,
) -> String {
    format!(
        r#"export class {name} {{
        /**
         * Get a single {name} by ID
         */
        static get(context: Context, id: ID): Promise<{view_name}>;

        /**
         * Fetch all {name}s that match the predicate
         */
        static fetch(context: Context, predicate: string): Promise<{view_name}[]>;

        /**
         * Subscribe to the set of {name}s that match the predicate
         */
        static subscribe(context: Context, predicate: string): {resultset_signal_name};

        /**
         * Create a new {name}
         */
        static create(transaction: Transaction, me: {pojo_interface}): Promise<{view_name}>;

        /**
         * Create a new {name} within an automatically created and committed transaction.
         */
        static create_one(context: Context, me: {pojo_interface}): Promise<{view_name}>;
}}"#
    )
}
