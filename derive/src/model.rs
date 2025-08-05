use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, punctuated::Punctuated, AngleBracketedGenericArguments, Data, DeriveInput, Fields, Type};

// Consider changing this to an attribute macro so we can modify the input struct? For now, users will have to also derive Debug.
pub fn derive_model_impl(stream: TokenStream) -> TokenStream {
    let input = parse_macro_input!(stream as DeriveInput);
    let name = input.ident.clone();
    let collection_str = name.to_string().to_lowercase();
    let view_name = format_ident!("{}View", name);
    let mutable_name = format_ident!("{}Mut", name);
    #[cfg(feature = "wasm")]
    let resultset_name = format_ident!("{}ResultSet", name);
    #[cfg(feature = "wasm")]
    let resultset_signal_name = format_ident!("{}ResultSetSignal", name);
    let clone_derive = if !get_model_flag(&input.attrs, "no_clone") {
        quote! { #[derive(Clone)] }
    } else {
        quote! {}
    };

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => fields.named.clone(),
            fields => {
                return syn::Error::new_spanned(fields, format!("Only named fields are supported this is a {:#?}", fields))
                    .to_compile_error()
                    .into()
            }
        },
        _ => return syn::Error::new_spanned(&name, "Only structs are supported").to_compile_error().into(),
    };

    // Split fields into active and ephemeral
    let mut active_fields = Vec::new();
    let mut ephemeral_fields = Vec::new();
    for field in fields.into_iter() {
        if get_model_flag(&field.attrs, "ephemeral") {
            ephemeral_fields.push(field);
        } else {
            active_fields.push(field);
        }
    }

    let active_field_visibility = active_fields.iter().map(|f| &f.vis).collect::<Vec<_>>();
    let active_field_names = active_fields.iter().map(|f| &f.ident).collect::<Vec<_>>();
    let active_field_name_strs = active_fields.iter().map(|f| f.ident.as_ref().unwrap().to_string().to_lowercase()).collect::<Vec<_>>();
    let projected_field_types = active_fields.iter().map(|f| f.ty.clone()).collect::<Vec<_>>();
    let active_field_types = match active_fields.iter().map(get_active_type).collect::<Result<Vec<_>, _>>() {
        Ok(values) => values,
        Err(e) => return e.to_compile_error().into(),
    };

    let projected_field_types_turbofish = projected_field_types.iter().map(as_turbofish).collect::<Vec<_>>();
    let active_field_types_turbofish = active_field_types.iter().map(as_turbofish).collect::<Vec<_>>();

    let ephemeral_field_names = ephemeral_fields.iter().map(|f| &f.ident).collect::<Vec<_>>();
    let ephemeral_field_types = ephemeral_fields.iter().map(|f| &f.ty).collect::<Vec<_>>();
    let ephemeral_field_visibility = ephemeral_fields.iter().map(|f| &f.vis).collect::<Vec<_>>();

    let wasm_attributes = if cfg!(feature = "wasm") {
        quote! {
            #[::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen]
        }
    } else {
        quote! {}
    };

    #[cfg(feature = "wasm")]
    let wasm_impl = {
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
        let tsify_impl = expand_ts_model_type(&input, pojo_interface.to_string()).unwrap_or_else(syn::Error::into_compile_error);

        // We have to generate the typescript interface for the static methods because the name of the pojo interface is different from the name of the model class
        // Maybe there's a more elegant way to do this later, but this gets the job done.
        let static_methods_ts = get_static_methods_ts(&name, &view_name, &resultset_signal_name, &pojo_interface);

        quote! {

            #tsify_impl

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
                        let signal = ::ankurah::derive_deps::ankurah_signals::Mut::new(#resultset_name::default());
                        let signal_clone = signal.clone();

                        let context2 = (*context).clone();
                        let handle2 = handle.clone();
                        let future = Box::pin(async move {
                            // Direct method call on Mut - no trait needed
                            let handle = context2
                                .subscribe(predicate.as_str(), move |changeset: ::ankurah::core::changes::ChangeSet<#view_name>| {
                                    signal_clone.set(#resultset_name(::std::sync::Arc::new(changeset.resultset)));
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
                            sig: Box::new(signal.read()),
                            handle: Box::new(handle)
                        })
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
    };

    #[cfg(not(feature = "wasm"))]
    let wasm_impl = quote! {};

    let expanded: proc_macro::TokenStream = quote! {

        #wasm_attributes
        #clone_derive
        pub struct #view_name {
            entity: ::ankurah::entity::Entity,
            #(
                #ephemeral_field_visibility #ephemeral_field_names: #ephemeral_field_types,
            )*
        }

        #[derive(Debug)]
        pub struct #mutable_name<'rec> {
            pub entity: &'rec ::ankurah::entity::Entity,
            #(#active_field_visibility #active_field_names: #active_field_types,)*
        }

        #wasm_impl
        impl ::ankurah::model::Model for #name {
            type View = #view_name;
            type Mutable<'rec> = #mutable_name<'rec>;
            fn collection() -> ankurah::derive_deps::ankurah_proto::CollectionId {
                #collection_str.into()
            }
            fn initialize_new_entity(&self, entity: &::ankurah::entity::Entity) {
                use ::ankurah::property::InitializeWith;
                #(
                    #active_field_types_turbofish::initialize_with(&entity, #active_field_name_strs.into(), &self.#active_field_names);
                )*
            }
        }

        impl ::ankurah::model::View for #view_name {
            type Model = #name;
            type Mutable<'rec> = #mutable_name<'rec>;

            // THINK ABOUT: to_model is the only thing that forces a clone requirement
            // Even though most Models will be clonable, maybe we shouldn't force it?
            // Also: nothing seems to be using this. Maybe it could be opt in
            fn to_model(&self) -> Result<Self::Model, ankurah::property::PropertyError> {
                Ok(#name {
                    #( #active_field_names: self.#active_field_names()?, )*
                    #( #ephemeral_field_names: self.#ephemeral_field_names.clone(), )*
                })
            }

            fn entity(&self) -> &::ankurah::entity::Entity {
                &self.entity
            }

            fn from_entity(entity: ::ankurah::entity::Entity) -> Self {
                use ::ankurah::model::View;
                assert_eq!(&Self::collection(), entity.collection());
                #view_name {
                    entity,
                    #(
                        #ephemeral_field_names: Default::default(),
                    )*
                }
            }
        }

        // TODO wasm-bindgen this
        impl #view_name {
            pub fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx ankurah::transaction::Transaction) -> Result<#mutable_name<'rec>, ankurah::policy::AccessDenied> {
                use ::ankurah::model::View;
                // TODO - get rid of this in favor of directly cloning the entity of the ModelView struct
                trx.edit::<#name>(&self.entity)
            }
        }

        #wasm_attributes
        impl #view_name {
            pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::EntityId {
                self.entity.id().clone()
            }
            #(
                #active_field_visibility fn #active_field_names(&self) -> Result<#projected_field_types, ankurah::property::PropertyError> {
                    use ankurah::property::{FromActiveType, FromEntity};
                    let active_result = #active_field_types_turbofish::from_entity(#active_field_name_strs.into(), &self.entity);
                    #projected_field_types_turbofish::from_active(active_result)
                }
            )*
            // #(
            //     #ephemeral_field_visibility fn #ephemeral_field_names(&self) -> &#ephemeral_field_types {
            //         &self.#ephemeral_field_names
            //     }
            // )*
        }

        // TODO - wasm-bindgen this - ah right, we need to remove the lifetime
        impl<'rec> ::ankurah::model::Mutable<'rec> for #mutable_name<'rec> {
            type Model = #name;
            type View = #view_name;

            fn entity(&self) -> &::ankurah::entity::Entity {
                &self.entity
            }

            fn new(entity: &'rec ::ankurah::entity::Entity) -> Self {
                use ankurah::{
                    model::Mutable,
                    property::FromEntity,
                };
                assert_eq!(entity.collection(), &Self::collection());
                Self {
                    entity,
                    #( #active_field_names: #active_field_types_turbofish::from_entity(#active_field_name_strs.into(), entity), )*
                }
            }
        }
        impl<'rec> #mutable_name<'rec> {
            pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::EntityId {
                self.entity.id().clone()
            }
            #(
                #active_field_visibility fn #active_field_names(&self) -> &#active_field_types {
                    &self.#active_field_names
                }
            )*
        }

        impl<'a> Into<ankurah::derive_deps::ankurah_proto::EntityId> for &'a #view_name {
            fn into(self) -> ankurah::derive_deps::ankurah_proto::EntityId {
                ankurah::View::id(self)
            }
        }

        impl<'a, 'rec> Into<ankurah::derive_deps::ankurah_proto::EntityId> for &'a #mutable_name<'rec> {
            fn into(self) -> ankurah::derive_deps::ankurah_proto::EntityId {
                ::ankurah::model::Mutable::id(self)
            }
        }
    }
    .into();

    expanded
}

static ACTIVE_TYPE_MOD_PREFIX: &str = "::ankurah::property::value";
fn get_active_type(field: &syn::Field) -> Result<syn::Type, syn::Error> {
    let active_type_ident = format_ident!("active_type");

    let mut active_type = None;
    // First check if there's an explicit attribute
    if let Some(active_type_attr) = field.attrs.iter().find(|attr| attr.path().get_ident() == Some(&active_type_ident)) {
        active_type = Some(active_type_attr.parse_args::<syn::Type>()?);
    } else {
        // Check for exact type matches and provide default Active types
        if let Type::Path(type_path) = &field.ty {
            let path_str = quote!(#type_path).to_string().replace(" ", "");
            match path_str.as_str() {
                // Add more default mappings here as needed
                "String" | "std::string::String" => {
                    let path = format!("{}::YrsString", ACTIVE_TYPE_MOD_PREFIX);
                    let yrs = syn::parse_str(&path).map_err(|_| syn::Error::new_spanned(&field.ty, "Failed to create YrsString path"))?;
                    active_type = Some(yrs);
                }
                _ => {
                    // Everything else should use `LWW`` by default.
                    // TODO: Return a list of compile_error! for these types to specify
                    // that these need to be `Serialize + for<'de> Deserialize<'de>``
                    let path = format!("{}::LWW", ACTIVE_TYPE_MOD_PREFIX);
                    let lww = syn::parse_str(&path).map_err(|_| syn::Error::new_spanned(&field.ty, "Failed to create YrsString path"))?;
                    active_type = Some(lww);
                }
            }
        };
    }

    if let Some(active_type) = active_type {
        Ok(ActiveFieldType::convert_type_with_projected(&active_type, &field.ty))
    } else {
        // If we get here, we don't have a supported default Active type
        let field_name = field.ident.as_ref().map(|i| i.to_string()).unwrap_or_else(|| "unnamed".to_string());
        let type_str = format!("{:?}", &field.ty);

        Err(syn::Error::new_spanned(
            &field.ty,
            format!(
                "No active value type found for field '{}' (type: {}). Please specify using #[active_type(Type)] attribute",
                field_name, type_str
            ),
        ))
    }
}

fn get_model_flag(attrs: &Vec<syn::Attribute>, flag_name: &str) -> bool {
    attrs.iter().any(|attr| {
        attr.path().segments.iter().any(|seg| seg.ident == "model")
            && attr.meta.require_list().ok().and_then(|list| list.parse_args::<syn::Ident>().ok()).is_some_and(|ident| ident == flag_name)
    })
}

// Parse the active field type
struct ActiveFieldType {
    pub base: syn::Type,
    pub generics: Option<syn::AngleBracketedGenericArguments>,
}

impl ActiveFieldType {
    pub fn convert_type_with_projected(ty: &syn::Type, projected: &syn::Type) -> syn::Type {
        let mut base = Self::from_type(ty);
        base.with_projected(projected);
        base.as_type()
    }

    pub fn from_type(ty: &syn::Type) -> Self {
        if let syn::Type::Path(path) = ty {
            if let Some(last_segment) = path.path.segments.last() {
                if let syn::PathArguments::AngleBracketed(generics) = &last_segment.arguments {
                    return Self { base: ty.clone(), generics: Some(generics.clone()) };
                }
            }
        }

        Self { base: ty.clone(), generics: None }
    }

    pub fn with_projected(&mut self, ty: &syn::Type) {
        let mut generics = match &self.generics {
            Some(generics) => generics.clone(),
            None => AngleBracketedGenericArguments {
                colon2_token: None,
                lt_token: Default::default(),
                args: Punctuated::default(),
                gt_token: Default::default(),
            },
        };

        // Replace inferred `_` with projected
        if let Some(last @ syn::GenericArgument::Type(syn::Type::Infer(_))) = generics.args.last_mut() {
            *last = syn::GenericArgument::Type(ty.clone());
        } else {
            // Otherwise just push to the end.
            generics.args.push(syn::GenericArgument::Type(ty.clone()));
        }

        self.generics = Some(generics);
    }

    pub fn as_type(&self) -> syn::Type {
        let mut new_type = self.base.clone();
        let syn::Type::Path(ref mut path) = new_type else {
            unimplemented!("Non-path types aren't supported for active types");
        };

        let Some(last_segment) = path.path.segments.last_mut() else {
            unreachable!("Need at least a single segment for type paths...?");
        };

        if let Some(generics) = &self.generics {
            last_segment.arguments = syn::PathArguments::AngleBracketed(generics.clone());
        }

        new_type
    }
}

fn as_turbofish(type_path: &syn::Type) -> proc_macro2::TokenStream {
    if let syn::Type::Path(path) = type_path {
        let mut without_generics = path.clone();
        let mut generics = syn::PathArguments::None;
        if let Some(last_segment) = without_generics.path.segments.last_mut() {
            generics = last_segment.arguments.clone();
            last_segment.arguments = syn::PathArguments::None;
        }

        if let syn::PathArguments::AngleBracketed(generics) = generics {
            quote! {
                #without_generics::#generics
            }
        } else {
            quote! {
                #without_generics
            }
        }
    } else {
        unimplemented!()
    }
}

/// We have incorporated the code from the `tsify` crate into this crate, with the intention to modify it to better fit the needs of the Ankurah project.
/// The original license files are preserved in the `tsify` directory.
#[cfg(feature = "wasm")]
pub fn expand_ts_model_type(input: &DeriveInput, interface_name: String) -> syn::Result<proc_macro2::TokenStream> {
    let cont = crate::tsify::container::Container::from_derive_input(input)?;

    let parser = crate::tsify::parser::Parser::new(&cont);
    let mut decl = parser.parse();

    decl.set_id(interface_name);

    let tokens = crate::tsify::wasm_bindgen::expand(&cont, decl);
    cont.check()?;

    Ok(tokens)
}

#[cfg(feature = "wasm")]
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
