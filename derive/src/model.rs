use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, punctuated::Punctuated, AngleBracketedGenericArguments, Data, DeriveInput, Fields, Type};

// Consider changing this to an attribute macro so we can modify the input struct? For now, users will have to also derive Debug.
pub fn derive_model_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident.clone();
    let name_str = name.to_string().to_lowercase();
    let view_name = format_ident!("{}View", name);
    let mutable_name = format_ident!("{}Mut", name);

    let clone_derive = if !get_model_flag(&input.attrs, "no_clone") {
        quote! { #[derive(Clone)] }
    } else {
        quote! {}
    };

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            fields => return syn::Error::new_spanned(fields, "Only named fields are supported").to_compile_error().into(),
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
            use ankurah::derive_deps::wasm_bindgen::prelude::*;
            #[wasm_bindgen]
        }
    } else {
        quote! {}
    };

    let expanded: proc_macro::TokenStream = quote! {
        impl ::ankurah::model::Model for #name {
            type View = #view_name;
            type Mutable<'rec> = #mutable_name<'rec>;
            fn collection() -> ankurah::derive_deps::ankurah_proto::CollectionId {
                #name_str.into()
            }
            fn create_entity(&self, id: ::ankurah::derive_deps::ankurah_proto::ID) -> ::ankurah::model::Entity {
                use ankurah::property::InitializeWith;

                let backends = ankurah::property::Backends::new();
                let entity = ankurah::model::Entity::create(
                    id,
                    Self::collection(),
                    backends
                );
                #(
                    #active_field_types_turbofish::initialize_with(&entity, #active_field_name_strs.into(), &self.#active_field_names);
                )*
                entity
            }
        }

        #wasm_attributes
        #clone_derive
        pub struct #view_name {
            entity: std::sync::Arc<::ankurah::model::Entity>,
            #(
                #ephemeral_field_visibility #ephemeral_field_names: #ephemeral_field_types,
            )*
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

            fn entity(&self) -> &std::sync::Arc<::ankurah::model::Entity> {
                &self.entity
            }

            fn from_entity(entity: std::sync::Arc<::ankurah::model::Entity>) -> Self {
                use ::ankurah::model::View;
                assert_eq!(Self::collection(), entity.collection);
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
            pub async fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx ankurah::transaction::Transaction) -> Result<#mutable_name<'rec>, ankurah::error::RetrievalError> {
                use ::ankurah::model::View;
                // TODO - get rid of this in favor of directly cloning the entity of the ModelView struct
                trx.edit::<#name>(self.id()).await
            }
        }

        #wasm_attributes
        impl #view_name {
            pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::ID {
                self.entity.id.clone()
            }
            #(
                #active_field_visibility fn #active_field_names(&self) -> Result<#projected_field_types, ankurah::property::PropertyError> {
                    use ankurah::property::{FromActiveType, FromEntity};
                    let active_result = #active_field_types_turbofish::from_entity(#active_field_name_strs.into(), self.entity.as_ref());
                    #projected_field_types_turbofish::from_active(active_result)
                }
            )*
            // #(
            //     #ephemeral_field_visibility fn #ephemeral_field_names(&self) -> &#ephemeral_field_types {
            //         &self.#ephemeral_field_names
            //     }
            // )*
        }

        #[derive(Debug)]
        pub struct #mutable_name<'rec> {
            entity: &'rec std::sync::Arc<::ankurah::model::Entity>,
            #(#active_field_visibility #active_field_names: #active_field_types,)*
        }

        impl<'rec> ::ankurah::model::Mutable<'rec> for #mutable_name<'rec> {
            type Model = #name;
            type View = #view_name;

            fn entity(&self) -> &std::sync::Arc<::ankurah::model::Entity> {
                &self.entity
            }

            fn new(entity: &'rec std::sync::Arc<::ankurah::model::Entity>) -> Self {
                use ankurah::{
                    model::Mutable,
                    property::FromEntity,
                };
                assert_eq!(entity.collection(), Self::collection());
                Self {
                    entity,
                    #( #active_field_names: #active_field_types_turbofish::from_entity(#active_field_name_strs.into(), entity), )*
                }
            }
        }

        impl<'rec> #mutable_name<'rec> {
            pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::ID {
                self.entity.id.clone()
            }
            #(
                #active_field_visibility fn #active_field_names(&self) -> &#active_field_types {
                    &self.#active_field_names
                }
            )*
        }

        impl<'a> Into<ankurah::derive_deps::ankurah_proto::ID> for &'a #view_name {
            fn into(self) -> ankurah::derive_deps::ankurah_proto::ID {
                ankurah::View::id(self)
            }
        }

        impl<'a, 'rec> Into<ankurah::derive_deps::ankurah_proto::ID> for &'a #mutable_name<'rec> {
            fn into(self) -> ankurah::derive_deps::ankurah_proto::ID {
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
        return Ok(ActiveFieldType::convert_type_with_projected(&active_type, &field.ty));
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
            && attr.meta.require_list().ok().and_then(|list| list.parse_args::<syn::Ident>().ok()).map_or(false, |ident| ident == flag_name)
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

        return Self { base: ty.clone(), generics: None };
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
