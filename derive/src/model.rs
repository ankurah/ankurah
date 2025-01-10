use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

// Consider changing this to an attribute macro so we can modify the input struct? For now, users will have to also derive Debug.
pub fn derive_model_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident.clone();
    let name_str = name.to_string().to_lowercase();
    let view_name = format_ident!("{}View", name);
    let mutable_name = format_ident!("{}Mut", name);

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            fields => return syn::Error::new_spanned(fields, "Only named fields are supported").to_compile_error().into(),
        },
        _ => return syn::Error::new_spanned(&name, "Only structs are supported").to_compile_error().into(),
    };

    let field_active_types = match fields.iter().map(get_active_type).collect::<Result<Vec<_>, _>>() {
        Ok(values) => values,
        Err(e) => return e.to_compile_error().into(),
    };

    let field_visibility = fields.iter().map(|f| &f.vis).collect::<Vec<_>>();

    let field_names = fields.iter().map(|f| &f.ident).collect::<Vec<_>>();
    let field_names_avoid_conflicts = fields
        .iter()
        .enumerate()
        .map(|(index, f)| match &f.ident {
            Some(ident) => format_ident!("field_{}", ident),
            None => format_ident!("field_{}", index),
        })
        .collect::<Vec<_>>();
    let field_name_strs = fields.iter().map(|f| f.ident.as_ref().unwrap().to_string().to_lowercase()).collect::<Vec<_>>();
    let field_types = fields.iter().map(|f| &f.ty).collect::<Vec<_>>();
    /*let field_indices = fields
    .iter()
    .enumerate()
    .map(|(index, _)| index)
    .collect::<Vec<_>>();*/

    let expanded: proc_macro::TokenStream = quote! {
        impl ankurah_core::model::Model for #name {
            type View = #view_name;
            type Mutable<'rec> = #mutable_name<'rec>;
            fn collection() -> &'static str {
                #name_str
            }
            fn create_entity(&self, id: ::ankurah_core::derive_deps::ankurah_proto::ID) -> ankurah_core::model::Entity {
                use ankurah_core::property::InitializeWith;

                let backends = ankurah_core::property::Backends::new();
                #(
                    #field_active_types::initialize_with(&backends, #field_name_strs.into(), &self.#field_names);
                )*
                ankurah_core::model::Entity::create(
                    id,
                    #name_str,
                    backends
                )
            }
        }

        use ankurah_core::derive_deps::wasm_bindgen::prelude::*;
        #[wasm_bindgen]
        #[derive(Debug, Clone)]
        pub struct #view_name {
            entity: std::sync::Arc<ankurah_core::model::Entity>,
        }

        impl ankurah_core::model::View for #view_name {
            type Model = #name;
            type Mutable<'rec> = #mutable_name<'rec>;

            fn to_model(&self) -> Self::Model {
                #name {
                    #( #field_names: self.#field_names(), )*
                }
            }

            fn entity(&self) -> &std::sync::Arc<ankurah_core::model::Entity> {
                &self.entity
            }

            fn from_entity(entity: std::sync::Arc<ankurah_core::model::Entity>) -> Self {
                use ankurah_core::model::View;
                assert_eq!(Self::collection(), entity.collection());
                #view_name {
                    entity,
                }
            }
        }

        // TODO wasm-bindgen this
        impl #view_name {
            pub async fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx ankurah_core::transaction::Transaction) -> Result<#mutable_name<'rec>, ankurah_core::error::RetrievalError> {
                use ankurah_core::model::View;
                trx.edit::<#name>(self.id()).await
            }
        }

        #[wasm_bindgen]
        impl #view_name {
            pub fn id(&self) -> ankurah_core::derive_deps::ankurah_proto::ID {
              self.entity.id.clone()
            }
            #(
                #field_visibility fn #field_names(&self) -> #field_types {
                    use ankurah_core::property::ProjectedValue;
                    #field_active_types::from_backends(#field_name_strs.into(), self.entity.backends()).projected()
                }
            )*
        }

        #[derive(Debug)]
        pub struct #mutable_name<'rec> {
            // TODO: Invert View and Mutable so that Mutable has an internal View, and View has id,backends, field projections
            entity: &'rec std::sync::Arc<ankurah_core::model::Entity>,

            // Field projections
            #(#field_visibility #field_names: #field_active_types,)*
            // TODO: Add fields for tracking changes and operation count
        }

        impl<'rec> ankurah_core::model::Mutable<'rec> for #mutable_name<'rec> {
            type Model = #name;
            type View = #view_name;

            fn entity(&self) -> &std::sync::Arc<ankurah_core::model::Entity> {
                &self.entity
            }

            fn new(entity: &'rec std::sync::Arc<ankurah_core::model::Entity>) -> Self {
                use ankurah_core::model::Mutable;
                assert_eq!(entity.collection(), Self::collection());
                #(
                    let #field_names_avoid_conflicts = #field_active_types::from_backends(#field_name_strs.into(), entity.backends());
                )*
                Self {
                    entity,
                    #( #field_names: #field_names_avoid_conflicts, )*
                }
            }
        }

        impl<'rec> #mutable_name<'rec> {
            #(
                #field_visibility fn #field_names(&self) -> &#field_active_types {
                    &self.#field_names
                }
            )*
        }

        impl<'a> Into<ankurah_core::derive_deps::ankurah_proto::ID> for &'a #view_name {
            fn into(self) -> ankurah_core::derive_deps::ankurah_proto::ID {
                ankurah_core::model::View::id(self)
            }
        }

        impl<'a, 'rec> Into<ankurah_core::derive_deps::ankurah_proto::ID> for &'a #mutable_name<'rec> {
            fn into(self) -> ankurah_core::derive_deps::ankurah_proto::ID {
                ankurah_core::model::Mutable::id(self)
            }
        }
    }
    .into();

    expanded
}

static ACTIVE_TYPE_MOD_PREFIX: &str = "::ankurah_core::property::value";
fn get_active_type(field: &syn::Field) -> Result<syn::Path, syn::Error> {
    let active_value_ident = format_ident!("active_value");

    // First check if there's an explicit attribute
    if let Some(active_value) = field.attrs.iter().find(|attr| attr.path().get_ident() == Some(&active_value_ident)) {
        let value_str = if let Ok(value) = active_value.parse_args::<syn::Ident>() {
            value.to_string()
        } else {
            let value = active_value
                .parse_args::<syn::Path>()
                .map_err(|_| syn::Error::new_spanned(active_value, "Expected an identifier or path for active_value"))?;
            quote!(#value).to_string()
        };

        if !value_str.contains("::") {
            let path = format!("{}::{}", ACTIVE_TYPE_MOD_PREFIX, value_str);
            return syn::parse_str(&path).map_err(|_| syn::Error::new_spanned(active_value, "Failed to parse active_value path"));
        }
        return syn::parse_str(&value_str).map_err(|_| syn::Error::new_spanned(active_value, "Failed to parse active_value path"));
    }

    // Check for exact type matches and provide default Active types
    let type_str = if let Type::Path(type_path) = &field.ty {
        let path_str = quote!(#type_path).to_string().replace(" ", "");
        match path_str.as_str() {
            "String" | "std::string::String" => {
                let path = format!("{}::YrsString", ACTIVE_TYPE_MOD_PREFIX);
                return syn::parse_str(&path).map_err(|_| syn::Error::new_spanned(&field.ty, "Failed to create YrsString path"));
            }
            // Add more default mappings here as needed
            _ => path_str,
        }
    } else {
        format!("{:?}", &field.ty)
    };

    // If we get here, we don't have a supported default Active type
    let field_name = field.ident.as_ref().map(|i| i.to_string()).unwrap_or_else(|| "unnamed".to_string());

    Err(syn::Error::new_spanned(
        &field.ty,
        format!(
            "No active value type found for field '{}' (type: {}). Please specify using #[active_value(Type)] attribute",
            field_name, type_str
        ),
    ))
}
