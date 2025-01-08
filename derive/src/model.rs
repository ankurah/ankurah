use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields};

// Consider changing this to an attribute macro so we can modify the input struct? For now, users will have to also derive Debug.
pub fn derive_model_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let name_str = name.to_string().to_lowercase();
    let view_name = format_ident!("{}View", name);
    let mutable_name = format_ident!("{}Mut", name);

    let fields = match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => &fields.named,
            _ => panic!("Only named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    let active_value_ident = format_ident!("active_value");
    let field_active_values = fields
        .iter()
        .map(|f| {
            let active_value = f.attrs.iter().find(|attr| attr.path().get_ident() == Some(&active_value_ident));
            match active_value {
                Some(active_value) => active_value.parse_args::<syn::Ident>().unwrap(),
                // TODO: Better error, should include which field ident and an example on how to use.
                None => panic!("All fields need an active value attribute"),
            }
        })
        .collect::<Vec<_>>();

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
                    #field_active_values::initialize_with(&backends, #field_name_strs.into(), &self.#field_names);
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
                    #field_active_values::from_backends(#field_name_strs.into(), self.entity.backends()).projected()
                }
            )*
        }

        #[derive(Debug)]
        pub struct #mutable_name<'rec> {
            // TODO: Invert View and Mutable so that Mutable has an internal View, and View has id,backends, field projections
            entity: &'rec std::sync::Arc<ankurah_core::model::Entity>,

            // Field projections
            #(#field_visibility #field_names: #field_active_values,)*
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
                    let #field_names_avoid_conflicts = #field_active_values::from_backends(#field_name_strs.into(), entity.backends());
                )*
                Self {
                    entity,
                    #( #field_names: #field_names_avoid_conflicts, )*
                }
            }
        }

        impl<'rec> #mutable_name<'rec> {
            #(
                #field_visibility fn #field_names(&self) -> &#field_active_values {
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
