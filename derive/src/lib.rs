use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

/// Map model field types to their corresponding value types
fn get_value_type(ty: &Type) -> proc_macro2::TokenStream {
    let type_str = quote!(#ty).to_string();
    let value_type = match type_str.as_str() {
        "String" => quote!(StringValue),
        "i32" => quote!(IntValue),
        "f64" => quote!(FloatValue),
        "bool" => quote!(BoolValue),
        // Add more mappings as needed
        _ => quote!(ankurah_core::types::value::Value<#ty>), // Default to a generic Value type
    };
    quote!(ankurah_core::types::value::#value_type).into()
}

// Consider changing this to an attribute macro so we can modify the input struct? For now, users will have to also derive Debug.
#[proc_macro_derive(Model, attributes(serde))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let name_str = name.to_string().to_lowercase();
    let record_name = format_ident!("{}Record", name);

    let fields = match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => &fields.named,
            _ => panic!("Only named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    let field_names = fields.iter().map(|f| &f.ident).collect::<Vec<_>>();
    let field_name_strs = fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap().to_string().to_lowercase())
        .collect::<Vec<_>>();
    let field_types = fields.iter().map(|f| &f.ty).collect::<Vec<_>>();

    // Update this to use the get_value_type function
    let field_value_types = fields
        .iter()
        .map(|f| get_value_type(&f.ty))
        .collect::<Vec<_>>();

    let expanded: proc_macro::TokenStream = quote! {
        impl ankurah_core::model::Model for #name {}

        #[derive(Debug)]
        pub struct #record_name {
            id: ankurah_core::types::ID,
            inner: std::sync::Arc<ankurah_core::model::RecordInner>,
            #(#field_names: #field_value_types,)*
            // TODO: Add fields for tracking changes and operation count
        }

        impl ankurah_core::model::Record for #record_name {
            type Model = #name;

            fn id(&self) -> ankurah_core::types::ID {
                self.id
            }

        }

        impl #record_name {
            pub fn new(node: &Node, model: #name) -> Self {
                use ankurah_core::types::traits::InitializeWith;
                let id = node.next_id();
                let inner = std::sync::Arc::new(ankurah_core::model::RecordInner {
                    collection: #name_str,
                    id,
                    transaction_manager: node.transaction_manager.clone(),
                });
                Self {
                    id,
                    #(#field_names: <#field_value_types>::initialize_with(inner.clone(), #field_name_strs, model.#field_names),)*
                    inner,
                }
            }
            #(
                pub fn #field_names(&self) -> &#field_value_types {
                    &self.#field_names
                }
            )*
        }
    }
    .into();

    TokenStream::from(expanded)
}
