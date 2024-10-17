use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields};

// Consider changing this to an attribute macro so we can modify the input struct? For now, users will have to also derive Debug.
#[proc_macro_derive(Model, attributes(serde))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let record_name = format_ident!("{}Record", name);

    let fields = match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => &fields.named,
            _ => panic!("Only named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    let field_names = fields.iter().map(|f| &f.ident);
    let field_types = fields.iter().map(|f| &f.ty);

    let getter_methods = field_names
        .clone()
        .zip(field_types.clone())
        .map(|(name, ty)| {
            quote! {
                pub fn #name(&self) -> &#ty {
                    &self.current.#name
                }
            }
        });

    // These need to be type specific. Each type will have one or more setters with different signatures.
    // For example string will have insert(offset, value), delete(offset, count), etc.
    // let setter_methods = field_names.clone().map(|name| {
    //     quote! {
    //         pub fn #name(&mut self, value: impl Into<String>) {
    //             // TODO emit operation and update current
    //         }
    //     }
    // });

    let expanded = quote! {
        impl ankurah_core::model::Model for #name {}

        #[derive(Debug)]
        pub struct #record_name {
            id: ankurah_core::types::ID,
            current: #name,
            // TODO: Add fields for tracking changes and operation count
        }

        impl ankurah_core::model::Record for #record_name {
            type Model = #name;

            fn id(&self) -> ankurah_core::types::ID {
                self.id
            }

            fn current(&self) -> &Self::Model {
                &self.current
            }
        }

        impl #record_name {
            pub fn new(node: &Node, model: #name) -> Self {
                Self {
                    id: node.next_id(),
                    current: model,
                    // TODO: Initialize fields for tracking changes and operation count
                }
            }

            #(#getter_methods)*
            // #(#setter_methods)*
        }
    };

    TokenStream::from(expanded)
}
