use proc_macro::TokenStream;
use quote::{format_ident, quote};
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
        _ => quote!(ankurah_core::property::value::Value<#ty>), // Default to a generic Value type
    };
    quote!(ankurah_core::property::value::#value_type)
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
    let field_indices = fields
        .iter()
        .enumerate()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();

    // Update this to use the get_value_type function
    let field_value_types = fields
        .iter()
        .map(|f| get_value_type(&f.ty))
        .collect::<Vec<_>>();

    let expanded: proc_macro::TokenStream = quote! {
        impl ankurah_core::model::Model for #name {}

        #[derive(Debug)]
        pub struct #record_name {
            id: ankurah_core::model::ID,
            inner: std::sync::Arc<ankurah_core::model::RecordInner>,

            yrs: std::sync::Arc<ankurah_core::property::backend::YrsBackend>,

            // Field projections
            #(#field_names: #field_value_types,)*
            // TODO: Add fields for tracking changes and operation count
        }

        impl ankurah_core::model::Record for #record_name {
            fn as_dyn_any(&self) -> &dyn std::any::Any {
                self as &dyn std::any::Any
            }

            fn id(&self) -> ankurah_core::model::ID {
                self.id
            }

            // I think we might not need inner.collection or ID. Might need new to return Arc<Self> though. Not sure.
            fn bucket_name(&self) -> &'static str {
                #name_str
            }

            fn record_state(&self) -> ankurah_core::storage::RecordState {
                ankurah_core::storage::RecordState {
                    yrs_state_buffer: self.yrs.to_state_buffer(),
                }
            }

            fn from_record_state(
                id: ankurah_core::model::ID,
                record_state: &ankurah_core::storage::RecordState,
            ) -> std::result::Result<Self, ankurah_core::error::RetrievalError>
            where
                Self: Sized,
            {
                let inner = std::sync::Arc::new(ankurah_core::model::RecordInner {
                    collection: #name_str,
                    id: id,
                });

                println!("yrs decode");
                let yrs_backend = std::sync::Arc::new(ankurah_core::property::backend::YrsBackend::from_state_buffer(inner.clone(), &record_state.yrs_state_buffer)?);
                println!("yrs worked");
                Ok(Self {
                    id: id,
                    inner: inner,
                    yrs: yrs_backend.clone(),
                    // TODO: Support other backends than Yrs, right now its just hardcoded YrsBackend.
                    #(
                        #field_names: #field_value_types::new(#field_name_strs, yrs_backend.clone()),
                    )*
                })
            }

            fn commit_record(&self, node: std::sync::Arc<ankurah_core::Node>) -> Result<()> {
                // TODO, throw this shit in the storage bucket it belongs to.
                Ok(())
            }
        }

        impl #record_name {
            pub fn new(node: &std::sync::Arc<ankurah_core::Node>, model: #name) -> Self {
                use ankurah_core::property::traits::InitializeWith;
                let id = node.next_id();

                let inner = std::sync::Arc::new(ankurah_core::model::RecordInner {
                    collection: #name_str,
                    id: id,
                });

                let yrs = std::sync::Arc::new(ankurah_core::property::backend::YrsBackend::new(inner.clone()));
                let backends = ankurah_core::property::backend::Backends {
                    yrs: yrs.clone(),
                };
                Self {
                    id: id,
                    inner: inner,
                    yrs: yrs,
                    #(#field_names: <#field_value_types>::initialize_with(&backends, #field_name_strs, model.#field_names),)*
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

    expanded
}
