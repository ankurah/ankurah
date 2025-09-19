use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

pub fn derive_property_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident.clone();

    // Generate the Property trait implementation
    let property_impl = quote! {
        impl ::ankurah::Property for #name {
            fn into_value(&self) -> std::result::Result<Option<::ankurah::value::Value>, ::ankurah::property::PropertyError> {
                let json_str = match ::ankurah::derive_deps::serde_json::to_string(self) {
                    Ok(s) => s,
                    Err(err) => return Err(::ankurah::property::PropertyError::SerializeError(Box::new(err))),
                };

                Ok(Some(::ankurah::value::Value::String(json_str)))
            }

            fn from_value(value: Option<::ankurah::value::Value>) -> Result<Self, ::ankurah::property::PropertyError> {
                match value {
                    Some(::ankurah::value::Value::String(s)) => {
                        match ::ankurah::derive_deps::serde_json::from_str(&s) {
                            Ok(value) => Ok(value),
                            Err(err) => Err(::ankurah::property::PropertyError::DeserializeError(Box::new(err))),
                        }
                    },
                    Some(other) => Err(::ankurah::property::PropertyError::InvalidVariant {
                        given: other,
                        ty: stringify!(#name).to_owned()
                    }),
                    None => Err(::ankurah::property::PropertyError::Missing),
                }
            }
        }
    };

    // Generate WASM wrapper types for this custom type
    let wrapper_impl = match crate::wrapper_macros::impl_wrapper_type_impl(&syn::Type::Path(syn::TypePath {
        qself: None,
        path: syn::Path::from(name.clone()),
    })) {
        Ok(tokens) => tokens,
        Err(_) => quote! {}, // If wrapper generation fails, just skip it
    };

    let expanded: proc_macro::TokenStream = quote! {
        #property_impl
        #wrapper_impl
    }
    .into();

    expanded
}
