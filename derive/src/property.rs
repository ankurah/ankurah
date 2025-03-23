use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

pub fn derive_property_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident.clone();

    let expanded: proc_macro::TokenStream = quote! {
        impl ::ankurah::Property for #name {
            fn into_value(&self) -> std::result::Result<Option<::ankurah::property::PropertyValue>, ::ankurah::property::PropertyError> {
                let json_str = match ::ankurah::derive_deps::serde_json::to_string(self) {
                    Ok(s) => s,
                    Err(err) => return Err(::ankurah::property::PropertyError::SerializeError(Box::new(err))),
                };

                Ok(Some(::ankurah::property::PropertyValue::String(json_str)))
            }

            fn from_value(value: Option<::ankurah::property::PropertyValue>) -> Result<Self, ::ankurah::property::PropertyError> {
                match value {
                    Some(::ankurah::property::PropertyValue::String(s)) => {
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
    }
    .into();

    expanded
}
