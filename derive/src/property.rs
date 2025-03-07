use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput};

pub fn derive_property_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident.clone();

    let mut variants = Vec::new();
    match input.data.clone() {
        Data::Struct(_data) => return syn::Error::new_spanned(&input, "Only named fields are supported").to_compile_error().into(),
        Data::Enum(data) => {
            for variant in data.variants {
                if !variant.fields.is_empty() {
                    return syn::Error::new_spanned(variant.fields, "Only non-tagged enums are supported").to_compile_error().into();
                }

                variants.push(variant.ident.clone());
                //panic!("discriminant: {:?}, ident: {:?}", variant.discriminant, variant.ident);
            }
        }
        _ => return syn::Error::new_spanned(&name, "Only non-tagged enums are supported").to_compile_error().into(),
    };

    let variants_str = variants.iter().map(|v| v.to_string().to_lowercase()).collect::<Vec<_>>();

    let expanded: proc_macro::TokenStream = quote! {
        impl ::ankurah::Property for #name {
            fn into_value(&self) -> std::result::Result<Option<::ankurah::property::PropertyValue>, ::ankurah::property::PropertyError> {
                let tag = match self {
                    #(
                        Self::#variants => #variants_str,
                    )*
                };
                Ok(Some(::ankurah::property::PropertyValue::String(tag.to_owned())))
            }

            fn from_value(value: Option<::ankurah::property::PropertyValue>) -> Result<Self, ::ankurah::property::PropertyError> {
                match value {
                    Some(::ankurah::property::PropertyValue::String(variant_str)) => match &*variant_str {
                        #(
                            #variants_str => Ok(Self::#variants),
                        )*
                        value => Err(::ankurah::property::PropertyError::InvalidValue { value: value.to_owned(), ty: "Visibility".to_owned() }),
                    },
                    Some(other) => Err(::ankurah::property::PropertyError::InvalidVariant { given: other, ty: "Visibility".to_owned() }),
                    None => Err(::ankurah::property::PropertyError::Missing),
                }
            }
        }
    }
    .into();

    expanded
}
