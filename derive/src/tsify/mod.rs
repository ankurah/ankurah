pub(crate) mod attrs;
pub(crate) mod comments;
pub(crate) mod container;
pub(crate) mod decl;
pub(crate) mod derive;
pub(crate) mod error_tracker;
pub(crate) mod parser;
pub(crate) mod type_alias;
pub(crate) mod typescript;
pub(crate) mod wasm_bindgen;

use syn::{parse_macro_input, DeriveInput};

pub(crate) fn declare_impl(args: proc_macro2::TokenStream, item: syn::Item) -> syn::Result<proc_macro2::TokenStream> {
    match item {
        syn::Item::Type(item) => type_alias::expand(item),
        syn::Item::Enum(item) => derive::expand_by_attr(args, item.into()),
        syn::Item::Struct(item) => derive::expand_by_attr(args, item.into()),
        _ => Err(syn::Error::new_spanned(args, "#[declare] can only be applied to a struct, enum, or type alias.")),
    }
}

// /// The `declare` macro, used in `#[declare]` annotations.
// #[proc_macro_attribute]
// pub fn declare(
//     args: proc_macro::TokenStream,
//     item: proc_macro::TokenStream,
// ) -> proc_macro::TokenStream {
//     let item: syn::Item = parse_macro_input!(item);
//     let args = proc_macro2::TokenStream::from(args);

//     declare_impl(args, item)
//         .unwrap_or_else(syn::Error::into_compile_error)
//         .into()
// }

// /// The `Tsify` derive macro, used in `#[derive(Tsify, ...)]` annotations.
// #[proc_macro_derive(Tsify, attributes(tsify, serde))]
pub fn derive_tsify(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let item: DeriveInput = parse_macro_input!(input);

    derive::expand(item).unwrap_or_else(syn::Error::into_compile_error).into()
}
