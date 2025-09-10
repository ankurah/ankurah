mod model;
mod property;
mod selection;
#[cfg(feature = "wasm")]
mod tsify;
#[cfg(feature = "wasm")]
mod wasm_signal;
mod wrapper_macros;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Model, attributes(active_type, ephemeral, model))]
pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    // Parse the model description
    let desc = match model::description::ModelDescription::parse(&input) {
        Ok(model) => model,
        Err(e) => return e.to_compile_error().into(),
    };

    let hygiene_module = quote::format_ident!("__ankurah_derive_impl_{}", to_snake_case(&desc.name().to_string()));
    let wasm_imports = if cfg!(feature = "wasm") {
        quote! { use ::ankurah::derive_deps::wasm_bindgen::prelude::*; }
    } else {
        quote! {}
    };

    // Generate implementations using the modular approach
    let model_impl = model::model::model_impl(&desc);
    let view_impl = model::view::view_impl(&desc);
    let mutable_impl = model::mutable::mutable_impl(&desc);
    #[cfg(feature = "wasm")]
    let wasm_impl = model::wasm::wasm_impl(&input, &desc);
    #[cfg(not(feature = "wasm"))]
    let wasm_impl = quote! {};

    let expanded = quote! {
        mod #hygiene_module {
            use super::*;
            #wasm_imports

            #model_impl
            #view_impl
            #mutable_impl
            #wasm_impl
        }
        pub use #hygiene_module::*;
    };

    expanded.into()
}

/// Convert a PascalCase identifier to snake_case
fn to_snake_case(ident: &str) -> String {
    ident
        .chars()
        .enumerate()
        .flat_map(|(i, c)| {
            if c.is_uppercase() && i > 0 {
                vec!['_', c.to_lowercase().next().unwrap()]
            } else {
                vec![c.to_lowercase().next().unwrap()]
            }
        })
        .collect()
}

#[cfg(feature = "wasm")]
#[proc_macro_derive(WasmSignal)]
pub fn derive_wasm_signal(input: TokenStream) -> TokenStream { wasm_signal::derive_wasm_signal_impl(input) }

#[proc_macro_derive(Property)]
pub fn derive_property(input: TokenStream) -> TokenStream { property::derive_property_impl(input) }

/// Generate a predicate at compile time
///
/// This macro supports both quoted and unquoted syntax for building predicates at compile time.
/// Variable values are captured from the local scope and embedded into the generated predicate.
///
/// # Examples
///
/// **Unquoted form** - The most terse syntax. Supports inlined variable substitution:
/// ```rust,ignore
/// // Expand variables into comparisons of the same name. Equivalent to status = {status}
/// let result = selection!({status});
/// // Default comparison is equality but you can prefix with >, <, >=, <=, !=
/// let result = selection!({name} AND {>age});
/// // Equivalent to the above
/// let result = selection!({name} AND age > {age});
/// ```
///
/// **Quoted form** - Required for quoted string literals and positional arguments:
/// ```rust,ignore
/// let result = selection!("status = 'active'");              // Pure literals
/// let result = selection!("status = 'active' AND {name}");   // Mixed: variable + literal
/// let result = selection!("status = 'active' AND {}", name); // Equivalent to the above
/// ```
#[proc_macro]
pub fn selection(input: TokenStream) -> TokenStream { selection::selection_macro(input) }

/// Convenience macro for fetch operations with predicate syntax.
///
/// This macro forwards all arguments (except the context) to the `selection!` macro
/// and then calls `fetch` on the context with the resulting predicate.
///
/// # Examples
///
/// **Unquoted form** - The most terse syntax. Supports inlined variable substitution:
/// ```rust,ignore
/// // Expand variables into comparisons of the same name. Equivalent to status = {status}
/// let results = fetch!(ctx, {status}).await?;
/// // Default comparison is equality but you can prefix with >, <, >=, <=, !=
/// let results = fetch!(ctx, {name} AND {>age}).await?;
/// // Equivalent to the above
/// let results = fetch!(ctx, {name} AND age > {age}).await?;
/// // Equivalent to:
/// let results = ctx.fetch(selection!({name} AND {>age})).await?;
/// ```
///
/// **Quoted form** - Required for quoted string literals and positional arguments:
/// ```rust,ignore
/// let results = fetch!(ctx, "status = 'active'").await?;              // Pure literals
/// let results = fetch!(ctx, "status = 'active' AND {name}").await?;   // Mixed: variable + literal
/// let results = fetch!(ctx, "status = 'active' AND {}", name).await?; // Equivalent to the above
/// ```
///
/// See [`ankurah_derive::selection!`] documentation for complete syntax details.
#[proc_macro]
pub fn fetch(input: TokenStream) -> TokenStream { selection::fetch_macro(input) }

/// Convenience macro for subscribe operations with predicate syntax.
///
/// This macro forwards all arguments (except the context and callback) to the `selection!` macro
/// and then calls `subscribe` on the context with the resulting predicate and callback,
/// returning a subscription handle.
///
/// # Examples
///
/// **Unquoted form** - The most terse syntax. Supports inlined variable substitution:
/// ```rust,ignore
/// // Expand variables into comparisons of the same name. Equivalent to status = {status}
/// let handle = subscribe!(ctx, callback, {status}).await?;
/// // Default comparison is equality but you can prefix with >, <, >=, <=, !=
/// let handle = subscribe!(ctx, callback, {name} AND {>age}).await?;
/// // Equivalent to the above
/// let handle = subscribe!(ctx, callback, {name} AND age > {age}).await?;
/// // Equivalent to:
/// let handle = ctx.subscribe(selection!({name} AND {>age}), callback).await?;
/// ```
///
/// **Quoted form** - Required for quoted string literals and positional arguments:
/// ```rust,ignore
/// let handle = subscribe!(ctx, callback, "status = 'active'").await?;              // Pure literals
/// let handle = subscribe!(ctx, callback, "status = 'active' AND {name}").await?;   // Mixed: variable + literal
/// let handle = subscribe!(ctx, callback, "status = 'active' AND {}", name).await?; // Equivalent to the above
/// ```
///
/// See [`ankurah_derive::selection!`] documentation for complete syntax details.
#[proc_macro]
pub fn subscribe(input: TokenStream) -> TokenStream { selection::subscribe_macro(input) }

/// Generate WASM wrappers for all types marked as "provided" in the backend config
///
/// This macro should be called once per property backend to generate standard wrappers.
/// Example usage in core/src/property/value/lww.rs:
/// ```rust,ignore
/// impl_provided_wrapper_types!("src/property/value/lww.ron");
/// ```
#[proc_macro]
pub fn impl_provided_wrapper_types(input: TokenStream) -> TokenStream {
    let config_filename = parse_macro_input!(input as syn::LitStr);
    match wrapper_macros::impl_provided_wrapper_types_impl(&config_filename.value()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Generate a WASM wrapper for a specific custom type
///
/// This macro generates a wrapper for types not covered by the "provided" set.
/// Example usage:
/// ```rust,ignore
/// impl_wrapper_type!(Complex);  // Generates LWWComplex, YrsStringComplex, etc.
/// ```
#[proc_macro]
pub fn impl_wrapper_type(input: TokenStream) -> TokenStream {
    let custom_type = parse_macro_input!(input as syn::Type);
    match wrapper_macros::impl_wrapper_type_impl(&custom_type) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
