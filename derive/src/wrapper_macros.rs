use proc_macro2::TokenStream;
use quote::quote;

use crate::model::backend::ActiveTypeDesc;

/// Load backend config from RON file
fn load_backend_config(config_filename: &str) -> syn::Result<crate::model::backend::BackendConfig> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map_err(|_| syn::Error::new(proc_macro2::Span::call_site(), "CARGO_MANIFEST_DIR not available"))?;

    let config_path = std::path::Path::new(&manifest_dir).join(config_filename);
    let config_bytes = std::fs::read(&config_path)
        .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), format!("Failed to read config file {:?}: {}", config_path, e)))?;

    ron::de::from_bytes(&config_bytes)
        .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), format!("Failed to parse config file {:?}: {}", config_path, e)))
}

/// Implementation for impl_provided_wrapper_types!() macro (WASM version)
/// Note: Generates code wrapped in #[cfg(feature = "wasm")] so it's conditionally compiled
/// in the target crate, not the derive crate.
pub fn impl_provided_wrapper_types_impl(config_filename: &str) -> syn::Result<TokenStream> {
    let config = load_backend_config(config_filename)?;

    let mut all_wrappers = Vec::new();

    // Generate wrappers for each provided type in each value config
    for value_config in &config.values {
        for provided_type in &config.provided_wrapper_types {
            let mut concrete_types = std::collections::HashMap::new();
            if value_config.generic_params.len() == 1 {
                let param = &value_config.generic_params[0];
                concrete_types.insert(param.clone(), provided_type.clone());
            }

            let backend = ActiveTypeDesc::new(config.clone(), value_config.clone(), concrete_types);
            let wrapper = backend.wasm_wrapper("local", None);
            all_wrappers.push(wrapper);
        }
    }

    let hygiene_module = quote::format_ident!("__ankurah_provided_wrappers_{}", config.backend_name.to_lowercase());

    // Wrap in cfg(feature = "wasm") so it compiles conditionally in the target crate
    // Note: This macro is called from within ankurah-core which has wasm-bindgen as a direct dependency
    Ok(quote! {
        #[cfg(feature = "wasm")]
        mod #hygiene_module {
            use super::*;
            use ::wasm_bindgen::prelude::*;
            #(#all_wrappers)*
        }
        #[cfg(feature = "wasm")]
        pub use #hygiene_module::*;
    })
}

/// Implementation for impl_provided_wrapper_types_uniffi!() macro (UniFFI version)
/// Note: Generates code wrapped in #[cfg(feature = "uniffi")] so it's conditionally compiled
/// in the target crate, not the derive crate.
pub fn impl_provided_wrapper_types_uniffi_impl(config_filename: &str) -> syn::Result<TokenStream> {
    let config = load_backend_config(config_filename)?;

    let mut all_wrappers = Vec::new();

    // Use uniffi_provided_wrapper_types if specified, otherwise fall back to provided_wrapper_types
    let uniffi_types = config.uniffi_provided_wrapper_types.as_ref().unwrap_or(&config.provided_wrapper_types);

    // Generate wrappers for each provided type in each value config
    for value_config in &config.values {
        for provided_type in uniffi_types {
            let mut concrete_types = std::collections::HashMap::new();
            if value_config.generic_params.len() == 1 {
                let param = &value_config.generic_params[0];
                concrete_types.insert(param.clone(), provided_type.clone());
            }

            let backend = ActiveTypeDesc::new(config.clone(), value_config.clone(), concrete_types);
            let wrapper = backend.uniffi_wrapper("local", None);
            all_wrappers.push(wrapper);
        }
    }

    let hygiene_module = quote::format_ident!("__ankurah_provided_wrappers_uniffi_{}", config.backend_name.to_lowercase());

    // Wrap in cfg(feature = "uniffi") so it compiles conditionally in the target crate
    Ok(quote! {
        #[cfg(feature = "uniffi")]
        mod #hygiene_module {
            use super::*;
            #(#all_wrappers)*
        }
        #[cfg(feature = "uniffi")]
        pub use #hygiene_module::*;
    })
}

/// Implementation for impl_wrapper_type!() macro (WASM version)
/// Note: Generates code wrapped in #[cfg(feature = "wasm")] so it's conditionally compiled
/// in the target crate, not the derive crate.
pub fn impl_wrapper_type_impl(custom_type: &syn::Type) -> syn::Result<TokenStream> {
    let backend_registry = crate::model::backend_registry::BackendRegistry::new()?;
    let custom_type_str = quote!(#custom_type).to_string().replace(" ", "");

    let mut all_wrappers = Vec::new();

    // Generate wrappers for this custom type in all backends that don't provide it
    for config in backend_registry.configs {
        for value_config in &config.values {
            // Check if this type is already provided by this backend
            if config.provided_wrapper_types.contains(&custom_type_str) {
                continue; // Skip - this backend provides this type
            }

            // Check if this custom type matches the backend's accepts pattern
            let accepts_regex = regex::Regex::new(&value_config.accepts).unwrap();
            if !accepts_regex.is_match(&custom_type_str) {
                continue; // Skip - this backend doesn't accept this type
            }

            // Generate wrapper for this custom type
            let mut concrete_types = std::collections::HashMap::new();
            if value_config.generic_params.len() == 1 {
                let param = &value_config.generic_params[0];
                concrete_types.insert(param.clone(), custom_type_str.clone());
            }

            let backend_desc = ActiveTypeDesc::new(config.clone(), value_config.clone(), concrete_types);
            let wrapper = backend_desc.wasm_wrapper("external", None);
            all_wrappers.push(wrapper);
        }
    }

    let hygiene_module = quote::format_ident!(
        "__ankurah_custom_wrappers_{}",
        custom_type_str.replace("<", "_").replace(">", "").replace(" ", "").replace(",", "_").replace("::", "")
    );

    // Wrap in cfg(feature = "wasm") so it compiles conditionally in the target crate
    // Note: This macro is called from external crates, so we use ::ankurah::derive_deps
    Ok(quote! {
        #[cfg(feature = "wasm")]
        mod #hygiene_module {
            use super::*;
            use ::ankurah::derive_deps::wasm_bindgen::prelude::*;
            #(#all_wrappers)*
        }
        #[cfg(feature = "wasm")]
        pub use #hygiene_module::*;
    })
}

/// Implementation for impl_wrapper_type!() macro (UniFFI version)
/// Note: Generates code wrapped in #[cfg(feature = "uniffi")] so it's conditionally compiled
/// in the target crate, not the derive crate.
pub fn impl_wrapper_type_uniffi_impl(custom_type: &syn::Type) -> syn::Result<TokenStream> {
    let backend_registry = crate::model::backend_registry::BackendRegistry::new()?;
    let custom_type_str = quote!(#custom_type).to_string().replace(" ", "");

    let mut all_wrappers = Vec::new();

    // Generate wrappers for this custom type in all backends that don't provide it
    for config in backend_registry.configs {
        for value_config in &config.values {
            // Check if this type is already provided by this backend
            if config.provided_wrapper_types.contains(&custom_type_str) {
                continue; // Skip - this backend provides this type
            }

            // Check if this custom type matches the backend's accepts pattern
            let accepts_regex = regex::Regex::new(&value_config.accepts).unwrap();
            if !accepts_regex.is_match(&custom_type_str) {
                continue; // Skip - this backend doesn't accept this type
            }

            // Generate wrapper for this custom type
            let mut concrete_types = std::collections::HashMap::new();
            if value_config.generic_params.len() == 1 {
                let param = &value_config.generic_params[0];
                concrete_types.insert(param.clone(), custom_type_str.clone());
            }

            let backend_desc = ActiveTypeDesc::new(config.clone(), value_config.clone(), concrete_types);
            let wrapper = backend_desc.uniffi_wrapper("external", None);
            all_wrappers.push(wrapper);
        }
    }

    let hygiene_module = quote::format_ident!(
        "__ankurah_custom_wrappers_uniffi_{}",
        custom_type_str.replace("<", "_").replace(">", "").replace(" ", "").replace(",", "_").replace("::", "")
    );

    // Wrap in cfg(feature = "uniffi") so it compiles conditionally in the target crate
    Ok(quote! {
        #[cfg(feature = "uniffi")]
        mod #hygiene_module {
            use super::*;
            #(#all_wrappers)*
        }
        #[cfg(feature = "uniffi")]
        pub use #hygiene_module::*;
    })
}
