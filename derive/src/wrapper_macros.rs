use proc_macro2::TokenStream;
use quote::quote;

use crate::model::backend::ActiveTypeDesc;

/// Implementation for impl_provided_wrapper_types!() macro
pub fn impl_provided_wrapper_types_impl(config_filename: &str) -> syn::Result<TokenStream> {
    // Punt if wasm feature isn't enabled
    if !cfg!(feature = "wasm") {
        return Ok(quote! {});
    }

    // Load the backend config using CARGO_MANIFEST_DIR + filename
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map_err(|_| syn::Error::new(proc_macro2::Span::call_site(), "CARGO_MANIFEST_DIR not available"))?;

    // Try to find the config file relative to the calling file's directory
    // Since we can't get the caller's file path directly, we'll use manifest_dir + relative path
    let config_path = std::path::Path::new(&manifest_dir).join(config_filename);
    let config_bytes = std::fs::read(&config_path)
        .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), format!("Failed to read config file {:?}: {}", config_path, e)))?;

    let config: crate::model::backend::BackendConfig = ron::de::from_bytes(&config_bytes)
        .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), format!("Failed to parse config file {:?}: {}", config_path, e)))?;

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

    Ok(quote! {
        mod #hygiene_module {
            use super::*;
            use ::wasm_bindgen::prelude::*;
            #(#all_wrappers)*
        }
        pub use #hygiene_module::*;
    })
}

/// Implementation for impl_wrapper_type!() macro  
pub fn impl_wrapper_type_impl(custom_type: &syn::Type) -> syn::Result<TokenStream> {
    // Punt if wasm feature isn't enabled
    if !cfg!(feature = "wasm") {
        return Ok(quote! {});
    }

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

    Ok(quote! {
        mod #hygiene_module {
            use super::*;
            use ::wasm_bindgen::prelude::*;
            #(#all_wrappers)*
        }
        pub use #hygiene_module::*;
    })
}
