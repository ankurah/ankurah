use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use syn::Type;

/// Backend configuration for code generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendConfig {
    pub backend_name: String,
    pub values: Vec<ValueConfig>,
}

/// Configuration for a specific value type within a backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueConfig {
    pub type_pattern: String,
    pub fully_qualified_type: String,
    pub accepts: String,
    pub generic_params: Vec<String>,
    pub materialized_pattern: String,
    pub methods: Vec<Method>,
}

/// Method configuration for code generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Method {
    pub name: String,
    pub args: Vec<(String, String)>, // (name, type)
    pub return_type: String,
    #[serde(default = "default_wasm_true")]
    pub wasm: bool,
}

fn default_wasm_true() -> bool { true }

/// Describes how to generate materialized wrappers for a specific ActiveValue type
pub struct BackendDescription {
    pub value_config: ValueConfig,
    pub concrete_types: HashMap<String, String>, // {T} -> Complex, etc.
    pub materialized_type_name: String,
}

impl BackendDescription {
    /// Generate the materialized wrapper struct and its implementations
    pub fn generate_wrapper(&self) -> TokenStream {
        let wrapper_name = format_ident!("{}", self.materialized_type_name);
        let original_type_str = self.substitute_generics(&self.value_config.fully_qualified_type);
        let original_type: Type = syn::parse_str(&original_type_str).expect("Failed to parse original type");

        // Generate method implementations (only for methods with wasm: true)
        let methods: Vec<TokenStream> = self
            .value_config
            .methods
            .iter()
            .filter(|method| method.wasm)
            .map(|method| {
                let method_name = format_ident!("{}", method.name);
                let args: Vec<TokenStream> = method
                    .args
                    .iter()
                    .map(|(arg_name, arg_type)| {
                        let arg_name = format_ident!("{}", arg_name);
                        let arg_type = self.substitute_generics(arg_type);
                        let arg_type: Type = syn::parse_str(&arg_type).expect("Failed to parse arg type");
                        quote! { #arg_name: #arg_type }
                    })
                    .collect();

                let return_type_str = self.substitute_generics(&method.return_type);

                // Generate method call: self.0.method_name(args...)
                let arg_names: Vec<syn::Ident> = method.args.iter().map(|(name, _)| format_ident!("{}", name)).collect();

                // Handle argument conversion for WASM (e.g., value -> &value for LWW::set)
                let converted_args: Vec<TokenStream> = method
                    .args
                    .iter()
                    .zip(arg_names.iter())
                    .map(|((_, arg_type), arg_name)| {
                        if arg_type == "{T}" && method.name == "set" {
                            // For LWW::set and similar, convert value parameter to reference
                            quote! { &#arg_name }
                        } else {
                            quote! { #arg_name }
                        }
                    })
                    .collect();

                // Handle Result types properly - don't double-wrap
                let (wasm_return_type_str, method_call) = if return_type_str.starts_with("Result<") {
                    // Already a Result, just change the error type
                    let inner_type = return_type_str.strip_prefix("Result<").unwrap().strip_suffix(">").unwrap();
                    let parts: Vec<&str> = inner_type.split(", ").collect();
                    let ok_type = parts[0];
                    (
                        format!("Result<{}, ::wasm_bindgen::JsValue>", ok_type),
                        quote! {
                            self.0.#method_name(#(#converted_args),*)
                                .map_err(|e| ::wasm_bindgen::JsValue::from(e.to_string()))
                        },
                    )
                } else {
                    // Not a Result, return directly
                    (return_type_str.clone(), quote! { self.0.#method_name(#(#converted_args),*) })
                };

                let wasm_return_type: Type = syn::parse_str(&wasm_return_type_str).expect("Failed to parse return type");

                quote! {
                    pub fn #method_name(&self, #(#args),*) -> #wasm_return_type {
                        #method_call
                    }
                }
            })
            .collect();

        quote! {
            #[wasm_bindgen]
            pub struct #wrapper_name(#original_type);

            #[wasm_bindgen]
            impl #wrapper_name {
                #(#methods)*
            }
        }
    }

    /// Substitute generic parameters in type/method signatures
    fn substitute_generics(&self, pattern: &str) -> String {
        let mut result = pattern.to_string();
        for (param, concrete_type) in &self.concrete_types {
            let placeholder = format!("{{{}}}", param);
            result = result.replace(&placeholder, concrete_type);
        }
        result
    }
}

/// Global backend configuration manager
pub struct BackendManager {
    configs: Vec<BackendConfig>, // Ordered list to ensure "first refusal" precedence
}

impl BackendManager {
    /// Create a new BackendManager with built-in and custom configs
    pub fn new() -> syn::Result<Self> {
        let mut configs = Vec::new();

        // Load built-in configs in order of precedence (YrsString before LWW)
        let yrs_bytes = include_bytes!("../../core/src/property/value/yrs.ron");
        let yrs_config: BackendConfig = ron::de::from_bytes(yrs_bytes)
            .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), format!("Failed to parse yrs.ron: {}", e)))?;
        configs.push(yrs_config);

        let lww_bytes = include_bytes!("../../core/src/property/value/lww.ron");
        let lww_config: BackendConfig = ron::de::from_bytes(lww_bytes)
            .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), format!("Failed to parse lww.ron: {}", e)))?;
        configs.push(lww_config);

        // TODO: Load custom configs from CARGO_MANIFEST_DIR/backends/
        // let manifest_dir = env!("CARGO_MANIFEST_DIR");
        // let backends_dir = format!("{}/backends", manifest_dir);
        // if std::path::Path::new(&backends_dir).exists() {
        //     for entry in std::fs::read_dir(&backends_dir)? {
        //         let entry = entry?;
        //         if entry.path().extension() == Some("ron".as_ref()) {
        //             let bytes = std::fs::read(entry.path())?;
        //             let config: BackendConfig = ron::de::from_bytes(&bytes)?;
        //             configs.push(config);
        //         }
        //     }
        // }

        Ok(Self { configs })
    }

    /// Generate all materialized wrapper types for a model's active fields
    pub fn generate_wasm_wrapper_impls(&self, model: &crate::model::description::ModelDescription) -> Vec<proc_macro2::TokenStream> {
        let active_field_types = match model.active_field_types() {
            Ok(types) => types,
            Err(_) => return vec![quote::quote! { compile_error!("Failed to get active field types"); }],
        };

        let mut wrapper_impls = Vec::new();
        let mut generated_types = std::collections::HashSet::new();

        for active_type in active_field_types.iter() {
            if let Some(backend_desc) = self.describe_active_type(active_type) {
                // Only generate the wrapper type once per materialized type name
                if !generated_types.contains(&backend_desc.materialized_type_name) {
                    let wrapper_impl = backend_desc.generate_wrapper();
                    wrapper_impls.push(wrapper_impl);
                    generated_types.insert(backend_desc.materialized_type_name.clone());
                }
            }
        }

        wrapper_impls
    }

    /// Generate WASM getter methods for a model's active fields
    pub fn generate_wasm_getter_methods(&self, model: &crate::model::description::ModelDescription) -> Vec<proc_macro2::TokenStream> {
        let active_field_types = match model.active_field_types() {
            Ok(types) => types,
            Err(_) => return vec![quote::quote! { compile_error!("Failed to get active field types"); }],
        };

        let mut getter_methods = Vec::new();

        for (field, active_type) in model.active_fields().iter().zip(active_field_types.iter()) {
            let field_name = field.ident.as_ref().unwrap();

            if let Some(backend_desc) = self.describe_active_type(active_type) {
                let wrapper_type_name = quote::format_ident!("{}", backend_desc.materialized_type_name);
                let wasm_method_name = quote::format_ident!("wasm_{}", field_name);
                let getter_method = quote::quote! {
                    #[wasm_bindgen(getter, js_name = #field_name)]
                    pub fn #wasm_method_name(&self) -> #wrapper_type_name {
                        #wrapper_type_name(self.#field_name())
                    }
                };
                getter_methods.push(getter_method);
            }
        }

        getter_methods
    }

    /// Find a backend description for the given ActiveValue type
    pub fn describe_active_type(&self, active_type: &Type) -> Option<BackendDescription> {
        let type_str = quote!(#active_type).to_string().replace(" ", "");

        // Try to match against explicit type patterns first (e.g., LWW<String>, YrsString)
        // Iterate in order of first refusal (YrsString before LWW)
        for config in &self.configs {
            for value_config in &config.values {
                if let Ok(regex) = regex::Regex::new(&value_config.type_pattern) {
                    if let Some(captures) = regex.captures(&type_str) {
                        // Extract generic parameters from captures
                        let mut concrete_types = HashMap::new();

                        // Map captures to generic parameters
                        for (i, param) in value_config.generic_params.iter().enumerate() {
                            if let Some(capture) = captures.get(i + 1) {
                                // Skip capture 0 (full match)
                                concrete_types.insert(param.clone(), capture.as_str().to_string());
                            }
                        }

                        // Generate materialized type name
                        let materialized_name = self.generate_materialized_name(value_config, &concrete_types);

                        return Some(BackendDescription {
                            value_config: value_config.clone(),
                            concrete_types,
                            materialized_type_name: materialized_name,
                        });
                    }
                }
            }
        }

        None
    }

    /// Find a backend for type inference (when no explicit active_type is specified)
    pub fn infer_active_type(&self, field_type: &Type) -> Option<BackendDescription> {
        let field_type_str = quote!(#field_type).to_string().replace(" ", "");

        // Try each backend's accepts pattern in order (YrsString before LWW)
        for config in &self.configs {
            for value_config in &config.values {
                if let Ok(regex) = regex::Regex::new(&value_config.accepts) {
                    if regex.is_match(&field_type_str) {
                        // For inference, we only support single generic parameter
                        if value_config.generic_params.len() != 1 {
                            continue; // Skip multi-parameter backends for inference
                        }

                        let mut concrete_types = HashMap::new();
                        let param = &value_config.generic_params[0];
                        concrete_types.insert(param.clone(), field_type_str.clone());

                        let materialized_name = self.generate_materialized_name(value_config, &concrete_types);

                        return Some(BackendDescription {
                            value_config: value_config.clone(),
                            concrete_types,
                            materialized_type_name: materialized_name,
                        });
                    }
                }
            }
        }

        None
    }

    /// Generate materialized type name by substituting generic parameters
    fn generate_materialized_name(&self, value_config: &ValueConfig, concrete_types: &HashMap<String, String>) -> String {
        let mut name = value_config.materialized_pattern.clone();
        for (param, concrete_type) in concrete_types {
            let placeholder = format!("{{{}}}", param);
            name = name.replace(&placeholder, concrete_type);
        }
        name
    }
}
