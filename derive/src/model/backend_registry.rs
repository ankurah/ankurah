use quote::quote;
use std::collections::HashMap;

use crate::model::backend::{ActiveTypeDesc, BackendConfig};

/// Global backend configuration manager
pub struct BackendRegistry {
    pub(crate) configs: Vec<BackendConfig>, // Ordered list to ensure "first refusal" precedence
}

impl BackendRegistry {
    /// Create a new BackendManager with built-in and custom configs
    pub fn new() -> syn::Result<Self> {
        let mut configs = Vec::new();

        // Load built-in configs in order of precedence (YrsString before LWW)
        let yrs_bytes = include_bytes!("../../default_backends/yrs.ron");
        let yrs_config: BackendConfig = ron::de::from_bytes(yrs_bytes)
            .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), format!("Failed to parse yrs.ron: {}", e)))?;
        configs.push(yrs_config);

        let lww_bytes = include_bytes!("../../default_backends/lww.ron");
        let lww_config: BackendConfig = ron::de::from_bytes(lww_bytes)
            .map_err(|e| syn::Error::new(proc_macro2::Span::call_site(), format!("Failed to parse lww.ron: {}", e)))?;
        configs.push(lww_config);

        Ok(Self { configs })
    }

    /// Resolve an active type from either explicit attribute content or field type for inference
    pub fn resolve_active_type(&self, field: &syn::Field) -> Option<ActiveTypeDesc> {
        // Check if field has an active_type attribute
        let active_type_attr = field.attrs.iter().find(|attr| attr.path().is_ident("active_type"));

        // Extract the attribute content if present
        let attr_content: Option<String> = if let Some(attr) = active_type_attr {
            if let Ok(meta) = attr.parse_args::<syn::Type>() {
                Some(quote!(#meta).to_string().replace(" ", ""))
            } else {
                None
            }
        } else {
            None
        };

        // Get the field type
        let field_type = &field.ty;
        let field_type_str = quote!(#field_type).to_string().replace(" ", "");

        // Iterate through configs in order of precedence (YrsString before LWW)
        for config in &self.configs {
            for value_config in &config.values {
                let type_pattern = regex::Regex::new(&value_config.type_pattern).ok()?;
                let accepts_pattern = regex::Regex::new(&value_config.accepts).ok()?;

                // If we have an explicit attribute, try to match it against type_pattern
                if let Some(ref attr_str) = attr_content {
                    let clean_attr = attr_str.replace(" ", "");
                    if let Some(captures) = type_pattern.captures(&clean_attr) {
                        let mut concrete_types = HashMap::new();

                        // Extract generic parameters from the match
                        for (i, param) in value_config.generic_params.iter().enumerate() {
                            if let Some(capture) = captures.get(i + 1) {
                                concrete_types.insert(param.clone(), capture.as_str().to_string());
                            } else {
                                // Infer from field type if not captured in attribute
                                concrete_types.insert(param.clone(), field_type_str.clone());
                            }
                        }

                        return Some(ActiveTypeDesc::new(config.clone(), value_config.clone(), concrete_types));
                    }
                }

                // If no explicit attribute or no match, try inference from field type
                if attr_content.is_none() && accepts_pattern.is_match(&field_type_str) && value_config.generic_params.len() == 1 {
                    let mut concrete_types = HashMap::new();
                    concrete_types.insert(value_config.generic_params[0].clone(), field_type_str.clone());
                    return Some(ActiveTypeDesc::new(config.clone(), value_config.clone(), concrete_types));
                }
            }
        }

        None
    }
}
