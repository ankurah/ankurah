use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{punctuated::Punctuated, AngleBracketedGenericArguments, Data, DeriveInput, Fields, Ident, Type, Visibility};

/// Encapsulates all the parsed information about a model and provides clean accessors
pub struct ModelDescription {
    // Basic identifiers
    name: Ident,

    // Field collections - only store what we actually parsed
    active_fields: Vec<syn::Field>,
    ephemeral_fields: Vec<syn::Field>,

    // Backend manager for configuration lookup
    pub(crate) backend_registry: crate::model::backend_registry::BackendRegistry,
}

impl ModelDescription {
    /// Parse a DeriveInput and create a ModelDescription
    pub fn parse(input: &DeriveInput) -> syn::Result<Self> {
        let name = input.ident.clone();

        let fields = match &input.data {
            Data::Struct(data) => match &data.fields {
                Fields::Named(fields) => fields.named.clone(),
                fields => return Err(syn::Error::new_spanned(fields, format!("Only named fields are supported this is a {:#?}", fields))),
            },
            _ => return Err(syn::Error::new_spanned(&name, "Only structs are supported")),
        };

        // Split fields into active and ephemeral
        let mut active_fields = Vec::new();
        let mut ephemeral_fields = Vec::new();
        for field in fields.into_iter() {
            if get_model_flag(&field.attrs, "ephemeral") {
                ephemeral_fields.push(field);
            } else {
                active_fields.push(field);
            }
        }

        // Load backend configurations at compile time
        let backend_registry = crate::model::backend_registry::BackendRegistry::new()?;

        Ok(Self { name, active_fields, ephemeral_fields, backend_registry })
    }

    // Basic identifier accessors
    pub fn name(&self) -> &Ident { &self.name }
    pub fn collection_str(&self) -> String { self.name.to_string().to_lowercase() }
    pub fn view_name(&self) -> Ident { format_ident!("{}View", self.name) }
    pub fn mutable_name(&self) -> Ident { format_ident!("{}Mut", self.name) }

    #[cfg(feature = "wasm")]
    pub fn resultset_name(&self) -> Ident { format_ident!("{}ResultSet", self.name) }
    #[cfg(feature = "wasm")]
    pub fn livequery_name(&self) -> Ident { format_ident!("{}LiveQuery", self.name) }
    #[cfg(feature = "wasm")]
    pub fn changeset_name(&self) -> Ident { format_ident!("{}ChangeSet", self.name) }

    // Computed accessors for active fields
    pub fn active_fields(&self) -> &[syn::Field] { &self.active_fields }
    pub fn active_field_visibility(&self) -> Vec<&Visibility> { self.active_fields.iter().map(|f| &f.vis).collect() }
    pub fn active_field_names(&self) -> Vec<&Option<Ident>> { self.active_fields.iter().map(|f| &f.ident).collect() }
    pub fn active_field_name_strs(&self) -> Vec<String> {
        self.active_fields.iter().map(|f| f.ident.as_ref().unwrap().to_string().to_lowercase()).collect()
    }
    pub fn projected_field_types(&self) -> Vec<&Type> { self.active_fields.iter().map(|f| &f.ty).collect() }
    /// Get ActiveTypeDesc for each active field
    pub fn active_field_descs(&self) -> syn::Result<Vec<crate::model::backend::ActiveTypeDesc>> {
        let mut results = Vec::new();
        let active_type_ident = format_ident!("active_type");

        for field in &self.active_fields {
            // Check if there's an explicit attribute, otherwise use field type for inference
            let input_type =
                if let Some(active_type_attr) = field.attrs.iter().find(|attr| attr.path().get_ident() == Some(&active_type_ident)) {
                    active_type_attr.parse_args::<syn::Type>()?
                } else {
                    field.ty.clone()
                };

            // Resolve the active type using the unified method
            if let Some(backend_desc) = self.backend_registry.resolve_active_type(field) {
                results.push(backend_desc);
            } else {
                // No backend match found
                let field_name = field.ident.as_ref().map(|i| i.to_string()).unwrap_or_else(|| "unnamed".to_string());
                let type_str = quote!(#input_type).to_string();
                return Err(syn::Error::new_spanned(
                    &field.ty,
                    format!(
                        "No active value type found for field '{}' (type: {}). Please specify using #[active_type(Type)] attribute or ensure the type matches a backend pattern",
                        field_name, type_str
                    ),
                ));
            }
        }
        Ok(results)
    }

    /// Get Rust types for active fields (for accessor method generation)
    pub fn active_field_types(&self) -> syn::Result<Vec<syn::Type>> {
        let descs = self.active_field_descs()?;
        let mut results = Vec::new();

        for (i, desc) in descs.iter().enumerate() {
            let rust_type = desc.rust_type().map_err(|e| {
                let field = &self.active_fields[i];
                syn::Error::new_spanned(&field.ty, format!("Failed to generate Rust type: {}", e))
            })?;
            results.push(rust_type);
        }

        Ok(results)
    }
    pub fn projected_field_types_turbofish(&self) -> Vec<TokenStream> { self.active_fields.iter().map(|f| as_turbofish(&f.ty)).collect() }
    pub fn active_field_types_turbofish(&self) -> syn::Result<Vec<proc_macro2::TokenStream>> {
        let active_types = self.active_field_types()?;
        Ok(active_types.iter().map(as_turbofish).collect())
    }

    // Computed accessors for ephemeral fields
    pub fn ephemeral_field_names(&self) -> Vec<&Option<Ident>> { self.ephemeral_fields.iter().map(|f| &f.ident).collect() }
    pub fn ephemeral_field_types(&self) -> Vec<&Type> { self.ephemeral_fields.iter().map(|f| &f.ty).collect() }
    pub fn ephemeral_field_visibility(&self) -> Vec<&Visibility> { self.ephemeral_fields.iter().map(|f| &f.vis).collect() }

    /// Generate WASM getter methods for this model's active fields
    pub fn generate_wasm_getter_methods(&self) -> Vec<proc_macro2::TokenStream> {
        let mut getter_methods = Vec::new();

        for field in self.active_fields().iter() {
            let field_name = field.ident.as_ref().unwrap();

            if let Some(backend_desc) = self.backend_registry.resolve_active_type(field) {
                // Get the fully qualified wrapper type path
                let wrapper_type_path = backend_desc.get_wrapper_type_path();

                // Parse the wrapper type path as a syn::Type
                let wrapper_type: syn::Type = match syn::parse_str(&wrapper_type_path) {
                    Ok(ty) => ty,
                    Err(_) => {
                        // Fallback: just use the wrapper name as an identifier
                        let wrapper_name = backend_desc.wrapper_type_name();
                        syn::parse_str(&wrapper_name).unwrap_or_else(|_| {
                            syn::Type::Path(syn::TypePath { qself: None, path: syn::Path::from(quote::format_ident!("UnknownWrapper")) })
                        })
                    }
                };

                // Generate the WASM getter method name (prefixed to avoid conflicts)
                let wasm_method_name = quote::format_ident!("wasm_{}", field_name);

                let getter_method = quote::quote! {
                    #[wasm_bindgen(getter, js_name = #field_name)]
                    pub fn #wasm_method_name(&self) -> #wrapper_type {
                        #wrapper_type(self.#field_name())
                    }
                };

                getter_methods.push(getter_method);
            }
        }

        getter_methods
    }
}

fn get_model_flag(attrs: &Vec<syn::Attribute>, flag_name: &str) -> bool {
    attrs.iter().any(|attr| {
        attr.path().segments.iter().any(|seg| seg.ident == "model")
            && attr.meta.require_list().ok().and_then(|list| list.parse_args::<syn::Ident>().ok()).is_some_and(|ident| ident == flag_name)
    })
}

fn as_turbofish(type_path: &syn::Type) -> proc_macro2::TokenStream {
    if let syn::Type::Path(path) = type_path {
        let mut without_generics = path.clone();
        let mut generics = syn::PathArguments::None;
        if let Some(last_segment) = without_generics.path.segments.last_mut() {
            generics = last_segment.arguments.clone();
            last_segment.arguments = syn::PathArguments::None;
        }

        if let syn::PathArguments::AngleBracketed(generics) = generics {
            quote! {
                #without_generics::#generics
            }
        } else {
            quote! {
                #without_generics
            }
        }
    } else {
        unimplemented!("as_turbofish is not supported for non-path types")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_active_type() {
        let mystruct = quote! {
            #[derive(Model)]
            pub struct Entry {
                total: i32, // defaults to LWW for numbers
                #[active_type(LWW)]
                pub count: i32,
                #[active_type(LWW<i32>)]
                pub timestamp: i32,
                #[active_type(LWW)]
                pub added: String,
                #[active_type(LWW<String>)]
                pub ip_address: String,
                pub node_id: String, // defaults to YrsString for strings
                #[active_type(YrsString<String>)]
                pub description: String,
                pub complex: Complex,
            }
        };

        let derive_input: syn::DeriveInput = syn::parse2(mystruct).unwrap();
        let desc = ModelDescription::parse(&derive_input).unwrap();

        let active_descs = desc.active_field_descs().unwrap();
        let active_types = desc.active_field_types().unwrap();

        // Test that all fields get resolved to active types
        assert_eq!(active_descs.len(), 8); // 8 fields

        // Test each field using the descriptors we already have
        let active_type_names = active_descs.iter().map(|desc| desc.rust_type_name().replace(" ", "")).collect::<Vec<String>>();
        let wrapper_type_paths = active_descs.iter().map(|desc| desc.get_wrapper_type_path()).collect::<Vec<String>>();

        assert_eq!(
            active_type_names,
            vec![
                "::ankurah::property::value::LWW<i32>",
                "::ankurah::property::value::LWW<i32>",
                "::ankurah::property::value::LWW<i32>",
                "::ankurah::property::value::LWW<String>",
                "::ankurah::property::value::LWW<String>",
                "::ankurah::property::value::YrsString<String>",
                "::ankurah::property::value::YrsString<String>",
                "::ankurah::property::value::LWW<Complex>" // Complex is assumed to be in scope
            ]
        );
        assert_eq!(
            wrapper_type_paths,
            vec![
                "::ankurah::property::value::lww::wasm::LWWi32",
                "::ankurah::property::value::lww::wasm::LWWi32",
                "::ankurah::property::value::lww::wasm::LWWi32",
                "::ankurah::property::value::lww::wasm::LWWString",
                "::ankurah::property::value::lww::wasm::LWWString",
                "::ankurah::property::value::yrs::wasm::YrsStringString",
                "::ankurah::property::value::yrs::wasm::YrsStringString",
                "LWWComplex", // Wrapper type for Complex is assumed to be in scope, generated by impl_wrapper_type!()
            ]
        );
    }
}
