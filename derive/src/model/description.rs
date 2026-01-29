use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, Ident, Type, Visibility};

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

    #[cfg(any(feature = "wasm", feature = "uniffi"))]
    pub fn resultset_name(&self) -> Ident { format_ident!("{}ResultSet", self.name) }
    #[cfg(any(feature = "wasm", feature = "uniffi"))]
    pub fn livequery_name(&self) -> Ident { format_ident!("{}LiveQuery", self.name) }
    #[cfg(any(feature = "wasm", feature = "uniffi"))]
    pub fn changeset_name(&self) -> Ident { format_ident!("{}ChangeSet", self.name) }
    #[cfg(any(feature = "wasm", feature = "uniffi"))]
    pub fn ref_name(&self) -> Ident { format_ident!("{}Ref", self.name) }

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

    /// Get the WASM wrapper name for a `Ref<T>` type (e.g., `Ref<Artist>` → `ArtistRef`).
    #[cfg(feature = "wasm")]
    pub fn wasm_wrapper_for_type(ty: &Type) -> Option<Ident> {
        if let Type::Path(type_path) = ty {
            let segment = type_path.path.segments.last()?;

            // Check if it's Ref<T>
            if segment.ident == "Ref" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                        // Extract the inner type name (e.g., "Artist" from Ref<Artist>)
                        if let Type::Path(inner_path) = inner_type {
                            let inner_name = inner_path.path.segments.last()?.ident.to_string();
                            return Some(format_ident!("{}Ref", inner_name));
                        }
                    }
                }
            }
        }
        None
    }

    /// Get fields that need WASM wrappers for their projected types (generics like Ref<T>)
    #[cfg(feature = "wasm")]
    pub fn ref_fields(&self) -> Vec<(&syn::Field, Ident)> {
        self.active_fields.iter().filter_map(|field| Self::wasm_wrapper_for_type(&field.ty).map(|wrapper| (field, wrapper))).collect()
    }

    /// Generate WASM getters for View - returns projected values (not active type wrappers).
    ///
    /// - `Ref<T>` → `TRef` (via `<T as Model>::RefWrapper`)
    /// - `Option<Ref<T>>` → `Option<TRef>`
    /// - Other → direct value
    ///
    /// Uses `__wasm_` prefix + `#[doc(hidden)]` to hide from Rust callers.
    #[cfg(feature = "wasm")]
    pub fn wasm_getters(&self) -> Vec<TokenStream> {
        let model_name = &self.name;
        let ref_field_names: std::collections::HashSet<_> =
            self.ref_fields().iter().map(|(f, _)| f.ident.as_ref().unwrap().to_string()).collect();

        // r() method - returns ModelRef for this model
        let r_method = quote! {
            #[doc(hidden)]
            #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, js_name = r)]
            pub fn __wasm_r(&self) -> <#model_name as ::ankurah::model::Model>::RefWrapper {
                self.r().into()
            }
        };

        let field_getters = self.active_fields
            .iter()
            .map(|field| {
                let field_name = field.ident.as_ref().unwrap();
                let wasm_method_name = format_ident!("__wasm_{}", field_name);
                let is_ref = ref_field_names.contains(&field_name.to_string());

                if is_ref {
                    // Ref<T> field: return RefModel wrapper
                    let inner_model = Self::extract_ref_inner_type(&field.ty)
                        .expect("ref_fields should only contain Ref<T> types");
                    quote! {
                        #[doc(hidden)]
                        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter, js_name = #field_name)]
                        pub fn #wasm_method_name(&self) -> Result<<#inner_model as ::ankurah::model::Model>::RefWrapper, ::ankurah::derive_deps::wasm_bindgen::JsValue> {
                            self.#field_name().map(|r| <#inner_model as ::ankurah::model::Model>::RefWrapper::from(r)).map_err(|e| ::ankurah::derive_deps::wasm_bindgen::JsValue::from(e.to_string()))
                        }
                    }
                } else if let Some(inner_model) = Self::extract_option_ref_inner_type(&field.ty) {
                    // Option<Ref<T>> field: return Option<RefModel>
                    quote! {
                        #[doc(hidden)]
                        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter, js_name = #field_name)]
                        pub fn #wasm_method_name(&self) -> Result<Option<<#inner_model as ::ankurah::model::Model>::RefWrapper>, ::ankurah::derive_deps::wasm_bindgen::JsValue> {
                            self.#field_name().map(|opt| opt.map(|r| <#inner_model as ::ankurah::model::Model>::RefWrapper::from(r))).map_err(|e| ::ankurah::derive_deps::wasm_bindgen::JsValue::from(e.to_string()))
                        }
                    }
                } else {
                    // Non-Ref field: simple wrapper
                    let projected_type = &field.ty;
                    quote! {
                        #[doc(hidden)]
                        #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter, js_name = #field_name)]
                        pub fn #wasm_method_name(&self) -> Result<#projected_type, ::ankurah::derive_deps::wasm_bindgen::JsValue> {
                            self.#field_name().map_err(|e| ::ankurah::derive_deps::wasm_bindgen::JsValue::from(e.to_string()))
                        }
                    }
                }
            });

        std::iter::once(r_method).chain(field_getters).collect()
    }

    /// Extract the inner type from Ref<T>, returning the T as a syn::Type
    #[cfg(feature = "wasm")]
    fn extract_ref_inner_type(ty: &Type) -> Option<Type> {
        if let Type::Path(type_path) = ty {
            let segment = type_path.path.segments.last()?;
            if segment.ident == "Ref" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                        return Some(inner_type.clone());
                    }
                }
            }
        }
        None
    }

    /// Extract the inner model type from Option<Ref<T>>, returning T as a syn::Type
    #[cfg(feature = "wasm")]
    fn extract_option_ref_inner_type(ty: &Type) -> Option<Type> {
        if let Type::Path(type_path) = ty {
            let segment = type_path.path.segments.last()?;
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(syn::GenericArgument::Type(inner_type)) = args.args.first() {
                        // inner_type is Ref<T>, extract T
                        return Self::extract_ref_inner_type(inner_type);
                    }
                }
            }
        }
        None
    }

    /// Generate UniFFI getter methods for View fields.
    /// Returns projected field values (String, i64, etc.) via the underlying View methods.
    /// Ref<T> fields return String (base64 EntityId) since UniFFI doesn't support generics.
    /// Uses `uniffi_` prefix internally with `#[uniffi::method(name = "...")]` to expose
    /// with original field names, avoiding conflicts with core Rust methods.
    #[cfg(all(feature = "uniffi", not(feature = "wasm")))]
    pub fn uniffi_view_getters(&self) -> Vec<TokenStream> {
        self.active_fields
            .iter()
            .map(|field| {
                let field_name = field.ident.as_ref().unwrap();
                let uniffi_method_name = format_ident!("uniffi_{}", field_name);
                let field_name_str = field_name.to_string();
                let projected_type = &field.ty;
                let type_str = quote!(#projected_type).to_string();

                // Check if it's a Ref<T> type
                if type_str.contains("Ref <") || type_str.starts_with("Ref<") {
                    // Ref<T> -> return base64 String
                    quote! {
                        #[uniffi::method(name = #field_name_str)]
                        pub fn #uniffi_method_name(&self) -> Result<String, ::ankurah::property::PropertyError> {
                            self.#field_name().map(|r| r.id().to_base64())
                        }
                    }
                } else if type_str.contains("Option < Ref") || type_str.contains("Option<Ref") {
                    // Option<Ref<T>> -> return Option<String>
                    quote! {
                        #[uniffi::method(name = #field_name_str)]
                        pub fn #uniffi_method_name(&self) -> Result<Option<String>, ::ankurah::property::PropertyError> {
                            self.#field_name().map(|opt| opt.map(|r| r.id().to_base64()))
                        }
                    }
                } else {
                    // Regular field - return as-is
                    quote! {
                        #[uniffi::method(name = #field_name_str)]
                        pub fn #uniffi_method_name(&self) -> Result<#projected_type, ::ankurah::property::PropertyError> {
                            self.#field_name()
                        }
                    }
                }
            })
            .collect()
    }

    /// Generate model-scoped WASM wrappers for custom active types.
    ///
    /// "Custom" = NOT in `provided_wrapper_types` (e.g., `LWW<Ref<Session>>`).
    /// Scoped as `Connection_LWWRefSession` to prevent symbol collisions.
    pub fn custom_active_type_wrappers(&self) -> Vec<proc_macro2::TokenStream> {
        let mut wrappers = Vec::new();
        let mut seen_wrappers = std::collections::HashSet::new();
        let model_name = self.name.to_string();

        for field in self.active_fields().iter() {
            if let Some(backend_desc) = self.backend_registry.resolve_active_type(field) {
                // Only generate wrapper for custom types (not provided by backend)
                if !backend_desc.is_provided_type() {
                    // Use model-scoped name to avoid collisions across models
                    let wrapper_name = backend_desc.wrapper_type_name_for_model(&model_name);
                    // Avoid generating duplicate wrappers for same type within this model
                    if !seen_wrappers.contains(&wrapper_name) {
                        seen_wrappers.insert(wrapper_name);
                        let wrapper = backend_desc.wasm_wrapper("external", Some(&model_name));
                        wrappers.push(wrapper);
                    }
                }
            }
        }

        wrappers
    }

    /// Generate WASM getters for Mutable - returns active type wrappers (e.g., `Connection_LWWRefSession`).
    ///
    /// Unlike View getters (which return projected values), these return the wrapper itself
    /// so callers can use `.get()` and `.set()`.
    pub fn mutable_wasm_getters(&self) -> Vec<proc_macro2::TokenStream> {
        let mut getter_methods = Vec::new();
        let model_name = self.name.to_string();

        for field in self.active_fields().iter() {
            let field_name = field.ident.as_ref().unwrap();

            if let Some(backend_desc) = self.backend_registry.resolve_active_type(field) {
                // For provided types, use the standard path; for custom types, use model-scoped name
                let wrapper_type_path = if backend_desc.is_provided_type() {
                    backend_desc.wrapper_type_path("local")
                } else {
                    // Custom type: use model-scoped wrapper name
                    backend_desc.wrapper_type_name_for_model(&model_name)
                };

                // Parse the wrapper type path as a syn::Type
                let wrapper_type: syn::Type = match syn::parse_str(&wrapper_type_path) {
                    Ok(ty) => ty,
                    Err(_) => {
                        // Fallback: just use the wrapper name as an identifier
                        let wrapper_name = if backend_desc.is_provided_type() {
                            backend_desc.wrapper_type_name()
                        } else {
                            backend_desc.wrapper_type_name_for_model(&model_name)
                        };
                        syn::parse_str(&wrapper_name).unwrap_or_else(|_| {
                            syn::Type::Path(syn::TypePath { qself: None, path: syn::Path::from(quote::format_ident!("UnknownWrapper")) })
                        })
                    }
                };

                // Generate the WASM getter method name (prefixed to avoid conflicts)
                let wasm_method_name = quote::format_ident!("wasm_{}", field_name);

                let getter_method = quote::quote! {
                    #[wasm_bindgen(wasm_bindgen = ::ankurah::derive_deps::wasm_bindgen, getter, js_name = #field_name)]
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
                "::ankurah::property::value::lww::LWWi32",
                "::ankurah::property::value::lww::LWWi32",
                "::ankurah::property::value::lww::LWWi32",
                "::ankurah::property::value::lww::LWWString",
                "::ankurah::property::value::lww::LWWString",
                "::ankurah::property::value::yrs::YrsStringString",
                "::ankurah::property::value::yrs::YrsStringString",
                "LWWComplex", // Wrapper type for Complex is assumed to be in scope, generated by impl_wrapper_type!()
            ]
        );
    }
}
