use proc_macro2::TokenStream;
use quote::quote;

/// Generate the Mutable struct and all its implementations
pub fn mutable_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let base = model.base();
    let mutable_name = model.mutable_name();
    let name = model.name();
    let view_name = model.view_name();
    let active_field_names = model.active_field_names();
    let active_field_indices: Vec<syn::Index> = (0..active_field_names.len()).map(syn::Index::from).collect();
    let active_field_types = match model.active_field_types() {
        Ok(types) => types,
        Err(_) => return quote! { compile_error!("Failed to generate active field types"); },
    };
    let active_field_types_turbofish = match model.active_field_types_turbofish() {
        Ok(types) => types,
        Err(_) => return quote! { compile_error!("Failed to generate active field types turbofish"); },
    };

    // FFI attributes for the struct and fields. A `no_ffi` model skips the
    // binding layers entirely, matching lib.rs's gating of wasm_impl: its
    // expansion has no bindgen imports in scope.
    #[cfg(feature = "wasm")]
    let (struct_attributes, field_attributes) = if model.no_ffi() { (quote! {}, quote! {}) } else { super::wasm::mutable_attributes() };

    #[cfg(all(feature = "uniffi", not(feature = "wasm")))]
    let (struct_attributes, field_attributes) = if model.no_ffi() { (quote! {}, quote! {}) } else { super::uniffi::mutable_attributes() };

    #[cfg(not(any(feature = "wasm", feature = "uniffi")))]
    let (struct_attributes, field_attributes) = (quote! {}, quote! {});

    // Generate WASM getter methods and wrapper definitions for custom types
    let (wasm_getter_impl, wasm_custom_wrappers) = if cfg!(feature = "wasm") && !model.no_ffi() {
        let getter_methods = model.mutable_wasm_getters();
        let custom_wrappers = model.custom_active_type_wrappers();
        (
            quote! {
                #[wasm_bindgen]
                impl #mutable_name {
                    #(#getter_methods)*
                }
            },
            quote! {
                #(#custom_wrappers)*
            },
        )
    } else {
        (quote! {}, quote! {})
    };

    let expanded = quote! {
        // Core Mutable struct (no lifetime, owned Entity)
        #struct_attributes
        #[derive(Debug)]
        pub struct #mutable_name {
            #field_attributes
            pub entity: #base::entity::Entity,
        }

        impl #base::model::Mutable for #mutable_name {
            type Model = #name;
            type View = #view_name;

            fn entity(&self) -> &#base::entity::Entity {
                &self.entity
            }

            fn new(entity: #base::entity::Entity) -> Self {
                assert_eq!(entity.collection(), &Self::collection());
                Self { entity }
            }
        }

        impl #mutable_name {
            pub fn id(&self) -> #base::proto::EntityId {
                self.entity.id()
            }

            #(
                pub fn #active_field_names(&self) -> Result<#active_field_types, #base::property::PropertyError> {
                    use #base::property::FromEntity;
                    let property = <#name as #base::model::Model>::descriptor().resolved_field(#active_field_indices, &self.entity)?;
                    Ok(#active_field_types_turbofish::from_entity(property, &self.entity))
                }
            )*
        }

        impl<'a> Into<#base::proto::EntityId> for &'a #mutable_name {
            fn into(self) -> #base::proto::EntityId {
                self.entity.id()
            }
        }

        // WASM wrapper types for custom types (auto-generated for types not in provided_wrapper_types)
        #wasm_custom_wrappers

        // WASM getter methods implementation (only generated when wasm feature is enabled)
        #wasm_getter_impl

    };

    expanded
}
