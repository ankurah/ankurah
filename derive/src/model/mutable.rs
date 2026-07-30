//! Generation of a model's typed transactional Mutable projection.
//!
//! [`mutable_impl`] emits the Mutable wrapper around an Entity, its typed active
//! property accessors, identity conversion, and optional WASM/UniFFI attributes
//! and wrapper types. The generated object exposes staged transaction state; it
//! does not own commit, authorization, or property-backend semantics. Model
//! metadata and View generation remain in their neighboring generator modules.

use proc_macro2::TokenStream;
use quote::quote;

/// Generate the Mutable struct and all its implementations
pub fn mutable_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let base = model.base();
    let mutable_name = model.mutable_name();
    let name = model.name();
    let view_name = model.view_name();
    // TODO - add this to the accessors
    let _active_field_visibility = model.active_field_visibility();
    let active_field_names = model.active_field_names();
    let active_field_name_strs = model.active_field_name_strs();
    let active_field_types = match model.active_field_types() {
        Ok(types) => types,
        Err(_) => return quote! { compile_error!("Failed to generate active field types"); },
    };
    let active_field_types_turbofish = match model.active_field_types_turbofish() {
        Ok(types) => types,
        Err(_) => return quote! { compile_error!("Failed to generate active field types turbofish"); },
    };

    // FFI attributes for the struct and fields. Models marked `no_ffi` skip
    // the wasm/uniffi binding layers entirely, matching lib.rs's gating of
    // wasm_impl: their expansion has no bindgen imports in scope.
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
                use #base::property::FromEntity;
                assert_eq!(entity.collection(), &Self::collection());
                Self {
                    // #( #active_field_names: #active_field_types_turbofish::from_entity(#active_field_name_strs.into(), &entity), )*
                    entity,
                }
                }
            }

        impl #mutable_name {
            pub fn id(&self) -> #base::proto::EntityId {
                self.entity.id()
            }

            #(
                pub fn #active_field_names(&self) -> #active_field_types {
                    use #base::property::FromEntity;
                    #active_field_types_turbofish::from_entity(#active_field_name_strs.into(), &self.entity)
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
