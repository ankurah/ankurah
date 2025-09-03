use proc_macro2::TokenStream;
use quote::quote;

/// Generate the Mutable struct and all its implementations
pub fn mutable_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let mutable_name = model.mutable_name();
    let name = model.name();
    let view_name = model.view_name();
    let active_field_visibility = model.active_field_visibility();
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

    quote! {
        #[derive(Debug)]
        pub struct #mutable_name<'rec> {
            pub entity: &'rec ::ankurah::entity::Entity,
            #(#active_field_visibility #active_field_names: #active_field_types,)*
        }

        // TODO - wasm-bindgen this - ah right, we need to remove the lifetime
        impl<'rec> ::ankurah::model::Mutable<'rec> for #mutable_name<'rec> {
            type Model = #name;
            type View = #view_name;

            fn entity(&self) -> &::ankurah::entity::Entity {
                self.entity
            }

            fn id(&self) -> ::ankurah::proto::EntityId {
                self.entity.id()
            }

            fn new(entity: &'rec ::ankurah::entity::Entity) -> Self {
                use ankurah::{
                    model::Mutable,
                    property::FromEntity,
                };
                assert_eq!(entity.collection(), &Self::collection());
                Self {
                    entity,
                    #( #active_field_names: #active_field_types_turbofish::from_entity(#active_field_name_strs.into(), entity), )*
                }
            }
        }

        impl<'rec> #mutable_name<'rec> {
            #(
                pub fn #active_field_names(&self) -> &#active_field_types {
                    &self.#active_field_names
                }
            )*
        }

        impl<'a, 'rec> Into<ankurah::proto::EntityId> for &'a #mutable_name<'rec> {
            fn into(self) -> ankurah::proto::EntityId {
                self.entity.id()
            }
        }
    }
}
