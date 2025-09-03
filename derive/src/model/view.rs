use proc_macro2::TokenStream;
use quote::quote;

/// Generate the View struct and all its implementations
pub fn view_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let view_name = model.view_name();
    let name = model.name();
    let mutable_name = model.mutable_name();
    let ephemeral_field_visibility = model.ephemeral_field_visibility();
    let ephemeral_field_names = model.ephemeral_field_names();
    let ephemeral_field_types = model.ephemeral_field_types();
    let active_field_names = model.active_field_names();
    let projected_field_types = model.projected_field_types();
    let projected_field_types_turbofish = model.projected_field_types_turbofish();
    let active_field_types_turbofish = match model.active_field_types_turbofish() {
        Ok(types) => types,
        Err(_) => return quote! { compile_error!("Failed to generate active field types"); },
    };
    let active_field_name_strs = model.active_field_name_strs();

    let hygeine_module = quote::format_ident!("__ankurah_derive_view_impl_{}", view_name);
    let (use_statements, struct_attributes, impl_attributes, getter_attributes) = if cfg!(feature = "wasm") {
        (
            quote! {
                use ::ankurah::derive_deps::wasm_bindgen::prelude::*;
            },
            quote! { #[wasm_bindgen] },
            quote! { #[wasm_bindgen] },
            quote! { #[wasm_bindgen(getter)] },
        )
    } else {
        (quote! {}, quote! {}, quote! {}, quote! {})
    };

    quote! {
        mod #hygeine_module {
            use super::*;
            #use_statements

            #struct_attributes
            #[derive(Clone, Debug, PartialEq)]
            pub struct #view_name {
                entity: ::ankurah::entity::Entity,
                #(
                    #ephemeral_field_visibility #ephemeral_field_names: #ephemeral_field_types,
                )*
            }

            impl ::ankurah::model::View for #view_name {
                type Model = #name;
                type Mutable<'rec> = #mutable_name<'rec>;

                // THINK ABOUT: to_model is the only thing that forces a clone requirement
                // Even though most Models will be clonable, maybe we shouldn't force it?
                // Also: nothing seems to be using this. Maybe it could be opt in
                fn to_model(&self) -> Result<Self::Model, ankurah::property::PropertyError> {
                    Ok(#name {
                        #( #active_field_names: self.#active_field_names()?, )*
                        #( #ephemeral_field_names: self.#ephemeral_field_names.clone(), )*
                    })
                }

                fn entity(&self) -> &::ankurah::entity::Entity {
                    &self.entity
                }

                fn from_entity(entity: ::ankurah::entity::Entity) -> Self {
                    use ::ankurah::model::View;
                    assert_eq!(&Self::collection(), entity.collection());
                    #view_name {
                        entity,
                        #(
                            #ephemeral_field_names: Default::default(),
                        )*
                    }
                }
            }

            impl ::ankurah::signals::Signal for #view_name {
                fn listen(&self, listener: ::ankurah::signals::broadcast::Listener) -> ::ankurah::signals::broadcast::ListenerGuard {
                    self.entity.broadcast().reference().listen(listener)
                }
                fn broadcast_id(&self) -> ::ankurah::signals::broadcast::BroadcastId {
                    self.entity.broadcast().id()
                }
            }

            impl ::ankurah::signals::Subscribe<#view_name> for #view_name {
                fn subscribe<F>(&self, listener: F) -> ::ankurah::signals::SubscriptionGuard
                where
                    F: ::ankurah::signals::subscribe::IntoSubscribeListener<#view_name>,
                {
                    ::ankurah::core::model::js_view_subscribe(self, listener)
                }
            }

            // TODO wasm-bindgen this
            impl #view_name {
                pub fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx ankurah::transaction::Transaction) -> Result<#mutable_name<'rec>, ankurah::policy::AccessDenied> {
                    use ::ankurah::model::View;
                    // TODO - get rid of this in favor of directly cloning the entity of the ModelView struct
                    trx.edit::<#name>(&self.entity)
                }
            }

            #impl_attributes
            impl #view_name {
                pub fn id(&self) -> ankurah::proto::EntityId {
                    self.entity.id().clone()
                }
                /// Manually track this View in the current observer
                pub fn track(&self) {
                    ::ankurah::signals::CurrentObserver::track(self);
                }
                #(
                    #getter_attributes
                    pub fn #active_field_names(&self) -> Result<#projected_field_types, ankurah::property::PropertyError> {
                        use ankurah::property::{FromActiveType, FromEntity};
                        ::ankurah::signals::CurrentObserver::track(self);
                        let active_result = #active_field_types_turbofish::from_entity(#active_field_name_strs.into(), &self.entity);
                        #projected_field_types_turbofish::from_active(active_result)
                    }
                )*
            }

            impl<'a> Into<ankurah::proto::EntityId> for &'a #view_name {
                fn into(self) -> ankurah::proto::EntityId {
                    self.entity.id()
                }
            }
        }
        pub use #hygeine_module::*;
    }
}
