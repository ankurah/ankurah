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
        Err(e) => return e.into_compile_error(),
    };
    let active_field_name_strs = model.active_field_name_strs();

    // WASM field getters (conditionally generated)
    #[cfg(feature = "wasm")]
    let wasm_field_getters_impl = {
        let wasm_getters = model.wasm_getters();
        quote! {
            #[wasm_bindgen]
            impl #view_name {
                #(#wasm_getters)*
            }
        }
    };
    #[cfg(not(feature = "wasm"))]
    let wasm_field_getters_impl = quote! {};

    let (struct_attributes, impl_attributes, id_getter_attr, wasm_edit_impl) = if cfg!(feature = "wasm") {
        let subscribe_ts = format!(
            r#"export interface {view_name} {{
    subscribe(callback: () => void): SubscriptionGuard;
}}"#
        );

        (
            quote! { #[wasm_bindgen] },
            quote! { #[wasm_bindgen] },
            quote! { #[wasm_bindgen(getter)] },
            quote! {
                #[wasm_bindgen(typescript_custom_section)]
                const TS_VIEW_SUBSCRIBE: &'static str = #subscribe_ts;

                #[wasm_bindgen]
                impl #view_name {
                    /// Edit this entity in a transaction (WASM version - returns owned Mutable)
                    #[wasm_bindgen(js_name = "edit")]
                    pub fn edit_wasm(&self, trx: &ankurah::transaction::Transaction) -> Result<#mutable_name, ::wasm_bindgen::JsValue> {
                        use ::ankurah::model::View;
                        match trx.edit::<#name>(&self.entity) {
                            Ok(mutable_borrow) => {
                                // Extract the core mutable from the borrow wrapper
                                Ok(mutable_borrow.into_core())
                            }
                            Err(e) => Err(::wasm_bindgen::JsValue::from(e.to_string()))
                        }
                    }

                    #[wasm_bindgen(skip_typescript, js_name = "subscribe")]
                    pub fn subscribe_wasm(&self, callback: ::ankurah::derive_deps::js_sys::Function) -> ::ankurah::signals::SubscriptionGuard {

                        let callback = ::ankurah::derive_deps::send_wrapper::SendWrapper::new(callback);
                        ::ankurah::core::model::view_subscribe_no_clone(self, move |_| {
                            let _ = callback.call0(
                                &::ankurah::derive_deps::wasm_bindgen::JsValue::NULL,
                            );
                        })
                    }
                }
            },
        )
    } else {
        (quote! {}, quote! {}, quote! {}, quote! {})
    };

    let expanded = quote! {

            #struct_attributes
            #[derive(Clone, Debug, PartialEq)]
            pub struct #view_name {
                entity: ::ankurah::entity::Entity,
                #(
                    #ephemeral_field_visibility #ephemeral_field_names: #ephemeral_field_types,
                )*
            }

            impl From<#view_name> for ::ankurah::ankql::ast::Expr {
                fn from(view: #view_name) -> ::ankurah::ankql::ast::Expr {
                    view.entity.id().into()
                }
            }

            impl From<&#view_name> for ::ankurah::ankql::ast::Expr {
                fn from(view: &#view_name) -> ::ankurah::ankql::ast::Expr {
                    view.entity.id().into()
                }
            }

            impl ::ankurah::model::View for #view_name {
                type Model = #name;
                type Mutable = #mutable_name;

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
                fn listen(&self, listener: ::ankurah::signals::signal::Listener) -> ::ankurah::signals::signal::ListenerGuard {
                    self.entity.broadcast().reference().listen(listener).into()
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
                    ::ankurah::core::model::view_subscribe(self, listener)
                }
            }

            impl #view_name {
                /// Edit this entity in a transaction (Rust version - returns MutableBorrow with lifetime)
                pub fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx ankurah::transaction::Transaction) -> Result<::ankurah::model::MutableBorrow<'rec, #mutable_name>, ankurah::policy::AccessDenied> {
                    use ::ankurah::model::View;
                    // TODO - get rid of this in favor of directly cloning the entity of the ModelView struct
                    trx.edit::<#name>(&self.entity)
                }
            }

            #wasm_edit_impl

            #impl_attributes
            impl #view_name {
                #id_getter_attr
                pub fn id(&self) -> ankurah::proto::EntityId {
                    self.entity.id().clone()
                }

                /// Manually track this View in the current observer
                pub fn track(&self) {
                    ::ankurah::signals::CurrentObserver::track(self);
                }
            }

            // Rust-only impl block: r() and all field getters (no wasm_bindgen)
            impl #view_name {
                /// Get a typed reference to this entity.
                ///
                /// This is useful when creating related entities that reference this one:
                /// ```ignore
                /// trx.create(&Album {
                ///     name: "OK Computer".to_string(),
                ///     artist: artist_view.r(),
                /// })
                /// ```
                pub fn r(&self) -> ::ankurah::property::Ref<#name> {
                    ::ankurah::property::Ref::new(self.entity.id())
                }

                #(
                    pub fn #active_field_names(&self) -> Result<#projected_field_types, ankurah::property::PropertyError> {
                        use ankurah::property::{FromActiveType, FromEntity};
                        ::ankurah::signals::CurrentObserver::track(self);
                        let active_result = #active_field_types_turbofish::from_entity(#active_field_name_strs.into(), &self.entity);
                        #projected_field_types_turbofish::from_active(active_result)
                    }
                )*
            }

            #wasm_field_getters_impl

            impl<'a> Into<ankurah::proto::EntityId> for &'a #view_name {
                fn into(self) -> ankurah::proto::EntityId {
                    self.entity.id()
                }
            }

            // From<&View> for Ref<Model> is implemented via blanket impl in entity_ref.rs

            impl From<#view_name> for ::ankurah::property::Ref<#name> {
                fn from(view: #view_name) -> Self {
                    ::ankurah::property::Ref::new(view.entity.id())
                }
            }
    };
    expanded
}
