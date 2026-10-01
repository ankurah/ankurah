use proc_macro2::TokenStream;
use quote::quote;

/// Generate the Model trait implementation
pub fn model_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let base = model.base();
    let name = model.name();
    let view_name = model.view_name();
    let mutable_name = model.mutable_name();
    let active_field_names = model.active_field_names();
    let active_field_indices: Vec<syn::Index> = (0..active_field_names.len()).map(syn::Index::from).collect();
    let active_field_types = match model.active_field_types() {
        Ok(types) => types,
        Err(e) => return e.into_compile_error(),
    };

    // Shared by registration and accessors inside this model's private derive module.
    let schema = match crate::model::schema::schema_impl(model) {
        Ok(tokens) => tokens,
        Err(e) => return e.into_compile_error(),
    };

    quote! {
        #schema

        impl #base::model::Model for #name {
            type View = #view_name;
            type Mutable = #mutable_name;

            fn descriptor() -> &'static #base::schema::ModelStructDescriptor { &__ANKURAH_MODEL_SCHEMA }

            fn initialize_new_entity(
                &self,
                entity: &#base::entity::LocalTrxEntity,
                model_id: #base::proto::ModelId,
                epoch: #base::schema::SystemEpoch,
            ) -> Result<(), #base::property::PropertyError> {
                entity.add_membership(model_id)?;
                use #base::property::InitializeWith;
                #(
                    <#active_field_types>::initialize_with(
                        entity,
                        __ANKURAH_MODEL_PROPERTIES[#active_field_indices].resolved_id(epoch)?,
                        &self.#active_field_names,
                    );
                )*
                Ok(())
            }
        }
    }
}
