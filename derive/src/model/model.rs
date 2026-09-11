use proc_macro2::TokenStream;
use quote::quote;

/// Generate the Model trait implementation
pub fn model_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let base = model.base();
    let name = model.name();
    let view_name = model.view_name();
    let mutable_name = model.mutable_name();
    let collection_str = model.collection_str();
    let active_field_names = model.active_field_names();
    let active_field_indices: Vec<syn::Index> = (0..active_field_names.len()).map(syn::Index::from).collect();
    let active_field_types_turbofish = match model.active_field_types_turbofish() {
        Ok(types) => types,
        Err(e) => return e.into_compile_error(),
    };

    // The compiled schema: static ModelStructDescriptor + fn schema().
    let schema_method = match crate::model::schema::schema_impl(model) {
        Ok(tokens) => tokens,
        Err(e) => return e.into_compile_error(),
    };

    quote! {
        impl #base::model::Model for #name {
            type View = #view_name;
            type Mutable = #mutable_name;
            #schema_method
            fn collection() -> #base::proto::CollectionId {
                #collection_str.into()
            }
            fn initialize_new_entity(
                &self,
                provisional: &mut #base::entity::ProvisionalEntity,
                model_id: #base::proto::ModelId,
                epoch: #base::schema::SystemEpoch,
            ) -> Result<(), #base::property::PropertyError> {
                provisional.add_membership(model_id);
                use #base::property::InitializeWith;
                #(
                    #active_field_types_turbofish::initialize_with(
                        &mut *provisional,
                        <Self as #base::model::Model>::descriptor().resolved_field_at(#active_field_indices, epoch)?,
                        &self.#active_field_names,
                    );
                )*
                Ok(())
            }
        }
    }
}
