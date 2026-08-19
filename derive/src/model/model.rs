use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Generate the Model trait implementation
pub fn model_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let base = model.base();
    let name = model.name();
    let view_name = model.view_name();
    let mutable_name = model.mutable_name();
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

    // RefWrapper associated type for WASM builds. A `no_ffi` model has no
    // wasm binding layer to supply one, so it gets a plain newtype: the
    // associated type is satisfied and nothing is exported.
    let ref_name = format_ident!("{}Ref", name);
    let ref_wrapper_type = if cfg!(feature = "wasm") {
        quote! {
            type RefWrapper = #ref_name;
        }
    } else {
        quote! {}
    };
    let internal_ref_wrapper = if cfg!(feature = "wasm") && model.no_ffi() {
        quote! {
            pub struct #ref_name(#base::property::Ref<#name>);
            impl From<#base::property::Ref<#name>> for #ref_name {
                fn from(r: #base::property::Ref<#name>) -> Self { Self(r) }
            }
            impl From<#ref_name> for #base::property::Ref<#name> {
                fn from(w: #ref_name) -> Self { w.0 }
            }
        }
    } else {
        quote! {}
    };

    quote! {
        #internal_ref_wrapper
        impl #base::model::Model for #name {
            type View = #view_name;
            type Mutable = #mutable_name;
            #ref_wrapper_type
            #schema_method
            fn initialize_new_entity(
                &self,
                provisional: &mut #base::entity::ProvisionalEntity,
                model_id: #base::proto::ModelId,
                epoch: #base::schema::SchemaEpoch,
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
