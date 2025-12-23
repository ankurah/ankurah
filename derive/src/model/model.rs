use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Generate the Model trait implementation
pub fn model_impl(model: &crate::model::description::ModelDescription) -> TokenStream {
    let name = model.name();
    let view_name = model.view_name();
    let mutable_name = model.mutable_name();
    let collection_str = model.collection_str();
    let active_field_names = model.active_field_names();
    let active_field_name_strs = model.active_field_name_strs();
    let active_field_types_turbofish = match model.active_field_types_turbofish() {
        Ok(types) => types,
        Err(e) => return e.into_compile_error(),
    };

    // RefWrapper associated type for WASM builds
    let ref_wrapper_type = if cfg!(feature = "wasm") {
        let ref_name = format_ident!("{}Ref", name);
        quote! {
            type RefWrapper = #ref_name;
        }
    } else {
        quote! {}
    };

    quote! {
        impl ::ankurah::model::Model for #name {
            type View = #view_name;
            type Mutable = #mutable_name;
            #ref_wrapper_type
            fn collection() -> ankurah::proto::CollectionId {
                #collection_str.into()
            }
            fn initialize_new_entity(&self, entity: &::ankurah::entity::Entity) {
                use ::ankurah::property::InitializeWith;
                #(
                    #active_field_types_turbofish::initialize_with(&entity, #active_field_name_strs.into(), &self.#active_field_names);
                )*
            }
        }
    }
}
