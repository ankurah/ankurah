use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields};

// Consider changing this to an attribute macro so we can modify the input struct? For now, users will have to also derive Debug.
pub fn derive_model_impl(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let name_str = name.to_string().to_lowercase();
    let record_name = format_ident!("{}Record", name);
    let scoped_record_name = format_ident!("{}ScopedRecord", name);

    let fields = match input.data {
        Data::Struct(ref data) => match data.fields {
            Fields::Named(ref fields) => &fields.named,
            _ => panic!("Only named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    let active_value_ident = format_ident!("active_value");
    let field_active_values = fields
        .iter()
        .map(|f| {
            let active_value = f
                .attrs
                .iter()
                .find(|attr| attr.path().get_ident() == Some(&active_value_ident));
            match active_value {
                Some(active_value) => active_value.parse_args::<syn::Ident>().unwrap(),
                // TODO: Better error, should include which field ident and an example on how to use.
                None => panic!("All fields need an active value attribute"),
            }
        })
        .collect::<Vec<_>>();

    let field_names = fields.iter().map(|f| &f.ident).collect::<Vec<_>>();
    let field_names_avoid_conflicts = fields
        .iter()
        .enumerate()
        .map(|(index, f)| match &f.ident {
            Some(ident) => format_ident!("field_{}", ident),
            None => format_ident!("field_{}", index),
        })
        .collect::<Vec<_>>();
    let field_name_strs = fields
        .iter()
        .map(|f| f.ident.as_ref().unwrap().to_string().to_lowercase())
        .collect::<Vec<_>>();
    let field_types = fields.iter().map(|f| &f.ty).collect::<Vec<_>>();
    let field_indices = fields
        .iter()
        .enumerate()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();

    let expanded: proc_macro::TokenStream = quote! {
        impl ankurah_core::model::Model for #name {
            type Record = #record_name;
            type ScopedRecord = #scoped_record_name;
            fn bucket_name() -> &'static str {
                #name_str
            }
            fn new_scoped_record(id: ankurah_core::model::ID, model: &Self) -> Self::ScopedRecord {
                use ankurah_core::property::InitializeWith;

                let backends = ankurah_core::property::Backends::new();
                #(
                    let #field_names_avoid_conflicts = #field_active_values::initialize_with(&backends, #field_name_strs, &model.#field_names);
                )*
                #scoped_record_name {
                    id: id,
                    backends: backends,
                    #( #field_names: #field_names_avoid_conflicts, )*
                }
            }
        }

        #[derive(Debug)]
        pub struct #record_name {
            scoped: #scoped_record_name,
        }

        impl ankurah_core::model::Record for #record_name {
            type Model = #name;
            type ScopedRecord = #scoped_record_name;

            fn id(&self) -> ankurah_core::model::ID {
                use ankurah_core::model::ScopedRecord;
                self.scoped.id()
            }

            fn to_model(&self) -> Self::Model {
                #name {
                    #( #field_names: self.#field_names(), )*
                }
            }
        }

        impl #record_name {
            pub fn new(node: &std::sync::Arc<ankurah_core::Node>, model: &#name) -> Self {
                let next_id = node.next_id();
                Self {
                    scoped: <#name as ankurah_core::Model>::new_scoped_record(next_id, model),
                }
            }

            pub fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx ankurah_core::transaction::Transaction) -> Result<&'rec #scoped_record_name, ankurah_core::error::RetrievalError> {
                use ankurah_core::model::Record;
                trx.edit::<#name>(self.id())
            }

            #(
                pub fn #field_names(&self) -> <#field_active_values as ankurah_core::property::ProjectedValue>::Projected {
                    let active = self.scoped.#field_names();
                    <#field_active_values as ankurah_core::property::ProjectedValue>::projected(&active)
                }
            )*
        }

        #[derive(Debug)]
        pub struct #scoped_record_name {
            id: ankurah_core::model::ID,
            backends: ankurah_core::property::Backends,

            // Field projections
            #(pub #field_names: #field_active_values,)*
            // TODO: Add fields for tracking changes and operation count
        }

        impl ankurah_core::model::ScopedRecord for #scoped_record_name {
            fn as_dyn_any(&self) -> &dyn std::any::Any {
                self as &dyn std::any::Any
            }

            fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn std::any::Any + std::marker::Send + std::marker::Sync> {
                self as Arc<dyn std::any::Any + std::marker::Send + std::marker::Sync>
            }

            fn id(&self) -> ankurah_core::model::ID {
                self.id
            }

            fn bucket_name(&self) -> &'static str {
                #name_str
            }

            fn record_state(&self) -> ankurah_core::storage::RecordState {
                ankurah_core::storage::RecordState::from_backends(&self.backends)
            }

            fn from_record_state(
                id: ankurah_core::model::ID,
                record_state: &ankurah_core::storage::RecordState,
            ) -> std::result::Result<Self, ankurah_core::error::RetrievalError>
            where
                Self: Sized,
            {
                let backends = ankurah_core::property::Backends::from_state_buffers(&record_state)?;
                #(
                    let #field_names_avoid_conflicts = #field_active_values::from_backends(#field_name_strs, &backends);
                )*
                Ok(Self {
                    id: id,
                    backends: backends,
                    #( #field_names: #field_names_avoid_conflicts, )*
                })
            }

            fn get_record_event(&self) -> Option<ankurah_core::property::backend::RecordEvent> {
                use ankurah_core::property::backend::PropertyBackend;
                let mut record_event = ankurah_core::property::backend::RecordEvent::new(self.id());
                record_event.extend(
                    ankurah_core::property::backend::YrsBackend::property_backend_name(),
                    self.backends.yrs.to_operations(),
                );

                if record_event.is_empty() {
                    None
                } else {
                    Some(record_event)
                }
            }
        }

        impl #scoped_record_name {
            #(
                pub fn #field_names(&self) -> &#field_active_values {
                    &self.#field_names
                }
            )*
        }

        impl<'a> Into<ankurah_core::ID> for &'a #record_name {
            fn into(self) -> ankurah_core::ID {
                self.id()
            }
        }

        impl<'a> Into<ankurah_core::ID> for &'a #scoped_record_name {
            fn into(self) -> ankurah_core::ID {
                self.id()
            }
        }
    }
    .into();

    expanded
}
