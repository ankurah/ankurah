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

    let field_visibility = fields
        .iter()
        .map(|f|  &f.vis)
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
    /*let field_indices = fields
        .iter()
        .enumerate()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();*/

    let expanded: proc_macro::TokenStream = quote! {
        impl ankurah_core::model::Model for #name {
            type Record = #record_name;
            type ScopedRecord = #scoped_record_name;
            fn bucket_name() -> &'static str {
                #name_str
            }
            fn new_record_inner(id: ankurah_core::ID, model: &Self) -> Self::ScopedRecord {
                let backends = ankurah_core::property::Backends::new();
                ankurah_core::model::RecordInner {
                    id: id,
                    backends: backends,
                }
            }
        }

        #[derive(Debug)]
        pub struct #record_name {
            pub inner: Arc<ankurah_core::model::RecordInner>,
        }

        impl ankurah_core::model::Record for #record_name {
            type Model = #name;
            type ScopedRecord = #scoped_record_name;

            fn id(&self) -> ankurah_core::ID {
                use ankurah_core::model::ScopedRecord;
                self.scoped.id()
            }

            fn to_model(&self) -> Self::Model {
                #name {
                    #( #field_names: self.#field_names(), )*
                }
            }

            fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx ankurah_core::transaction::Transaction) -> Result<&'rec #scoped_record_name, ankurah_core::error::RetrievalError> {
                use ankurah_core::model::Record;
                trx.edit::<#name>(self.id())
            }

            fn from_record_inner(inner: Arc<ankurah_core::model::RecordInner>) -> Self {
                #record_name {
                    inner: inner,
                }
            }
        }

        impl #record_name {
            #(
                #field_visibility fn #field_names(&self) -> #field_types {
                    let active = self.scoped.#field_names();
                    <#field_active_values as ankurah_core::property::ProjectedValue>::projected(&active)
                }
            )*
        }

        #[derive(Debug)]
        pub struct #scoped_record_name {
            // TODO: Invert Record and ScopedRecord so that ScopedRecord has an internal Record, and Record has id,backends, field projections
            // parent: RecordParent<#name>,

            inner: RecordInner,

            // Field projections
            #(#field_visibility #field_names: #field_active_values,)*
            // TODO: Add fields for tracking changes and operation count
        }

        impl ankurah_core::model::ScopedRecord for #scoped_record_name {
            fn as_dyn_any(&self) -> &dyn std::any::Any {
                self as &dyn std::any::Any
            }

            fn as_arc_dyn_any(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + std::marker::Send + std::marker::Sync> {
                self as std::sync::Arc<dyn std::any::Any + std::marker::Send + std::marker::Sync>
            }

            fn from_record_inner(inner: std::sync::Arc<ankurah_core::model::RecordInner>) -> Self {
                assert_eq!(inner.bucket_name, #name_str);
                #(
                    let #field_names_avoid_conflicts = #field_active_values::from_backends(#field_name_strs.into(), &inner.backends);
                )*
                Self {
                    inner: inner,
                    #( #field_names: #field_names_avoid_conflicts, )*
                }
            }

            fn record_inner(&self) -> std::sync::Arc<ankurah_core::model::RecordInner> {
                self.inner.clone()
            }

            fn id(&self) -> ankurah_core::ID {
                self.inner.id
            }

            fn bucket_name(&self) -> &'static str {
                #name_str
            }
            
            fn backends(&self) -> &ankurah_core::property::Backends {
                &self.inner.backends
            }

            fn from_record_state(
                id: ankurah_core::ID,
                record_state: &ankurah_core::storage::RecordState,
            ) -> Result<Self, ankurah_core::error::RetrievalError>
            where
                Self: Sized,
            {
                let backends = ankurah_core::property::Backends::from_state_buffers(&record_state)?;
                Ok(Self::from_backends(id, backends))
            }

            fn get_record_event(&self) -> Option<ankurah_core::property::backend::RecordEvent> {
                use ankurah_core::property::backend::PropertyBackend;
                let mut record_event = ankurah_core::property::backend::RecordEvent {
                    id: self.id(),
                    bucket_name: self.bucket_name(),
                    operations: self.backends.to_operations(),
                };

                if record_event.is_empty() {
                    None
                } else {
                    Some(record_event)
                }
            }
        }

        impl #scoped_record_name {
            #(
                #field_visibility fn #field_names(&self) -> &#field_active_values {
                    &self.#field_names
                }
            )*
        }

        impl<'a> Into<ankurah_core::ID> for &'a #record_name {
            fn into(self) -> ankurah_core::ID {
                ankurah_core::model::Record::id(self)
            }
        }

        impl<'a> Into<ankurah_core::ID> for &'a #scoped_record_name {
            fn into(self) -> ankurah_core::ID {
                ankurah_core::model::ScopedRecord::id(self)
            }
        }
    }
    .into();

    expanded
}
