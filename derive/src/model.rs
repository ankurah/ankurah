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

    let field_visibility = fields.iter().map(|f| &f.vis).collect::<Vec<_>>();

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
            type ScopedRecord<'rec> = #scoped_record_name<'rec>;
            fn bucket_name() -> &'static str {
                #name_str
            }
            fn create_record(&self, id: ::ankurah_core::derive_deps::ankurah_proto::ID) -> ankurah_core::model::RecordInner {
                use ankurah_core::property::InitializeWith;

                let backends = ankurah_core::property::Backends::new();
                #(
                    #field_active_values::initialize_with(&backends, #field_name_strs.into(), &self.#field_names);
                )*
                ankurah_core::model::RecordInner::create(
                    id,
                    #name_str,
                    backends
                )
            }
        }

        use ankurah_core::derive_deps::wasm_bindgen::prelude::*;
        #[wasm_bindgen]
        #[derive(Debug)]
        pub struct #record_name {
            inner: std::sync::Arc<ankurah_core::model::RecordInner>,
        }

        impl ankurah_core::model::Record for #record_name {
            type Model = #name;
            type ScopedRecord<'rec> = #scoped_record_name<'rec>;

            fn to_model(&self) -> Self::Model {
                #name {
                    #( #field_names: self.#field_names(), )*
                }
            }

            fn record_inner(&self) -> &std::sync::Arc<ankurah_core::model::RecordInner> {
                &self.inner
            }

            fn from_record_inner(inner: std::sync::Arc<ankurah_core::model::RecordInner>) -> Self {
                use ankurah_core::model::Record;
                assert_eq!(Self::bucket_name(), inner.bucket_name());
                #record_name {
                    inner: inner,
                }
            }
        }

        // TODO wasm-bindgen this
        impl #record_name {
            pub async fn edit<'rec, 'trx: 'rec>(&self, trx: &'trx ankurah_core::transaction::Transaction) -> Result<#scoped_record_name<'rec>, ankurah_core::error::RetrievalError> {
                use ankurah_core::model::Record;
                trx.edit::<#name>(self.id()).await
            }
        }

        #[wasm_bindgen]
        impl #record_name {
            pub fn id(&self) -> ankurah_core::derive_deps::ankurah_proto::ID {
              self.inner.id.clone()
            }
            #(
                #field_visibility fn #field_names(&self) -> #field_types {
                    use ankurah_core::property::ProjectedValue;
                    #field_active_values::from_backends(#field_name_strs.into(), self.inner.backends()).projected()
                }
            )*
        }

        #[derive(Debug)]
        pub struct #scoped_record_name<'rec> {
            // TODO: Invert Record and ScopedRecord so that ScopedRecord has an internal Record, and Record has id,backends, field projections
            //parent: RecordParent<#name>,
            inner: &'rec std::sync::Arc<ankurah_core::model::RecordInner>,

            // Field projections
            #(#field_visibility #field_names: #field_active_values,)*
            // TODO: Add fields for tracking changes and operation count
        }

        impl<'rec> ankurah_core::model::ScopedRecord<'rec> for #scoped_record_name<'rec> {
            type Model = #name;
            type Record = #record_name;

            fn record_inner(&self) -> &std::sync::Arc<ankurah_core::model::RecordInner> {
                &self.inner
            }

            fn new(inner: &'rec std::sync::Arc<ankurah_core::model::RecordInner>) -> Self {
                use ankurah_core::model::ScopedRecord;
                assert_eq!(inner.bucket_name(), Self::bucket_name());
                #(
                    let #field_names_avoid_conflicts = #field_active_values::from_backends(#field_name_strs.into(), inner.backends());
                )*
                Self {
                    inner: inner,
                    #( #field_names: #field_names_avoid_conflicts, )*
                }
            }
        }

        impl<'rec> #scoped_record_name<'rec> {
            #(
                #field_visibility fn #field_names(&self) -> &#field_active_values {
                    &self.#field_names
                }
            )*
        }

        impl<'a> Into<ankurah_core::derive_deps::ankurah_proto::ID> for &'a #record_name {
            fn into(self) -> ankurah_core::derive_deps::ankurah_proto::ID {
                ankurah_core::model::Record::id(self)
            }
        }

        impl<'a, 'rec> Into<ankurah_core::derive_deps::ankurah_proto::ID> for &'a #scoped_record_name<'rec> {
            fn into(self) -> ankurah_core::derive_deps::ankurah_proto::ID {
                ankurah_core::model::ScopedRecord::id(self)
            }
        }
    }
    .into();

    expanded
}
