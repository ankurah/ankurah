// #[cfg(feature = "wasm")]
// mod wasm;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, punctuated::Punctuated, AngleBracketedGenericArguments, Data, DeriveInput, Fields, Type};

// Consider changing this to an attribute macro so we can modify the input struct? For now, users will have to also derive Debug.
pub fn derive_model_impl(stream: TokenStream) -> TokenStream {
    let input = parse_macro_input!(stream as DeriveInput);
    let name = input.ident.clone();
    let collection_str = name.to_string().to_lowercase();
    let view_name = format_ident!("{}View", name);
    let mutable_name = format_ident!("{}Mut", name);

    let clone_derive = if !has_flag(&input.attrs, "no_clone") {
        quote! { #[derive(Clone)] }
    } else {
        quote! {}
    };

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unit => return syn::Error::new_spanned(&name, "Unit structs are not supported").to_compile_error().into(),
            fields => {
                return syn::Error::new_spanned(fields, format!("Only named fields are supported this is a {:#?}", fields))
                    .to_compile_error()
                    .into()
            }
        },
        _ => return syn::Error::new_spanned(&name, "Only structs are supported").to_compile_error().into(),
    };

    let generated = match process_fields(fields) {
        Ok(g) => g,
        Err(e) => return e.to_compile_error().into(),
    };

    let ephemeral_field_names = generated.ephemeral_fields.iter().map(|f| &f.ident).collect::<Vec<_>>();
    let ephemeral_field_types = generated.ephemeral_fields.iter().map(|f| &f.ty).collect::<Vec<_>>();
    let ephemeral_field_visibility = generated.ephemeral_fields.iter().map(|f| &f.vis).collect::<Vec<_>>();

    let wasm_attributes = if cfg!(feature = "wasm") {
        quote! {
            #[::ankurah::derive_deps::wasm_bindgen::prelude::wasm_bindgen]
        }
    } else {
        quote! {}
    };

    // #[cfg(feature = "wasm")]
    // let wasm_impl = wasm::wasm_impl(&name, &view_name);
    // #[cfg(not(feature = "wasm"))]
    let wasm_impl = quote! {};

    let expanded: proc_macro::TokenStream = quote! {
        #wasm_attributes
        #clone_derive
        pub struct #view_name {
            entity: ::ankurah::entity::Entity,
            #(#generated.field_declarations,)*
        }

        pub struct #mutable_name<'rec> {
            pub entity: &'rec ::ankurah::entity::Entity,
        }

        #wasm_impl
        impl ::ankurah::model::Model for #name {
            type View = #view_name;
            type Mutable<'rec> = #mutable_name<'rec>;
            fn collection() -> ankurah::derive_deps::ankurah_proto::CollectionId {
                #collection_str.into()
            }
            fn initialize_new_entity(&self, entity: &::ankurah::entity::Entity) {
                use ::ankurah::property::InitializeWith;
                #(#generated.entity_initializers)*
            }
        }

        impl ::ankurah::model::View for #view_name {
            type Model = #name;
            type Mutable<'rec> = #mutable_name<'rec>;

            // THINK ABOUT: to_model is the only thing that forces a clone requirement
            // Even though most Models will be clonable, maybe we shouldn't force it?
            // Also: nothing seems to be using this. Maybe it could be opt in
            fn to_model(&self) -> Result<Self::Model, ankurah::property::PropertyError> {
                Ok(#name {
                    #(#generated.model_extractors),*
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
                    #(#generated.default_initializers),*
                }
            }

            fn reference(&self) -> ::ankurah::core::property::value::Ref<Self::Model> {
                ::ankurah::core::property::value::Ref { id: self.id(), phantom: ::std::marker::PhantomData }
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

        #wasm_attributes
        impl #view_name {
            pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::EntityId {
                self.entity.id().clone()
            }
            #(#generated.view_accessors)*
        }

        // TODO - wasm-bindgen this - ah right, we need to remove the lifetime
        impl<'rec> ::ankurah::model::Mutable<'rec> for #mutable_name<'rec> {
            type Model = #name;
            type View = #view_name;

            fn entity(&self) -> &::ankurah::entity::Entity {
                &self.entity
            }

            fn new(entity: &'rec ::ankurah::entity::Entity) -> Self {
                Self { entity }
            }
            fn reference(&self) -> ::ankurah::core::property::value::Ref<Self::Model> {
                ::ankurah::core::property::value::Ref { id: self.id(), phantom: ::std::marker::PhantomData }
            }
        }
        impl<'rec> #mutable_name<'rec> {
            pub fn id(&self) -> ankurah::derive_deps::ankurah_proto::EntityId {
                self.entity.id().clone()
            }
            #(#generated.mutable_accessors)*
        }

        impl<'a> Into<ankurah::derive_deps::ankurah_proto::EntityId> for &'a #view_name {
            fn into(self) -> ankurah::derive_deps::ankurah_proto::EntityId {
                ankurah::View::id(self)
            }
        }

        impl<'a, 'rec> Into<ankurah::derive_deps::ankurah_proto::EntityId> for &'a #mutable_name<'rec> {
            fn into(self) -> ankurah::derive_deps::ankurah_proto::EntityId {
                ::ankurah::model::Mutable::id(self)
            }
        }
    }
    .into();

    expanded
}

static ACTIVE_TYPE_MOD_PREFIX: &str = "::ankurah::property::value";
fn get_active_type(field: &syn::Field) -> Result<syn::Type, syn::Error> {
    let active_type_ident = format_ident!("active_type");

    let mut active_type = None;
    // First check if there's an explicit attribute
    if let Some(active_type_attr) = field.attrs.iter().find(|attr| attr.path().get_ident() == Some(&active_type_ident)) {
        active_type = Some(active_type_attr.parse_args::<syn::Type>()?);
    } else {
        // Check for exact type matches and provide default Active types
        if let Type::Path(type_path) = &field.ty {
            let path_str = quote!(#type_path).to_string().replace(" ", "");
            match path_str.as_str() {
                // Add more default mappings here as needed
                "String" | "std::string::String" => {
                    let path = format!("{}::YrsString", ACTIVE_TYPE_MOD_PREFIX);
                    let yrs = syn::parse_str(&path).map_err(|_| syn::Error::new_spanned(&field.ty, "Failed to create YrsString path"))?;
                    active_type = Some(yrs);
                }
                _ => {
                    // Everything else should use `LWW`` by default.
                    // TODO: Return a list of compile_error! for these types to specify
                    // that these need to be `Serialize + for<'de> Deserialize<'de>``
                    let path = format!("{}::LWW", ACTIVE_TYPE_MOD_PREFIX);
                    let lww = syn::parse_str(&path).map_err(|_| syn::Error::new_spanned(&field.ty, "Failed to create YrsString path"))?;
                    active_type = Some(lww);
                }
            }
        };
    }

    if let Some(active_type) = active_type {
        Ok(ActiveFieldType::convert_type_with_projected(&active_type, &field.ty))
    } else {
        // If we get here, we don't have a supported default Active type
        let field_name = field.ident.as_ref().map(|i| i.to_string()).unwrap_or_else(|| "unnamed".to_string());
        let type_str = format!("{:?}", &field.ty);

        Err(syn::Error::new_spanned(
            &field.ty,
            format!(
                "No active value type found for field '{}' (type: {}). Please specify using #[active_type(Type)] attribute",
                field_name, type_str
            ),
        ))
    }
}

fn has_flag(attrs: &Vec<syn::Attribute>, flag_name: &str) -> bool {
    for attr in attrs {
        if let Some(ident) = attr.path().get_ident() {
            if ident == flag_name {
                return true;
            }
        }
    }

    false
}

// Parse the active field type
struct ActiveFieldType {
    pub base: syn::Type,
    pub generics: Option<syn::AngleBracketedGenericArguments>,
}

impl ActiveFieldType {
    pub fn convert_type_with_projected(ty: &syn::Type, projected: &syn::Type) -> syn::Type {
        let mut base = Self::from_type(ty);
        base.with_projected(projected);
        base.as_type()
    }

    pub fn from_type(ty: &syn::Type) -> Self {
        if let syn::Type::Path(path) = ty {
            if let Some(last_segment) = path.path.segments.last() {
                if let syn::PathArguments::AngleBracketed(generics) = &last_segment.arguments {
                    return Self { base: ty.clone(), generics: Some(generics.clone()) };
                }
            }
        }

        Self { base: ty.clone(), generics: None }
    }

    pub fn with_projected(&mut self, ty: &syn::Type) {
        let mut generics = match &self.generics {
            Some(generics) => generics.clone(),
            None => AngleBracketedGenericArguments {
                colon2_token: None,
                lt_token: Default::default(),
                args: Punctuated::default(),
                gt_token: Default::default(),
            },
        };

        // Replace inferred `_` with projected
        if let Some(last @ syn::GenericArgument::Type(syn::Type::Infer(_))) = generics.args.last_mut() {
            *last = syn::GenericArgument::Type(ty.clone());
        } else {
            // Otherwise just push to the end.
            generics.args.push(syn::GenericArgument::Type(ty.clone()));
        }

        self.generics = Some(generics);
    }

    pub fn as_type(&self) -> syn::Type {
        let mut new_type = self.base.clone();
        let syn::Type::Path(ref mut path) = new_type else {
            unimplemented!("Non-path types aren't supported for active types");
        };

        let Some(last_segment) = path.path.segments.last_mut() else {
            unreachable!("Need at least a single segment for type paths...?");
        };

        if let Some(generics) = &self.generics {
            last_segment.arguments = syn::PathArguments::AngleBracketed(generics.clone());
        }

        new_type
    }
}

fn as_turbofish(type_path: &syn::Type) -> proc_macro2::TokenStream {
    if let syn::Type::Path(path) = type_path {
        let mut without_generics = path.clone();
        let mut generics = syn::PathArguments::None;
        if let Some(last_segment) = without_generics.path.segments.last_mut() {
            generics = last_segment.arguments.clone();
            last_segment.arguments = syn::PathArguments::None;
        }

        if let syn::PathArguments::AngleBracketed(generics) = generics {
            quote! {
                #without_generics::#generics
            }
        } else {
            quote! {
                #without_generics
            }
        }
    } else {
        unimplemented!()
    }
}

/// We have incorporated the code from the `tsify` crate into this crate, with the intention to modify it to better fit the needs of the Ankurah project.
/// The original license files are preserved in the `tsify` directory.
#[cfg(feature = "wasm")]
pub fn expand_ts_model_type(input: &DeriveInput, interface_name: String) -> syn::Result<proc_macro2::TokenStream> {
    let cont = crate::tsify::container::Container::from_derive_input(input)?;

    let parser = crate::tsify::parser::Parser::new(&cont);
    let mut decl = parser.parse();

    decl.set_id(interface_name);

    let tokens = crate::tsify::wasm_bindgen::expand(&cont, decl);
    cont.check()?;

    Ok(tokens)
}

// Replace the type_is_ref function
fn type_is_ref(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Path(path) => {
            if let Some(last_segment) = path.path.segments.last() {
                let ident = &last_segment.ident;
                if ident == "Ref" {
                    return true;
                }
                // Check for Option<Ref<T>>
                if ident == "Option" {
                    if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
                        if let Some(syn::GenericArgument::Type(syn::Type::Path(inner_path))) = args.args.first() {
                            if let Some(inner_segment) = inner_path.path.segments.last() {
                                return inner_segment.ident == "Ref";
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    false
}

fn get_ref_segment_and_args(path: &syn::TypePath) -> Option<(&syn::PathSegment, &syn::PathArguments)> {
    let last_segment = path.path.segments.last()?;
    if last_segment.ident == "Ref" {
        return Some((last_segment, &last_segment.arguments));
    }

    if last_segment.ident == "Option" {
        let args = match &last_segment.arguments {
            syn::PathArguments::AngleBracketed(args) => args,
            _ => return None,
        };

        let inner_type = match args.args.first() {
            Some(syn::GenericArgument::Type(syn::Type::Path(ref_path))) => ref_path,
            _ => return None,
        };

        let ref_segment = inner_type.path.segments.last()?;
        if ref_segment.ident == "Ref" {
            return Some((ref_segment, &ref_segment.arguments));
        }
    }
    None
}

fn get_ref_type_name(is_optional: bool, make_mutable: bool) -> &'static str {
    match (is_optional, make_mutable) {
        (true, true) => "OptionalMutableRef",
        (true, false) => "OptionalActiveRef",
        (false, true) => "MutableRef",
        (false, false) => "ActiveRef",
    }
}

fn build_ref_type(field: &syn::Field, make_mutable: bool) -> Result<syn::Type, syn::Error> {
    let ty = &field.ty;
    let type_path = match ty {
        syn::Type::Path(p) => p,
        _ => return Err(syn::Error::new_spanned(field, "Expected a type path")),
    };

    let is_optional = type_path.path.segments.last().map_or(false, |s| s.ident == "Option");

    // Get the Ref segment and its type arguments
    let (ref_segment, ref_args) =
        get_ref_segment_and_args(type_path).ok_or_else(|| syn::Error::new_spanned(field, "Expected Ref or Option<Ref>"))?;

    // Create a new path that's identical except for the last segment
    let mut new_path = type_path.clone();
    if let Some(last) = new_path.path.segments.last_mut() {
        last.ident = format_ident!("{}", get_ref_type_name(is_optional, make_mutable));
        last.arguments = ref_args.clone();
    }

    Ok(syn::Type::Path(new_path))
}

fn get_view_field_type(field: &syn::Field) -> Result<syn::Type, syn::Error> {
    if type_is_ref(&field.ty) {
        build_ref_type(field, false)
    } else {
        Ok(field.ty.clone())
    }
}

fn get_mutable_field_type(field: &syn::Field) -> Result<syn::Type, syn::Error> {
    if type_is_ref(&field.ty) {
        build_ref_type(field, true)
    } else {
        get_active_type(field)
    }
}

#[derive(Default)]
struct GeneratedFields {
    view_accessors: Vec<proc_macro2::TokenStream>,
    mutable_accessors: Vec<proc_macro2::TokenStream>,
    ephemeral_fields: Vec<syn::Field>,
    entity_initializers: Vec<proc_macro2::TokenStream>,
    model_extractors: Vec<proc_macro2::TokenStream>,
    field_declarations: Vec<proc_macro2::TokenStream>,
    default_initializers: Vec<proc_macro2::TokenStream>,
}

fn process_fields(fields: &Punctuated<syn::Field, syn::token::Comma>) -> Result<GeneratedFields, syn::Error> {
    let mut result = GeneratedFields::default();

    for field in fields {
        let name = &field.ident;
        let vis = &field.vis;
        let ty = &field.ty;
        let name_str = name.as_ref().unwrap().to_string().to_lowercase();

        if has_flag(&field.attrs, "ephemeral") {
            result.ephemeral_fields.push(field.clone());
            result.model_extractors.push(quote! {
                #name: self.#name.clone()
            });
            result.field_declarations.push(quote! {
                #vis #name: #ty
            });
            result.default_initializers.push(quote! {
                #name: Default::default()
            });
            continue;
        }

        // Generate model extractor for active fields
        result.model_extractors.push(quote! {
            #name: self.#name()?
        });

        // Generate entity initializer for non-ephemeral fields
        let active_type = if type_is_ref(&field.ty) { build_ref_type(field, false)? } else { get_active_type(field)? };
        let active_type_turbofish = as_turbofish(&active_type);

        result.entity_initializers.push(quote! {
            #active_type_turbofish::initialize_with(&entity, #name_str.into(), &self.#name);
        });

        if type_is_ref(&field.ty) {
            let active_type = build_ref_type(field, false)?;
            let mutable_type = build_ref_type(field, true)?;
            let mutable_type_turbofish = as_turbofish(&mutable_type);

            result.view_accessors.push(quote! {
                #vis fn #name(&self) -> Result<#active_type, ::ankurah::property::PropertyError> {
                    ::ankurah::property::FromEntity::from_entity(#name_str.into(), &self.entity)
                }
            });

            result.mutable_accessors.push(quote! {
                #vis fn #name(&self) -> #mutable_type {
                    #mutable_type_turbofish::from_entity(#name_str.into(), self.entity)
                }
            });
        } else {
            let projected_type = &field.ty;
            let active_type = get_active_type(field)?;
            let active_type_turbofish = as_turbofish(&active_type);

            result.view_accessors.push(quote! {
                #vis fn #name(&self) -> Result<#projected_type, ::ankurah::property::PropertyError> {
                    let active_result = <#active_type>::from_entity(#name_str.into(), &self.entity);
                    <#projected_type>::from_active(active_result)
                }
            });

            result.mutable_accessors.push(quote! {
                #vis fn #name(&self) -> #active_type {
                    #active_type_turbofish::from_entity(#name_str.into(), self.entity)
                }
            });
        }
    }

    Ok(result)
}
