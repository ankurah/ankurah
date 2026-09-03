//! Compiled-schema emission for `#[derive(Model)]`.

use ankurah_core_types::SystemProperty;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{spanned::Spanned, Type};

use crate::model::description::ModelDescription;

struct ValueTypeMapping<'a> {
    optional: bool,
    inner: &'a Type,
}

pub fn validate_schema_attrs(model: &ModelDescription) -> syn::Result<()> {
    let collection = model.collection_str();
    if model.system().is_none() && collection.starts_with(crate::model::RESERVED_COLLECTION_PREFIX) {
        return Err(syn::Error::new(
            model.name().span(),
            format!("collection '{collection}' uses the reserved `_ankurah_` prefix, which is reserved for system collections; rename the model"),
        ));
    }

    let mut property_names = std::collections::BTreeSet::new();
    for field in model.active_fields() {
        let ident = field.ident.as_ref().expect("named field");
        let name = ident.to_string().to_lowercase();
        if !property_names.insert(name.clone()) {
            return Err(syn::Error::new(ident.span(), format!("duplicate property name '{name}' after normalization")));
        }
        if let Some(id) = property_str_attr(&field.attrs, "id")? {
            validate_explicit_id(&id).map_err(|msg| syn::Error::new(field.ty.span(), msg))?;
        }
    }

    if let Some(id) = model.explicit_id() {
        validate_explicit_id(id).map_err(|msg| syn::Error::new(model.name().span(), msg))?;
    }

    Ok(())
}

pub fn schema_impl(model: &ModelDescription) -> syn::Result<TokenStream> {
    let base = model.base();
    let collection = model.collection_str();

    let name = model.name();
    let name_str = name.to_string();

    let system_tokens = match model.system() {
        Some(system) => {
            let variant = format_ident!("{}", system.variant_name());
            quote! { ::core::option::Option::Some(#base::proto::SystemModel::#variant) }
        }
        None => quote! { ::core::option::Option::None },
    };
    let model_resolved = match model.system() {
        Some(system) => {
            let variant = format_ident!("{}", system.variant_name());
            quote! { #base::schema::SchemaOnceCell::Pinned(#base::proto::ModelId::System(#base::proto::SystemModel::#variant)) }
        }
        None => quote! { #base::schema::SchemaOnceCell::per_epoch() },
    };

    let active_types = model.active_field_types()?;
    let mut field_tokens = Vec::with_capacity(model.active_fields().len());
    for (field, active_type) in model.active_fields().iter().zip(active_types.iter()) {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_name = field_ident.to_string();
        let display_name = field_name.to_lowercase();
        let field_resolved = match model.system() {
            Some(_) => {
                let system = SystemProperty::from_name(&display_name).ok_or_else(|| {
                    syn::Error::new(
                        field_ident.span(),
                        format!(
                            "field '{display_name}' is not a built-in system property; a #[model(system = \"...\")] model may only declare fields named after ankurah_proto::SystemProperty variants"
                        ),
                    )
                })?;
                let variant = format_ident!("{}", system.variant_name());
                quote! { #base::schema::SchemaOnceCell::Pinned(#base::proto::PropertyId::System(#base::proto::SystemProperty::#variant)) }
            }
            None => quote! { #base::schema::SchemaOnceCell::per_epoch() },
        };

        let mapping = map_value_type(&field.ty);
        let optional = mapping.optional;
        let inner = mapping.inner;
        let value_type = quote! { <#inner as #base::property::Property>::VALUE_TYPE };

        let target_collection = reference_target(&field.ty).and_then(type_head).map(|name| name.to_lowercase());
        let target_collection_tokens = option_str_tokens(target_collection.as_deref());

        let backend = quote! { <#active_type as #base::property::ActiveType>::BACKEND };

        let renamed_from = property_str_attr(&field.attrs, "renamed_from")?;
        let renamed_from_tokens = option_str_tokens(renamed_from.as_deref());

        let explicit_id = property_str_attr(&field.attrs, "id")?;
        if let Some(ref id) = explicit_id {
            validate_explicit_id(id).map_err(|msg| syn::Error::new(field.ty.span(), msg))?;
        }
        let explicit_id_tokens = option_str_tokens(explicit_id.as_deref());

        let field_build_id = ulid::Ulid::new().to_bytes();
        field_tokens.push(quote! {
            #base::schema::StructProperty {
                field: #field_name,
                name: #display_name,
                renamed_from: #renamed_from_tokens,
                backend: #backend,
                value_type: #value_type,
                target_label: #target_collection_tokens,
                optional: #optional,
                explicit_id: #explicit_id_tokens,
                build_id: [#(#field_build_id),*],
                resolved: #field_resolved,
            }
        });
    }

    let model_explicit_id = model.explicit_id();
    if let Some(id) = model_explicit_id {
        validate_explicit_id(id).map_err(|msg| syn::Error::new(name.span(), msg))?;
    }
    let model_explicit_id_tokens = option_str_tokens(model_explicit_id);

    let field_count = field_tokens.len();
    let model_build_id = ulid::Ulid::new().to_bytes();
    Ok(quote! {
        fn descriptor() -> &'static #base::schema::ModelStructDescriptor {
            static __ANKURAH_MODEL_PROPERTIES: [#base::schema::StructProperty; #field_count] = [
                #(#field_tokens),*
            ];
            static __ANKURAH_MODEL_SCHEMA: #base::schema::ModelStructDescriptor = #base::schema::ModelStructDescriptor {
                label: #collection,
                name: #name_str,
                properties: &__ANKURAH_MODEL_PROPERTIES,
                system: #system_tokens,
                explicit_id: #model_explicit_id_tokens,
                build_id: [#(#model_build_id),*],
                resolved: #model_resolved,
            };
            &__ANKURAH_MODEL_SCHEMA
        }
    })
}

fn map_value_type(ty: &Type) -> ValueTypeMapping<'_> {
    if let Some(inner) = option_inner(ty) {
        return ValueTypeMapping { optional: true, inner };
    }
    ValueTypeMapping { optional: false, inner: ty }
}

/// The last path segment identifier of a type (e.g. `String`,
/// `crate::property::value::Json` -> "Json"), if it is a path type.
fn type_head(ty: &Type) -> Option<String> {
    if let Type::Path(p) = ty {
        p.path.segments.last().map(|s| s.ident.to_string())
    } else {
        None
    }
}

/// If `ty` is `Option<Inner>`, return `Inner`.
fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(p) = ty else { return None };
    let seg = p.path.segments.last()?;
    if seg.ident != "Option" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &seg.arguments else { return None };
    args.args.iter().find_map(|a| if let syn::GenericArgument::Type(t) = a { Some(t) } else { None })
}

/// If `ty` is `Ref<T>` or `Option<Ref<T>>`, return `T`.
fn reference_target(ty: &Type) -> Option<&Type> {
    let ty = option_inner(ty).unwrap_or(ty);
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    if segment.ident != "Ref" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else { return None };
    args.args.iter().find_map(|arg| match arg {
        syn::GenericArgument::Type(target) => Some(target),
        _ => None,
    })
}

fn property_str_attr(attrs: &[syn::Attribute], key: &str) -> syn::Result<Option<String>> {
    let mut found = None;
    for attr in attrs {
        if !attr.path().is_ident("property") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            let ident = meta.path.get_ident().map(|i| i.to_string());
            match ident.as_deref() {
                Some("renamed_from") | Some("id") => {
                    let value = meta.value()?;
                    let lit: syn::LitStr = value.parse()?;
                    if meta.path.is_ident(key) {
                        found = Some(lit.value());
                    }
                    Ok(())
                }
                _ => Err(meta.error("unsupported #[property(...)] key; expected `renamed_from` or `id`")),
            }
        })?;
    }
    Ok(found)
}

/// Emit `Some("...")` or `None` for an `Option<&'static str>` field.
fn option_str_tokens(value: Option<&str>) -> TokenStream {
    match value {
        Some(s) => quote! { ::core::option::Option::Some(#s) },
        None => quote! { ::core::option::Option::None },
    }
}

/// Validate an explicit-id attribute value as a well-formed EntityId
/// encoding, by the same parse the runtime performs.
fn validate_explicit_id(s: &str) -> Result<(), String> {
    ankurah_core_types::EntityId::from_base64(s)
        .map(|_| ())
        .map_err(|error| format!("explicit id {s:?} is not a valid EntityId encoding: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_id_validation_matches_the_runtime_parse() {
        // 32 zero bytes -> 43 'A' characters.
        assert!(validate_explicit_id("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").is_ok());
        // A 22-character (16-byte) string does not decode.
        assert!(validate_explicit_id("AAAAAAAAAAAAAAAAAAAAAA").is_err());
        // Bad charset, padding, and noncanonical trailing bits all refuse.
        assert!(validate_explicit_id("******UGBwgJCgsMDQ4PEAAAAAAAAAAAAAAAAAAAAAA").is_err());
        assert!(validate_explicit_id("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=").is_err());
        assert!(validate_explicit_id("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB").is_err());
    }

    /// `Option<T>` unwraps to the inner type (whose `Property` impl
    /// declares the value_type) with `optional = true`; a bare type is
    /// itself, required.
    #[test]
    fn option_unwraps_to_inner() {
        let ty: Type = syn::parse_str("Option < Visibility >").unwrap();
        let m = map_value_type(&ty);
        assert!(m.optional);
        let inner: Type = syn::parse_str("Visibility").unwrap();
        assert_eq!(m.inner, &inner);

        let ty: Type = syn::parse_str("String").unwrap();
        assert!(!map_value_type(&ty).optional);
    }
}
