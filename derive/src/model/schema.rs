//! Compiled-schema emission for `#[derive(Model)]`.
//!
//! This module generates the static `ModelStructDescriptor`
//! and the `Model::descriptor()` method. Two facts per field are NORMATIVE.
//! A property's original model scope and name locate its identity; registration
//! checks these compiled facts against the immutable canonical pair (exact
//! backend and a mutually castable value type) and refuses an incompatible
//! binding rather than registering another property identity.
//!
//! - `backend`: the backend-registry name the active type resolves to,
//!   declared by the active type's `ActiveType::BACKEND` const.
//! - `value_type`: a lowercased `core::value::ValueType` variant, declared
//!   by the field's ORIGINAL Rust type (before active-type wrapping)
//!   through its `Property` impl's `VALUE_TYPE` const; the derive carries
//!   no value-type vocabulary of its own.
//!
//! Attributes parsed here, by the kind of thing each value parses as:
//! - `#[property(renamed_from = "...")]`: a property display name (the
//!   transient rename hint).
//! - `#[model(id = "...")]` / `#[property(id = "...")]`: the EntityId of an
//!   EXISTING catalog entity to bind against, verified at registration; it
//!   never registers a new definition. Built-in system identities are not
//!   catalog entities and cannot appear here.
//! - `#[model(system = "...")]`: a built-in SystemModel variant, parsed in
//!   description.rs. A compile-time System ID: never registered, and
//!   the collection is pinned to its reserved system label.
//! - `#[model(base = "...")]`: the path through which generated Rust code
//!   addresses core. This is independent of FFI generation.
//! - `#[model(no_ffi)]`: omit WASM and UniFFI surfaces for this model while
//!   retaining its normal Rust model, view, and mutable APIs.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{spanned::Spanned, Type};

use crate::model::description::ModelDescription;

/// The `(optional, inner)` shape of one field: `inner` is the
/// Option-unwrapped type, whose `Property` impl declares the field's
/// value_type through its associated const.
struct ValueTypeMapping<'a> {
    optional: bool,
    inner: &'a Type,
}

/// Validate the schema-affecting attributes and collection name, producing
/// a SINGLE compile error (no cascade) when something is wrong. Called
/// early in `derive_model`, before any impl is generated, so a bad model
/// yields one actionable diagnostic instead of a pile of downstream
/// trait-bound errors.
///
/// Checks: the reserved `_ankurah_` collection prefix, and
/// the URL-safe-base64/16-byte shape of every `#[property(id = "...")]` and
/// `#[model(id = "...")]` explicit binding. It also surfaces any
/// malformed `#[property(...)]` list.
pub fn validate_schema_attrs(model: &ModelDescription) -> syn::Result<()> {
    // The `_ankurah_` prefix is reserved for system collections; a user
    // model must never claim it, complementing the
    // receiver-side structural protection.
    let collection = model.collection_str();
    if model.system().is_none() && collection.starts_with("_ankurah_") {
        return Err(syn::Error::new(
            model.name().span(),
            format!("collection '{collection}' uses the reserved `_ankurah_` prefix, which is reserved for system collections; rename the model"),
        ));
    }

    for field in model.active_fields() {
        // Surfaces any malformed #[property(...)] list (the walk LitStr-parses
        // every anchor/id value it passes) and validates #[property(id)].
        if let Some(id) = property_str_attr(&field.attrs, "id")? {
            validate_explicit_id(&id).map_err(|msg| syn::Error::new(field.ty.span(), msg))?;
        }
    }

    // A system model's identity is built in; it cannot also bind a catalog
    // entity id.
    if model.system().is_some() && model.explicit_id().is_some() {
        return Err(syn::Error::new(
            model.name().span(),
            "#[model(system = ...)] and #[model(id = ...)] are mutually exclusive: a built-in identity is not a catalog entity",
        ));
    }

    if let Some(id) = model.explicit_id() {
        validate_explicit_id(&id).map_err(|msg| {
            syn::Error::new(
                model.name().span(),
                format!("{msg}; #[model(id = ...)] takes the EntityId of an existing model catalog entity -- for a built-in system model use #[model(system = \"...\")]"),
            )
        })?;
    }

    Ok(())
}

/// Generate the `static ModelStructDescriptor` + `fn descriptor()` for the Model
/// impl. Returns a compile error token stream if a field type cannot be
/// mapped or an explicit-id attribute is malformed. Assumes
/// [`validate_schema_attrs`] already ran (it re-derives the same facts, so
/// it is safe to call independently).
pub fn schema_impl(model: &ModelDescription) -> syn::Result<TokenStream> {
    let base = model.base();
    let collection = model.collection_str();

    let name = model.name();
    let name_str = name.to_string();
    // The declaring module's path, captured by lib.rs in the user's module.
    // The descriptor static lives inside the hygiene module, so it reads the
    // const through `super::`. Unique ids hash NAMES ONLY (module path,
    // struct, field), so they survive a change of a field's type.
    let module_path_const = model.module_path_const();

    // Per-field descriptors, in declaration order (ephemeral fields already
    // excluded by ModelDescription's active/ephemeral split).
    let descs = model.active_field_descs()?;
    let mut field_tokens = Vec::with_capacity(model.active_fields().len());

    for (field, desc) in model.active_fields().iter().zip(descs.iter()) {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_name = field_ident.to_string();
        // Display name matches the runtime property key: the derive macro
        // lowercases field names for initialize_new_entity
        // (description.rs active_field_name_strs), so mirror that here.
        let display_name = field_name.to_lowercase();

        let mapping = map_value_type(&field.ty);
        let optional = mapping.optional;
        // The field's own `Property` impl declares its value_type (an
        // associated const, resolved inside the static initializer). The
        // derive carries no value-type vocabulary of its own.
        let inner = mapping.inner;
        let value_type = quote! { <#inner as #base::property::Property>::VALUE_TYPE };

        // Ref<T> names its target model by source label in the registration
        // descriptor. Source model labels are the lowercased model type name
        // (ModelDescription::collection_str), so derive the same static value
        // from T here; Option<Ref<T>> unwraps through reference_target.
        let target_collection = reference_target(&field.ty).and_then(type_head).map(|name| name.to_lowercase());
        let target_collection_tokens = option_str_tokens(target_collection.as_deref());

        // The active type declares which backend stores it (an associated
        // const, resolved inside the static initializer). Like value_type,
        // the derive tabulates nothing.
        let active_type = desc.rust_type_with_context(if model.uses_crate_paths() { "local" } else { "external" })?;
        let backend = quote! { <#active_type as #base::property::ActiveType>::BACKEND };

        // #[property(renamed_from = "...")]: the transient rename hint. Applied by the registration executor before lookup-or-create,
        // guarded; removable from source once every target system has seen
        // it.
        let renamed_from = property_str_attr(&field.attrs, "renamed_from")?;
        let renamed_from_tokens = option_str_tokens(renamed_from.as_deref());

        // #[property(id = "...")]: explicit binding to a known property
        // entity. Validated as URL-safe base64 / 16 bytes.
        let explicit_id = property_str_attr(&field.attrs, "id")?;
        if let Some(ref id) = explicit_id {
            validate_explicit_id(id).map_err(|msg| syn::Error::new(field.ty.span(), msg))?;
        }
        let explicit_id_tokens = option_str_tokens(explicit_id.as_deref());

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
                unique_id: #base::proto::UniqueFieldId::from_names(super::#module_path_const, #name_str, #field_name),
            }
        });
    }

    // #[model(id = "...")]: explicit binding to a known model entity.
    let model_explicit_id = model.explicit_id();
    if let Some(id) = model_explicit_id {
        validate_explicit_id(id).map_err(|msg| syn::Error::new(name.span(), msg))?;
    }
    let model_explicit_id_tokens = option_str_tokens(model_explicit_id);

    // A system model pins its built-in System ID into the descriptor;
    // every registration path short-circuits on it.
    let system_tokens = match model.system() {
        Some(variant) => quote! { ::core::option::Option::Some(#base::proto::SystemModel::#variant) },
        None => quote! { ::core::option::Option::None },
    };

    // A private static so the returned reference is `&'static` with zero
    // per-call cost. Named distinctly to avoid colliding with anything in
    // the hygiene module.
    Ok(quote! {
        fn descriptor() -> &'static #base::schema::ModelStructDescriptor {
            static __ANKURAH_MODEL_SCHEMA: #base::schema::ModelStructDescriptor = #base::schema::ModelStructDescriptor {
                label: #collection,
                name: #name_str,
                properties: &[
                    #(#field_tokens),*
                ],
                explicit_id: #model_explicit_id_tokens,
                system: #system_tokens,
                unique_id: #base::proto::UniqueStructId::from_names(super::#module_path_const, #name_str),
            };
            &__ANKURAH_MODEL_SCHEMA
        }
    })
}

// Accessor-side resolution (each derived accessor resolving its compiled
// label or embedded explicit id to a PropertyId before touching a backend)
// returns with the propertyid-resolution PR; its generator lived here.

/// The `(optional, inner)` mapping for a field type: `Option<T>` unwraps to
/// `T` with `optional = true`; everything else is itself. The inner type's
/// `Property` impl is the value-type authority, so there is nothing else to
/// derive here.
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

fn type_head_is(ty: &Type, name: &str) -> bool { type_head(ty).as_deref() == Some(name) }

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

/// Parse a `#[property(key = "value")]` string attribute off a field. There
/// may be several `#[property(...)]` attributes; the LAST value for `key`
/// wins (consistent with how Rust attributes accumulate). Returns an error
/// on a `property` attribute that is not a `key = "lit"` name-value list, or
/// whose value is not a string literal.
fn property_str_attr(attrs: &[syn::Attribute], key: &str) -> syn::Result<Option<String>> {
    let mut found = None;
    for attr in attrs {
        if !attr.path().is_ident("property") {
            continue;
        }
        // #[property(renamed_from = "...", id = "...")] -- a comma-separated
        // list of name = "value" pairs.
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
        // 16 zero bytes -> "AAAAAAAAAAAAAAAAAAAAAA" (22 chars).
        assert!(validate_explicit_id("AAAAAAAAAAAAAAAAAAAAAA").is_ok());
        // 20 chars decode to fewer than 16 bytes.
        assert!(validate_explicit_id("AAAAAAAAAAAAAAAAAAAA").is_err());
        // Bad charset, padding, and noncanonical trailing bits all refuse.
        assert!(validate_explicit_id("******UGBwgJCgsMDQ4PEA").is_err());
        assert!(validate_explicit_id("AAAAAAAAAAAAAAAAAAAAA=").is_err());
        assert!(validate_explicit_id("AAAAAAAAAAAAAAAAAAAAAB").is_err());
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
