//! Compiled-schema emission for `#[derive(Model)]` (work package A11a;
//! specs/model-property-metadata/rfc.md sections 4, 5.8, 5.9).
//!
//! This module generates the `static` [`ankurah::core::schema::ModelSchema`]
//! and the `Model::schema()` method. Two facts per field are NORMATIVE.
//! A property's minting model and name locate its identity; registration
//! checks these compiled facts against the immutable canonical pair (exact
//! backend and a mutually castable value type) and refuses an incompatible
//! binding rather than registering another property identity.
//!
//! - `backend`: the backend-registry name the active type resolves to
//!   ("yrs" / "lww"), obtained from the backend registry
//!   ([`ActiveTypeDesc::backend_key`]).
//! - `value_type`: a lowercased `core::value::ValueType` variant, mapped
//!   from the field's ORIGINAL Rust type (before active-type wrapping) via
//!   the RFC section 4 table.
//!
//! Attributes parsed here: `#[property(renamed_from = "...")]` (the
//! transient rename hint, RFC 5.8) and `#[property(id = "...")]` /
//! `#[model(id = "...")]` (explicit binding, RFC 5.9). Explicit-id values
//! are validated at compile time as URL-safe base64 of exactly 16 bytes.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{spanned::Spanned, Type};

use crate::model::description::ModelDescription;

/// Where a field's normative `value_type` string comes from.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ValueTypeSource {
    /// A built-in row of the RFC section 4 table: emitted as a string
    /// literal, exactly as before.
    Literal(&'static str),
    /// Outside the built-in table: emitted as
    /// `<Inner as Property>::VALUE_TYPE`, so the type's own `Property` impl
    /// declares its normative value_type (an associated const, resolved at
    /// compile time inside the static initializer). `#[derive(Property)]`
    /// pins "string" to match its JSON-string serialization; hand-written
    /// impls declare whatever `Value` variant their `into_value` produces.
    TraitConst,
}

/// The `(value_type source, optional)` mapping for one field, from its
/// original Rust type per the RFC section 4 normative table. `inner` is the
/// Option-unwrapped type (the type whose `Property` impl governs).
struct ValueTypeMapping<'a> {
    source: ValueTypeSource,
    optional: bool,
    inner: &'a Type,
}

/// Validate the schema-affecting attributes and collection name, producing
/// a SINGLE compile error (no cascade) when something is wrong. Called
/// early in `derive_model`, before any impl is generated, so a bad model
/// yields one actionable diagnostic instead of a pile of downstream
/// trait-bound errors.
///
/// Checks: the reserved `_ankurah_` collection prefix (RFC section 4), and
/// the URL-safe-base64/16-byte shape of every `#[property(id = "...")]` and
/// `#[model(id = "...")]` explicit binding (RFC 5.9). It also surfaces any
/// malformed `#[property(...)]` list.
pub fn validate_schema_attrs(model: &ModelDescription) -> syn::Result<()> {
    // The `_ankurah_` prefix is reserved for system collections; a user
    // model must never claim it (RFC section 4, "reserved and rejected for
    // user-model schema labels at derive time"), complementing the
    // receiver-side structural protection.
    let collection = model.collection_str();
    if collection.starts_with("_ankurah_") {
        return Err(syn::Error::new(
            model.name().span(),
            format!(
                "collection '{collection}' uses the reserved `_ankurah_` prefix, which is reserved for system collections \
                 (specs/model-property-metadata/rfc.md section 4); rename the model"
            ),
        ));
    }

    for field in model.active_fields() {
        // Surfaces any malformed #[property(...)] list (the walk LitStr-parses
        // every anchor/id value it passes) and validates #[property(id)].
        if let Some(id) = property_str_attr(&field.attrs, "id")? {
            validate_explicit_id(&id).map_err(|msg| syn::Error::new(field.ty.span(), msg))?;
        }
    }

    if let Some(id) = model_str_attr(model, "id")? {
        validate_explicit_id(&id).map_err(|msg| syn::Error::new(model.name().span(), msg))?;
    }

    Ok(())
}

/// Generate the `static SCHEMA: ModelSchema` + `fn schema()` for the Model
/// impl. Returns a compile error token stream if a field type cannot be
/// mapped or an explicit-id attribute is malformed. Assumes
/// [`validate_schema_attrs`] already ran (it re-derives the same facts, so
/// it is safe to call independently).
pub fn schema_impl(model: &ModelDescription) -> syn::Result<TokenStream> {
    let collection = model.collection_str();

    let name = model.name();
    let name_str = name.to_string();

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
        let value_type = match mapping.source {
            ValueTypeSource::Literal(s) => quote! { #s },
            ValueTypeSource::TraitConst => {
                let inner = mapping.inner;
                quote! { <#inner as ::ankurah::Property>::VALUE_TYPE }
            }
        };

        // Ref<T> names its target model by source label in the registration
        // descriptor. Source model labels are the lowercased model type name
        // (ModelDescription::collection_str), so derive the same static value
        // from T here; Option<Ref<T>> unwraps through reference_target.
        let target_collection = reference_target(&field.ty).and_then(type_head).map(|name| name.to_lowercase());
        let target_collection_tokens = option_str_tokens(target_collection.as_deref());

        let backend = desc.backend_key().map_err(|msg| syn::Error::new(field.ty.span(), msg))?;

        // #[property(renamed_from = "...")]: the transient rename hint (RFC
        // 5.8). Applied by the registration executor before lookup-or-create,
        // guarded; removable from source once every target system has seen
        // it.
        let renamed_from = property_str_attr(&field.attrs, "renamed_from")?;
        let renamed_from_tokens = option_str_tokens(renamed_from.as_deref());

        // #[property(id = "...")]: explicit binding to a known property
        // entity (RFC 5.9). Validated as URL-safe base64 / 16 bytes.
        let explicit_id = property_str_attr(&field.attrs, "id")?;
        if let Some(ref id) = explicit_id {
            validate_explicit_id(id).map_err(|msg| syn::Error::new(field.ty.span(), msg))?;
        }
        let explicit_id_tokens = option_str_tokens(explicit_id.as_deref());

        field_tokens.push(quote! {
            ::ankurah::core::schema::FieldSchema {
                field: #field_name,
                name: #display_name,
                renamed_from: #renamed_from_tokens,
                backend: #backend,
                value_type: #value_type,
                target_collection: #target_collection_tokens,
                optional: #optional,
                explicit_id: #explicit_id_tokens,
            }
        });
    }

    // #[model(id = "...")]: explicit binding to a known model entity.
    let model_explicit_id = model_str_attr(model, "id")?;
    if let Some(ref id) = model_explicit_id {
        validate_explicit_id(id).map_err(|msg| syn::Error::new(name.span(), msg))?;
    }
    let model_explicit_id_tokens = option_str_tokens(model_explicit_id.as_deref());

    // A private static so the returned reference is `&'static` with zero
    // per-call cost. Named distinctly to avoid colliding with anything in
    // the hygiene module.
    Ok(quote! {
        fn schema() -> &'static ::ankurah::core::schema::ModelSchema {
            static __ANKURAH_MODEL_SCHEMA: ::ankurah::core::schema::ModelSchema = ::ankurah::core::schema::ModelSchema {
                collection: #collection,
                name: #name_str,
                properties: &[
                    #(#field_tokens),*
                ],
                explicit_id: #model_explicit_id_tokens,
            };
            &__ANKURAH_MODEL_SCHEMA
        }
    })
}

/// Generate each derive-generated accessor's property-id resolution.
///
/// Ordinary fields resolve their compiled label through the view's model
/// projection. An explicit binding embeds the already-validated entity id.
/// Either way, the active property accessor receives only a `PropertyId` (or
/// the resolution error), never a display name.
pub(crate) fn active_field_resolution_tokens(
    model: &ModelDescription,
    entity: TokenStream,
    model_id: TokenStream,
) -> syn::Result<Vec<TokenStream>> {
    model
        .active_fields()
        .iter()
        .map(|field| {
            let field_ident = field.ident.as_ref().expect("named field");
            let display_name = field_ident.to_string().to_lowercase();
            let explicit_id = property_str_attr(&field.attrs, "id")?;
            let explicit_id_tokens = match explicit_id {
                Some(id) => {
                    let bytes = ankurah_core_types::EntityId::from_base64(&id)
                        .map_err(|error| {
                            syn::Error::new(
                                field.ty.span(),
                                format!("explicit id {id:?} is not a valid EntityId encoding: {error} (RFC 5.9)"),
                            )
                        })?
                        .to_bytes();
                    quote! { ::core::option::Option::Some(::ankurah::proto::EntityId::from_bytes([#(#bytes),*])) }
                }
                None => quote! { ::core::option::Option::None },
            };
            Ok(quote! {
                ::ankurah::property::resolve_property_id(
                    #entity,
                    #model_id,
                    #display_name,
                    #explicit_id_tokens,
                )
            })
        })
        .collect()
}

/// Map a field's original Rust type to its `(value_type, optional)` per the
/// RFC section 4 normative table. `Option<T>` unwraps to the inner mapping
/// with `optional = true`.
///
/// Unrecognized types map to "string": a custom `#[derive(Property)]` type
/// serializes to `Value::String` (derive/src/property.rs `into_value`
/// returns `Value::String(json_str)`), so "string" is the value_type its
/// data actually carries at runtime. Types that resolve to NO backend are
/// already rejected earlier (description.rs active_field_descs), so this
/// fallback only ever applies to backend-resolvable custom Property types,
/// for which it is correct rather than a guess.
fn map_value_type(ty: &Type) -> ValueTypeMapping<'_> {
    if let Some(inner) = option_inner(ty) {
        let inner_mapping = map_value_type(inner);
        return ValueTypeMapping { optional: true, ..inner_mapping };
    }
    ValueTypeMapping { source: scalar_value_type(ty), optional: false, inner: ty }
}

/// The value_type source for a non-Option type per the normative table.
fn scalar_value_type(ty: &Type) -> ValueTypeSource {
    use ValueTypeSource::Literal;
    // Ref<T> is a reference: value_type "entityid" (RFC 4; the table's
    // reference row is (entityid, target_model=Some)).
    if type_head_is(ty, "Ref") {
        return Literal("entityid");
    }
    match type_head(ty).as_deref() {
        Some("String") => Literal("string"),
        Some("i16") => Literal("i16"),
        Some("i32") => Literal("i32"),
        Some("i64") => Literal("i64"),
        Some("f64") => Literal("f64"),
        Some("bool") => Literal("bool"),
        Some("EntityId") => Literal("entityid"),
        // Vec<u8> -> binary; a Vec of anything else is not in the table but
        // also does not resolve to a backend, so it never reaches here.
        Some("Vec") => Literal("binary"),
        // Json, or any path suffix ending in Json (e.g.
        // crate::property::value::Json), maps to "json".
        Some("Json") => Literal("json"),
        // Anything outside the table declares its own value_type through
        // its Property impl (associated const; RFC 4 erratum 2 resolution).
        _ => ValueTypeSource::TraitConst,
    }
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

/// Parse a `#[model(key = "value")]` string attribute off the struct.
/// Coexists with the existing flag form `#[model(ephemeral)]`-style parsing
/// (get_model_flag): a bare-ident meta and unrelated keys are skipped, a
/// `key = "lit"` is captured, and a `key = <non-string>` value is a hard
/// error (previously it was silently ignored, so `#[model(id = 5)]` would
/// fall back to derivation without a diagnostic).
fn model_str_attr(model: &ModelDescription, key: &str) -> syn::Result<Option<String>> {
    let mut found = None;
    let mut bad_value: Option<syn::Error> = None;
    for attr in model.struct_attrs() {
        if !attr.path().is_ident("model") {
            continue;
        }
        // Ignore parse failures from the flag form (e.g. #[model(ephemeral)]),
        // which is not a name-value list; only capture name = "value".
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident(key) {
                // Only ours if it is a name-value; a bare flag is skipped.
                if let Ok(value) = meta.value() {
                    match value.parse::<syn::LitStr>() {
                        Ok(lit) => found = Some(lit.value()),
                        Err(_) => {
                            bad_value = Some(syn::Error::new(meta.path.span(), format!("#[model({key} = ...)] expects a string literal")));
                        }
                    }
                }
            }
            // Consume any value token for unrelated keys so the walk
            // continues past `key = value` we do not handle.
            if meta.input.peek(syn::Token![=]) {
                let value = meta.value()?;
                let _: syn::Expr = value.parse()?;
            }
            Ok(())
        });
    }
    if let Some(err) = bad_value {
        return Err(err);
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
/// encoding (RFC 5.9), by the same parse the runtime performs.
fn validate_explicit_id(s: &str) -> Result<(), String> {
    ankurah_core_types::EntityId::from_base64(s)
        .map(|_| ())
        .map_err(|error| format!("explicit id {s:?} is not a valid EntityId encoding: {error} (RFC 5.9)"))
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

    #[test]
    fn value_type_table() {
        use ValueTypeSource::{Literal, TraitConst};
        let cases: &[(&str, ValueTypeSource, bool)] = &[
            ("String", Literal("string"), false),
            ("Option < String >", Literal("string"), true),
            ("i16", Literal("i16"), false),
            ("i32", Literal("i32"), false),
            ("i64", Literal("i64"), false),
            ("f64", Literal("f64"), false),
            ("bool", Literal("bool"), false),
            ("Option < bool >", Literal("bool"), true),
            ("Vec < u8 >", Literal("binary"), false),
            ("Json", Literal("json"), false),
            ("crate :: property :: value :: Json", Literal("json"), false),
            ("Ref < Artist >", Literal("entityid"), false),
            ("Option < Ref < Artist > >", Literal("entityid"), true),
            ("EntityId", Literal("entityid"), false),
            // Custom Property types declare their own value_type through the
            // trait const (RFC 4 erratum 2 resolution); the derive emits
            // `<Visibility as Property>::VALUE_TYPE`.
            ("Visibility", TraitConst, false),
            ("Option < Visibility >", TraitConst, true),
        ];
        for (ty_str, expected_source, expected_opt) in cases {
            let ty: Type = syn::parse_str(ty_str).unwrap();
            let m = map_value_type(&ty);
            assert_eq!(m.source, *expected_source, "value_type source for {ty_str}");
            assert_eq!(m.optional, *expected_opt, "optional for {ty_str}");
        }
    }

    /// The Option-unwrapped inner type drives the trait-const emission:
    /// `Option<Visibility>` must emit `<Visibility as Property>::VALUE_TYPE`
    /// (the inner type), never `<Option<Visibility> as ...>`.
    #[test]
    fn trait_const_uses_the_unwrapped_inner_type() {
        let ty: Type = syn::parse_str("Option < Visibility >").unwrap();
        let m = map_value_type(&ty);
        assert_eq!(m.source, ValueTypeSource::TraitConst);
        let inner: Type = syn::parse_str("Visibility").unwrap();
        assert_eq!(m.inner, &inner);
    }
}
