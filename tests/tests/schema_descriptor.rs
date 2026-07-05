//! The compiled schema `#[derive(Model)]` emits (work package A11a;
//! specs/model-property-metadata/rfc.md sections 4, 5.8, 5.9). Asserts the
//! NORMATIVE (backend, value_type) mapping row-by-row, the anchor and
//! explicit-id attributes, ephemeral exclusion, and the RegisterSchema
//! descriptor conversion. Ends with an end-to-end registration built from a
//! Model::schema() and driven through the durable/ephemeral harness.

mod common;
use ankurah::core::schema::registration_request;
use ankurah::property::{Json, Ref};
use ankurah::proto::{self, schema_id, PropertyRef};
use ankurah::value::Value;
use ankurah::Model;
use common::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// A referenced model, so Ref<T> has a target.
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct DescArtist {
    pub name: String,
}

/// A model exercising EVERY row of the RFC section 4 normative table, plus
/// the attribute surfaces. Field order is meaningful: the schema preserves
/// declaration order.
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct DescAllTypes {
    // String (default YrsString) -> ("yrs", "string")
    pub yrs_name: String,
    // Option<String> -> ("lww", "string", optional). NOTE: the RFC section 4
    // table lists `Option<String> (YrsString) -> "yrs"`, but the ACTUAL
    // in-tree backend registry resolves it to LWW: yrs's `accepts` is
    // `^String$` (bare String only), while lww lists `Option<String>` among
    // its provided types (core/src/property/value/{yrs,lww}.ron). The schema
    // records what the backend registry actually resolves, which is the
    // authoritative source for the (backend, value_type) identity pair; the
    // divergence from the RFC prose is a documented finding for A11a.
    pub yrs_opt: Option<String>,
    // #[active_type(LWW)] String -> ("lww", "string")
    #[active_type(LWW)]
    pub lww_str: String,
    // LWW<i16> -> ("lww", "i16")
    pub num_i16: i16,
    // LWW<i32> -> ("lww", "i32")
    pub num_i32: i32,
    // LWW<i64> -> ("lww", "i64")
    pub num_i64: i64,
    // LWW<f64> -> ("lww", "f64")
    pub num_f64: f64,
    // LWW<bool> -> ("lww", "bool")
    pub flag: bool,
    // LWW<Vec<u8>> -> ("lww", "binary")
    pub blob: Vec<u8>,
    // LWW<Json> -> ("lww", "json")
    pub doc: Json,
    // Ref<T> -> ("lww", "entityid")
    pub artist: Ref<DescArtist>,
    // Option<i32> -> ("lww", "i32", optional)
    pub maybe_i32: Option<i32>,
    // Option<Ref<T>> -> ("lww", "entityid", optional)
    pub maybe_artist: Option<Ref<DescArtist>>,
    // An ephemeral field: EXCLUDED from the schema entirely.
    #[model(ephemeral)]
    pub scratch: String,
}

/// Every normative-table row is present with the exact (backend,
/// value_type, optional) descriptor and in declaration order; ephemeral
/// fields are excluded.
#[test]
fn schema_covers_every_normative_row() {
    let schema = DescAllTypes::schema();
    assert_eq!(schema.collection, "descalltypes");
    assert_eq!(schema.name, "DescAllTypes");
    assert_eq!(schema.explicit_id, None);

    // (field, name, anchor, backend, value_type, optional)
    let expected: &[(&str, &str, &str, &str, &str, bool)] = &[
        ("yrs_name", "yrs_name", "yrs_name", "yrs", "string", false),
        // Option<String> -> lww (in-tree resolution; see field comment).
        ("yrs_opt", "yrs_opt", "yrs_opt", "lww", "string", true),
        ("lww_str", "lww_str", "lww_str", "lww", "string", false),
        ("num_i16", "num_i16", "num_i16", "lww", "i16", false),
        ("num_i32", "num_i32", "num_i32", "lww", "i32", false),
        ("num_i64", "num_i64", "num_i64", "lww", "i64", false),
        ("num_f64", "num_f64", "num_f64", "lww", "f64", false),
        ("flag", "flag", "flag", "lww", "bool", false),
        ("blob", "blob", "blob", "lww", "binary", false),
        ("doc", "doc", "doc", "lww", "json", false),
        ("artist", "artist", "artist", "lww", "entityid", false),
        ("maybe_i32", "maybe_i32", "maybe_i32", "lww", "i32", true),
        ("maybe_artist", "maybe_artist", "maybe_artist", "lww", "entityid", true),
    ];

    assert_eq!(schema.properties.len(), expected.len(), "ephemeral `scratch` must be excluded");
    for (i, (field, name, anchor, backend, value_type, optional)) in expected.iter().enumerate() {
        let f = &schema.properties[i];
        assert_eq!(f.field, *field, "field[{i}] name");
        assert_eq!(f.name, *name, "field[{i}] display name");
        assert_eq!(f.anchor, *anchor, "field[{i}] anchor");
        assert_eq!(f.backend, *backend, "field[{i}] backend");
        assert_eq!(f.value_type, *value_type, "field[{i}] value_type");
        assert_eq!(f.optional, *optional, "field[{i}] optional");
        assert_eq!(f.explicit_id, None, "field[{i}] explicit_id");
    }

    // The ephemeral field is nowhere in the schema.
    assert!(schema.field_by_name("scratch").is_none());
}

// -- anchor attribute --------------------------------------------------------

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct DescRenamed {
    // Renamed from "name": the anchor pins the derivation lineage while the
    // display name (the field name, lowercased) moves.
    #[property(anchor = "name")]
    pub headline: String,
}

#[test]
fn anchor_attribute_pins_derivation_name() {
    let schema = DescRenamed::schema();
    let f = &schema.properties[0];
    assert_eq!(f.field, "headline");
    assert_eq!(f.name, "headline", "display name is the (lowercased) field name");
    assert_eq!(f.anchor, "name", "anchor is the pinned earlier name");
}

// -- explicit id attributes --------------------------------------------------

// 16 zero bytes as URL-safe base64 (no padding) = 22 'A's; a valid EntityId.
const ZERO_ID_B64: &str = "AAAAAAAAAAAAAAAAAAAAAA";

#[derive(Model, Debug, Serialize, Deserialize)]
#[model(id = "AAAAAAAAAAAAAAAAAAAAAA")]
pub struct DescBound {
    #[property(id = "AAAAAAAAAAAAAAAAAAAAAA")]
    pub label: String,
    pub other: String,
}

#[test]
fn explicit_id_attributes_reflected() {
    let schema = DescBound::schema();
    assert_eq!(schema.explicit_id, Some(ZERO_ID_B64), "model explicit id");
    assert_eq!(schema.properties[0].explicit_id, Some(ZERO_ID_B64), "property explicit id");
    assert_eq!(schema.properties[1].explicit_id, None, "unbound field has no explicit id");
}

// -- registration_request conversion ----------------------------------------

#[test]
fn registration_request_from_schema() {
    let (models, properties, memberships) = registration_request(DescAllTypes::schema());

    assert_eq!(models.len(), 1);
    assert_eq!(models[0].collection, "descalltypes");
    assert_eq!(models[0].name, "DescAllTypes");

    assert_eq!(properties.len(), 13);
    // Spot-check the reference row: entityid, and no pre-derived target
    // model id (target_model is per-system, filled by later lifecycle work).
    let artist = properties.iter().find(|p| p.anchor == "artist").unwrap();
    assert_eq!(artist.minting_collection, "descalltypes");
    assert_eq!((artist.backend.as_str(), artist.value_type.as_str()), ("lww", "entityid"));
    assert_eq!(artist.target_model, None);
    assert_eq!(artist.explicit_id, None);

    // Non-explicit memberships reference the property by anchor within the
    // request; optionality rides the membership.
    assert_eq!(memberships.len(), 13);
    let yrs_opt = memberships.iter().find(|m| matches!(&m.property, PropertyRef::Anchor(a) if a == "yrs_opt")).unwrap();
    assert!(yrs_opt.optional, "Option<String> is optional per contract");
    let flag = memberships.iter().find(|m| matches!(&m.property, PropertyRef::Anchor(a) if a == "flag")).unwrap();
    assert!(!flag.optional);
}

#[test]
fn registration_request_honors_explicit_ids() {
    let (_models, properties, memberships) = registration_request(DescBound::schema());

    // The bound field carries its explicit id as a PropertyDescriptor
    // binding and its membership references the property by Id, not anchor.
    let label = properties.iter().find(|p| p.anchor == "label").unwrap();
    assert!(label.explicit_id.is_some(), "explicit-id binding preserved");
    let bound_id = label.explicit_id.unwrap();

    let label_ms = memberships.iter().find(|m| matches!(&m.property, PropertyRef::Id(id) if *id == bound_id));
    assert!(label_ms.is_some(), "bound field's membership references the property by id");

    // The unbound field references by anchor.
    let other_ms = memberships.iter().find(|m| matches!(&m.property, PropertyRef::Anchor(a) if a == "other"));
    assert!(other_ms.is_some());
}

// -- end-to-end registration through the harness -----------------------------

async fn catalog_values(
    node: &Node<SledStorageEngine, PermissiveAgent>,
    collection: &str,
    id: EntityId,
) -> anyhow::Result<BTreeMap<String, Option<Value>>> {
    use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
    let storage = node.collections.get(&collection.into()).await?;
    let state = storage.get_state(id).await?;
    let buffer = state.payload.state.state_buffers.0.get("lww").expect("catalog entities are LWW").clone();
    Ok(LWWBackend::from_state_buffer(&buffer)?.property_values())
}

/// Build a RegisterSchema request from `Model::schema()` via
/// `registration_request`, send it to a schema-less durable node, and
/// confirm the catalog resolves the fields with the derived ids and the
/// normative (backend, value_type) descriptors.
#[tokio::test]
async fn register_from_model_schema_end_to_end() -> anyhow::Result<()> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    // The whole point: the request is built from the compiled schema, no
    // hand-written descriptors.
    let (models, properties, memberships) = registration_request(DescAllTypes::schema());
    let request = proto::NodeRequestBody::RegisterSchema { models, properties, memberships };

    match client.request(server.id, &DEFAULT_CONTEXT, request).await? {
        proto::NodeResponseBody::Success => {}
        other => panic!("expected Success, got {other}"),
    }

    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = schema_id::model_entity_id(&root, "descalltypes");

    // The model entity exists with its collection + display name.
    let model = catalog_values(&server, "_ankurah_model", model_id).await?;
    assert_eq!(model.get("collection"), Some(&Some(Value::String("descalltypes".into()))));
    assert_eq!(model.get("name"), Some(&Some(Value::String("DescAllTypes".into()))));

    // Every active field resolves to a property entity with the exact
    // normative descriptor pair the schema declared, at the derived id.
    for f in DescAllTypes::schema().properties {
        let property_id = schema_id::property_entity_id(&root, &model_id, f.anchor, f.backend, f.value_type);
        let property = catalog_values(&server, "_ankurah_property", property_id).await?;
        assert_eq!(property.get("backend"), Some(&Some(Value::String(f.backend.into()))), "backend for {}", f.field);
        assert_eq!(property.get("value_type"), Some(&Some(Value::String(f.value_type.into()))), "value_type for {}", f.field);
        assert_eq!(property.get("name"), Some(&Some(Value::String(f.anchor.into()))), "anchor/name for {}", f.field);
        assert_eq!(property.get("minted_for"), Some(&Some(Value::EntityId(model_id))), "minted_for for {}", f.field);

        // And the (model, property) membership exists with the field's
        // optionality.
        let membership_id = schema_id::membership_entity_id(&root, &model_id, &property_id);
        let membership = catalog_values(&server, "_ankurah_model_property", membership_id).await?;
        assert_eq!(membership.get("property"), Some(&Some(Value::EntityId(property_id))), "membership property for {}", f.field);
        assert_eq!(membership.get("optional"), Some(&Some(Value::Bool(f.optional))), "membership optional for {}", f.field);
    }

    Ok(())
}
