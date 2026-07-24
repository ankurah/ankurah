//! The compiled schema `#[derive(Model)]` emits (work package A11a;
//! specs/model-property-metadata/rfc.md sections 4, 5.8, 5.9). Asserts the
//! NORMATIVE (backend, value_type) mapping row-by-row, the renamed_from and
//! explicit-id attributes, ephemeral exclusion, and the RegisterSchema
//! descriptor conversion. Ends with an end-to-end registration built from a
//! Model::schema() and driven through the durable/ephemeral harness, sourcing
//! the allocated ids from the SchemaRegistered response.

mod common;
use ankql::ast::PropertyId;
use ankurah::core::schema::registration_request;
use ankurah::property::{Json, Ref};
use ankurah::proto::{self, PropertyRef};
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
    // Option<String> -> ("lww", "string", optional): yrs accepts only bare
    // String (`^String$`), and a yrs text cannot represent None as distinct
    // from "" -- RFC section 4 erratum 1, ratified 2026-07-05.
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

    // (field, name, backend, value_type, optional)
    let expected: &[(&str, &str, &str, &str, bool)] = &[
        ("yrs_name", "yrs_name", "yrs", "string", false),
        // Option<String> -> lww (in-tree resolution; see field comment).
        ("yrs_opt", "yrs_opt", "lww", "string", true),
        ("lww_str", "lww_str", "lww", "string", false),
        ("num_i16", "num_i16", "lww", "i16", false),
        ("num_i32", "num_i32", "lww", "i32", false),
        ("num_i64", "num_i64", "lww", "i64", false),
        ("num_f64", "num_f64", "lww", "f64", false),
        ("flag", "flag", "lww", "bool", false),
        ("blob", "blob", "lww", "binary", false),
        ("doc", "doc", "lww", "json", false),
        ("artist", "artist", "lww", "entityid", false),
        ("maybe_i32", "maybe_i32", "lww", "i32", true),
        ("maybe_artist", "maybe_artist", "lww", "entityid", true),
    ];

    assert_eq!(schema.properties.len(), expected.len(), "ephemeral `scratch` must be excluded");
    for (i, (field, name, backend, value_type, optional)) in expected.iter().enumerate() {
        let f = &schema.properties[i];
        assert_eq!(f.field, *field, "field[{i}] name");
        assert_eq!(f.name, *name, "field[{i}] display name");
        assert_eq!(f.backend, *backend, "field[{i}] backend");
        assert_eq!(f.value_type, *value_type, "field[{i}] value_type");
        let target = matches!(*field, "artist" | "maybe_artist").then_some("descartist");
        assert_eq!(f.target_collection, target, "field[{i}] reference target");
        assert_eq!(f.optional, *optional, "field[{i}] optional");
        assert_eq!(f.renamed_from, None, "field[{i}] renamed_from");
        assert_eq!(f.explicit_id, None, "field[{i}] explicit_id");
    }

    // The ephemeral field is nowhere in the schema.
    assert!(schema.field_by_name("scratch").is_none());
}

// -- renamed_from attribute --------------------------------------------------

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct DescRenamed {
    // Renamed from "name": the transient rename hint moves the lineage to the
    // new display name (the field name, lowercased) WITHOUT re-keying.
    #[property(renamed_from = "name")]
    pub headline: String,
}

#[test]
fn renamed_from_attribute_carries_the_hint() {
    let schema = DescRenamed::schema();
    let f = &schema.properties[0];
    assert_eq!(f.field, "headline");
    assert_eq!(f.name, "headline", "display name is the (lowercased) field name");
    assert_eq!(f.renamed_from, Some("name"), "renamed_from carries the prior name as the rename hint");
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

mod explicit_binding_v1 {
    use super::*;

    /// The catalog definition used by this model is named `catalog_score`;
    /// the local field deliberately has a different name. Its canonical i32
    /// type is also narrower than this binder's castable i64 declaration.
    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct BoundMetric {
        #[property(id = "AAAAAAAAAAAAAAAAAAAAAA")]
        pub local_score: i64,
    }
}

mod explicit_binding_v2 {
    use super::*;

    /// A second exact schema shape for the same collection and property id,
    /// used to exercise the offline fully-bound reassertion path.
    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct BoundMetric {
        #[property(id = "AAAAAAAAAAAAAAAAAAAAAA")]
        pub offline_alias: i64,
    }
}

mod explicit_binding_conflict {
    use super::*;

    /// Same compiled local name as v1, but ordinary by-name identity. Once
    /// both declarations are ensured, resolution must reject the ambiguity
    /// instead of silently preferring the explicit or catalog candidate.
    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct BoundMetric {
        pub local_score: i64,
    }
}

mod explicit_text_binding {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct BoundText {
        #[property(id = "AQEBAQEBAQEBAQEBAQEBAQ")]
        pub local_text: String,
    }
}

mod ordinary_yrs_alias {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct ClashingText {
        pub local_text: String,
    }
}

mod explicit_yrs_alias {
    use super::*;

    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct ClashingText {
        #[property(id = "AgICAgICAgICAgICAgICAg")]
        pub local_text: String,
    }
}

mod explicitly_shared_name {
    use super::*;

    /// This collection deliberately shares a property minted in another
    /// model's scope. The literal id is what makes that sharing intentional.
    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct SharedPlaylist {
        #[property(id = "AwMDAwMDAwMDAwMDAwMDAw")]
        pub name: String,
    }
}

mod ordinary_same_name {
    use super::*;

    /// The same local shape without the explicit id is an ordinary allocator
    /// request and must not inherit an explicitly shared property by name.
    #[derive(Model, Debug, Serialize, Deserialize)]
    pub struct SharedPlaylist {
        pub name: String,
    }
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
    // Spot-check the reference row: entityid plus the target model's
    // collection, which the executor resolves to target_model (RFC 5.2).
    let artist = properties.iter().find(|p| p.name == "artist").unwrap();
    assert_eq!(artist.minting_collection, "descalltypes");
    assert_eq!((artist.backend.as_str(), artist.value_type.as_str()), ("lww", "entityid"));
    assert_eq!(artist.target_collection.as_deref(), Some("descartist"));
    assert_eq!(artist.explicit_id, None);

    // Non-explicit memberships reference the property by name within the
    // request; optionality rides the membership.
    assert_eq!(memberships.len(), 13);
    let yrs_opt = memberships.iter().find(|m| matches!(&m.property, PropertyRef::Name(n) if n == "yrs_opt")).unwrap();
    assert!(yrs_opt.optional, "Option<String> is optional per contract");
    let flag = memberships.iter().find(|m| matches!(&m.property, PropertyRef::Name(n) if n == "flag")).unwrap();
    assert!(!flag.optional);
}

#[test]
fn registration_request_honors_explicit_ids() {
    let (_models, properties, memberships) = registration_request(DescBound::schema());

    // The bound field carries its explicit id as a PropertyDescriptor
    // binding and its membership references the property by Id, not name.
    let label = properties.iter().find(|p| p.name == "label").unwrap();
    assert!(label.explicit_id.is_some(), "explicit-id binding preserved");
    let bound_id = label.explicit_id.unwrap();

    let label_ms = memberships.iter().find(|m| matches!(&m.property, PropertyRef::Id(id) if *id == bound_id));
    assert!(label_ms.is_some(), "bound field's membership references the property by id");

    // The unbound field references by name.
    let other_ms = memberships.iter().find(|m| matches!(&m.property, PropertyRef::Name(n) if n == "other"));
    assert!(other_ms.is_some());
}

// -- end-to-end registration through the harness -----------------------------

async fn catalog_values(node: &Node<SledStorageEngine, PermissiveAgent>, id: EntityId) -> anyhow::Result<BTreeMap<String, Option<Value>>> {
    use ankql::ast::PropertyId;
    use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
    let state = node.storage.get_state(id).await?;
    let buffer = state.payload.state.state_buffers.0.get("lww").expect("catalog entities are LWW").clone();
    // Catalog collections use frozen system-property identities; project to
    // their registered names for the
    // by-string lookups the assertions use.
    Ok(LWWBackend::from_state_buffer(&buffer)?
        .property_values()
        .into_iter()
        .map(|(k, v)| match k {
            PropertyId::System(property) => (property.as_str().to_owned(), v),
            other => (format!("{other:?}"), v),
        })
        .collect())
}

fn catalog_lww_event(id: EntityId, parent: proto::Clock, fields: Vec<(&str, Value)>) -> proto::Event {
    use ankql::ast::PropertyId;
    use ankurah::core::property::backend::PropertyBackend;
    let backend = ankurah::core::property::backend::LWWBackend::new();
    for (name, value) in fields {
        backend.set(
            PropertyId::System(proto::SystemProperty::from_name(name).expect("catalog event field is a system property")),
            Some(value),
        );
    }
    let operations = backend.to_operations().unwrap().expect("catalog event has fields");
    proto::Event { entity_id: id, operations: proto::OperationSet(BTreeMap::from([("lww".to_string(), operations)])), parent }
}

fn catalog_event_context(collection: &str, event: proto::Event) -> proto::ModelContext<proto::Attested<proto::Event>> {
    let model = ankurah::core::schema::system_model_id(collection).expect("catalog event targets a built-in model");
    proto::ModelContext::new(model, proto::Attested::opt(event, None))
}

async fn wait_property_name(node: &Node<SledStorageEngine, PermissiveAgent>, id: EntityId, expected: &str) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if node.catalog.property_by_id(&id).as_ref().map(|property| property.name.as_str()) == Some(expected) {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("catalog property {id} did not reach name {expected:?}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn stored_bound_metric_value(
    node: &Node<SledStorageEngine, PermissiveAgent>,
    entity: EntityId,
    property: EntityId,
) -> anyhow::Result<Option<Value>> {
    use ankql::ast::PropertyId;
    use ankurah::core::property::backend::PropertyBackend;
    let state = node.storage.get_state(entity).await?;
    let buffer = state.payload.state.state_buffers.0.get("lww").expect("BoundMetric is LWW");
    let backend = ankurah::core::property::backend::LWWBackend::from_state_buffer(buffer)?;
    Ok(backend.entry(&PropertyId::EntityId(property)).flatten())
}

async fn stored_bound_text(
    node: &Node<SledStorageEngine, PermissiveAgent>,
    entity: EntityId,
    property: EntityId,
) -> anyhow::Result<Option<String>> {
    use ankql::ast::PropertyId;
    use ankurah::core::property::backend::PropertyBackend;
    let state = node.storage.get_state(entity).await?;
    let buffer = state.payload.state.state_buffers.0.get("yrs").expect("BoundText is Yrs");
    let backend = ankurah::core::property::backend::YrsBackend::from_state_buffer(buffer)?;
    Ok(backend.get_string(&PropertyId::EntityId(property)))
}

/// Build a RegisterSchema request from `Model::schema()` via
/// `registration_request`, send it to a schema-less durable node, and
/// confirm the catalog holds each field at the allocator-assigned id (sourced
/// from the SchemaRegistered response) with the normative (backend,
/// value_type) descriptors.
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

    let (reg_models, reg_properties, reg_memberships) = match client.request(server.id, &DEFAULT_CONTEXT, request).await? {
        proto::NodeResponseBody::SchemaRegistered { models, properties, memberships } => (models, properties, memberships),
        other => panic!("expected SchemaRegistered, got {other}"),
    };

    // The allocator resolved the ids; index them by display name for lookup.
    let model_id = reg_models.iter().find(|m| m.collection == "descalltypes").expect("model returned").id;
    let artist_model_id = reg_models.iter().find(|m| m.collection == "descartist").expect("reference target model returned").id;
    let property_ids: BTreeMap<String, EntityId> = reg_properties.iter().map(|p| (p.name.clone(), p.id)).collect();

    // The model entity exists with its collection + display name.
    let model = catalog_values(&server, model_id).await?;
    assert_eq!(model.get("label"), Some(&Some(Value::String("descalltypes".into()))));
    assert_eq!(model.get("name"), Some(&Some(Value::String("DescAllTypes".into()))));

    // Every active field is present as a property entity with the exact
    // normative descriptor pair the schema declared, at the allocated id.
    for f in DescAllTypes::schema().properties {
        let property_id = property_ids[f.name];
        let property = catalog_values(&server, property_id).await?;
        assert_eq!(property.get("backend"), Some(&Some(Value::String(f.backend.into()))), "backend for {}", f.field);
        assert_eq!(property.get("value_type"), Some(&Some(Value::String(f.value_type.into()))), "value_type for {}", f.field);
        assert_eq!(property.get("name"), Some(&Some(Value::String(f.name.into()))), "name for {}", f.field);
        assert_eq!(property.get("minted_for"), Some(&Some(Value::EntityId(model_id))), "minted_for for {}", f.field);
        let registered = reg_properties.iter().find(|p| p.id == property_id).expect("registered property returned");
        match f.target_collection {
            Some("descartist") => {
                assert_eq!(registered.target_model, Some(artist_model_id), "target_model response for {}", f.field);
                assert_eq!(
                    property.get("target_model"),
                    Some(&Some(Value::EntityId(artist_model_id))),
                    "stored target_model for {}",
                    f.field
                );
            }
            None => assert_eq!(registered.target_model, None, "non-reference {} has no target", f.field),
            Some(other) => panic!("unexpected target collection {other}"),
        }

        // And the (model, property) membership exists with the field's
        // optionality, at the allocated membership id.
        let membership_id = reg_memberships.iter().find(|m| m.property == property_id).expect("membership for property").id;
        let membership = catalog_values(&server, membership_id).await?;
        assert_eq!(membership.get("property"), Some(&Some(Value::EntityId(property_id))), "membership property for {}", f.field);
        assert_eq!(membership.get("optional"), Some(&Some(Value::Bool(f.optional))), "membership optional for {}", f.field);
    }

    Ok(())
}

/// An explicit property id is the derived field's runtime identity, not just
/// registration metadata. This covers Model initialization, View reads,
/// Mutable edits, predicate/ORDER BY resolution through the compiled alias,
/// canonical writer casting for an already-Id LWW entry, catalog rename
/// stability, and the offline fully-bound reassertion path.
#[tokio::test]
async fn explicit_id_drives_derived_access_and_query_aliases() -> anyhow::Result<()> {
    let property_id = EntityId::from_base64(ZERO_ID_B64)?;
    let text_property_id = EntityId::from_base64("AQEBAQEBAQEBAQEBAQEBAQ")?;
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;

    // Preseed a DDL-authored property at the fixed id. Explicit registration
    // references definitions authored elsewhere; it never mints its id.
    let property_event = catalog_lww_event(
        property_id,
        proto::Clock::default(),
        vec![
            ("minted_for", Value::EntityId(EntityId::new())),
            ("name", Value::String("catalog_score".into())),
            ("backend", Value::String("lww".into())),
            ("value_type", Value::String("i32".into())),
        ],
    );
    let text_property_event = catalog_lww_event(
        text_property_id,
        proto::Clock::default(),
        vec![
            ("minted_for", Value::EntityId(EntityId::new())),
            ("name", Value::String("catalog_text".into())),
            ("backend", Value::String("yrs".into())),
            ("value_type", Value::String("string".into())),
        ],
    );
    server
        .commit_remote_transaction(
            &DEFAULT_CONTEXT,
            proto::TransactionId::new(),
            vec![
                catalog_event_context("_ankurah_property", property_event),
                catalog_event_context("_ankurah_property", text_property_event),
            ],
        )
        .await?;
    wait_property_name(&server, property_id, "catalog_score").await?;
    wait_property_name(&server, text_property_id, "catalog_text").await?;

    let client = ephemeral_sled_setup().await?;
    let connection = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    let ctx = client.context_async(DEFAULT_CONTEXT).await;
    let bound_model = ctx.register::<explicit_binding_v1::BoundMetric>().await?;
    let empty_text: Vec<explicit_text_binding::BoundTextView> = ctx.fetch("local_text = 'none' ORDER BY local_text ASC").await?;
    assert!(empty_text.is_empty(), "predicate first use registers and resolves the explicit local alias");

    assert_eq!(
        client.catalog.resolve(&bound_model, "local_score"),
        Some(PropertyId::EntityId(property_id)),
        "ensured compiled alias resolves by id"
    );
    assert_eq!(
        client.catalog.resolve(&bound_model, "catalog_score"),
        Some(PropertyId::EntityId(property_id)),
        "canonical name remains resolvable"
    );

    let first_id = {
        let trx = ctx.begin();
        let metric = trx.create(&explicit_binding_v1::BoundMetric { local_score: 41 }).await?;
        let id = metric.id();
        trx.commit().await?;
        id
    };
    assert_eq!(
        stored_bound_metric_value(&server, first_id, property_id).await?,
        Some(Value::I32(41)),
        "explicit I64 write canonicalizes to I32"
    );

    let view = ctx.get::<explicit_binding_v1::BoundMetricView>(first_id).await?;
    assert_eq!(view.local_score()?, 41_i64, "derived View reads the literal id and casts back to the compiled type");
    let by_alias: Vec<explicit_binding_v1::BoundMetricView> = ctx.fetch("local_score = 41 ORDER BY local_score DESC").await?;
    assert_eq!(by_alias.iter().map(|metric| metric.id()).collect::<Vec<_>>(), vec![first_id]);

    let text_id = {
        let trx = ctx.begin();
        let text = trx.create(&explicit_text_binding::BoundText { local_text: "alpha".into() }).await?;
        let id = text.id();
        trx.commit().await?;
        id
    };
    assert_eq!(stored_bound_text(&server, text_id, text_property_id).await?.as_deref(), Some("alpha"));
    let text_view = ctx.get::<explicit_text_binding::BoundTextView>(text_id).await?;
    assert_eq!(text_view.local_text()?.as_str(), "alpha");
    let trx = ctx.begin();
    text_view.edit(&trx)?.local_text().replace("beta")?;
    trx.commit().await?;
    assert_eq!(stored_bound_text(&server, text_id, text_property_id).await?.as_deref(), Some("beta"));

    // Rename only the catalog display name. The compiled explicit alias and
    // every generated accessor keep addressing the same property id.
    let property_head = server.storage.get_state(property_id).await?.payload.state.head;
    let rename = catalog_lww_event(property_id, property_head, vec![("name", Value::String("renamed_score".into()))]);
    server
        .commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![catalog_event_context("_ankurah_property", rename)])
        .await?;
    wait_property_name(&server, property_id, "renamed_score").await?;
    wait_property_name(&client, property_id, "renamed_score").await?;

    let trx = ctx.begin();
    view.edit(&trx)?.local_score().set(&42)?;
    trx.commit().await?;
    assert_eq!(stored_bound_metric_value(&server, first_id, property_id).await?, Some(Value::I32(42)));
    assert_eq!(ctx.get::<explicit_binding_v1::BoundMetricView>(first_id).await?.local_score()?, 42_i64);
    let sorted: Vec<explicit_binding_v1::BoundMetricView> = ctx.fetch("local_score >= 0 ORDER BY local_score ASC").await?;
    assert_eq!(sorted.iter().map(|metric| metric.id()).collect::<Vec<_>>(), vec![first_id]);

    // A different compiled shape for the same collection has not latched.
    // With the durable peer gone, the reassertion is unavailable; the
    // explicit id + live membership are sufficient proof that every field is
    // already bound compatibly, so creation may stage safely by Id.
    drop(connection);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let trx = ctx.begin();
    let offline = trx.create(&explicit_binding_v2::BoundMetric { offline_alias: 7 }).await?;
    let offline_id = offline.id();
    assert_eq!(offline.offline_alias().get()?, 7_i64);
    drop(offline);

    let _reconnected = LocalProcessConnection::new(&server, &client).await?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client did not reconnect");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    trx.commit().await?;
    assert_eq!(stored_bound_metric_value(&server, offline_id, property_id).await?, Some(Value::I32(7)));
    assert_eq!(ctx.get::<explicit_binding_v2::BoundMetricView>(offline_id).await?.offline_alias()?, 7_i64);

    ctx.register::<explicit_binding_conflict::BoundMetric>().await?;
    assert_eq!(
        client.catalog.resolve(&bound_model, "local_score"),
        None,
        "explicit and ordinary ensured declarations for one local name must fail ambiguous resolution"
    );

    // The same ambiguity must fail closed on the ordinary writer before any
    // unresolved Name residue can reach a registered user-model event. Column-
    // layering (#307) resolves the accessor eagerly, so this fails closed at
    // `create` rather than deferring to `commit`; either surfacing point is
    // acceptable (create-time is strictly stronger: nothing is left to commit).
    let trx = ctx.begin();
    let error = match trx.create(&explicit_binding_conflict::BoundMetric { local_score: 99 }).await {
        Err(error) => error,
        Ok(_) => trx.commit().await.expect_err("an ambiguous ordinary field must fail before emitting Name residue"),
    };
    assert!(
        error.to_string().contains("property 'local_score'") && error.to_string().contains("is ambiguous"),
        "expected the admitted-alias ambiguity, got: {error}"
    );

    Ok(())
}

/// A membership can intentionally share a property minted for another model,
/// but that membership does not turn the property into an ordinary by-name
/// allocation in the receiving model's scope. In particular, the no-peer
/// fallback must not mistake the shared membership for proof that an ordinary
/// compiled field is fully bound.
#[tokio::test]
async fn offline_ordinary_field_does_not_capture_explicitly_shared_same_name() -> anyhow::Result<()> {
    let property_id = EntityId::from_base64("AwMDAwMDAwMDAwMDAwMDAw")?;
    let foreign_minting_model = EntityId::new();
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;

    let shared_property = catalog_lww_event(
        property_id,
        proto::Clock::default(),
        vec![
            ("minted_for", Value::EntityId(foreign_minting_model)),
            ("name", Value::String("name".into())),
            ("backend", Value::String("yrs".into())),
            ("value_type", Value::String("string".into())),
        ],
    );
    server
        .commit_remote_transaction(
            &DEFAULT_CONTEXT,
            proto::TransactionId::new(),
            vec![catalog_event_context("_ankurah_property", shared_property)],
        )
        .await?;
    wait_property_name(&server, property_id, "name").await?;

    let client = ephemeral_sled_setup().await?;
    let connection = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    let ctx = client.context_async(DEFAULT_CONTEXT).await;
    ctx.register::<explicitly_shared_name::SharedPlaylist>().await?;

    let receiving_model = client.catalog.model_by_label("sharedplaylist").expect("receiving model registered").id;
    assert_ne!(receiving_model, foreign_minting_model);
    assert_eq!(client.catalog.property_by_id(&property_id).expect("shared property present").minted_for, Some(foreign_minting_model));
    assert!(client.catalog.membership(&receiving_model, &property_id).is_some(), "explicit registration created the sharing membership");
    assert_eq!(
        client.catalog.resolve(&ankurah::ModelId::EntityId(receiving_model), "name"),
        Some(PropertyId::EntityId(property_id)),
        "the explicit compiled binding legitimately resolves the shared local alias"
    );

    drop(connection);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !client.get_durable_peers().is_empty() {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("client still has a durable peer after disconnect");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let trx = ctx.begin();
    let error = trx
        .create(&ordinary_same_name::SharedPlaylist { name: "must not capture sharing".into() })
        .await
        .expect_err("ordinary offline fallback must require matching minting provenance");
    assert!(
        error.to_string().contains("unconfirmed schema"),
        "expected the ordinary declaration to require allocator confirmation, got: {error}"
    );

    Ok(())
}

/// Yrs must resolve a new ordinary field before touching its CRDT document:
/// unlike LWW, its root history cannot be re-keyed at commit. An ambiguity
/// introduced after an ordinary entity was created therefore fails reads,
/// mutations, and later initialization normally, while the explicit accessor
/// remains usable by its literal id.
#[tokio::test]
async fn yrs_ordinary_alias_ambiguity_never_uses_a_name_root() -> anyhow::Result<()> {
    let explicit_id = EntityId::from_base64("AgICAgICAgICAgICAgICAg")?;
    let server = durable_sled_setup().await?;
    server.catalog.wait_catalog_ready().await;

    let explicit_property = catalog_lww_event(
        explicit_id,
        proto::Clock::default(),
        vec![
            ("minted_for", Value::EntityId(EntityId::new())),
            ("name", Value::String("catalog_clash_text".into())),
            ("backend", Value::String("yrs".into())),
            ("value_type", Value::String("string".into())),
        ],
    );
    server
        .commit_remote_transaction(
            &DEFAULT_CONTEXT,
            proto::TransactionId::new(),
            vec![catalog_event_context("_ankurah_property", explicit_property)],
        )
        .await?;
    wait_property_name(&server, explicit_id, "catalog_clash_text").await?;

    let client = ephemeral_sled_setup().await?;
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    let ctx = client.context_async(DEFAULT_CONTEXT).await;
    let clashing_model = ctx.register::<ordinary_yrs_alias::ClashingText>().await?;

    let ordinary_id = {
        let trx = ctx.begin();
        let text = trx.create(&ordinary_yrs_alias::ClashingText { local_text: "ordinary".into() }).await?;
        let id = text.id();
        trx.commit().await?;
        id
    };
    assert_eq!(ctx.get::<ordinary_yrs_alias::ClashingTextView>(ordinary_id).await?.local_text()?, "ordinary");

    let first_use_error = ctx
        .fetch::<explicit_yrs_alias::ClashingTextView>("local_text = 'ordinary'")
        .await
        .expect_err("exact explicit-id first use must not resolve through the ordinary same-named binding");
    assert!(
        first_use_error.to_string().contains("property 'local_text'") && first_use_error.to_string().contains("is ambiguous"),
        "the newly admitted explicit binding must make the local alias ambiguous, got: {first_use_error}"
    );
    assert_eq!(
        client.catalog.resolve(&clashing_model, "local_text"),
        None,
        "the ordinary and explicit ensured declarations deliberately make the local alias ambiguous"
    );

    let ordinary = ctx.get::<ordinary_yrs_alias::ClashingTextView>(ordinary_id).await?;
    let access_error = ordinary.local_text().expect_err("ordinary Yrs reads must reject an ambiguous resolver-bound alias");
    assert!(
        access_error.to_string().contains("property 'local_text'") && access_error.to_string().contains("is ambiguous"),
        "unexpected read error: {access_error}"
    );

    let trx = ctx.begin();
    let mutation_error = ordinary
        .edit(&trx)?
        .local_text()
        .replace("must not create a Name root")
        .expect_err("ordinary Yrs mutations must reject an ambiguous resolver-bound alias");
    assert!(
        mutation_error.to_string().contains("property 'local_text'") && mutation_error.to_string().contains("is ambiguous"),
        "unexpected mutation error: {mutation_error}"
    );

    let trx = ctx.begin();
    let initialization_error = match trx.create(&ordinary_yrs_alias::ClashingText { local_text: "also rejected".into() }).await {
        Ok(_) => panic!("ordinary Yrs initialization unexpectedly accepted an ambiguous alias"),
        Err(error) => error,
    };
    assert!(
        initialization_error.to_string().contains("property 'local_text'") && initialization_error.to_string().contains("is ambiguous"),
        "unexpected initialization error: {initialization_error}"
    );

    let explicit_entity = {
        let trx = ctx.begin();
        let text = trx.create(&explicit_yrs_alias::ClashingText { local_text: "explicit".into() }).await?;
        assert_eq!(text.local_text().value().as_deref(), Some("explicit"));
        let id = text.id();
        trx.commit().await?;
        id
    };
    assert_eq!(ctx.get::<explicit_yrs_alias::ClashingTextView>(explicit_entity).await?.local_text()?, "explicit");

    Ok(())
}
