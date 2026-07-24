//! RFC 5.2 (specs/model-property-metadata/rfc.md, rev 4): registration is an UPSERT protocol operation executed
//! by the durable node as the system's allocator. These tests drive
//! RegisterSchema over the wire with no model code on the server, exactly
//! the way a schema-less durable node serves ephemeral clients, and pin
//! the allocation semantics: ids come from the SchemaRegistered response,
//! repeats are pure no-ops, rename hints move lineages without re-keying,
//! retypes never fork (a castable declaration reuses the identity against
//! the CANONICAL value_type, a non-castable one refuses; rfc.md 5.6 as
//! amended 2026-07-10), and the exists-aware policy verb gates actual
//! creations.

mod common;
use ankql::ast::PropertyId;
use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
use ankurah::core::value::Value;
use common::*;
use std::collections::BTreeMap;

const MODEL: &str = "_ankurah_model";
const PROPERTY: &str = "_ankurah_property";
const MEMBERSHIP: &str = "_ankurah_model_property";

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

fn catalog_model(collection: &str) -> ankurah::ModelId {
    ankurah::core::schema::system_model_id(collection).expect("catalog helper only accepts built-in collection labels")
}

fn property(name: &str, renamed_from: Option<&str>, backend: &str, value_type: &str) -> proto::PropertyDescriptor {
    proto::PropertyDescriptor {
        minting_collection: "album".into(),
        name: name.into(),
        renamed_from: renamed_from.map(|s| s.to_string()),
        backend: backend.into(),
        value_type: value_type.into(),
        target_collection: None,
        explicit_id: None,
    }
}

fn album_request() -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![property("name", None, "yrs", "string")],
        memberships: vec![proto::MembershipDescriptor {
            collection: "album".into(),
            property: proto::PropertyRef::Name("name".into()),
            optional: false,
        }],
    }
}

async fn catalog_values(node: &TestNode, collection: &str, id: EntityId) -> anyhow::Result<BTreeMap<String, Option<Value>>> {
    let _model = catalog_model(collection);
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

async fn catalog_head(node: &TestNode, collection: &str, id: EntityId) -> anyhow::Result<proto::Clock> {
    let _model = catalog_model(collection);
    Ok(node.storage.get_state(id).await?.payload.state.head)
}

async fn connected_pair(
) -> anyhow::Result<(TestNode, TestNode, LocalProcessConnection<SledStorageEngine, PermissiveAgent, SledStorageEngine, PermissiveAgent>)> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    Ok((server, client, conn))
}

/// Unpack a SchemaRegistered response (the resolved definitions).
fn expect_registered(
    resp: proto::NodeResponseBody,
) -> (Vec<proto::RegisteredModel>, Vec<proto::RegisteredProperty>, Vec<proto::RegisteredMembership>) {
    match resp {
        proto::NodeResponseBody::SchemaRegistered { models, properties, memberships } => (models, properties, memberships),
        other => panic!("expected SchemaRegistered, got {other}"),
    }
}

fn expect_error(resp: proto::NodeResponseBody, needle: &str) {
    match resp {
        proto::NodeResponseBody::Error(e) => assert!(e.contains(needle), "expected error containing '{needle}', got: {e}"),
        other => panic!("expected Error containing '{needle}', got {other}"),
    }
}

/// A schema-less durable node executes registration from descriptors
/// alone: it allocates real ids, creates catalog entities via ordinary
/// events, and returns the resolved definitions in the response.
#[tokio::test]
async fn register_schema_creates_catalog_entities() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let (models, properties, memberships) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    assert_eq!((models.len(), properties.len(), memberships.len()), (1, 1, 1));
    let (model_id, property_id, membership_id) = (models[0].id, properties[0].id, memberships[0].id);
    assert_eq!(properties[0].model, model_id, "provenance scope is the request's model");

    let model = catalog_values(&server, MODEL, model_id).await?;
    assert_eq!(model.get("label"), Some(&Some(Value::String("album".into()))));
    assert_eq!(model.get("name"), Some(&Some(Value::String("Album".into()))));

    let property = catalog_values(&server, PROPERTY, property_id).await?;
    assert_eq!(property.get("minted_for"), Some(&Some(Value::EntityId(model_id))));
    assert_eq!(property.get("name"), Some(&Some(Value::String("name".into()))));
    assert_eq!(property.get("backend"), Some(&Some(Value::String("yrs".into()))));
    assert_eq!(property.get("value_type"), Some(&Some(Value::String("string".into()))));

    let membership = catalog_values(&server, MEMBERSHIP, membership_id).await?;
    assert_eq!(membership.get("model"), Some(&Some(Value::EntityId(model_id))));
    assert_eq!(membership.get("property"), Some(&Some(Value::EntityId(property_id))));
    assert_eq!(membership.get("optional"), Some(&Some(Value::Bool(false))));

    Ok(())
}

/// The upsert's idempotence: a repeat registration finds every lookup key,
/// emits ZERO events (heads unchanged), and returns the same ids.
#[tokio::test]
async fn registration_is_idempotent() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let (model_id, property_id, membership_id) = (first.0[0].id, first.1[0].id, first.2[0].id);

    let heads_before = (
        catalog_head(&server, MODEL, model_id).await?,
        catalog_head(&server, PROPERTY, property_id).await?,
        catalog_head(&server, MEMBERSHIP, membership_id).await?,
    );

    let second = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    assert_eq!((second.0[0].id, second.1[0].id, second.2[0].id), (model_id, property_id, membership_id), "the upsert returns the same ids");

    let heads_after = (
        catalog_head(&server, MODEL, model_id).await?,
        catalog_head(&server, PROPERTY, property_id).await?,
        catalog_head(&server, MEMBERSHIP, membership_id).await?,
    );
    assert_eq!(heads_before, heads_after, "re-registration must not mint new events");

    Ok(())
}

/// RFC 5.8: the renamed_from hint moves the lineage to the new name
/// WITHOUT re-keying (same property id), and no-ops once applied.
#[tokio::test]
async fn renamed_from_moves_the_lineage() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let property_id = first.1[0].id;

    // Rename: "name" -> "title", hinted.
    let rename = || proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![property("title", Some("name"), "yrs", "string")],
        memberships: vec![],
    };
    let renamed = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename()).await?);
    assert_eq!(renamed.1[0].id, property_id, "the hint preserves the lineage id");
    let values = catalog_values(&server, PROPERTY, property_id).await?;
    assert_eq!(values.get("name"), Some(&Some(Value::String("title".into()))), "display name follows the rename");

    // Applied hint no-ops: same id, head unchanged.
    let head_before = catalog_head(&server, PROPERTY, property_id).await?;
    let again = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename()).await?);
    assert_eq!(again.1[0].id, property_id);
    assert_eq!(catalog_head(&server, PROPERTY, property_id).await?, head_before, "an applied hint is a pure no-op");

    Ok(())
}

/// A full descriptor's absent reference target means clear. Both the ordinary
/// hit and rename paths emit a tombstone, and a later retarget remains mutable.
#[tokio::test]
async fn target_model_can_be_cleared_retargeted_and_cleared_on_rename() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let initial = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            name: "artist".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "entityid".into(),
            target_collection: Some("artist".into()),
            explicit_id: None,
        }],
        memberships: vec![],
    };
    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, initial).await?);
    let property_id = first.1[0].id;
    let target_model = first.1[0].target_model.expect("initial target resolved");

    let clear = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            name: "artist".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "entityid".into(),
            target_collection: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };
    let cleared = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, clear).await?);
    assert_eq!(cleared.1[0].id, property_id);
    assert_eq!(cleared.1[0].target_model, None);
    assert_eq!(catalog_values(&server, PROPERTY, property_id).await?.get("target_model"), Some(&None));

    let retarget = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            name: "artist".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "entityid".into(),
            target_collection: Some("artist".into()),
            explicit_id: None,
        }],
        memberships: vec![],
    };
    let retargeted = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, retarget).await?);
    assert_eq!(retargeted.1[0].target_model, Some(target_model));

    let rename = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            name: "performer".into(),
            renamed_from: Some("artist".into()),
            backend: "lww".into(),
            value_type: "entityid".into(),
            target_collection: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };
    let renamed = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename).await?);
    assert_eq!(renamed.1[0].id, property_id);
    assert_eq!(renamed.1[0].target_model, None);
    let values = catalog_values(&server, PROPERTY, property_id).await?;
    assert_eq!(values.get("target_model"), Some(&None));

    Ok(())
}

/// The hint's GUARD (RFC 5.8): it applies only when the current-name
/// lookup misses. A property already live under the current name wins and
/// the hint never merges identities. Also pins the policy-permitted
/// stale-writer fork: a hintless registration of the retired name
/// allocates a FRESH identity.
#[tokio::test]
async fn rename_hint_guard_and_stale_writer_fork() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let lineage = first.1[0].id;

    // Rename "name" -> "title".
    let rename = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![property("title", Some("name"), "yrs", "string")],
        memberships: vec![],
    };
    assert_eq!(expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename).await?).1[0].id, lineage);

    // A stale writer (pre-rename code) re-registers the retired name with
    // no hint: on this permissive system it allocates a fresh identity --
    // the visible fork, never a silent resurrection of the lineage.
    let stale = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![property("name", None, "yrs", "string")],
        memberships: vec![],
    };
    let fork = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, stale).await?).1[0].id;
    assert_ne!(fork, lineage, "a hintless retired-name registration allocates fresh (the policy-governed fork)");

    // A later hinted registration must NOT hijack the fork: the
    // current-name lookup ("title") hits the lineage, so the hint no-ops
    // and the fork keeps its name.
    let hinted_again = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![property("title", Some("name"), "yrs", "string")],
        memberships: vec![],
    };
    assert_eq!(expect_registered(client.request(server.id, &DEFAULT_CONTEXT, hinted_again).await?).1[0].id, lineage);
    let fork_values = catalog_values(&server, PROPERTY, fork).await?;
    assert_eq!(fork_values.get("name"), Some(&Some(Value::String("name".into()))), "the guard never renames the fork");

    Ok(())
}

/// Chained renames descend provenance-ordered (plan decision 18): each
/// hint application advances the head, recency decides, and the lineage id
/// never changes.
#[tokio::test]
async fn chained_renames_win_by_recency_not_tiebreak() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let property_id = first.1[0].id;

    let mut last_head = catalog_head(&server, PROPERTY, property_id).await?;
    for (from, to) in [("name", "title"), ("title", "caption"), ("caption", "headline")] {
        let rename = proto::NodeRequestBody::RegisterSchema {
            models: vec![],
            properties: vec![property(to, Some(from), "yrs", "string")],
            memberships: vec![],
        };
        let resolved = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename).await?);
        assert_eq!(resolved.1[0].id, property_id, "the lineage id never changes across the chain");
        let values = catalog_values(&server, PROPERTY, property_id).await?;
        assert_eq!(values.get("name"), Some(&Some(Value::String((*to).into()))), "latest rename must win");
        let head = catalog_head(&server, PROPERTY, property_id).await?;
        assert_ne!(head, last_head, "each rename must advance the head (descend, not fork)");
        last_head = head;
    }

    Ok(())
}

/// rfc.md 5.6 as amended 2026-07-10: the property lookup key is (model,
/// name) and the value_type is CANONICAL -- fixed at allocation, never
/// changed by registration. A same-name registration declaring a mutually
/// castable type REUSES the identity, and the response carries the CANONICAL
/// type (the requester's cast target), not the declaration.
#[tokio::test]
async fn castable_retype_reuses_the_identity_and_keeps_the_canonical_type() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let first = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![property("name", None, "lww", "string")],
        memberships: vec![],
    };
    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, first).await?);
    let string_id = first.1[0].id;
    assert_eq!(first.1[0].value_type, "string");

    let retype = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![property("name", None, "lww", "i64")],
        memberships: vec![],
    };
    let retyped = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, retype).await?);
    assert_eq!(retyped.1[0].id, string_id, "a castable retype reuses the identity, never a fork");
    assert_eq!(retyped.1[0].value_type, "string", "the response carries the CANONICAL type, the requester's cast target");

    let values = catalog_values(&server, PROPERTY, string_id).await?;
    assert_eq!(values.get("value_type"), Some(&Some(Value::String("string".into()))), "the canonical type never changes");

    Ok(())
}

/// A non-castable type pair refuses the registration loudly, and the
/// canonical definition is untouched. Changing a canonical type is a
/// deliberate migration (#303), never a model-struct edit.
#[tokio::test]
async fn non_castable_retype_refuses_registration() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let first = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![property("name", None, "lww", "string")],
        memberships: vec![],
    };
    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, first).await?);
    let string_id = first.1[0].id;

    let retype = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![property("name", None, "lww", "binary")],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, retype).await?, "not castable");

    let values = catalog_values(&server, PROPERTY, string_id).await?;
    assert_eq!(values.get("value_type"), Some(&Some(Value::String("string".into()))), "the refusal changes nothing");

    Ok(())
}

/// Conflicting same-name descriptors in ONE request meet the same
/// compatibility bar as a catalog hit: a castable duplicate coalesces onto
/// the first occurrence's resolution, a non-castable one refuses the whole
/// request instead of being silently absorbed.
#[tokio::test]
async fn in_flight_duplicate_descriptors_meet_the_compatibility_bar() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    // Castable duplicate (string then i64): coalesces, one property, the
    // first declaration fixes the canonical type.
    let castable = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![property("year", None, "lww", "string"), property("year", None, "lww", "i64")],
        memberships: vec![],
    };
    let registered = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, castable).await?);
    assert_eq!(registered.1.len(), 1, "duplicates coalesce onto one property");
    assert_eq!(registered.1[0].value_type, "string", "the first occurrence fixes the canonical type");

    // Non-castable duplicate (string then binary): the request refuses.
    let conflicting = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![property("length", None, "lww", "string"), property("length", None, "lww", "binary")],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, conflicting).await?, "not castable");

    Ok(())
}

/// A backend change is never castable (a different CRDT algebra, not a
/// cast): refuse, regardless of the value types.
#[tokio::test]
async fn backend_change_refuses_registration() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let yrs_id = first.1[0].id;

    let rebackend = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![property("name", None, "lww", "string")],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, rebackend).await?, "not castable");

    let values = catalog_values(&server, PROPERTY, yrs_id).await?;
    assert_eq!(values.get("backend"), Some(&Some(Value::String("yrs".into()))), "the refusal changes nothing");

    Ok(())
}

/// Membership `optional` flips are provenance-ordered metadata updates:
/// the newest registration's stance wins deterministically.
#[tokio::test]
async fn membership_optional_flip_updates() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let request = |optional: bool| proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![property("name", None, "yrs", "string")],
        memberships: vec![proto::MembershipDescriptor {
            collection: "album".into(),
            property: proto::PropertyRef::Name("name".into()),
            optional,
        }],
    };

    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, request(false)).await?);
    let membership_id = first.2[0].id;

    for expected in [true, false, true] {
        let resolved = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, request(expected)).await?);
        assert_eq!(resolved.2[0].id, membership_id, "the membership id is stable across flips");
        let membership = catalog_values(&server, MEMBERSHIP, membership_id).await?;
        assert_eq!(membership.get("optional"), Some(&Some(Value::Bool(expected))), "the newest optionality stance must win");
    }

    Ok(())
}

/// Model display names are ordinary follow-up metadata: they rename and
/// REVERT to the collection name (the follow-up is emitted whenever the
/// catalog's current name differs from the descriptor's).
#[tokio::test]
async fn model_display_name_renames_and_reverts() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let model_id = first.0[0].id;

    let model = catalog_values(&server, MODEL, model_id).await?;
    assert_eq!(model.get("name"), Some(&Some(Value::String("Album".into()))));

    let rename = |name: &str| proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: name.into(), explicit_id: None }],
        properties: vec![],
        memberships: vec![],
    };

    let resolved = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename("Discography")).await?);
    assert_eq!(resolved.0[0].id, model_id, "the model id is stable across display renames");
    let model = catalog_values(&server, MODEL, model_id).await?;
    assert_eq!(model.get("name"), Some(&Some(Value::String("Discography".into()))));

    // Revert to the collection name itself.
    expect_registered(client.request(server.id, &DEFAULT_CONTEXT, rename("album")).await?);
    let model = catalog_values(&server, MODEL, model_id).await?;
    assert_eq!(model.get("name"), Some(&Some(Value::String("album".into()))), "display name reverts to the collection name");

    Ok(())
}

/// A reference-typed property names its target model by COLLECTION; the
/// executor resolves it, allocating a stub model on miss (RFC 5.2), which
/// the response includes.
#[tokio::test]
async fn target_collection_resolves_and_allocates_on_miss() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let request = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            name: "artist".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "entityid".into(),
            target_collection: Some("artist".into()),
            explicit_id: None,
        }],
        memberships: vec![],
    };
    let (models, properties, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, request).await?);

    // The stub target model rides the response beside the declared model.
    let artist_model = models.iter().find(|m| m.collection == "artist").expect("stub target model allocated and returned");
    assert_eq!(properties[0].target_model, Some(artist_model.id));

    let values = catalog_values(&server, PROPERTY, properties[0].id).await?;
    assert_eq!(values.get("target_model"), Some(&Some(Value::EntityId(artist_model.id))));
    let stub = catalog_values(&server, MODEL, artist_model.id).await?;
    assert_eq!(stub.get("label"), Some(&Some(Value::String("artist".into()))));

    Ok(())
}

/// RFC 5.9: explicit-id binding references an existing property (sharing).
/// Absence and an incompatible canonical declaration hard-fail.
#[tokio::test]
async fn explicit_id_binding_and_sharing() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let property_id = first.1[0].id;

    // Model B shares album's property by explicit id, with its own
    // (differing) optionality stance.
    let share = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "playlist".into(), name: "Playlist".into(), explicit_id: None }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "playlist".into(),
            name: "name".into(),
            renamed_from: None,
            backend: "yrs".into(),
            value_type: "string".into(),
            target_collection: None,
            explicit_id: Some(property_id),
        }],
        memberships: vec![proto::MembershipDescriptor {
            collection: "playlist".into(),
            property: proto::PropertyRef::Id(property_id),
            optional: true,
        }],
    };
    let shared = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, share).await?);
    assert_eq!(shared.1[0].id, property_id, "binding resolves to the shared property, never mints");
    let membership_id = shared.2[0].id;

    let membership = catalog_values(&server, MEMBERSHIP, membership_id).await?;
    assert_eq!(membership.get("property"), Some(&Some(Value::EntityId(property_id))));
    assert_eq!(membership.get("optional"), Some(&Some(Value::Bool(true))), "optionality is per contract");

    // Binding an id that does not exist never mints.
    let missing = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "playlist".into(),
            name: "ghost".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "string".into(),
            target_collection: None,
            explicit_id: Some(EntityId::new()),
        }],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, missing).await?, "does not exist");

    // A backend change is always incompatible, regardless of value_type.
    let mismatch = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "playlist".into(),
            name: "name".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "i64".into(),
            target_collection: None,
            explicit_id: Some(property_id),
        }],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, mismatch).await?, "not castable to/from");

    Ok(())
}

/// An explicit property binding follows the same canonical-type rule as a
/// name-keyed hit: mutually castable drift is admitted and the response
/// reports the bound property's canonical type.
#[tokio::test]
async fn explicit_id_binding_accepts_castable_type_drift() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let canonical = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![property("year", None, "lww", "i32")],
        memberships: vec![],
    };
    let property_id = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, canonical).await?).1[0].id;

    let drifted = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "playlist".into(), name: "Playlist".into(), explicit_id: None }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "playlist".into(),
            name: "year".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "i64".into(),
            target_collection: None,
            explicit_id: Some(property_id),
        }],
        memberships: vec![],
    };
    let bound = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, drifted).await?);
    assert_eq!(bound.1[0].id, property_id);
    assert_eq!(bound.1[0].value_type, "i32", "response carries the canonical type");
    let values = catalog_values(&server, PROPERTY, property_id).await?;
    assert_eq!(values.get("value_type"), Some(&Some(Value::String("i32".into()))));

    Ok(())
}

/// A membership cannot bind directly to an unknown property id. The
/// rejection happens before any of the request's planned creations commit.
#[tokio::test]
async fn dangling_membership_property_id_refuses_before_writes() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    let missing = EntityId::new();
    let request = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "dangling".into(), name: "Dangling".into(), explicit_id: None }],
        properties: vec![],
        memberships: vec![proto::MembershipDescriptor {
            collection: "dangling".into(),
            property: proto::PropertyRef::Id(missing),
            optional: false,
        }],
    };

    expect_error(client.request(server.id, &DEFAULT_CONTEXT, request).await?, "does not exist");
    assert_eq!(server.catalog.model_id_for("dangling"), None, "planned model creation must not commit on refusal");

    Ok(())
}

/// RFC 5.9 for models: explicit model binding verifies and never mints,
/// and a property in the same request resolves under the BOUND model id.
#[tokio::test]
async fn explicit_model_id_binding() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let album_model = first.0[0].id;

    let bind = |collection: &str, explicit_id, properties| proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: collection.into(), name: collection.into(), explicit_id }],
        properties,
        memberships: vec![],
    };

    // Binding an id that does not exist never mints.
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, bind("album", Some(EntityId::new()), vec![])).await?, "does not exist");

    // Binding a different collection to album's model entity is a mismatch.
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, bind("albumx", Some(album_model), vec![])).await?, "binder declares");

    // A matching binding succeeds, and a property in the same request
    // resolves under the BOUND model id.
    let genre = proto::PropertyDescriptor {
        minting_collection: "album".into(),
        name: "genre".into(),
        renamed_from: None,
        backend: "lww".into(),
        value_type: "string".into(),
        target_collection: None,
        explicit_id: None,
    };
    let bound = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, bind("album", Some(album_model), vec![genre])).await?);
    assert_eq!(bound.1[0].model, album_model, "the property minted under the bound model id");
    let values = catalog_values(&server, PROPERTY, bound.1[0].id).await?;
    assert_eq!(values.get("name"), Some(&Some(Value::String("genre".into()))));
    assert_eq!(values.get("minted_for"), Some(&Some(Value::EntityId(album_model))));

    Ok(())
}

/// Ephemeral nodes never execute registration; they forward it.
#[tokio::test]
async fn ephemeral_node_refuses_execution() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    // server -> client direction: the ephemeral node is asked to execute.
    let resp = server.request(client.id, &DEFAULT_CONTEXT, album_request()).await?;
    expect_error(resp, "durable");
    Ok(())
}

// -- the exists-aware policy verb (RFC 5.7, plan decision 26) ---------------

/// One configurable PermissiveAgent clone driving every policy-verb test in
/// this file. It is permissive everywhere except the customization points the
/// tests exercise: a schema plan gate, a per-event gate, a collection-access
/// gate, and an optional name-keyed catalog filter. A `Default` instance is
/// fully permissive -- the inert client side.
#[derive(Clone, Default)]
struct ProbeAgent {
    on_schema_registration:
        Option<std::sync::Arc<dyn Fn(&ankurah::policy::RegistrationPlan) -> Result<(), ankurah::policy::AccessDenied> + Send + Sync>>,
    on_event: Option<std::sync::Arc<dyn Fn(&ankurah::ModelId, &proto::Event) -> Result<(), ankurah::policy::AccessDenied> + Send + Sync>>,
    on_collection_access: Option<std::sync::Arc<dyn Fn(&ankurah::ModelId) -> Result<(), ankurah::policy::AccessDenied> + Send + Sync>>,
    filter_catalog_properties: bool,
}

#[async_trait::async_trait]
impl ankurah::policy::PolicyAgent for ProbeAgent {
    type ContextData = &'static ankurah::policy::DefaultContext;

    fn sign_request<SE: ankurah::core::storage::StorageEngine, C>(
        &self,
        _node: &ankurah::core::node::NodeInner<SE, Self>,
        cdata: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, ankurah::policy::AccessDenied>
    where
        C: ankurah::core::util::Iterable<Self::ContextData>,
    {
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: ankurah::core::storage::StorageEngine, A>(
        &self,
        _node: &Node<SE, Self>,
        auth: &A,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ankurah::core::error::ValidationError>
    where
        A: ankurah::core::util::Iterable<proto::AuthData> + Send + Sync,
    {
        Ok(auth.iterable().map(|_| DEFAULT_CONTEXT).collect())
    }

    fn check_event<SE: ankurah::core::storage::StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &ankurah::core::entity::Entity,
        _entity_after: &ankurah::core::entity::Entity,
        event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, ankurah::policy::AccessDenied> {
        if let Some(hook) = &self.on_event {
            hook(_entity_after.collection(), event)?;
        }
        Ok(None)
    }

    fn check_schema_registration<SE: ankurah::core::storage::StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        plan: &ankurah::policy::RegistrationPlan,
    ) -> Result<(), ankurah::policy::AccessDenied> {
        if let Some(hook) = &self.on_schema_registration {
            hook(plan)?;
        }
        Ok(())
    }

    fn validate_received_event<SE: ankurah::core::storage::StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::EntityId,
        _model: &proto::ModelId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), ankurah::policy::AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: ankurah::core::storage::StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _state: &proto::EntityState,
    ) -> Option<proto::Attestation> {
        None
    }

    fn validate_received_state<SE: ankurah::core::storage::StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _from_node: &proto::EntityId,
        _model: &proto::ModelId,
        _state: &proto::Attested<proto::EntityState>,
    ) -> Result<(), ankurah::policy::AccessDenied> {
        Ok(())
    }

    fn can_access_collection<C>(&self, _data: &C, collection: &ankurah::ModelId) -> Result<(), ankurah::policy::AccessDenied>
    where C: ankurah::core::util::Iterable<Self::ContextData> {
        if let Some(hook) = &self.on_collection_access {
            hook(collection)?;
        }
        Ok(())
    }

    fn filter_predicate<C>(
        &self,
        _data: &C,
        collection: &ankurah::ModelId,
        predicate: ankql::ast::Predicate,
    ) -> Result<ankql::ast::Predicate, ankurah::policy::AccessDenied>
    where
        C: ankurah::core::util::Iterable<Self::ContextData>,
    {
        if self.filter_catalog_properties && *collection == ankurah::core::schema::property_collection() {
            let name_filter = ankql::parser::parse_selection("name != '__never__'").expect("static catalog policy filter parses").predicate;
            return Ok(ankql::ast::Predicate::And(Box::new(predicate), Box::new(name_filter)));
        }
        Ok(predicate)
    }

    fn check_read<C>(
        &self,
        _data: &C,
        _id: &proto::EntityId,
        _collection: &ankurah::ModelId,
        _state: &proto::State,
        _resolver: Option<std::sync::Weak<dyn ankurah::core::schema::CatalogResolver>>,
    ) -> Result<(), ankurah::policy::AccessDenied>
    where
        C: ankurah::core::util::Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(
        &self,
        _data: &C,
        _collection: &ankurah::ModelId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), ankurah::policy::AccessDenied>
    where
        C: ankurah::core::util::Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_write(
        &self,
        _context: &Self::ContextData,
        _entity: &ankurah::core::entity::Entity,
        _event: Option<&proto::Event>,
    ) -> Result<(), ankurah::policy::AccessDenied> {
        Ok(())
    }

    fn validate_causal_assertion<SE: ankurah::core::storage::StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer_id: &proto::EntityId,
        _head_relation: &proto::CausalAssertion,
    ) -> Result<(), ankurah::policy::AccessDenied> {
        Ok(())
    }
}

/// Catalog fields are the frozen, name-keyed bootstrap schema. A policy
/// filter that references one of those fields must therefore pass through
/// without trying to resolve it through the catalog being initialized. Doing
/// so would make the property-catalog query recursively call
/// `ensure_subscribed` and wait on its own readiness latch forever.
#[tokio::test]
async fn name_filtered_catalog_subscription_bootstraps_without_recursion() -> anyhow::Result<()> {
    let agent = ProbeAgent { filter_catalog_properties: true, ..Default::default() };
    let server = Node::new_durable(std::sync::Arc::new(SledStorageEngine::new_test()?), agent.clone());
    server.system.create().await?;
    let client = Node::new(std::sync::Arc::new(SledStorageEngine::new_test()?), agent);
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let (_, properties, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let property_id = properties[0].id;

    tokio::time::timeout(std::time::Duration::from_secs(5), client.context_async(DEFAULT_CONTEXT))
        .await
        .expect("a name-filtered catalog subscription must not recurse into its own warm");
    assert!(client.catalog.is_catalog_ready());
    let album_model = client.catalog.model_id_for("album").expect("album is registered");
    assert_eq!(client.catalog.resolve(&album_model, "name"), Some(PropertyId::EntityId(property_id)));

    Ok(())
}

/// The exists-aware gate: a plan that would CREATE a forbidden definition
/// is refused BEFORE anything is emitted (no catalog entities appear), and
/// an unrelated registration on the same node passes.
#[tokio::test]
async fn check_schema_registration_gates_creates() -> anyhow::Result<()> {
    let deny_forbidden = ProbeAgent {
        on_schema_registration: Some(std::sync::Arc::new(|plan: &ankurah::policy::RegistrationPlan| {
            if plan.creates_models.iter().any(|(_, m)| m.collection == "forbidden") {
                return Err(ankurah::policy::AccessDenied::ByPolicy("schema definition for 'forbidden' is not writable"));
            }
            Ok(())
        })),
        ..Default::default()
    };
    let server = Node::new_durable(std::sync::Arc::new(SledStorageEngine::new_test().unwrap()), deny_forbidden.clone());
    server.system.create().await?;
    let client = Node::new(std::sync::Arc::new(SledStorageEngine::new_test().unwrap()), deny_forbidden);
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let forbidden = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "forbidden".into(), name: "Forbidden".into(), explicit_id: None }],
        properties: vec![],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, forbidden).await?, "policy");
    assert!(server.catalog.model_by_label("forbidden").is_none(), "a refused plan emits nothing");

    // An unrelated collection passes the same gate.
    expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    assert!(server.catalog.model_by_label("album").is_some());

    Ok(())
}

/// The policy verb fires once for a plan with effects and is SKIPPED for a
/// pure no-op re-registration (REN 2 second ruling: predicate-resolution
/// paths lean on the upsert's idempotence, so a no-op registration must not consult the
/// agent -- zero events, zero policy calls, full definitions returned).
#[tokio::test]
async fn policy_verb_skipped_on_noop_reregistration() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let count = std::sync::Arc::new(AtomicUsize::new(0));
    let counting = {
        let count = count.clone();
        ProbeAgent {
            on_schema_registration: Some(std::sync::Arc::new(move |_plan: &ankurah::policy::RegistrationPlan| {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })),
            ..Default::default()
        }
    };
    let server = Node::new_durable(std::sync::Arc::new(SledStorageEngine::new_test().unwrap()), counting.clone());
    server.system.create().await?;
    let client = Node::new(std::sync::Arc::new(SledStorageEngine::new_test().unwrap()), counting);
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let (models1, props1, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    assert_eq!(count.load(Ordering::SeqCst), 1, "a plan with creates consults the agent exactly once");

    let (models2, props2, _) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    assert_eq!(count.load(Ordering::SeqCst), 1, "a pure no-op re-registration skips the policy verb");
    assert_eq!(models1[0].id, models2[0].id, "the skipped no-op still returns the same ids");
    assert_eq!(props1[0].id, props2[0].id, "the skipped no-op still returns the same ids");

    Ok(())
}

/// Rows currently stored for `collection` (allocator-truth assertions).
async fn count_rows<PA>(node: &Node<SledStorageEngine, PA>, collection: &str) -> anyhow::Result<usize>
where PA: ankurah::policy::PolicyAgent + Send + Sync + 'static {
    let model = catalog_model(collection);
    let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    Ok(node.storage.fetch_states(&model, &selection).await?.len())
}

/// Duplicate descriptors inside ONE request must coalesce onto a single
/// allocation (the in-flight tables are consulted before the catalog map,
/// which only learns this request's ids at the post-commit fold).
#[tokio::test]
async fn duplicate_descriptors_in_one_request_do_not_double_allocate() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let request = proto::NodeRequestBody::RegisterSchema {
        models: vec![
            proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None },
            proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None },
        ],
        properties: vec![property("name", None, "yrs", "string"), property("name", None, "yrs", "string")],
        memberships: vec![
            proto::MembershipDescriptor { collection: "album".into(), property: proto::PropertyRef::Name("name".into()), optional: false },
            proto::MembershipDescriptor { collection: "album".into(), property: proto::PropertyRef::Name("name".into()), optional: false },
        ],
    };
    let (models, properties, memberships) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, request).await?);
    assert_eq!(models.len(), 1, "duplicate model descriptors coalesce (first occurrence wins)");
    assert_eq!(properties.len(), 1, "duplicate property descriptors coalesce");
    assert_eq!(memberships.len(), 1, "duplicate membership descriptors coalesce");

    assert_eq!(count_rows(&server, MODEL).await?, 1, "exactly one model row durable");
    assert_eq!(count_rows(&server, PROPERTY).await?, 1, "exactly one property row durable");
    assert_eq!(count_rows(&server, MEMBERSHIP).await?, 1, "exactly one membership row durable");

    Ok(())
}

/// Registration validates the complete logical transaction before appending
/// any event. A mid-batch policy denial therefore leaves no partial catalog
/// rows, and a retry creates exactly one model, property, and membership.
#[tokio::test]
async fn registration_policy_denial_is_failure_atomic() -> anyhow::Result<()> {
    let armed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let deny_property_once = {
        let armed = armed.clone();
        ProbeAgent {
            on_event: Some(std::sync::Arc::new(move |model: &ankurah::ModelId, _event: &proto::Event| {
                // The built-in property catalog routes by its system name.
                let property_model = proto::ModelId::System(proto::SystemModel::Property);
                if *model == property_model && armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    return Err(ankurah::policy::AccessDenied::ByPolicy("property event denied once"));
                }
                Ok(())
            })),
            ..Default::default()
        }
    };
    let server = Node::new_durable(std::sync::Arc::new(SledStorageEngine::new_test().unwrap()), deny_property_once);
    server.system.create().await?;
    let client = Node::new(std::sync::Arc::new(SledStorageEngine::new_test().unwrap()), ProbeAgent::default());
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    // First attempt: the property event is denied while validating the whole
    // logical transaction, before any event is appended or state is committed.
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?, "denied");
    assert_eq!(count_rows(&server, MODEL).await?, 0, "a denied registration must not leave a model row");
    assert_eq!(count_rows(&server, PROPERTY).await?, 0, "the denied property event must not be durable");
    assert_eq!(count_rows(&server, MEMBERSHIP).await?, 0, "a denied registration must not leave a membership row");

    // Retry (agent now allows): one complete catalog transaction commits.
    let (models, properties, memberships) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    assert_eq!(count_rows(&server, MODEL).await?, 1, "the retry creates exactly one model row");
    assert_eq!(count_rows(&server, PROPERTY).await?, 1, "the retry completes the property");
    assert_eq!(count_rows(&server, MEMBERSHIP).await?, 1, "and the membership");
    assert_eq!(models.len(), 1);
    assert_eq!(properties.len(), 1);
    assert_eq!(memberships.len(), 1);

    Ok(())
}

/// No legitimate registration names a reserved collection: the system and
/// catalog collections route by name and have no catalog model entities of
/// their own, so a descriptor naming one could only route ordinary traffic into a
/// protected collection. The executor refuses the reserved prefix in every
/// position a request can name a collection -- the model itself, a
/// property's minting or target collection, and a membership -- up front,
/// before policy, lookups, or writes (the executor-side twin of the
/// descriptor-ingest guard in catalog.rs).
#[tokio::test]
async fn reserved_collection_prefix_refuses_registration() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    // As the model collection (a known catalog collection, and a novel name
    // under the prefix: the rule is the prefix, not an allowlist).
    for collection in ["_ankurah_model", "_ankurah_custom"] {
        let as_model = proto::NodeRequestBody::RegisterSchema {
            models: vec![proto::ModelDescriptor { collection: collection.into(), name: "Model".into(), explicit_id: None }],
            properties: vec![],
            memberships: vec![],
        };
        expect_error(client.request(server.id, &DEFAULT_CONTEXT, as_model).await?, "reserved prefix");
    }

    // As a property's minting collection.
    let as_minting = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "_ankurah_property".into(),
            name: "extra".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "string".into(),
            target_collection: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, as_minting).await?, "reserved prefix");

    // As a property's target collection, riding along with an otherwise
    // ordinary request: the whole request is refused before any write.
    let as_target = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into(), explicit_id: None }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            name: "owner".into(),
            renamed_from: None,
            backend: "lww".into(),
            value_type: "entity_ref".into(),
            target_collection: Some("_ankurah_model".into()),
            explicit_id: None,
        }],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, as_target).await?, "reserved prefix");

    // As a membership's collection.
    let as_membership = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![],
        memberships: vec![proto::MembershipDescriptor {
            collection: "_ankurah_model_property".into(),
            property: proto::PropertyRef::Name("optional".into()),
            optional: true,
        }],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, as_membership).await?, "reserved prefix");

    // The refusals happened before the plan committed anything: the catalog
    // holds no model or property rows (not even the "album" rider).
    assert_eq!(count_rows(&server, MODEL).await?, 0, "no model row survives a refused request");
    assert_eq!(count_rows(&server, PROPERTY).await?, 0, "no property row survives a refused request");

    Ok(())
}
