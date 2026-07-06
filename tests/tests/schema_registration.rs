//! RFC 5.2: registration is a protocol operation executed by durable
//! nodes. These tests drive RegisterSchema over the wire with no model
//! code on the server, exactly the way a schema-less durable node serves
//! ephemeral clients.

mod common;
use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
use ankurah::core::value::Value;
use ankurah::proto::schema_id;
use common::*;
use std::collections::BTreeMap;

const MODEL: &str = "_ankurah_model";
const PROPERTY: &str = "_ankurah_property";
const MEMBERSHIP: &str = "_ankurah_model_property";

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

fn album_request() -> proto::NodeRequestBody {
    proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into() }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "name".into(),
            anchored: false,
            name: "name".into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![proto::MembershipDescriptor {
            collection: "album".into(),
            property: proto::PropertyRef::Anchor("name".into()),
            optional: false,
        }],
    }
}

async fn catalog_values(node: &TestNode, collection: &str, id: EntityId) -> anyhow::Result<BTreeMap<String, Option<Value>>> {
    let storage = node.collections.get(&collection.into()).await?;
    let state = storage.get_state(id).await?;
    let buffer = state.payload.state.state_buffers.0.get("lww").expect("catalog entities are LWW").clone();
    Ok(LWWBackend::from_state_buffer(&buffer)?.property_values())
}

async fn catalog_head(node: &TestNode, collection: &str, id: EntityId) -> anyhow::Result<proto::Clock> {
    let storage = node.collections.get(&collection.into()).await?;
    Ok(storage.get_state(id).await?.payload.state.head)
}

async fn connected_pair(
) -> anyhow::Result<(TestNode, TestNode, LocalProcessConnection<SledStorageEngine, PermissiveAgent, SledStorageEngine, PermissiveAgent>)> {
    let server = durable_sled_setup().await?;
    let client = ephemeral_sled_setup().await?;
    let conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;
    Ok((server, client, conn))
}

fn expect_success(resp: proto::NodeResponseBody) {
    match resp {
        proto::NodeResponseBody::Success => {}
        other => panic!("expected Success, got {other}"),
    }
}

fn expect_error(resp: proto::NodeResponseBody, needle: &str) {
    match resp {
        proto::NodeResponseBody::Error(e) => assert!(e.contains(needle), "expected error containing '{needle}', got: {e}"),
        other => panic!("expected Error containing '{needle}', got {other}"),
    }
}

/// A schema-less durable node executes registration from descriptors
/// alone: catalog entities appear with derived ids, frozen genesis
/// fields, and follow-up metadata.
#[tokio::test]
async fn register_schema_creates_catalog_entities() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = schema_id::model_entity_id(&root, "album");
    let property_id = schema_id::property_entity_id(&root, &model_id, "name", "yrs", "string");
    let membership_id = schema_id::membership_entity_id(&root, &model_id, &property_id);

    let model = catalog_values(&server, MODEL, model_id).await?;
    assert_eq!(model.get("collection"), Some(&Some(Value::String("album".into()))));
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

/// Re-issuing the same registration converges on identical events: the
/// second pass is a pure no-op (heads unchanged), which is also what two
/// concurrent registrations of the same schema reduce to.
#[tokio::test]
async fn registration_is_idempotent() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = schema_id::model_entity_id(&root, "album");
    let property_id = schema_id::property_entity_id(&root, &model_id, "name", "yrs", "string");
    let membership_id = schema_id::membership_entity_id(&root, &model_id, &property_id);

    let heads_before = (
        catalog_head(&server, MODEL, model_id).await?,
        catalog_head(&server, PROPERTY, property_id).await?,
        catalog_head(&server, MEMBERSHIP, membership_id).await?,
    );

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    let heads_after = (
        catalog_head(&server, MODEL, model_id).await?,
        catalog_head(&server, PROPERTY, property_id).await?,
        catalog_head(&server, MEMBERSHIP, membership_id).await?,
    );
    assert_eq!(heads_before, heads_after, "re-registration must not mint new events");

    Ok(())
}

/// RFC 5.8: a rename keeps the derived id through the anchor, and a later
/// unrelated field re-using the retired display name is refused.
#[tokio::test]
async fn anchor_rename_and_reuse_refusal() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = schema_id::model_entity_id(&root, "album");
    let property_id = schema_id::property_entity_id(&root, &model_id, "name", "yrs", "string");

    // Rename: anchor pins the lineage, display name moves.
    let rename = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "name".into(),
            anchored: true,
            name: "title".into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, rename).await?);
    let property = catalog_values(&server, PROPERTY, property_id).await?;
    assert_eq!(property.get("name"), Some(&Some(Value::String("title".into()))), "display name follows the rename");

    // A brand-new field named "name" (default anchor) would silently
    // re-mint the renamed lineage's id: refused, demanding an anchor.
    let reuse = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "name".into(),
            anchored: false,
            name: "name".into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, reuse).await?, "anchor");

    Ok(())
}

/// RFC 5.8 rename-back: an EXPRESS anchor whose display name equals the
/// anchor is a deliberate lineage reference (the descriptor's `anchored`
/// bit), so it passes the reuse guard and restores the display name. The
/// byte-identical descriptor WITHOUT the attribute stays refused: that
/// shape is a brand-new field accidentally colliding with a retired name.
#[tokio::test]
async fn anchored_rename_back_restores_display_name() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = schema_id::model_entity_id(&root, "album");
    let property_id = schema_id::property_entity_id(&root, &model_id, "name", "yrs", "string");

    let request = |anchored: bool, name: &str| proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "name".into(),
            anchored,
            name: name.into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };

    // Rename away: display name becomes "title".
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, request(true, "title")).await?);
    let property = catalog_values(&server, PROPERTY, property_id).await?;
    assert_eq!(property.get("name"), Some(&Some(Value::String("title".into()))));

    // Rename back WITHOUT the attribute: indistinguishable from a new field
    // re-using the retired name; refused.
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, request(false, "name")).await?, "anchor");

    // Rename back WITH the express anchor: allowed, display name restored.
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, request(true, "name")).await?);
    let property = catalog_values(&server, PROPERTY, property_id).await?;
    assert_eq!(property.get("name"), Some(&Some(Value::String("name".into()))), "rename-back restores the display name");

    Ok(())
}

/// Follow-up metadata is PROVENANCE-ORDERED (RFC 5.8): each successive
/// rename descends the previous one, so recency decides. Under
/// genesis-parented follow-ups (the previous behavior) every rename after
/// the first was concurrent with its predecessor and won by event-id hash,
/// making this test a coin flip; head-parenting makes it deterministic.
#[tokio::test]
async fn chained_renames_win_by_recency_not_tiebreak() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = schema_id::model_entity_id(&root, "album");
    let property_id = schema_id::property_entity_id(&root, &model_id, "name", "yrs", "string");

    let rename = |name: &str| proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "name".into(),
            anchored: true,
            name: name.into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![],
    };

    let mut last_head = catalog_head(&server, PROPERTY, property_id).await?;
    for display in ["title", "caption", "headline"] {
        expect_success(client.request(server.id, &DEFAULT_CONTEXT, rename(display)).await?);
        let property = catalog_values(&server, PROPERTY, property_id).await?;
        assert_eq!(property.get("name"), Some(&Some(Value::String((*display).into()))), "latest rename must win");
        let head = catalog_head(&server, PROPERTY, property_id).await?;
        assert_ne!(head, last_head, "each rename must advance the head (descend, not fork)");
        last_head = head;
    }

    Ok(())
}

/// Membership `optional` flips are provenance-ordered metadata updates:
/// the newest registration's stance wins deterministically.
#[tokio::test]
async fn membership_optional_flip_updates() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let request = |optional: bool| proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: "Album".into() }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "album".into(),
            anchor: "name".into(),
            anchored: false,
            name: "name".into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: None,
        }],
        memberships: vec![proto::MembershipDescriptor {
            collection: "album".into(),
            property: proto::PropertyRef::Anchor("name".into()),
            optional,
        }],
    };

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, request(false)).await?);
    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = schema_id::model_entity_id(&root, "album");
    let property_id = schema_id::property_entity_id(&root, &model_id, "name", "yrs", "string");
    let membership_id = schema_id::membership_entity_id(&root, &model_id, &property_id);

    for expected in [true, false, true] {
        expect_success(client.request(server.id, &DEFAULT_CONTEXT, request(expected)).await?);
        let membership = catalog_values(&server, MEMBERSHIP, membership_id).await?;
        assert_eq!(membership.get("optional"), Some(&Some(Value::Bool(expected))), "the newest optionality stance must win");
    }

    Ok(())
}

/// Model display names are ordinary follow-up metadata: they rename and,
/// unlike before, REVERT to the collection name (the follow-up is emitted
/// whenever the catalog's current name differs from the descriptor's, not
/// only when the descriptor's name differs from the collection).
#[tokio::test]
async fn model_display_name_renames_and_reverts() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    let root = server.system.root().unwrap().payload.entity_id;
    let model_id = schema_id::model_entity_id(&root, "album");
    let model = catalog_values(&server, MODEL, model_id).await?;
    assert_eq!(model.get("name"), Some(&Some(Value::String("Album".into()))));

    let rename = |name: &str| proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "album".into(), name: name.into() }],
        properties: vec![],
        memberships: vec![],
    };

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, rename("Discography")).await?);
    let model = catalog_values(&server, MODEL, model_id).await?;
    assert_eq!(model.get("name"), Some(&Some(Value::String("Discography".into()))));

    // Revert to the collection name itself.
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, rename("album")).await?);
    let model = catalog_values(&server, MODEL, model_id).await?;
    assert_eq!(model.get("name"), Some(&Some(Value::String("album".into()))), "display name reverts to the collection name");

    Ok(())
}

/// RFC 5.9: explicit-id binding references an existing property (sharing);
/// absence hard-fails, and a (backend, value_type) mismatch hard-fails.
#[tokio::test]
async fn explicit_id_binding_and_sharing() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    expect_success(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);

    let root = server.system.root().unwrap().payload.entity_id;
    let album_id = schema_id::model_entity_id(&root, "album");
    let property_id = schema_id::property_entity_id(&root, &album_id, "name", "yrs", "string");

    // Model B shares album's property by explicit id, with its own
    // (differing) optionality stance.
    let share = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "playlist".into(), name: "Playlist".into() }],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "playlist".into(),
            anchor: "name".into(),
            anchored: false,
            name: "name".into(),
            backend: "yrs".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: Some(property_id),
        }],
        memberships: vec![proto::MembershipDescriptor {
            collection: "playlist".into(),
            property: proto::PropertyRef::Id(property_id),
            optional: true,
        }],
    };
    expect_success(client.request(server.id, &DEFAULT_CONTEXT, share).await?);

    let playlist_id = schema_id::model_entity_id(&root, "playlist");
    let membership_id = schema_id::membership_entity_id(&root, &playlist_id, &property_id);
    let membership = catalog_values(&server, MEMBERSHIP, membership_id).await?;
    assert_eq!(membership.get("property"), Some(&Some(Value::EntityId(property_id))));
    assert_eq!(membership.get("optional"), Some(&Some(Value::Bool(true))), "optionality is per contract");

    // Binding an id that does not exist never mints.
    let missing = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "playlist".into(),
            anchor: "ghost".into(),
            anchored: false,
            name: "ghost".into(),
            backend: "lww".into(),
            value_type: "string".into(),
            target_model: None,
            explicit_id: Some(EntityId::new()),
        }],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, missing).await?, "does not exist");

    // Retyped binder: declared (backend, value_type) must match.
    let mismatch = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![proto::PropertyDescriptor {
            minting_collection: "playlist".into(),
            anchor: "name".into(),
            anchored: false,
            name: "name".into(),
            backend: "lww".into(),
            value_type: "i64".into(),
            target_model: None,
            explicit_id: Some(property_id),
        }],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, mismatch).await?, "binder declares");

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
