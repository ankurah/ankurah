//! RFC 5.2 (rev 4): registration is an UPSERT protocol operation executed
//! by the durable node as the system's allocator. These tests drive
//! RegisterSchema over the wire with no model code on the server, exactly
//! the way a schema-less durable node serves ephemeral clients, and pin
//! the allocation semantics: ids come from the SchemaRegistered response,
//! repeats are pure no-ops, rename hints move lineages without re-keying,
//! retypes mint fresh identities, and the exists-aware policy verb gates
//! actual creations.

mod common;
use ankurah::core::property::backend::{LWWBackend, PropertyBackend};
use ankurah::core::value::Value;
use common::*;
use std::collections::BTreeMap;

const MODEL: &str = "_ankurah_model";
const PROPERTY: &str = "_ankurah_property";
const MEMBERSHIP: &str = "_ankurah_model_property";

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

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

/// RFC 5.1: the type pair is in the lookup key, so a retype mints a NEW
/// property identity; both live under one display name (sibling-gate
/// territory on the read side).
#[tokio::test]
async fn retype_mints_a_distinct_identity() -> anyhow::Result<()> {
    let (server, client, _conn) = connected_pair().await?;

    let first = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    let string_id = first.1[0].id;

    let retype = proto::NodeRequestBody::RegisterSchema {
        models: vec![],
        properties: vec![property("name", None, "lww", "i64")],
        memberships: vec![],
    };
    let retyped = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, retype).await?);
    let i64_id = retyped.1[0].id;
    assert_ne!(i64_id, string_id, "a retype is a new identity, never a merge");

    let values = catalog_values(&server, PROPERTY, i64_id).await?;
    assert_eq!(values.get("value_type"), Some(&Some(Value::String("i64".into()))));
    let old = catalog_values(&server, PROPERTY, string_id).await?;
    assert_eq!(old.get("value_type"), Some(&Some(Value::String("string".into()))), "the original lineage is untouched");

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
    assert_eq!(stub.get("collection"), Some(&Some(Value::String("artist".into()))));

    Ok(())
}

/// RFC 5.9: explicit-id binding references an existing property (sharing);
/// absence hard-fails, and a (backend, value_type) mismatch hard-fails.
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

    // Retyped binder: declared (backend, value_type) must match.
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
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, mismatch).await?, "binder declares");

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
/// this file. It is permissive everywhere except the three customization
/// points the tests exercise, each an optional hook closure: a schema plan
/// gate, a per-event gate, and a collection-access gate. A `Default` (all
/// hooks unset) instance is fully permissive -- the inert client side.
#[derive(Clone, Default)]
struct ProbeAgent {
    on_schema_registration:
        Option<std::sync::Arc<dyn Fn(&ankurah::policy::RegistrationPlan) -> Result<(), ankurah::policy::AccessDenied> + Send + Sync>>,
    on_event: Option<std::sync::Arc<dyn Fn(&proto::Event) -> Result<(), ankurah::policy::AccessDenied> + Send + Sync>>,
    on_collection_access: Option<std::sync::Arc<dyn Fn(&proto::CollectionId) -> Result<(), ankurah::policy::AccessDenied> + Send + Sync>>,
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
            hook(event)?;
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
        _state: &proto::Attested<proto::EntityState>,
    ) -> Result<(), ankurah::policy::AccessDenied> {
        Ok(())
    }

    fn can_access_collection<C>(&self, _data: &C, collection: &proto::CollectionId) -> Result<(), ankurah::policy::AccessDenied>
    where C: ankurah::core::util::Iterable<Self::ContextData> {
        if let Some(hook) = &self.on_collection_access {
            hook(collection)?;
        }
        Ok(())
    }

    fn filter_predicate<C>(
        &self,
        _data: &C,
        _collection: &proto::CollectionId,
        predicate: ankql::ast::Predicate,
    ) -> Result<ankql::ast::Predicate, ankurah::policy::AccessDenied>
    where
        C: ankurah::core::util::Iterable<Self::ContextData>,
    {
        Ok(predicate)
    }

    fn check_read<C>(
        &self,
        _data: &C,
        _id: &proto::EntityId,
        _collection: &proto::CollectionId,
        _state: &proto::State,
    ) -> Result<(), ankurah::policy::AccessDenied>
    where
        C: ankurah::core::util::Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(&self, _data: &C, _event: &proto::Attested<proto::Event>) -> Result<(), ankurah::policy::AccessDenied>
    where C: ankurah::core::util::Iterable<Self::ContextData> {
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
    assert!(server.catalog.model_by_collection("forbidden").is_none(), "a refused plan emits nothing");

    // An unrelated collection passes the same gate.
    expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    assert!(server.catalog.model_by_collection("album").is_some());

    Ok(())
}

/// The policy verb fires once for a plan with effects and is SKIPPED for a
/// pure no-op re-registration (REN 2 second ruling: read paths lean on the
/// upsert's idempotence, so a no-op registration must not consult the
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
    let storage = node.collections.get(&collection.into()).await?;
    let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    Ok(storage.fetch_states(&selection).await?.len())
}

/// The stored entity ids for `collection`, for identity-stability pins.
async fn row_ids<PA>(node: &Node<SledStorageEngine, PA>, collection: &str) -> anyhow::Result<Vec<EntityId>>
where PA: ankurah::policy::PolicyAgent + Send + Sync + 'static {
    let storage = node.collections.get(&collection.into()).await?;
    let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
    Ok(storage.fetch_states(&selection).await?.into_iter().map(|s| s.payload.entity_id).collect())
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

/// The commit batch is NOT transactional (maintainer ruling 2026-07-06):
/// a mid-batch check_event denial leaves earlier catalog events durable.
/// Identity must survive that: the retry's allocator lookup double-checks
/// STORAGE on its map miss (the aborted commit skipped the map fold) and
/// converges on the stored model id instead of re-minting.
#[tokio::test]
async fn partial_registration_retry_does_not_double_allocate() -> anyhow::Result<()> {
    let armed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let deny_property_once = {
        let armed = armed.clone();
        ProbeAgent {
            on_event: Some(std::sync::Arc::new(move |event: &proto::Event| {
                if event.collection.as_str() == PROPERTY && armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
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

    // First attempt: the model creation commits, then the property event is
    // denied mid-batch -- a durable partial, and no map fold.
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?, "denied");
    assert_eq!(count_rows(&server, MODEL).await?, 1, "partial: the model event preceded the denial and is durable");
    assert_eq!(count_rows(&server, PROPERTY).await?, 0, "the denied property event must not be durable");
    let stored = row_ids(&server, MODEL).await?;

    // Retry (agent now allows): the map missed the fold, so the allocator's
    // storage-backed lookup must find the stored model and reuse its id.
    let (models, properties, memberships) = expect_registered(client.request(server.id, &DEFAULT_CONTEXT, album_request()).await?);
    assert_eq!(models[0].id, stored[0], "retry converges on the stored model identity (no re-mint)");
    assert_eq!(count_rows(&server, MODEL).await?, 1, "still exactly one model row after the retry");
    assert_eq!(count_rows(&server, PROPERTY).await?, 1, "the retry completes the property");
    assert_eq!(count_rows(&server, MEMBERSHIP).await?, 1, "and the membership");
    assert_eq!(properties.len(), 1);
    assert_eq!(memberships.len(), 1);

    Ok(())
}

/// Schema knowledge follows collection access (RFC 5.7 addendum, ruling
/// 2026-07-06): the executor requires can_access_collection on every
/// collection a request names, BEFORE any lookup -- so the verb-skipped
/// no-op upsert cannot serve as an existence oracle. A denied principal
/// gets the same refusal whether the collection exists or not.
#[tokio::test]
async fn noop_probe_requires_collection_access() -> anyhow::Result<()> {
    let armed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let deny_secret = {
        let armed = armed.clone();
        ProbeAgent {
            on_collection_access: Some(std::sync::Arc::new(move |collection: &proto::CollectionId| {
                if armed.load(std::sync::atomic::Ordering::SeqCst) && collection.as_str().starts_with("secret") {
                    return Err(ankurah::policy::AccessDenied::ByPolicy("no access to secret collections"));
                }
                Ok(())
            })),
            ..Default::default()
        }
    };
    let server = Node::new_durable(std::sync::Arc::new(SledStorageEngine::new_test().unwrap()), deny_secret);
    server.system.create().await?;
    let client = Node::new(std::sync::Arc::new(SledStorageEngine::new_test().unwrap()), ProbeAgent::default());
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    // Setup (gate disarmed): "secret" gets registered legitimately.
    let secret = || proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "secret".into(), name: "Secret".into(), explicit_id: None }],
        properties: vec![],
        memberships: vec![],
    };
    expect_registered(client.request(server.id, &DEFAULT_CONTEXT, secret()).await?);

    // Gate armed: the no-op probe of the EXISTING collection is refused
    // before any lookup -- no ids escape.
    armed.store(true, std::sync::atomic::Ordering::SeqCst);
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, secret()).await?, "Access denied");

    // And a NEVER-registered collection under the same rule gets the same
    // refusal shape: existence is not distinguishable.
    let unseen = proto::NodeRequestBody::RegisterSchema {
        models: vec![proto::ModelDescriptor { collection: "secretx".into(), name: "SecretX".into(), explicit_id: None }],
        properties: vec![],
        memberships: vec![],
    };
    expect_error(client.request(server.id, &DEFAULT_CONTEXT, unseen).await?, "Access denied");

    Ok(())
}
