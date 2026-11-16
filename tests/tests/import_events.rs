//! Entities made outside a node, entered into durable stores with `Node::import_events`.
//!
//! An entity's id is the hash of its genesis's own bytes, so a genesis is the same entity on every
//! node that stores it, whatever system root it names, or none. These tests pin that two durable
//! nodes under different system roots store one id and one state for such an entity, the state
//! `genesis_state` derives without a node, and that one node's structs bind to catalog definitions
//! another node registered once those are imported.

mod common;

use ankurah::core::entity::genesis_state;
use ankurah::core::storage::StorageEngine;
use ankurah::{policy::DEFAULT_CONTEXT as c, proto, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use common::{Album, AlbumView};
use std::collections::BTreeSet;
use std::sync::Arc;

type TestNode = Node<SledStorageEngine, PermissiveAgent>;

async fn durable_node() -> Result<TestNode> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    Ok(node)
}

/// Every event of every catalog definition `node` stores, each entity's genesis first.
async fn catalog_definition_events(node: &TestNode) -> Result<Vec<proto::Attested<proto::Event>>> {
    let mut events = Vec::new();
    for model in [proto::SystemModel::Model, proto::SystemModel::Property, proto::SystemModel::ModelProperty] {
        let selection = ankurah::ankql::ast::Selection {
            predicate: ankurah::ankql::ast::Predicate::MemberOf(proto::ModelId::System(model)),
            order_by: None,
            limit: None,
        };
        for state in node.storage.fetch_states(&selection).await? {
            let mut entity_events = node.storage.dump_entity_events(state.payload.entity_id).await?;
            entity_events.sort_by_key(|event| !event.payload.is_entity_create());
            events.extend(entity_events);
        }
    }
    Ok(events)
}

async fn stored_state(node: &TestNode, id: proto::EntityId) -> Result<proto::State> { Ok(node.storage.get_state(id).await?.payload.state) }

/// Node `a` registers `Album`; its definitions, which name `a`'s system root, are imported into `b`.
/// `b` then binds `Album` to them instead of registering its own, and stores each definition with the
/// state `a` stores and `genesis_state` derives. An entity minted outside both nodes, naming no system
/// root, is then one entity with one state in both stores and reads as an `Album` on `b`. Importing
/// the definitions again changes nothing.
#[tokio::test]
async fn imported_definitions_and_minted_entities_are_the_same_entities_under_two_roots() -> Result<()> {
    let a = durable_node().await?;
    let b = durable_node().await?;
    assert_ne!(a.system.root_id(), b.system.root_id(), "two durable stores mint two system roots");

    let trx = a.context_async(c).await?.begin();
    let on_a = trx.create(&Album { name: "Kind of Blue".to_owned(), year: "1959".to_owned() }).await?.id();
    trx.commit().await?;
    let album_model = stored_state(&a, on_a).await?.memberships;

    let definitions = catalog_definition_events(&a).await?;
    assert!(!definitions.is_empty(), "registering Album stored its definitions on a");
    b.import_events(&definitions).await?;

    for event in definitions.iter().filter(|event| event.payload.is_entity_create()) {
        let id = event.payload.entity_id;
        let on_b = stored_state(&b, id).await?;
        assert_eq!(on_b, genesis_state(&event.payload).await?.state, "b stores the state the genesis alone makes");
        assert_eq!(on_b, stored_state(&a, id).await?, "one definition holds one state on both nodes");
    }

    let trx = b.context_async(c).await?.begin();
    let on_b = trx.create(&Album { name: "A Love Supreme".to_owned(), year: "1965".to_owned() }).await?.id();
    trx.commit().await?;
    assert_eq!(stored_state(&b, on_b).await?.memberships, album_model, "b's Album binds to the definition a registered");
    let ids = |events: &[proto::Attested<proto::Event>]| events.iter().map(|event| event.payload.entity_id).collect::<BTreeSet<_>>();
    assert_eq!(ids(&catalog_definition_events(&b).await?), ids(&definitions), "b mints no definition of its own");

    let membership = album_model.iter().map(|model| proto::Operation::Membership(proto::Membership::Add(*model))).collect();
    let minted =
        proto::Event::genesis_from_parts(None, [7; 32], 1_720_000_000_000, proto::AuthorId::Unknown, proto::OperationSet(membership));
    a.import_events(&[minted.clone().into()]).await?;
    b.import_events(&[minted.clone().into()]).await?;
    let made = genesis_state(&minted).await?;
    assert_eq!(made.entity_id, minted.entity_id);
    assert_eq!(stored_state(&a, minted.entity_id).await?, made.state);
    assert_eq!(stored_state(&b, minted.entity_id).await?, made.state);
    let view = b.context_async(c).await?.get::<AlbumView>(minted.entity_id).await?;
    assert_eq!(view.id(), minted.entity_id, "the minted entity reads as an Album on b");

    let before: Vec<_> = definitions.iter().map(|event| event.payload.entity_id).collect();
    b.import_events(&definitions).await?;
    for id in before {
        assert_eq!(stored_state(&b, id).await?, stored_state(&a, id).await?, "importing again changes nothing");
        assert_eq!(b.storage.dump_entity_events(id).await?.len(), a.storage.dump_entity_events(id).await?.len());
    }
    Ok(())
}

/// An ephemeral node keeps no store of its own to import into, so it refuses with a typed error.
#[tokio::test]
async fn an_ephemeral_node_refuses_to_import() -> Result<()> {
    let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    let genesis = proto::Event::genesis_from_parts(None, [7; 32], 0, proto::AuthorId::Unknown, proto::OperationSet(Vec::new()));
    let error = node.import_events(&[genesis.into()]).await.expect_err("an ephemeral node imports nothing");
    assert!(matches!(error, ankurah::core::error::MutationError::NotDurable), "{error}");
    Ok(())
}
