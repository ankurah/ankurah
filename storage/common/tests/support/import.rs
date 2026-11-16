//! Shared node-level case for events made outside a node, entered with `Node::import_events`.
//!
//! An entity's id is the hash of its genesis's own bytes, so a genesis is the same entity on every node
//! that stores it, whatever system root it names, or none. Definitions one durable node registered are,
//! once imported into another node under a different root, the definitions that node's structs bind to;
//! an entity minted outside both nodes is one entity with one state in both stores; and importing again
//! leaves everything stored as it was, later writes included.

use std::{collections::BTreeSet, sync::Arc};

use ankurah::core::{entity::genesis_state, storage::StorageEngine};
use ankurah::{policy::DEFAULT_CONTEXT as c, proto, Model, Node, PermissiveAgent};

#[derive(Model, Debug)]
pub struct Release {
    pub title: String,
    pub year: String,
}

async fn durable_node<SE: StorageEngine + Send + Sync + 'static>(engine: SE) -> anyhow::Result<Node<SE, PermissiveAgent>> {
    let node = Node::new_durable(Arc::new(engine), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    Ok(node)
}

/// Every event of every catalog definition `node` stores, each entity's genesis first.
async fn catalog_definition_events<SE: StorageEngine + Send + Sync + 'static>(
    node: &Node<SE, PermissiveAgent>,
) -> anyhow::Result<Vec<proto::Attested<proto::Event>>> {
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

fn ids(events: &[proto::Attested<proto::Event>]) -> BTreeSet<proto::EntityId> {
    events.iter().map(|event| event.payload.entity_id).collect()
}

async fn stored_state<SE: StorageEngine>(node: &Node<SE, PermissiveAgent>, id: proto::EntityId) -> anyhow::Result<proto::State> {
    Ok(node.storage.get_state(id).await?.payload.state)
}

pub async fn check<SE: StorageEngine + Send + Sync + 'static>(a: SE, b: SE) -> anyhow::Result<()> {
    let a = durable_node(a).await?;
    let b = durable_node(b).await?;
    assert_ne!(a.system.root_id(), b.system.root_id(), "two durable stores mint two system roots");

    // Definitions a registered, which name a's root, bind b's struct once imported.
    let trx = a.context_async(c).await?.begin();
    let on_a = trx.create(&Release { title: "Kind of Blue".to_owned(), year: "1959".to_owned() }).await?.id();
    trx.commit().await?;
    let release_model = stored_state(&a, on_a).await?.memberships;
    let definitions = catalog_definition_events(&a).await?;
    assert!(!definitions.is_empty(), "registering Release stored its definitions on a");
    b.import_events(&definitions).await?;
    for event in definitions.iter().filter(|event| event.payload.is_entity_create()) {
        let id = event.payload.entity_id;
        let on_b = stored_state(&b, id).await?;
        assert_eq!(on_b, genesis_state(&event.payload).await?.state, "b stores the state the genesis alone makes");
        assert_eq!(on_b, stored_state(&a, id).await?, "one definition holds one state on both nodes");
    }
    let trx = b.context_async(c).await?.begin();
    let on_b = trx.create(&Release { title: "A Love Supreme".to_owned(), year: "1965".to_owned() }).await?.id();
    trx.commit().await?;
    assert_eq!(stored_state(&b, on_b).await?.memberships, release_model, "b's Release binds to the definition a registered");
    assert_eq!(ids(&catalog_definition_events(&b).await?), ids(&definitions), "b registers no definition of its own");

    // An entity minted outside both nodes, naming no system root, is one entity with one state in both.
    let membership = release_model.iter().map(|model| proto::Operation::Membership(proto::Membership::Add(*model))).collect();
    let minted =
        proto::Event::genesis_from_parts(None, [7; 32], 1_720_000_000_000, proto::AuthorId::Unknown, proto::OperationSet(membership));
    a.import_events(&[minted.clone().into()]).await?;
    b.import_events(&[minted.clone().into()]).await?;
    let made = genesis_state(&minted).await?;
    assert_eq!(made.entity_id, minted.entity_id);
    assert_eq!(stored_state(&a, minted.entity_id).await?, made.state);
    assert_eq!(stored_state(&b, minted.entity_id).await?, made.state);
    let view = b.context_async(c).await?.get::<ReleaseView>(minted.entity_id).await?;
    assert_eq!(view.id(), minted.entity_id, "the minted entity reads as a Release on b");

    // Importing again leaves every stored record as it was, including a write made after the import.
    let context = b.context_async(c).await?;
    let trx = context.begin();
    context.get::<ReleaseView>(on_b).await?.edit(&trx)?.year()?.replace("1964")?;
    trx.commit().await?;
    let created = b.storage.dump_entity_events(on_b).await?;
    let later = b.storage.get_state(on_b).await?;
    b.import_events(&created).await?;
    b.import_events(&definitions).await?;
    assert_eq!(b.storage.get_state(on_b).await?, later, "the later state of an entity survives importing its events again");
    for id in ids(&definitions) {
        assert_eq!(stored_state(&b, id).await?, stored_state(&a, id).await?, "importing definitions again changes nothing");
        assert_eq!(b.storage.dump_entity_events(id).await?.len(), a.storage.dump_entity_events(id).await?.len());
    }
    Ok(())
}
