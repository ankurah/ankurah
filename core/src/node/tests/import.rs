//! A durable node importing events made outside it: what an import refuses, what importing a held
//! event again leaves alone that a peer's commit of it writes, and how an import and a registration
//! order around the catalog.

use crate::{
    entity::Entity,
    error::{MutationError, RetrievalError, ValidationError},
    model::Model,
    node::{event_admissibility::InadmissibleEvent, handler::commit_transaction, Node, NodeInner},
    policy::{AccessDenied, DefaultContext, PermissiveAgent, PolicyAgent, DEFAULT_CONTEXT},
    property::backend::{LWWBackend, PropertyBackend},
    schema::{ModelStructDescriptor, PerSystemOnceCell, StructProperty},
    storage::StorageEngine,
    test_utils::TestStorage,
    util::Iterable,
    value::Value,
};
use ankql::ast::{Predicate, Resolved};
use ankurah_proto::{
    self as proto, Attestation, AttestationSet, Attested, AuthorId, Clock, EntityId, EntityState, Event, Membership, ModelId, Operation,
    OperationSet, PropertyId, TransactionId,
};
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(ankurah_derive::Model, Debug)]
#[model(base = "crate", no_ffi)]
pub struct ImportedRecord {
    pub title: String,
}

type TestNode = Node<TestStorage, PermissiveAgent>;

async fn durable_node() -> anyhow::Result<(Arc<TestStorage>, TestNode)> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    Ok((storage, node))
}

/// What [`StateAttester`] attests every state with.
const ATTESTED: &[u8] = b"attested by the node's agent";

/// An agent that permits every write and attests every state a write stores, so a test sees which
/// writes store a state with its agent's attestation.
#[derive(Clone)]
struct StateAttester;

#[async_trait::async_trait]
impl PolicyAgent for StateAttester {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C: Iterable<Self::ContextData>>(
        &self,
        _: &NodeInner<SE, Self>,
        _: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied> {
        Ok(vec![])
    }

    async fn check_request<SE: StorageEngine, A: Iterable<proto::AuthData> + Send + Sync>(
        &self,
        _: &Node<SE, Self>,
        _: &A,
        _: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError> {
        Ok(vec![])
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &Self::ContextData,
        _: &Entity,
        _: &Entity,
        _: &Event,
    ) -> Result<Option<Attestation>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &Attested<Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &EntityState) -> Option<Attestation> {
        Some(Attestation(ATTESTED.to_vec()))
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &Attested<EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C: Iterable<Self::ContextData>>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied> { Ok(Predicate::True) }

    fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &EntityId,
        _: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

fn member(byte: u8) -> Operation { Operation::Membership(Membership::Add(ModelId::EntityId(EntityId::from_bytes([byte; 32])))) }

fn set(property: PropertyId, value: &str) -> anyhow::Result<Operation> {
    let backend = LWWBackend::new();
    backend.set(property, Some(Value::String(value.into())));
    Ok(Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() })
}

/// `node` registers `ImportedRecord`; returns its model id and every event of its definitions: the
/// model, its one property, and the link between them.
async fn registered_definitions(storage: &TestStorage, node: &TestNode) -> anyhow::Result<(EntityId, Vec<Attested<Event>>)> {
    node.context_async(DEFAULT_CONTEXT).await?.resolve_model_id::<ImportedRecord>().await?;
    let (model, _) = node.catalog.model_by_label(ImportedRecord::descriptor().label)?.unwrap();
    let (property, _) = node.catalog.property_by_name(&model, "title")?.unwrap();
    let (link, _) = node.catalog.membership(&model, &property)?.unwrap();
    let mut events = Vec::new();
    for id in [model, property, link] {
        events.extend(storage.dump_entity_events(id).await?);
    }
    Ok((model, events))
}

fn refused_as_system_entity(result: Result<(), MutationError>, id: EntityId) -> bool {
    matches!(result, Err(MutationError::InadmissibleEvent(InadmissibleEvent::SystemEntity(refused))) if refused == id)
}

/// Another node's root would leave this store with two roots, which will not open, and a change to this
/// node's own root would leave the root its system loaded behind, so an import refuses both and stores
/// neither.
#[tokio::test]
async fn an_import_neither_brings_a_root_nor_changes_the_nodes_own() -> anyhow::Result<()> {
    let (storage, node) = durable_node().await?;
    let (other_storage, other) = durable_node().await?;

    let foreign_root = other.system.root_id().unwrap();
    let foreign = other_storage.dump_entity_events(foreign_root).await?;
    assert!(refused_as_system_entity(node.import_events(&foreign).await, foreign_root));
    assert!(matches!(storage.get_state(foreign_root).await, Err(RetrievalError::EntityNotFound(_))));

    let root = node.system.root_id().unwrap();
    let before = storage.get_state(root).await?;
    let change: Attested<Event> =
        Event::update(root, before.payload.state.head.clone(), AuthorId::Unknown, OperationSet(vec![member(9)])).into();
    assert!(refused_as_system_entity(node.import_events(&[change]).await, root));
    assert_eq!(storage.get_state(root).await?, before, "the node's root is unchanged");
    Ok(())
}

/// A node that already holds an imported event changes nothing it stores: the stored event keeps the
/// attestation it first arrived with, the later state stays, and so does the attestation a later write
/// stored with it, which a privileged import could not reproduce.
#[tokio::test]
async fn importing_a_held_genesis_again_leaves_the_stored_record_alone() -> anyhow::Result<()> {
    let (storage, node) = durable_node().await?;
    let property = PropertyId::EntityId(EntityId::from_bytes([2; 32]));
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet(vec![member(1), set(property, "first")?]));
    let update =
        Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, OperationSet(vec![set(property, "later")?]));
    let attested = Attested::opt(genesis.clone(), Some(Attestation(b"carried by the first import".to_vec())));
    node.import_events(&[attested.clone(), update.into()]).await?;

    let mut stored = storage.get_state(genesis.entity_id).await?;
    stored.attestations = AttestationSet(vec![Attestation(b"attested by a later write".to_vec())]);
    storage.set_state(stored.clone());

    node.import_events(&[genesis.clone().into()]).await?;
    assert_eq!(storage.get_events(vec![genesis.id()]).await?, vec![attested], "the stored event keeps its attestation");
    assert_eq!(storage.get_state(genesis.entity_id).await?, stored, "the later state and its attestation are kept");
    Ok(())
}

/// An import stores a state with no attestation, since it commits on the node's own authority. A peer
/// that then commits the same event has the state written again and attested by the node's agent, as
/// every peer commit has: only an import leaves the state of an entity it already holds alone.
#[tokio::test]
async fn a_peer_commit_of_an_imported_event_stores_the_state_its_agent_attests() -> anyhow::Result<()> {
    let storage = Arc::new(TestStorage::default());
    let node = Node::new_durable(storage.clone(), StateAttester);
    node.system.create().await?;
    node.wait_ready().await?;
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet(vec![member(1)]));
    node.import_events(&[genesis.clone().into()]).await?;
    assert_eq!(storage.get_state(genesis.entity_id).await?.attestations, AttestationSet(vec![]), "an import attests nothing");

    commit_transaction(&node, &DEFAULT_CONTEXT, TransactionId::new(), vec![genesis.clone().into()]).await?;
    assert_eq!(storage.get_state(genesis.entity_id).await?.attestations, AttestationSet(vec![Attestation(ATTESTED.to_vec())]));
    Ok(())
}

/// The events of one import commit together or not at all.
#[tokio::test]
async fn an_import_that_fails_stores_none_of_its_events() -> anyhow::Result<()> {
    let (storage, node) = durable_node().await?;
    let valid = Event::genesis(None, AuthorId::Unknown, OperationSet(vec![member(1)]));
    let invalid = Event::genesis(None, AuthorId::Unknown, OperationSet::default());

    let result = node.import_events(&[valid.clone().into(), invalid.into()]).await;
    assert!(matches!(result, Err(MutationError::InadmissibleEvent(InadmissibleEvent::MissingGenesisMembership))));
    assert!(matches!(storage.get_state(valid.entity_id).await, Err(RetrievalError::EntityNotFound(_))));
    assert!(storage.dump_entity_events(valid.entity_id).await?.is_empty());
    assert!(node.entities.get(&valid.entity_id).is_none());
    Ok(())
}

/// A registration that starts while an import is committing waits for it, then binds to the imported
/// definitions instead of registering its own. Without the wait it would find no definition for its
/// label and register a second one.
#[tokio::test]
async fn a_registration_waits_for_an_import_in_flight() -> anyhow::Result<()> {
    let (a_storage, a) = durable_node().await?;
    let (model, definitions) = registered_definitions(&a_storage, &a).await?;
    let (b_storage, b) = durable_node().await?;

    let (entered_tx, entered) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    *b_storage.hold_commit.lock().unwrap() = Some((entered_tx, release_rx));
    let import = tokio::spawn({
        let b = b.clone();
        async move { b.import_events(&definitions).await }
    });
    entered.await?;
    assert!(b.catalog.allocator.try_lock().is_err(), "the import holds the catalog's allocator while it commits");

    let context = b.context_async(DEFAULT_CONTEXT).await?;
    let registration = context.resolve_model_id::<ImportedRecord>();
    tokio::pin!(registration);
    assert!(futures::poll!(&mut registration).is_pending(), "the registration waits while the import commits");

    release.send(()).unwrap();
    import.await??;
    assert_eq!(registration.await?, ModelId::EntityId(model), "the registration binds to the imported definition");
    let labelled = b.catalog.model_labels().into_iter().filter(|(_, label)| label == ImportedRecord::descriptor().label).count();
    assert_eq!(labelled, 1, "b registers no definition of its own");
    Ok(())
}

/// A model pinned by id binds, with every property, to that row through local resolution, even when
/// another row holds the same label and the catalog's label index keeps the other one, as when a store
/// imports a minted definition of a label it already registered.
#[tokio::test]
async fn a_pinned_model_binds_by_its_id_beside_another_row_under_its_label() -> anyhow::Result<()> {
    let (a_storage, a) = durable_node().await?;
    let (imported, definitions) = registered_definitions(&a_storage, &a).await?;
    let (b_storage, b) = durable_node().await?;
    let (own, _) = registered_definitions(&b_storage, &b).await?;
    b.import_events(&definitions).await?;

    let label = ImportedRecord::descriptor().label;
    let (indexed, _) = b.catalog.model_by_label(label)?.unwrap();
    let pinned = if indexed == imported { own } else { imported };
    assert_ne!(pinned, indexed, "the label index keeps the other row");

    let schema = ImportedRecord::descriptor();
    // Fresh resolution cells: the compiled struct's own cells already hold b's first binding.
    let properties: &'static [StructProperty] = Box::leak(
        schema.properties.iter().map(|property| StructProperty { resolved: PerSystemOnceCell::per_epoch(), ..*property }).collect(),
    );
    let descriptor: &'static ModelStructDescriptor = Box::leak(Box::new(ModelStructDescriptor {
        label: schema.label,
        name: schema.name,
        properties,
        system: None,
        explicit_id: Some(Box::leak(pinned.to_base64().into_boxed_str())),
        build_id: [0; 16],
        resolved: PerSystemOnceCell::per_epoch(),
    }));
    let epoch = b.system.system_epoch().unwrap();
    b.catalog.resolve_local(&mut descriptor.registrant(epoch))?;
    assert_eq!(descriptor.resolved.get(epoch)?, ModelId::EntityId(pinned));
    for property in properties {
        let (expected, _) = b.catalog.property_by_name(&pinned, property.name)?.unwrap();
        assert_eq!(property.resolved.get(epoch)?, PropertyId::EntityId(expected), "{} binds under the pinned model", property.name);
    }
    Ok(())
}

/// An ephemeral node keeps no store of its own to import into, so it refuses with a typed error.
#[tokio::test]
async fn an_ephemeral_node_refuses_to_import() -> anyhow::Result<()> {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet(vec![member(1)]));
    assert!(matches!(node.import_events(&[genesis.into()]).await, Err(MutationError::NotDurable)));
    Ok(())
}
