mod common;

use ankql::ast::Predicate;
use ankurah::core::{
    entity::Entity,
    error::{IngestError, MutationError, ValidationError},
    node::{Node as NodeAlias, NodeInner},
    policy::{AccessDenied, DefaultContext, PolicyAgent, DEFAULT_CONTEXT},
    property::backend::{lww::LWWBackend, PropertyBackend},
    storage::StorageEngine,
    util::Iterable,
    value::Value,
};
use ankurah::proto::{self, Attested, EventId};
use ankurah::{Model, Node};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use common::Record;

/// Permissive agent that denies check_event for ids in the deny set and
/// records the (before, after) heads every check_event observed. The
/// recording is the C4-19 pin: a creation preview must show the policy an
/// EMPTY before-head, not an already-mutated entity on both sides.
#[derive(Clone)]
struct DenyingWriteAgent {
    deny_events: Arc<Mutex<HashSet<EventId>>>,
    observed: Arc<Mutex<Vec<(EventId, proto::Clock, proto::Clock)>>>,
}

impl DenyingWriteAgent {
    fn new() -> Self { Self { deny_events: Arc::new(Mutex::new(HashSet::new())), observed: Arc::new(Mutex::new(Vec::new())) } }
}

#[async_trait]
impl PolicyAgent for DenyingWriteAgent {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &NodeInner<SE, Self>,
        cdata: &C,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &NodeAlias<SE, Self>,
        auth: &A,
        _request: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        Ok(auth.iterable().map(|_| DEFAULT_CONTEXT).collect())
    }

    fn check_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _cdata: &Self::ContextData,
        entity_before: &Entity,
        entity_after: &Entity,
        event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        self.observed.lock().unwrap().push((event.id(), entity_before.head(), entity_after.head()));
        if self.deny_events.lock().unwrap().contains(&event.id()) {
            return Err(AccessDenied::ByPolicy("denied by test agent"));
        }
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::EntityId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &NodeAlias<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> {
        None
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::EntityId,
        _state: &Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn can_access_collection<C>(&self, _data: &C, _collection: &proto::CollectionId) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    fn filter_predicate<C>(&self, _data: &C, _collection: &proto::CollectionId, predicate: Predicate) -> Result<Predicate, AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(predicate)
    }

    fn check_read<C>(
        &self,
        _data: &C,
        _id: &proto::EntityId,
        _collection: &proto::CollectionId,
        _state: &proto::State,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(&self, _data: &C, _event: &Attested<proto::Event>) -> Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _peer_id: &proto::EntityId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

/// Forge a Record LWW event setting `title`, parented on the given clock.
fn forge_title_event(entity_id: proto::EntityId, parent: proto::Clock, title: &str) -> proto::Event {
    let backend = LWWBackend::new();
    backend.set("title".into(), Some(Value::String(title.to_owned())));
    let ops = backend.to_operations().unwrap().expect("LWW backend with a write produces operations");
    ankurah_tests::gen::stamped_event(
        entity_id,
        Record::collection(),
        proto::OperationSet(BTreeMap::from([("lww".to_owned(), ops)])),
        parent,
    )
}

/// R2 (D1 plan section 5): remote-commit atomicity. A policy denial mid
/// batch must leave NOTHING durable: no committed events, no state buffer,
/// no head advance. Red today: the per-event loop commits event 1, then
/// propagates the denial of event 2 with `?`, leaving a committed prefix
/// with an advanced head and no notification. The error must also carry the
/// typed PolicyDenied class rather than a bare AccessDenied.
#[tokio::test]
async fn test_remote_commit_denial_leaves_nothing_durable() -> Result<()> {
    let agent = DenyingWriteAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), agent.clone());
    server.system.create().await?;

    let entity_id = proto::EntityId::new();
    let e0 = forge_title_event(entity_id, proto::Clock::default(), "t0");
    let e1 = forge_title_event(entity_id, proto::Clock::from(vec![e0.id()]), "t1");
    let e2 = forge_title_event(entity_id, proto::Clock::from(vec![e1.id()]), "t2");
    agent.deny_events.lock().unwrap().insert(e1.id());

    let events = vec![Attested::opt(e0, None), Attested::opt(e1, None), Attested::opt(e2, None)];
    let err = server
        .commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), events)
        .await
        .expect_err("a mid-batch policy denial must fail the transaction");

    assert!(matches!(err, MutationError::Ingest(IngestError::PolicyDenied(_))), "expected typed PolicyDenied, got {err:?}");

    // Nothing durable: no events in the log, no state buffer.
    let collection = server.collections.get(&Record::collection()).await?;
    assert!(
        collection.dump_entity_events(entity_id).await?.is_empty(),
        "no event of a denied transaction may be durable (the committed prefix must not survive)"
    );
    assert!(collection.get_state(entity_id).await.is_err(), "no state buffer may exist for a denied transaction");

    // No resident holds an advanced head.
    if let Some(resident) = server.get_resident_entity(entity_id) {
        assert!(resident.head().is_empty(), "a denied transaction must not leave an advanced resident, got {}", resident.head());
    }

    Ok(())
}

/// R3 (D1 plan section 5): creation-rejection, the C4-19 arm. A rejected
/// creation must leave no mutated resident entity, and the policy phase
/// must see the creation as a PREVIEW: before-head empty, after-head at
/// the creation event. Red today: the lane applies a creation directly to
/// the real resident BEFORE policy runs and passes the same mutated entity
/// as both before and after, so the agent observes a non-empty before-head
/// and the denial strands a mutated resident until its last reference
/// drops.
#[tokio::test]
async fn test_rejected_creation_previews_and_leaves_no_resident() -> Result<()> {
    let agent = DenyingWriteAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), agent.clone());
    server.system.create().await?;

    let entity_id = proto::EntityId::new();
    let creation = forge_title_event(entity_id, proto::Clock::default(), "denied-at-birth");
    let creation_id = creation.id();
    agent.deny_events.lock().unwrap().insert(creation_id.clone());

    let err = server
        .commit_remote_transaction(&DEFAULT_CONTEXT, proto::TransactionId::new(), vec![Attested::opt(creation, None)])
        .await
        .expect_err("a denied creation must fail the transaction");
    assert!(matches!(err, MutationError::Ingest(IngestError::PolicyDenied(_))), "expected typed PolicyDenied, got {err:?}");

    // The policy phase saw a true preview: empty before, creation after.
    let observed = agent.observed.lock().unwrap().clone();
    assert_eq!(observed.len(), 1, "exactly one policy check for the single event");
    let (seen_id, before_head, after_head) = &observed[0];
    assert_eq!(seen_id, &creation_id);
    assert!(
        before_head.is_empty(),
        "policy must see the pre-creation entity (empty head) as before, got {before_head}; the real entity must not be mutated ahead of the decision"
    );
    assert_eq!(after_head, &proto::Clock::from(vec![creation_id.clone()]), "policy must see the previewed creation as after");

    // Nothing durable and no mutated resident survives the denial.
    let collection = server.collections.get(&Record::collection()).await?;
    assert!(collection.dump_entity_events(entity_id).await?.is_empty());
    assert!(collection.get_state(entity_id).await.is_err());
    if let Some(resident) = server.get_resident_entity(entity_id) {
        assert!(resident.head().is_empty(), "a denied creation must not leave a mutated resident, got {}", resident.head());
    }

    Ok(())
}
