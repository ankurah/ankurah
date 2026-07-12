mod common;

use ankql::ast::Predicate;
use ankurah::core::{
    entity::Entity,
    error::{IngestError, MutationError, ValidationError},
    node::{Node as NodeInnerAlias, NodeInner, WeakNode},
    policy::{AccessDenied, DefaultContext, PolicyAgent, DEFAULT_CONTEXT},
    storage::StorageEngine,
    util::Iterable,
};
use ankurah::proto::{self, Attested};
use ankurah::{Model, Mutable, Node};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::{Album, AlbumView};

/// Permissive except for `check_event` on the album collection, where only
/// the first event is allowed. Used to pin commit failure atomicity: a
/// denial partway through a multi-entity transaction must leave NOTHING
/// durable.
#[derive(Clone)]
struct DenySecondAlbumEventAgent {
    album_checks: Arc<AtomicUsize>,
}

impl DenySecondAlbumEventAgent {
    fn new() -> Self { Self { album_checks: Arc::new(AtomicUsize::new(0)) } }
}

#[async_trait]
impl PolicyAgent for DenySecondAlbumEventAgent {
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
        _node: &NodeInnerAlias<SE, Self>,
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
        _node: &NodeInnerAlias<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &Entity,
        entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        // #330: proto::Event carries a model id, not a collection name; read the
        // collection from the entity instead (identical for this event).
        if entity_after.collection().as_str() == "album" && self.album_checks.fetch_add(1, Ordering::SeqCst) >= 1 {
            return Err(AccessDenied::ByPolicy("test agent denies the second album event"));
        }
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &NodeInnerAlias<SE, Self>,
        _from_node: &proto::EntityId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &NodeInnerAlias<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> {
        None
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &NodeInnerAlias<SE, Self>,
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
        _resolver: Option<std::sync::Weak<dyn ankurah::core::schema::CatalogResolver>>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(
        &self,
        _data: &C,
        _collection: &proto::CollectionId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &NodeInnerAlias<SE, Self>,
        _peer_id: &proto::EntityId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

/// V7: commit_local_trx must run every policy check before persisting any
/// event. With the fused loop, the first entity's event was already durable
/// when the second entity's check was denied, leaving an orphaned event in
/// storage on a failed transaction.
#[tokio::test]
async fn test_multi_entity_commit_denial_leaves_nothing_durable() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), DenySecondAlbumEventAgent::new());
    node.system.create().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    let trx = ctx.begin();
    let album1 = trx.create(&Album { name: "First".to_owned(), year: "2001".to_owned() }).await?;
    let album2 = trx.create(&Album { name: "Second".to_owned(), year: "2002".to_owned() }).await?;
    let id1 = album1.id();
    let id2 = album2.id();

    let result = trx.commit().await;
    assert!(result.is_err(), "commit must fail when any event is denied, got {result:?}");

    // Failure atomicity: no event for EITHER entity may be durable.
    let collection = ctx.collection(&Album::collection()).await?;
    for (label, id) in [("first", id1), ("second", id2)] {
        let events = collection.dump_entity_events(id).await?;
        assert!(events.is_empty(), "{label} entity must have zero durable events after denied commit, found {}", events.len());
    }

    // And neither entity is fetchable.
    let results = ctx.fetch::<AlbumView>("year = '2001' OR year = '2002'").await?;
    assert!(results.is_empty(), "no entity from the failed transaction may be visible");

    Ok(())
}

/// Permissive agent that records the (before, after) heads of every album
/// check_event. Pins what the policy phase actually shows the agent.
#[derive(Clone)]
struct RecordingAgent {
    observed: Arc<Mutex<Vec<(proto::EventId, proto::Clock, proto::Clock)>>>,
}

impl RecordingAgent {
    fn new() -> Self { Self { observed: Arc::new(Mutex::new(Vec::new())) } }
}

#[async_trait]
impl PolicyAgent for RecordingAgent {
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
        _node: &NodeInnerAlias<SE, Self>,
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
        _node: &NodeInnerAlias<SE, Self>,
        _cdata: &Self::ContextData,
        entity_before: &Entity,
        entity_after: &Entity,
        event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        if entity_after.collection().as_str() == "album" {
            self.observed.lock().unwrap().push((event.id(), entity_before.head(), entity_after.head()));
        }
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &NodeInnerAlias<SE, Self>,
        _from_node: &proto::EntityId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &NodeInnerAlias<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> {
        None
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &NodeInnerAlias<SE, Self>,
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
        _resolver: Option<std::sync::Weak<dyn ankurah::core::schema::CatalogResolver>>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(
        &self,
        _data: &C,
        _collection: &proto::CollectionId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &NodeInnerAlias<SE, Self>,
        _peer_id: &proto::EntityId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

/// M7: the local commit lane previews each event on a fork of the canonical
/// entity. When a concurrent transaction advanced the entity between this
/// transaction's fork and its commit, the policy must see an after-state
/// that includes BOTH the incoming event and the concurrent history it will
/// actually merge with (the preview the remote lane got in M6). Before M7
/// the after-state was a fork of the transaction's own stale snapshot: it
/// omitted the concurrent event, showing the policy a state the entity
/// would never hold.
#[tokio::test]
async fn test_local_commit_preview_includes_concurrent_history() -> Result<()> {
    let agent = RecordingAgent::new();
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), agent.clone());
    node.system.create().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    // Genesis commit, then two transactions fork the same head.
    let trx0 = ctx.begin();
    let album = trx0.create(&Album { name: "Origin".to_owned(), year: "2000".to_owned() }).await?;
    let album_id = album.id();
    trx0.commit().await?;

    let view = ctx.get::<AlbumView>(album_id).await?;
    let trx_a = ctx.begin();
    let trx_b = ctx.begin();
    let mut_a = view.edit(&trx_a)?;
    let mut_b = view.edit(&trx_b)?;
    mut_a.name().replace("A")?;
    mut_b.year().replace("2001")?;

    // B lands first: the canonical entity advances past A's fork point.
    let event_b = trx_b.commit_and_return_events().await?.remove(0).id();
    let event_a = trx_a.commit_and_return_events().await?.remove(0).id();

    let observed = agent.observed.lock().unwrap();
    let (_, before, after) =
        observed.iter().find(|(id, _, _)| *id == event_a).expect("check_event must run for the concurrent commit").clone();

    assert!(before.contains(&event_b), "policy before-state must be the canonical entity at policy time, got {before:?}");
    assert!(after.contains(&event_a), "preview must include the event under review, got {after:?}");
    assert!(after.contains(&event_b), "preview must include the concurrent history the entity will merge with, got {after:?}");

    Ok(())
}

/// An agent whose check_event ATTESTS album events. Pins that the commit
/// lanes carry the returned attestation onto the committed (and relayed)
/// event; the shared phase one attaches it in staging.
#[derive(Clone)]
struct AttestingAgent;

#[async_trait]
impl PolicyAgent for AttestingAgent {
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
        _node: &NodeInnerAlias<SE, Self>,
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
        _node: &NodeInnerAlias<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &Entity,
        entity_after: &Entity,
        event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        if entity_after.collection().as_str() == "album" {
            return Ok(Some(proto::Attestation(vec![0xA7])));
        }
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &NodeInnerAlias<SE, Self>,
        _from_node: &proto::EntityId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &NodeInnerAlias<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> {
        None
    }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &NodeInnerAlias<SE, Self>,
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
        _resolver: Option<std::sync::Weak<dyn ankurah::core::schema::CatalogResolver>>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(
        &self,
        _data: &C,
        _collection: &proto::CollectionId,
        _event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &NodeInnerAlias<SE, Self>,
        _peer_id: &proto::EntityId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

/// The attestation check_event returns must survive onto the committed
/// event: it is the policy's provenance mark, and peers receive it on the
/// relayed transaction. Pins the shared phase one's staging attachment
/// (both commit lanes run it).
#[tokio::test]
async fn test_check_event_attestation_survives_to_the_log() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), AttestingAgent);
    node.system.create().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    let trx = ctx.begin();
    let album = trx.create(&Album { name: "Attested".to_owned(), year: "2026".to_owned() }).await?;
    let album_id = album.id();
    trx.commit().await?;

    let collection = ctx.collection(&Album::collection()).await?;
    let events = collection.dump_entity_events(album_id).await?;
    assert_eq!(events.len(), 1, "one creation event");
    assert!(
        events[0].attestations.iter().any(|a| a.0 == vec![0xA7]),
        "the check_event attestation must be carried on the committed event, got {:?}",
        events[0].attestations
    );

    Ok(())
}

/// M7: a policy denial on the local commit lane surfaces through the typed
/// ingest taxonomy, the same surface the remote lane adopted in M6, instead
/// of the bare AccessDenied passthrough.
#[tokio::test]
async fn test_local_commit_denial_is_typed() -> Result<()> {
    let node = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), DenySecondAlbumEventAgent::new());
    node.system.create().await?;
    let ctx = node.context(DEFAULT_CONTEXT)?;

    let trx = ctx.begin();
    trx.create(&Album { name: "First".to_owned(), year: "2001".to_owned() }).await?;
    trx.create(&Album { name: "Second".to_owned(), year: "2002".to_owned() }).await?;

    let err = trx.commit().await.expect_err("commit must fail when any event is denied");
    assert!(matches!(err, MutationError::Ingest(IngestError::PolicyDenied(_))), "expected typed PolicyDenied, got {err:?}");

    Ok(())
}
