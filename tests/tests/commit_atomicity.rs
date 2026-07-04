mod common;

use ankql::ast::Predicate;
use ankurah::core::{
    entity::Entity,
    error::ValidationError,
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
use std::sync::Arc;

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
        _entity_after: &Entity,
        event: &proto::Event,
    ) -> Result<Option<proto::Attestation>, AccessDenied> {
        if event.collection.as_str() == "album" && self.album_checks.fetch_add(1, Ordering::SeqCst) >= 1 {
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
