//! Nodes that share no system root connect and exchange entities: whether a
//! peer is durable follows from how it was reached and what policy allows.

mod common;

use ankql::ast::{Predicate, Resolved};
use ankurah::core::{connector::PeerProvenance, node::NodeInner, util::Iterable};
use ankurah::entity::Entity;
use ankurah::error::ValidationError;
use ankurah::policy::{AccessDenied, Admission, DefaultContext, PeerTrust, PolicyAgent};
use async_trait::async_trait;
use common::*;
use std::sync::{Arc, Mutex};

/// The store materializes no built-in model but the catalog's: there is no system collection to hold a root.
async fn assert_no_system_collection(storage: &SledStorageEngine) -> anyhow::Result<()> {
    for model in storage.list_materializations().await? {
        assert!(matches!(model, proto::ModelId::EntityId(_)) || ankurah::core::schema::is_catalog_model(&model), "{model}");
    }
    Ok(())
}

#[tokio::test]
async fn a_durable_and_an_ephemeral_node_exchange_entities_with_no_system_root() -> anyhow::Result<()> {
    let server_storage = Arc::new(SledStorageEngine::new_test()?);
    let client_storage = Arc::new(SledStorageEngine::new_test()?);
    let server = Node::new_durable(server_storage.clone(), PermissiveAgent::new());
    let client = Node::new(client_storage.clone(), PermissiveAgent::new());
    let _connection = LocalProcessConnection::new(&server, &client).await?;
    let server_context = server.context_async(DEFAULT_CONTEXT).await?;
    let client_context = client.context_async(DEFAULT_CONTEXT).await?;

    let transaction = client_context.begin();
    let from_client = transaction.create(&Album { name: "minted by the client".into(), year: "2026".into() }).await?.id();
    transaction.commit().await?;
    let transaction = server_context.begin();
    let from_server = transaction.create(&Album { name: "minted by the server".into(), year: "2026".into() }).await?.id();
    transaction.commit().await?;

    assert_eq!(server_context.get::<AlbumView>(from_client).await?.name()?, "minted by the client");
    assert_eq!(client_context.get::<AlbumView>(from_server).await?.name()?, "minted by the server");
    assert_no_system_collection(&server_storage).await?;
    assert_no_system_collection(&client_storage).await?;
    Ok(())
}

/// Permissive in everything, and trusts the listed peers as durable however they connected.
#[derive(Clone)]
struct TrustListedPeers(Arc<Mutex<Vec<proto::NodeId>>>);

#[async_trait]
impl PolicyAgent for TrustListedPeers {
    type ContextData = &'static DefaultContext;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _: &NodeInner<SE, Self>,
        cdata: &C,
        _: &proto::NodeRequest,
    ) -> Result<Vec<proto::AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(cdata.iterable().map(|_| proto::AuthData(vec![])).collect())
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _: &Node<SE, Self>,
        auth: &A,
        _: &proto::NodeRequest,
    ) -> Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<proto::AuthData> + Send + Sync,
    {
        Ok(auth.iterable().map(|_| DEFAULT_CONTEXT).collect())
    }

    fn check_write_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &Self::ContextData,
        _: &Entity,
        _: &Entity,
        _: &proto::Event,
    ) -> Result<Admission, AccessDenied> {
        Ok(Admission::Allow)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _: &Node<SE, Self>, _: &proto::EntityState) -> Option<Vec<u8>> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::Attested<proto::EntityState>,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn query_predicate<C>(&self, _: &C) -> Result<Predicate<Resolved>, AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(Predicate::True)
    }

    fn check_write(&self, _: &Self::ContextData, _: &Entity, _: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _: &Node<SE, Self>,
        _: &proto::NodeId,
        _: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }

    fn peer_trust(&self, peer: &proto::NodeId, _: PeerProvenance, claimed_durable: bool) -> PeerTrust {
        PeerTrust { durable: claimed_durable && self.0.lock().unwrap().contains(peer) }
    }
}

/// Two durable stores created independently, each configured to trust the
/// other, relay their commits to each other although nothing in either store
/// names the other. Each registered its models on its own, so the check reads
/// stored state rather than typed views: two catalogs registered apart do not
/// converge on one model id.
#[tokio::test]
async fn two_independently_created_durable_stores_exchange_when_configured_to_trust_each_other() -> anyhow::Result<()> {
    let trusted = Arc::new(Mutex::new(Vec::new()));
    let (first_storage, second_storage) = (Arc::new(SledStorageEngine::new_test()?), Arc::new(SledStorageEngine::new_test()?));
    let first = Node::new_durable(first_storage.clone(), TrustListedPeers(trusted.clone()));
    let second = Node::new_durable(second_storage.clone(), TrustListedPeers(trusted.clone()));
    trusted.lock().unwrap().extend([first.id, second.id]);

    let _connection = LocalProcessConnection::new(&first, &second).await?;
    assert_eq!(first.get_durable_peers(), vec![second.id]);
    assert_eq!(second.get_durable_peers(), vec![first.id], "the accepting side trusts the dialer because policy lists it");

    let mut minted = Vec::new();
    for (node, name) in [(&first, "minted by the first store"), (&second, "minted by the second store")] {
        let transaction = node.context_async(DEFAULT_CONTEXT).await?.begin();
        minted.push(transaction.create(&Album { name: name.into(), year: "2026".into() }).await?.id());
        transaction.commit().await?;
    }
    for storage in [&first_storage, &second_storage] {
        for id in &minted {
            assert!(storage.get_state(*id).await.is_ok(), "each store holds both entities");
            assert!(!storage.dump_entity_events(*id).await?.is_empty());
        }
        assert_no_system_collection(storage).await?;
    }
    Ok(())
}
