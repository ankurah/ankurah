//! Tests that check_request errors are properly propagated back to the client.
//!
//! This test ensures that when the server's PolicyAgent rejects a request via check_request,
//! the client receives an error response rather than hanging indefinitely.

mod common;

use ankql::ast::Predicate;
use ankurah::core::node::ContextData;
use ankurah::core::util::Iterable;
use ankurah::error::ValidationError;
use ankurah::policy::{AccessDenied, PolicyAgent, DEFAULT_CONTEXT};
use ankurah::proto::{self, AuthData};
use ankurah::storage::StorageEngine;
use ankurah::{Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use common::Album;

/// A PolicyAgent that rejects all incoming requests at the check_request stage.
/// Used to test that check_request errors are properly propagated to clients.
#[derive(Clone, Default)]
pub struct RejectingAgent;

/// Marker type for RejectingAgent's context data
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct RejectingContext;

impl ContextData for RejectingContext {}

#[async_trait]
impl PolicyAgent for RejectingAgent {
    type ContextData = RejectingContext;

    fn sign_request<SE: StorageEngine, C>(
        &self,
        _node: &ankurah::core::node::NodeInner<SE, Self>,
        _cdata: &C,
        _request: &proto::NodeRequest,
    ) -> std::result::Result<Vec<AuthData>, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(vec![AuthData(vec![])])
    }

    async fn check_request<SE: StorageEngine, A>(
        &self,
        _node: &Node<SE, Self>,
        _auth: &A,
        _request: &proto::NodeRequest,
    ) -> std::result::Result<Vec<Self::ContextData>, ValidationError>
    where
        A: Iterable<AuthData> + Send + Sync,
    {
        // Always reject - this simulates invalid credentials or authorization failure
        Err(ValidationError::ValidationFailed("Request rejected by RejectingAgent".to_string()))
    }

    fn check_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _cdata: &Self::ContextData,
        _entity_before: &ankurah::entity::Entity,
        _entity_after: &ankurah::entity::Entity,
        _event: &proto::Event,
    ) -> std::result::Result<Option<proto::Attestation>, AccessDenied> {
        Ok(None)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _received_from_node: &proto::EntityId,
        _event: &proto::Attested<proto::Event>,
    ) -> std::result::Result<(), AccessDenied> {
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &Node<SE, Self>, _state: &proto::EntityState) -> Option<proto::Attestation> { None }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _received_from_node: &proto::EntityId,
        _state: &proto::Attested<proto::EntityState>,
    ) -> std::result::Result<(), AccessDenied> {
        Ok(())
    }

    fn can_access_collection<C>(&self, _data: &C, _collection: &proto::CollectionId) -> std::result::Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    fn filter_predicate<C>(
        &self,
        _data: &C,
        _collection: &proto::CollectionId,
        predicate: Predicate,
    ) -> std::result::Result<Predicate, AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(predicate)
    }

    fn check_read<C>(
        &self,
        _data: &C,
        _id: &proto::EntityId,
        _collection: &proto::CollectionId,
        _state: &proto::State,
    ) -> std::result::Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        Ok(())
    }

    fn check_read_event<C>(&self, _data: &C, _event: &proto::Attested<proto::Event>) -> std::result::Result<(), AccessDenied>
    where C: Iterable<Self::ContextData> {
        Ok(())
    }

    fn check_write(
        &self,
        _context: &Self::ContextData,
        _entity: &ankurah::entity::Entity,
        _event: Option<&proto::Event>,
    ) -> std::result::Result<(), AccessDenied> {
        Ok(())
    }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &Node<SE, Self>,
        _peer_id: &proto::EntityId,
        _head_relation: &proto::CausalAssertion,
    ) -> std::result::Result<(), AccessDenied> {
        Ok(())
    }
}

/// Test that when the server's check_request fails, the client receives an error
/// rather than hanging indefinitely waiting for a response.
#[tokio::test]
async fn check_request_error_returns_to_client() -> Result<()> {
    // Server uses RejectingAgent - will reject all incoming requests
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), RejectingAgent);
    server.system.create().await?;

    // Client uses PermissiveAgent - allows local operations
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    // Connect client to server
    let _conn = LocalProcessConnection::new(&server, &client).await?;
    client.system.wait_system_ready().await;

    let client_ctx = client.context(DEFAULT_CONTEXT)?;

    // Try to create an entity on the client - this should fail when relaying to server
    // because the server's check_request will reject it
    let trx = client_ctx.begin();
    trx.create(&Album { name: "Test Album".into(), year: "2024".into() }).await?;

    // The commit should return an error (not hang!) because the server rejected the request
    let result = trx.commit().await;

    assert!(result.is_err(), "Commit should fail when server rejects the request");

    // Verify the error message contains our rejection reason
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("rejected") || err_msg.contains("Request rejected"), "Error should indicate rejection, got: {}", err_msg);

    Ok(())
}
