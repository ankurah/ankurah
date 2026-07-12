mod common;

use ankql::ast::Predicate;
use ankurah::core::{
    entity::Entity,
    error::ValidationError,
    node::{Node as NodeAlias, NodeInner},
    policy::{AccessDenied, Admission, DefaultContext, PolicyAgent, DEFAULT_CONTEXT},
    storage::StorageEngine,
    util::Iterable,
};
use ankurah::proto::{self, Attested, EventId};
use ankurah::{Model, Node, PermissiveAgent};
use ankurah_connector_local_process::LocalProcessConnection;
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::{Pet, PetView};

/// Permissive agent with two observation/override points for bridge policy
/// tests: counts validate_received_event calls, and denies check_read_event
/// for ids in the deny set.
#[derive(Clone)]
struct BridgePolicyAgent {
    validate_calls: Arc<AtomicUsize>,
    deny_read_events: Arc<Mutex<HashSet<EventId>>>,
}

impl BridgePolicyAgent {
    fn new() -> Self { Self { validate_calls: Arc::new(AtomicUsize::new(0)), deny_read_events: Arc::new(Mutex::new(HashSet::new())) } }
}

#[async_trait]
impl PolicyAgent for BridgePolicyAgent {
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
        _entity_before: &Entity,
        _entity_after: &Entity,
        _event: &proto::Event,
    ) -> Result<Admission, AccessDenied> {
        Ok(Admission::Allow)
    }

    fn validate_received_event<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::NodeId,
        _event: &proto::Attested<proto::Event>,
    ) -> Result<(), AccessDenied> {
        self.validate_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn attest_state<SE: StorageEngine>(&self, _node: &NodeAlias<SE, Self>, _state: &proto::EntityState) -> Admission { Admission::Allow }

    fn validate_received_state<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _from_node: &proto::NodeId,
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
        _resolver: Option<std::sync::Weak<dyn ankurah::core::property::PropertyResolver>>,
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
        event: &Attested<proto::Event>,
    ) -> Result<(), AccessDenied>
    where
        C: Iterable<Self::ContextData>,
    {
        if self.deny_read_events.lock().unwrap().contains(&event.payload.id()) {
            return Err(AccessDenied::ByPolicy("event read denied by test agent"));
        }
        Ok(())
    }

    fn check_write(&self, _data: &Self::ContextData, _entity: &Entity, _event: Option<&proto::Event>) -> Result<(), AccessDenied> { Ok(()) }

    fn validate_causal_assertion<SE: StorageEngine>(
        &self,
        _node: &NodeAlias<SE, Self>,
        _peer_id: &proto::NodeId,
        _assertion: &proto::CausalAssertion,
    ) -> Result<(), AccessDenied> {
        Ok(())
    }
}

/// Receive side: EventBridge events must pass validate_received_event like
/// every other transport path; transport must not decide trust.
#[tokio::test]
async fn test_event_bridge_events_are_policy_validated_on_receive() -> Result<()> {
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());
    server.system.create().await?;
    let client_agent = BridgePolicyAgent::new();
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), client_agent.clone());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(DEFAULT_CONTEXT)?;
    let ctx_c = client.context(DEFAULT_CONTEXT)?;

    let pet_id = {
        let trx = ctx_s.begin();
        let pet = trx.create(&Pet { name: "bridge-validate".to_string(), age: "1".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    // Client learns the entity at its initial head (state snapshot, no events).
    let query = format!("id = '{}'", pet_id);
    let initial = ctx_c.fetch::<PetView>(query.as_str()).await?;
    assert_eq!(initial.len(), 1);
    let validate_calls_before = client_agent.validate_calls.load(Ordering::SeqCst);

    // Server advances two events; the client's re-fetch is served by a bridge.
    for age in ["2", "3"] {
        let trx = ctx_s.begin();
        ctx_s.get::<PetView>(pet_id).await?.edit(&trx)?.age().replace(age)?;
        trx.commit().await?;
    }
    let results = ctx_c.fetch::<PetView>(query.as_str()).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].age().unwrap(), "3");

    let validated = client_agent.validate_calls.load(Ordering::SeqCst) - validate_calls_before;
    assert!(validated >= 2, "both bridge events must pass validate_received_event, saw {validated} calls");

    Ok(())
}

/// Send side: events the read policy hides must not leak through a bridge.
/// The producer gives up on the bridge entirely and falls back to a state
/// snapshot. The behind-client then cannot VERIFY the snapshot's lineage
/// (verification would require the hidden event, which GetEvents also
/// filters), so it conservatively refuses to adopt and stays stale. That is
/// the intended posture: staleness is the price of redaction, and the
/// security property pinned here is that the hidden event never reaches the
/// client through any path. (Redaction-tolerant catch-up is a phase 2
/// design question under the snapshot-authority RFC.)
#[tokio::test]
async fn test_event_bridge_respects_read_policy_on_send() -> Result<()> {
    let server_agent = BridgePolicyAgent::new();
    let server = Node::new_durable(Arc::new(SledStorageEngine::new_test().unwrap()), server_agent.clone());
    server.system.create().await?;
    let client = Node::new(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new());

    let _conn = LocalProcessConnection::new(&client, &server).await?;
    client.system.wait_system_ready().await;

    let ctx_s = server.context(DEFAULT_CONTEXT)?;
    let ctx_c = client.context(DEFAULT_CONTEXT)?;

    let pet_id = {
        let trx = ctx_s.begin();
        let pet = trx.create(&Pet { name: "bridge-redact".to_string(), age: "1".to_string() }).await?;
        let id = pet.id();
        trx.commit().await?;
        id
    };

    let query = format!("id = '{}'", pet_id);
    let initial = ctx_c.fetch::<PetView>(query.as_str()).await?;
    assert_eq!(initial.len(), 1);

    // Two more server events; the first one is read-denied for peers.
    let denied_id = {
        let trx = ctx_s.begin();
        ctx_s.get::<PetView>(pet_id).await?.edit(&trx)?.age().replace("2")?;
        trx.commit_and_return_events().await?[0].id()
    };
    server_agent.deny_read_events.lock().unwrap().insert(denied_id.clone());
    let open_id = {
        let trx = ctx_s.begin();
        ctx_s.get::<PetView>(pet_id).await?.edit(&trx)?.age().replace("3")?;
        trx.commit_and_return_events().await?[0].id()
    };

    // Re-fetch: the bridge would need the denied event, so it must be
    // suppressed entirely. The snapshot fallback arrives but cannot be
    // lineage-verified without the hidden event, so the client refuses it
    // (the fetch surfaces the per-item failure) rather than adopting
    // unverifiable state.
    let refetch = ctx_c.fetch::<PetView>(query.as_str()).await;
    assert!(refetch.is_err(), "client must not silently adopt state whose lineage it cannot verify, got {refetch:?}");

    // The security property: the hidden event never reached the client, and
    // neither did the rest of the redacted window (a partial chain would
    // lose operations). The client's view of the entity is stale but honest.
    let collection_c = ctx_c.collection(&Pet::collection()).await?;
    let ids: HashSet<_> = collection_c.dump_entity_events(pet_id).await?.iter().map(|e| e.payload.id()).collect();
    assert!(!ids.contains(&denied_id), "the read-denied event must not reach the client through any path");
    // The non-denied tip MAY reach the client: its verification attempt
    // fetches readable events through GetEvents, which applies policy per
    // event. What matters is that the hidden event stays hidden and the
    // unverifiable state is not adopted.
    let _ = open_id;
    assert_eq!(ctx_c.get::<PetView>(pet_id).await?.age().unwrap(), "1", "client remains at its last verified state");

    Ok(())
}
