//! Real Nodes wired to the virtual transport.
//!
//! Each `SimNode` is a genuine `ankurah` durable or ephemeral Node backed by an
//! in-memory sled engine (the same engine the integration tests use). The only
//! thing swapped out is the transport: instead of a `LocalProcessConnection`,
//! every node holds a `SimSender` per peer that captures outbound messages for
//! the scheduler. Nothing about the Node's ingest, applier, staging, or
//! head-maintenance logic is mocked.
//!
//! Nodes are addressed by a stable logical index (0, 1, 2, ...) assigned at
//! construction. Scheduling, faults, and the trace key on that index and never
//! on the node's ed25519 public-key id, which would leak key entropy into the
//! schedule.

use ankurah::proto::{self, Attested};
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use ed25519_dalek::SigningKey;
use std::{collections::BTreeMap, sync::Arc};

use super::model::SimRecord;
use super::transport::{Captured, SimSender};
use ankurah::core::{
    property::{
        backend::{lww::LWWBackend, PropertyBackend},
        PropertyKey,
    },
    storage::StorageEngine,
    value::Value,
};
use ankurah::Model;

fn signing_key(index: usize) -> SigningKey {
    let mut seed = [0xA7; 32];
    seed[24..].copy_from_slice(&(index as u64).to_be_bytes());
    SigningKey::from_bytes(&seed)
}

/// Build the byte-identical system root used by both halves of the simulation
/// determinism audit. Production correctly draws fresh genesis entropy; this
/// harness supplies fixed entropy and pre-seeds storage so entity genesis ids
/// can still bind a real, stable system identity.
fn deterministic_system_root(founder: proto::NodeId) -> anyhow::Result<(proto::Event, Attested<proto::EntityState>)> {
    let source = LWWBackend::new();
    source.set(PropertyKey::name("item"), Some(Value::String(serde_json::to_string(&proto::sys::Item::SysRoot { founder })?)));
    let lww_ops = source.to_operations()?.expect("a system-root write produces operations");
    let operations = proto::OperationSet(BTreeMap::from([("lww".to_owned(), lww_ops.clone())]));
    let model = ankurah::core::schema::well_known_model_id(ankurah::core::system::SYSTEM_COLLECTION_ID)
        .expect("system collection has a well-known model id");
    let system = None;
    let nonce = [0x53; 32];
    let timestamp = 1_700_000_000_000;
    let entity_id = proto::EntityId::from(proto::EventId::from_genesis_parts(&system, &nonce, timestamp, &operations));
    let event = proto::Event {
        model,
        entity_id,
        parent: proto::Clock::default(),
        body: proto::EventBody::Genesis { system, nonce, timestamp, operations },
    };

    let materialized = LWWBackend::new();
    materialized.apply_operations_with_event(&lww_ops, event.id())?;
    let state = proto::State {
        state_buffers: proto::StateBuffers(BTreeMap::from([("lww".to_owned(), materialized.to_state_buffer()?)])),
        head: proto::Clock::from(vec![event.id()]),
    };
    Ok((event, Attested::opt(proto::EntityState { entity_id, model, state }, None)))
}

/// A real Node plus its logical index and the shared capture sink.
pub struct SimNode {
    pub index: usize,
    pub durable: bool,
    pub node: Node<SledStorageEngine, PermissiveAgent>,
    captured: Captured,
}

impl SimNode {
    pub fn id(&self) -> proto::NodeId { self.node.id }

    /// Register `peer` as a connected peer of this node, installing a
    /// `SimSender` so anything this node sends *to* `peer` is captured for the
    /// scheduler. Mirrors `Node::register_peer` as `LocalProcessConnection`
    /// would, including the durable-peer bookkeeping that drives system join.
    pub fn connect_pair(a: &SimNode, b: &SimNode) {
        let a_handshake = a.node.begin_peer_handshake();
        let b_handshake = b.node.begin_peer_handshake();
        let a_challenge = a_handshake.challenge();
        let b_challenge = b_handshake.challenge();

        a.node
            .register_peer(
                b.node.presence(a_challenge),
                a_handshake,
                b_challenge,
                Box::new(SimSender::new(a.index, b.id(), a.captured.clone())),
            )
            .expect("sim peer has a valid challenge-bound presence");
        b.node
            .register_peer(
                a.node.presence(b_challenge),
                b_handshake,
                a_challenge,
                Box::new(SimSender::new(b.index, a.id(), b.captured.clone())),
            )
            .expect("sim peer has a valid challenge-bound presence");
    }

    /// Ingest a forged batch of events directly through the production remote
    /// commit path, as if this node had received a `CommitTransaction` it
    /// accepted. Used to seed a change at an origin node deterministically.
    /// The events flow through the real applier (staging, BFS lineage, policy,
    /// commit_event, set_state).
    pub async fn origin_commit(&self, events: Vec<Attested<proto::Event>>) -> Result<(), ankurah::error::MutationError> {
        let txid = proto::TransactionId::new();
        self.node.commit_remote_transaction(&ankurah::policy::DEFAULT_CONTEXT, txid, events).await
    }

    /// Register a live query for `SimRecord` on this (ephemeral) node against a
    /// predicate. This drives the real subscription relay to register a context
    /// for the durable peer, which the `SubscriptionUpdate` applier
    /// (`apply_updates`) requires before it will process a batch from that peer.
    /// The returned `LiveQuery` must be held for the subscription's lifetime.
    /// The scheduler must be settled after this so the SubscribeQuery request
    /// and its response flow.
    pub fn subscribe(&self, predicate: &str) -> Result<ankurah::LiveQuery<super::model::SimRecordView>, anyhow::Error> {
        let ctx = self.node.context(ankurah::policy::DEFAULT_CONTEXT)?;
        Ok(ctx.query::<super::model::SimRecordView>(predicate)?)
    }

    /// Read the canonical materialized state of an entity as this node would
    /// serve it to a client: the resident (in-memory) entity if one is held,
    /// else the persisted state buffer. The resident view is authoritative
    /// because the EventOnly apply path advances the in-memory entity and
    /// commits events but does not rewrite the storage state buffer, so reading
    /// storage alone under-reports convergence. Returns `None` if the node has
    /// neither a resident entity nor a persisted state for `entity`. The
    /// returned `proto::State` (state buffers + head clock) is the byte-
    /// comparable unit the convergence invariant equates across nodes.
    pub async fn entity_state(&self, entity: proto::EntityId) -> Option<proto::State> {
        if let Some(resident) = self.node.get_resident_entity(entity) {
            if let Ok(state) = resident.to_state() {
                return Some(state);
            }
        }
        let collection = self.node.collections.get(&SimRecord::collection()).await.ok()?;
        match collection.get_state(entity).await {
            Ok(state) => Some(state.payload.state),
            Err(_) => None,
        }
    }

    /// Every entity id this node currently holds materialized state for, in the
    /// `SimRecord` collection. A full table scan via a match-all selection.
    pub async fn known_entities(&self) -> Vec<proto::EntityId> {
        let Ok(collection) = self.node.collections.get(&SimRecord::collection()).await else {
            return Vec::new();
        };
        let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        match collection.fetch_states(&selection).await {
            Ok(states) => states.into_iter().map(|s| s.payload.entity_id).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// The set of event ids durably stored for an entity on this node.
    pub async fn stored_event_ids(&self, entity: proto::EntityId) -> Vec<proto::EventId> {
        self.stored_events(entity).await.into_iter().map(|e| e.payload.id()).collect()
    }

    /// The full attested event lineage durably stored for an entity on this
    /// node. The harness propagates this whole coherent history so a receiver
    /// can always ground it regardless of delivery order (an isolated edit for
    /// an unseen entity is correctly rejected by the empty-head guard, so
    /// propagating only the newest event would strand out-of-order receivers).
    pub async fn stored_events(&self, entity: proto::EntityId) -> Vec<Attested<proto::Event>> {
        let Ok(collection) = self.node.collections.get(&SimRecord::collection()).await else {
            return Vec::new();
        };
        collection.dump_entity_events(entity).await.unwrap_or_default()
    }
}

/// Build `n` nodes: node 0 is durable and owns the system root; the rest are
/// ephemeral and join node 0's system. All nodes share one capture sink.
/// Returns the nodes and the shared `Captured` for the scheduler.
pub async fn build_nodes(n: usize, captured: Captured) -> anyhow::Result<Vec<SimNode>> {
    assert!(n >= 1, "need at least one node");
    let mut nodes = Vec::with_capacity(n);

    // Node 0: durable, opened over a deterministic pre-seeded system root.
    // Its signing key matches the founder recorded in that root.
    let durable_key = signing_key(0);
    let durable_engine = Arc::new(SledStorageEngine::new_test()?);
    let system_collection = durable_engine.collection(&ankurah::core::system::SYSTEM_COLLECTION_ID.into()).await?;
    let (root_genesis, root_state) = deterministic_system_root((&durable_key.verifying_key()).into())?;
    system_collection.add_event(&Attested::opt(root_genesis, None)).await?;
    system_collection.set_state(root_state).await?;
    let durable = Node::new_durable_with_signing_key(durable_engine, PermissiveAgent::new(), durable_key);
    durable.system.wait_system_ready().await;
    nodes.push(SimNode { index: 0, durable: true, node: durable, captured: captured.clone() });

    // Nodes 1..n: ephemeral.
    for index in 1..n {
        let node = Node::new_with_signing_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), signing_key(index));
        nodes.push(SimNode { index, durable: false, node, captured: captured.clone() });
    }

    // #330: forged wire envelopes carry a model-definition id, and ingress
    // `resolve_model` rejects any id its catalog cannot route. The harness
    // forges events/states directly (bypassing schema registration and the
    // catalog relay), so seed every node's catalog with the `SimRecord` model
    // at the fixed `sim_model_id()`. A direct upsert (not the durable
    // allocator) keeps the id byte-identical across nodes and across runs; the
    // map is only ever cleared by hard_reset, which the clean system-join here
    // never triggers, so the seed persists for the run.
    let sim_model = proto::RegisteredModel {
        id: super::model::sim_model_id(),
        collection: SimRecord::collection().as_str().to_string(),
        name: "SimRecord".to_string(),
    };
    for node in &nodes {
        node.node.catalog.upsert_registered(std::slice::from_ref(&sim_model), &[], &[]);
    }

    Ok(nodes)
}
