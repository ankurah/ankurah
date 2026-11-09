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
//! construction. Scheduling, faults, and the trace key on that index. Each
//! node's key, and so its id and every identity it mints under, derives from
//! the simulation seed and that index.

use ankurah::core::connector::PeerProvenance;
use ankurah::core::node::NodeKey;
use ankurah::core::test_helpers::commit_transaction;
use ankurah::proto::{self, Attested};
use ankurah::storage::StorageEngine;
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

use super::model::{Field, SimRecord};
use super::transport::{Captured, SimSender};
use ankurah::Model;

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
    /// would, including initial system adoption from a durable peer.
    /// `provenance` is this node's side of the connection: `Configured` if it dialed.
    pub async fn connect_to(&self, peer: &SimNode, provenance: PeerProvenance) {
        let sender = SimSender::new(self.index, peer.id(), self.captured.clone());
        let challenge = self.node.issue_challenge();
        let presence = peer.node.presence(challenge.challenge()).await.expect("sim peers do not halt during setup");
        self.node
            .register_peer(presence, challenge, provenance, Box::new(sender))
            .await
            .expect("sim peers share this build's protocol version");
    }

    /// Ingest a forged batch of events directly through the production remote
    /// commit path, as if this node had received a `CommitTransaction` it
    /// accepted. Used to seed a change at an origin node deterministically.
    /// The events flow through the real applier (staging, BFS lineage, policy,
    /// commit_event, set_state).
    pub async fn origin_commit(&self, events: Vec<Attested<proto::Event>>) -> anyhow::Result<()> {
        let txid = proto::TransactionId::new();
        commit_transaction(&self.node, &ankurah::policy::DEFAULT_CONTEXT, txid, events).await?;
        Ok(())
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
        match self.node.storage.get_state(entity).await {
            Ok(state) => Some(state.payload.state),
            Err(_) => None,
        }
    }

    /// Every entity id this node currently holds materialized state for, in the
    /// `SimRecord` collection. A full table scan via a match-all selection.
    pub async fn known_entities(&self) -> Vec<proto::EntityId> {
        let model = proto::ModelId::EntityId(super::model::sim_model_id());
        let selection = ankql::ast::Predicate::MemberOf(model).into();
        match self.node.storage.fetch_states(&selection).await {
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
        self.node.storage.dump_entity_events(entity).await.unwrap_or_default()
    }
}

/// Build `n` nodes: node 0 is durable and holds the forged catalog; the rest
/// are ephemeral and connect to it. All nodes share one capture sink, and
/// each node's key derives from `seed` and its index.
/// Returns the nodes and the shared `Captured` for the scheduler.
pub async fn build_nodes(n: usize, seed: u64, captured: Captured) -> anyhow::Result<Vec<SimNode>> {
    assert!(n >= 1, "need at least one node");
    let mut nodes = Vec::with_capacity(n);

    // Plant the deterministic catalog before the durable node starts.
    let durable_engine = Arc::new(SledStorageEngine::new_test()?);
    crate::catalog_forge::plant(&*durable_engine, super::model::forged_catalog_rows()).await?;
    let durable = Node::new_durable_with_key(durable_engine, PermissiveAgent::new(), sim_node_key(seed, 0));
    durable.wait_ready().await?;
    nodes.push(SimNode { index: 0, durable: true, node: durable, captured: captured.clone() });

    // Nodes 1..n: ephemeral.
    for index in 1..n {
        let node = Node::new_with_key(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new(), sim_node_key(seed, index));
        nodes.push(SimNode { index, durable: false, node, captured: captured.clone() });
    }

    // Ephemeral descriptors are seeded after those nodes become ready.
    seed_sim_schema(&nodes[0])?;

    Ok(nodes)
}

/// The key of the node at `index` in the simulation `seed`: a hash of both, so
/// a run's node ids and minted entity ids repeat with its seed.
fn sim_node_key(seed: u64, index: usize) -> NodeKey {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"ankurah.sim.node-key.v0");
    hasher.update(seed.to_be_bytes());
    hasher.update((index as u64).to_be_bytes());
    NodeKey::from_seed(hasher.finalize().into())
}

/// Seed and bind the deterministic `SimRecord` catalog on one node. The
/// node's system must be READY (the seeded binding resolves under its
/// system epoch).
pub fn seed_sim_schema(node: &SimNode) -> anyhow::Result<()> {
    ankurah::core::test_helpers::seed_registered_schema(&node.node, SimRecord::descriptor(), &sim_model_definition())?;
    Ok(())
}

/// Binds the deterministic `SimRecord` schema into one node's descriptor.
fn sim_model_definition() -> proto::RegisteredModel {
    proto::RegisteredModel {
        id: super::model::sim_model_id(),
        label: SimRecord::descriptor().label.to_owned(),
        name: "SimRecord".to_string(),
        properties: [Field::Title, Field::Body]
            .into_iter()
            .map(|field| proto::RegisteredProperty {
                // The build_id echo is the correlator seeding binds each
                // resolution onto its compiled field with, so the forged
                // definition must carry the descriptor's real per-build ids.
                build_id: SimRecord::descriptor().field_by_name(field.name()).expect("SimRecord declares this field").build_id,
                id: super::model::sim_property_id(field),
                membership_id: super::model::sim_membership_id(field),
                name: field.name().to_string(),
                backend: "lww".to_string(),
                value_type: "string".to_string(),
                target_model: None,
                minted_for: Some(super::model::sim_model_id()),
                optional: false,
            })
            .collect(),
    }
}
