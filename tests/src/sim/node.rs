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
//! on the node's ULID id, which is random and would leak entropy into the
//! schedule.

use ankurah::proto::{self, Attested};
use ankurah::storage::StorageEngine;
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use std::sync::Arc;

use super::model::{sim_collection, Field, SimRecord};
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
    pub fn id(&self) -> proto::EntityId { self.node.id }

    /// Register `peer` as a connected peer of this node, installing a
    /// `SimSender` so anything this node sends *to* `peer` is captured for the
    /// scheduler. Mirrors `Node::register_peer` as `LocalProcessConnection`
    /// would, including the durable-peer bookkeeping that drives system join.
    pub fn connect_to(&self, peer: &SimNode) {
        let sender = SimSender::new(self.index, peer.id(), self.captured.clone());
        self.node
            .register_peer(
                proto::Presence {
                    node_id: peer.id(),
                    durable: peer.durable,
                    system_root: peer.node.system.root(),
                    protocol_version: proto::PROTOCOL_VERSION,
                },
                Box::new(sender),
            )
            .expect("simulation peers use the current protocol version");
    }

    /// Ingest a forged batch of events directly through the production remote
    /// commit path, as if this node had received a `CommitTransaction` it
    /// accepted. Used to seed a change at an origin node deterministically.
    /// The events flow through the real applier (staging, BFS lineage, policy,
    /// commit_event, set_state).
    pub async fn origin_commit(&self, events: Vec<Attested<proto::Event>>) -> Result<(), ankurah::error::MutationError> {
        let txid = proto::TransactionId::new();
        let events = events.into_iter().map(|event| proto::ModelContext::new(sim_collection(), event)).collect();
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
        match self.node.storage.get_state(entity).await {
            Ok(state) => Some(state.payload.state),
            Err(_) => None,
        }
    }

    /// Every entity id this node currently holds materialized state for, in the
    /// `SimRecord` collection. A full table scan via a match-all selection.
    pub async fn known_entities(&self) -> Vec<proto::EntityId> {
        let selection = ankql::ast::Selection { predicate: ankql::ast::Predicate::True, order_by: None, limit: None };
        match self.node.storage.fetch_states(&sim_collection(), &selection).await {
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

/// Build `n` nodes: node 0 is durable and owns the system root; the rest are
/// ephemeral and join node 0's system. All nodes share one capture sink.
/// Returns the nodes and the shared `Captured` for the scheduler.
pub async fn build_nodes(n: usize, captured: Captured) -> anyhow::Result<Vec<SimNode>> {
    assert!(n >= 1, "need at least one node");
    let mut nodes = Vec::with_capacity(n);

    // Node 0: durable, creates the system.
    let durable = Node::new_durable(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
    durable.system.create().await?;
    nodes.push(SimNode { index: 0, durable: true, node: durable, captured: captured.clone() });

    // Nodes 1..n: ephemeral.
    for index in 1..n {
        let node = Node::new(Arc::new(SledStorageEngine::new_test()?), PermissiveAgent::new());
        nodes.push(SimNode { index, durable: false, node, captured: captured.clone() });
    }

    // The harness forges events and states directly, bypassing schema
    // registration and the catalog relay. Seed a complete deterministic
    // `SimRecord` catalog on every node so ingress can route the model and the
    // forged payloads use the same property ids that typed query registration
    // retains. Direct upserts keep every id byte-identical across nodes and
    // runs; hard_reset is not part of these scenarios.
    let sim_model = proto::RegisteredModel {
        id: super::model::sim_model_id(),
        collection: SimRecord::schema().collection.to_owned(),
        name: "SimRecord".to_string(),
    };
    let sim_properties: Vec<_> = [Field::Title, Field::Body]
        .into_iter()
        .map(|field| proto::RegisteredProperty {
            id: super::model::sim_property_id(field),
            model: sim_model.id,
            name: field.name().to_string(),
            backend: "lww".to_string(),
            value_type: "string".to_string(),
            target_model: None,
        })
        .collect();
    let sim_memberships: Vec<_> = [Field::Title, Field::Body]
        .into_iter()
        .map(|field| proto::RegisteredMembership {
            id: super::model::sim_membership_id(field),
            model: sim_model.id,
            property: super::model::sim_property_id(field),
            optional: false,
        })
        .collect();
    for node in &nodes {
        node.node.catalog.upsert_registered(std::slice::from_ref(&sim_model), &sim_properties, &sim_memberships);
    }

    Ok(nodes)
}
