//! Nodes, entities, peers and snapshots that the state sync tests share.

use super::{tree::fixtures::snapshot_of, Snapshot};
use crate::{
    connector::{PeerSender, SendError},
    entity::Entity,
    node::{applier::NodeApplier, Node, WeakNode},
    policy::PermissiveAgent,
    property::backend::{LWWBackend, PropertyBackend},
    retrieval::{LocalEventGetter, LocalStateGetter, SuspenseEvents},
    test_utils::TestStorage,
    value::Value,
};
use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core_types::{IndexKeyPart, KeySpec, ValueType};
use ankurah_proto::{self as proto, Attested, EntityState};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;

pub(super) type TestNode = Node<TestStorage, PermissiveAgent>;

/// A loaded node over an empty test store.
pub(super) async fn node() -> anyhow::Result<TestNode> {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    Ok(node)
}

/// A durable node over an empty test store, ready to answer comparisons.
pub(super) async fn durable() -> anyhow::Result<TestNode> {
    let node = Node::new_durable(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.create().await?;
    node.wait_ready().await?;
    Ok(node)
}

/// The model [`labeled`] genesis events join.
pub(super) fn model() -> proto::ModelId { proto::ModelId::EntityId(proto::EntityId::from_bytes([7; 32])) }

/// Every member of [`model`].
pub(super) fn members() -> Selection<Resolved> { Predicate::MemberOf(model()).into() }

/// An event that sets the label to `label`: a genesis joining [`model`], or an
/// update of `parent`.
pub(super) fn labeled(parent: Option<&proto::Event>, label: &str) -> anyhow::Result<proto::Event> {
    Ok(match parent {
        Some(parent) => proto::Event::update(
            parent.entity_id,
            proto::Clock::singleton(parent),
            proto::AuthorId::Unknown,
            proto::OperationSet(vec![set_label(label)?]),
        ),
        None => genesis_in(model(), label)?,
    })
}

/// A genesis joining `model` that sets the label to `label`.
pub(super) fn genesis_in(model: proto::ModelId, label: &str) -> anyhow::Result<proto::Event> {
    let join = proto::Operation::Membership(proto::Membership::Add(model));
    Ok(proto::Event::genesis(None, proto::AuthorId::Unknown, proto::OperationSet(vec![join, set_label(label)?])))
}

fn set_label(label: &str) -> anyhow::Result<proto::Operation> {
    let backend = LWWBackend::new();
    backend.set(proto::PropertyId::System(proto::SystemProperty::Label), Some(Value::String(label.into())));
    Ok(proto::Operation::Backend { backend: "lww".into(), operations: backend.to_operations()?.unwrap() })
}

/// Commit `event` on `node` as a peer's push.
pub(super) async fn apply(node: &TestNode, event: &proto::Event) -> anyhow::Result<()> {
    let events = LocalEventGetter::new(node.storage.clone(), node.durable);
    events.stage_event(event.clone());
    NodeApplier::apply_update(
        node,
        &proto::EntityId::from_bytes([8; 32]),
        proto::SubscriptionUpdateItem {
            entity_id: event.entity_id,
            content: proto::UpdateContent::EventOnly(vec![Attested::from(event.clone()).into()]),
            predicate_relevance: vec![],
        },
        &events,
        &LocalStateGetter::new(node.storage.clone()),
    )
    .await?;
    Ok(())
}

/// A member of [`model`] labeled `label`, committed on each of `nodes`.
pub(super) async fn create(nodes: &[&TestNode], label: &str) -> anyhow::Result<proto::Event> {
    let genesis = labeled(None, label)?;
    for node in nodes {
        apply(node, &genesis).await?;
    }
    Ok(genesis)
}

#[derive(Clone)]
struct Recorder(proto::EntityId, mpsc::UnboundedSender<proto::NodeMessage>);

impl PeerSender for Recorder {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        self.1.send(message).map_err(|_| SendError::ConnectionClosed)
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.0 }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// A peer of `node` that records every message `node` sends it.
pub(super) async fn recording_peer(node: &TestNode) -> anyhow::Result<(proto::EntityId, mpsc::UnboundedReceiver<proto::NodeMessage>)> {
    let peer = proto::EntityId::random();
    Ok((peer, recording_connection(node, peer).await?))
}

/// A new connection from `peer` to `node`, replacing any it had, that records
/// every message `node` sends on it.
pub(super) async fn recording_connection(
    node: &TestNode,
    peer: proto::EntityId,
) -> anyhow::Result<mpsc::UnboundedReceiver<proto::NodeMessage>> {
    let (sender, messages) = mpsc::unbounded_channel();
    let presence = proto::Presence { node_id: peer, durable: false, system_root: None, protocol_version: proto::PROTOCOL_VERSION };
    node.register_peer(presence, Box::new(Recorder(peer, sender))).await?;
    Ok(messages)
}

/// The items of the updates in `messages`, until `count` have arrived; then
/// none may follow within a short wait.
pub(super) async fn pushed(
    messages: &mut mpsc::UnboundedReceiver<proto::NodeMessage>,
    count: usize,
) -> Vec<Vec<proto::SubscriptionUpdateItem>> {
    let mut updates = Vec::new();
    let mut items = 0;
    while items < count {
        match tokio::time::timeout(Duration::from_secs(5), messages.recv()).await.expect("the pushes arrive") {
            Some(proto::NodeMessage::Update(update)) => {
                let proto::NodeUpdateBody::SubscriptionUpdate { items: pushed } = update.body;
                items += pushed.len();
                updates.push(pushed);
            }
            Some(_) => {}
            None => panic!("the node dropped its peer"),
        }
    }
    let later = tokio::time::timeout(Duration::from_millis(100), messages.recv()).await;
    assert!(!matches!(later, Ok(Some(proto::NodeMessage::Update(_)))), "no push beyond the expected items");
    updates
}

/// Hands each message to `to`'s `handle_message` in a task of its own, as an
/// in-process connector does, and records the requests it carries.
#[derive(Clone)]
struct Forward {
    to: WeakNode<TestStorage, PermissiveAgent>,
    to_id: proto::EntityId,
    requests: Arc<Mutex<Vec<String>>>,
}

impl PeerSender for Forward {
    fn send_message(&self, message: proto::NodeMessage) -> Result<(), SendError> {
        if let proto::NodeMessage::Request { request, .. } = &message {
            self.requests.lock().unwrap().push(request.body.to_string());
        }
        let node = self.to.upgrade().ok_or(SendError::ConnectionClosed)?;
        tokio::spawn(async move {
            let _ = node.handle_message(message).await;
        });
        Ok(())
    }
    fn recipient_node_id(&self) -> proto::EntityId { self.to_id }
    fn cloned(&self) -> Box<dyn PeerSender> { Box::new(self.clone()) }
}

/// Connect ephemeral `requester` to durable `responder` in this process, and
/// return the log of every request the requester sends.
pub(super) async fn connect(responder: &TestNode, requester: &TestNode) -> anyhow::Result<Arc<Mutex<Vec<String>>>> {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let to_responder = Forward { to: responder.weak(), to_id: responder.id, requests: requests.clone() };
    let to_requester = Forward { to: requester.weak(), to_id: requester.id, requests: Arc::default() };
    // The ephemeral node adopts the durable's system before the durable learns of it.
    requester.register_peer(responder.presence().await?, Box::new(to_responder)).await?;
    responder.register_peer(requester.presence().await?, Box::new(to_requester)).await?;
    requester.wait_ready().await?;
    Ok(requests)
}

/// The spec keyed by entity id alone that admits every entity.
pub(super) fn spec() -> proto::TreeSpec { proto::TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True) }

/// A spec keyed by one integer property, the entity's position.
pub(super) fn position_spec() -> proto::TreeSpec {
    proto::TreeSpec::new(KeySpec::new(vec![IndexKeyPart::asc(position(), ValueType::I64)]), ankql::ast::Predicate::True)
}

fn position() -> proto::PropertyId { proto::PropertyId::EntityId(proto::EntityId::from_bytes([42; 32])) }

/// A snapshot of `states` under [`spec`].
pub(super) async fn snapshot(states: Vec<Attested<EntityState>>) -> anyhow::Result<Snapshot> { snapshot_of(spec(), states).await }

/// `count` entities of random ids at one genesis head, each with a label of
/// `payload` bytes.
pub(super) fn states(count: usize, payload: usize) -> Vec<Attested<EntityState>> {
    let backend = LWWBackend::new();
    backend.set(proto::PropertyId::System(proto::SystemProperty::Label), Some(Value::String("x".repeat(payload))));
    crate::test_helpers::commit_backend(&backend, 1).unwrap();
    let buffer = backend.to_state_buffer().unwrap();
    (0..count)
        .map(|_| Attested {
            payload: EntityState {
                entity_id: proto::EntityId::random(),
                state: proto::State {
                    head: proto::Clock::genesis(proto::EventId::from_bytes([1; 32])),
                    state_buffers: proto::StateBuffers([("lww".into(), buffer.clone())].into()),
                    ..Default::default()
                },
            },
            attestations: Default::default(),
        })
        .collect()
}

/// Entity `id` at `position` under [`position_spec`], at a genesis head made of `head`.
pub(super) fn positioned_state(id: u32, position: i64, head: u8) -> anyhow::Result<Attested<EntityState>> {
    let mut bytes = [0; 32];
    bytes[..4].copy_from_slice(&id.to_be_bytes());
    let backend = LWWBackend::new();
    backend.set(self::position(), Some(Value::I64(position)));
    crate::test_helpers::commit_backend(&backend, head)?;
    Ok(Attested {
        payload: EntityState {
            entity_id: proto::EntityId::from_bytes(bytes),
            state: proto::State {
                head: proto::Clock::genesis(proto::EventId::from_bytes([head; 32])),
                state_buffers: proto::StateBuffers([("lww".into(), backend.to_state_buffer()?)].into()),
                ..Default::default()
            },
        },
        attestations: Default::default(),
    })
}

/// Make `id` resident on `node`, loading it from storage; the returned handle keeps it so.
pub(super) async fn resident(node: &TestNode, id: proto::EntityId) -> anyhow::Result<Entity> {
    Ok(node
        .entities
        .get_or_retrieve(&LocalStateGetter::new(node.storage.clone()), &LocalEventGetter::new(node.storage.clone(), node.durable), &id)
        .await?
        .unwrap())
}
