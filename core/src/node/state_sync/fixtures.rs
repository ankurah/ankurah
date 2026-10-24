//! Nodes, entity states and snapshots that the comparison's and repair
//! building's tests share.

use super::{tree::fixtures::snapshot_of, Snapshot};
use crate::{
    entity::Entity,
    node::Node,
    policy::PermissiveAgent,
    property::backend::{LWWBackend, PropertyBackend},
    retrieval::{LocalEventGetter, LocalStateGetter},
    test_utils::TestStorage,
    value::Value,
};
use ankurah_core_types::{IndexKeyPart, KeySpec, ValueType};
use ankurah_proto::{self as proto, Attested, EntityState};
use std::sync::Arc;

pub(super) type TestNode = Node<TestStorage, PermissiveAgent>;

/// A loaded node over an empty test store.
pub(super) async fn node() -> anyhow::Result<TestNode> {
    let node = Node::new(Arc::new(TestStorage::default()), PermissiveAgent::new());
    node.system.wait_loaded().await?;
    Ok(node)
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
