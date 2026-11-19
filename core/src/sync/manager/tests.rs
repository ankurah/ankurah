//! Query-driven residency and reconciliation from canonical storage.

use std::collections::BTreeMap;
use std::sync::Arc;

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Resolved, Selection};
use ankurah_core_types::{EntityId, PropertyId, Value, ValueType};
use ankurah_digest_tree::{DigestGroup, DigestTreeView, Encode, LtHash16, MemberId, Payload, Ristretto255};
use ankurah_proto::{Attested, Clock, EntityState, EventId, ModelId, State, StateBuffers, SystemModel, SystemProperty};
use futures::TryStreamExt;

use super::*;
use crate::property::backend::{lww::LWWBackend, PropertyBackend};
use crate::storage::StorageTransaction;
use crate::test_utils::TestStorage;

mod continuation;
mod lifetime;
mod limited;
mod maintenance;
mod opening;

fn model() -> ModelId { ModelId::EntityId(EntityId::from_bytes([80; 32])) }
fn score() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([90; 32])) }
fn active() -> PropertyId { PropertyId::EntityId(EntityId::from_bytes([91; 32])) }

#[derive(Clone)]
struct Version {
    id: EntityId,
    score: i64,
    active: Option<bool>,
    head: Clock,
}

impl Version {
    fn state(&self) -> anyhow::Result<Attested<EntityState>> {
        let mut values = vec![(score(), Value::I64(self.score))];
        values.extend(self.active.map(|value| (active(), Value::Bool(value))));
        let mut state = state(self.id, model(), values)?;
        state.payload.state.head = self.head.clone();
        Ok(state)
    }
}

fn version(id: u8, score: i64, active: bool) -> Version {
    Version { id: EntityId::from_bytes([id; 32]), score, active: Some(active), head: Clock::genesis(EventId::from_bytes([id; 32])) }
}

/// All ordinary selections are model-scoped: catalog Property rows are never members.
fn selection(active_only: bool, limit: Option<u64>, descending: bool) -> Selection<Resolved> {
    let membership = Predicate::MemberOf(model());
    Selection {
        predicate: if active_only {
            Predicate::And(
                Box::new(membership),
                Box::new(Predicate::Comparison {
                    left: Box::new(Expr::Path(active().into())),
                    operator: ComparisonOperator::Equal,
                    right: Box::new(Expr::Literal(Value::Bool(true))),
                }),
            )
        } else {
            membership
        },
        order_by: Some(vec![OrderByItem {
            path: score().into(),
            direction: if descending { OrderDirection::Desc } else { OrderDirection::Asc },
        }]),
        limit,
    }
}

fn state(
    id: EntityId,
    membership: ModelId,
    values: impl IntoIterator<Item = (PropertyId, Value)>,
) -> anyhow::Result<Attested<EntityState>> {
    let backend = LWWBackend::new();
    for (property, value) in values {
        backend.set(property, Some(value));
    }
    let event = crate::test_helpers::commit_backend(&backend, id.to_bytes()[0])?;
    Ok(Attested::opt(
        EntityState {
            entity_id: id,
            state: State {
                state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                memberships: [membership].into_iter().collect(),
                head: Clock::singleton(&event),
            },
        },
        None,
    ))
}

fn property_state(property: PropertyId, name: &str, value_type: &str) -> anyhow::Result<Attested<EntityState>> {
    let PropertyId::EntityId(id) = property else { unreachable!("fixture catalog properties have entity ids") };
    state(
        id,
        ModelId::System(SystemModel::Property),
        [
            (PropertyId::System(SystemProperty::Name), Value::String(name.into())),
            (PropertyId::System(SystemProperty::Backend), Value::String("lww".into())),
            (PropertyId::System(SystemProperty::ValueType), Value::String(value_type.into())),
        ],
    )
}

struct Fixture<G: DigestGroup = Ristretto255> {
    storage: Arc<TestStorage>,
    publication: Arc<tokio::sync::Mutex<()>>,
    manager: TreeManager<TestStorage, G>,
}

impl<G: DigestGroup> Fixture<G> {
    fn new(capacity: usize) -> anyhow::Result<Self> {
        let storage = Arc::new(TestStorage::default());
        storage.set_state(property_state(score(), "score", "i64")?);
        storage.set_state(property_state(active(), "active", "bool")?);
        let publication = Arc::new(tokio::sync::Mutex::new(()));
        let manager = TreeManager::new(Arc::clone(&storage), Arc::clone(&publication), CacheSettings { bytes: 1 << 20 }, capacity);
        Ok(Self { storage, publication, manager })
    }

    /// Initial canonical state, before any tree opens.
    fn seed(&self, versions: &[Version]) -> anyhow::Result<()> {
        for version in versions {
            self.storage.set_state(version.state()?);
        }
        Ok(())
    }

    /// Commit canonical state, then reconcile before releasing publication.
    async fn commit(&self, changes: &[(Option<Version>, Option<Version>)]) -> anyhow::Result<()> {
        let _publication = self.publication.lock().await;
        let mut trx = self.storage.transaction();
        write_versions(&mut trx, changes).await?;
        self.manager.invalidate();
        trx.commit().await?.committed()?;
        self.manager.maintain_locked(false).await?;
        Ok(())
    }
}

async fn write_versions(trx: &mut impl StorageTransaction, changes: &[(Option<Version>, Option<Version>)]) -> anyhow::Result<()> {
    for (before, after) in changes {
        let Some(version) = after.as_ref().or(before.as_ref()) else { continue };
        let mut state = version.state()?;
        if after.is_none() {
            state.payload.state.memberships.clear();
        }
        let expected = before.as_ref().map(|version| version.head.clone()).unwrap_or_default();
        trx.set_state(&expected, &state).await?;
    }
    Ok(())
}

async fn ids<G: DigestGroup>(
    view: &DigestTreeView<TreeProperty, EntityMemberId, EntityHead, G, EngineAdapter<TestStorage, TreeProperty, EntityHead, G>>,
) -> anyhow::Result<Vec<EntityId>> {
    Ok(view.range(..)?.map_ok(|leaf| leaf.id.0).try_collect().await?)
}

#[test]
fn member_encodings_preserve_identity_and_clock_generations() -> anyhow::Result<()> {
    let id = EntityMemberId(EntityId::from_bytes([7; 32]));
    assert_eq!(id.to_bytes().as_ref(), &[7; 32]);
    assert_eq!(EntityMemberId::from_bytes(&[7; 32]), Some(id));
    assert!(EntityMemberId::from_bytes(&[7; 31]).is_none());
    let head = EntityHead(Clock::new(vec![(9, EventId::from_bytes([2; 32])), (3, EventId::from_bytes([1; 32]))])?);
    let mut digest_bytes = vec![1; 32];
    digest_bytes.extend_from_slice(&[2; 32]);
    assert_eq!(head.digest_bytes().as_ref(), digest_bytes.as_slice());
    let mut stored = 2u64.to_le_bytes().to_vec();
    stored.extend_from_slice(&3u32.to_le_bytes());
    stored.extend_from_slice(&[1; 32]);
    stored.extend_from_slice(&9u32.to_le_bytes());
    stored.extend_from_slice(&[2; 32]);
    assert_eq!(head.encode(), stored);
    assert_eq!(EntityHead::decode(&stored)?, head);
    Ok(())
}
