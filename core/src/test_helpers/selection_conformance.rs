//! Query order, snapshot order and digest-tree order must agree before LIMIT
//! can identify the same members in each. Expected orders below were checked
//! against raw PostgreSQL 16.16 VALUES queries in a database with C collation;
//! neither the storage query builder nor a Rust comparator supplies the oracle.
//!
//! The reference tree is built from the complete fixture through core's
//! production member and key-spec conversion. It never receives an engine's
//! selected results. Engine crates supply only a fresh storage engine.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    sync::Arc,
};

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, PropertyPath, Resolved, Selection};
use ankurah_digest_tree::Ristretto255;
use ankurah_proto::{Attested, Clock, EntityId, EntityState, ModelId, PropertyId, State, StateBuffers, SystemModel, SystemProperty};
use futures::TryStreamExt;

use crate::{
    entity::TemporaryEntity,
    property::backend::{LWWBackend, PropertyBackend},
    reactor::AbstractEntity,
    resultset::EntityResultSet,
    selection::filter::Filterable,
    storage::{StateScan, StorageEngine, StorageTransaction},
    sync::{CacheSettings, TreeManager},
    value::Value,
};

fn id(byte: u8) -> EntityId { EntityId::from_bytes([byte; 32]) }
fn model() -> ModelId { ModelId::EntityId(id(100)) }
fn score() -> PropertyId { PropertyId::EntityId(id(201)) }
fn group() -> PropertyId { PropertyId::EntityId(id(202)) }
fn document() -> PropertyId { PropertyId::EntityId(id(203)) }
fn active() -> PropertyId { PropertyId::EntityId(id(204)) }
fn absent_document() -> PropertyId { PropertyId::EntityId(id(205)) }

/// Numeric ordering, absent values, raw ID ties and implicit ID ordering.
pub async fn numeric_ordering<SE: StorageEngine + 'static>(storage: Arc<SE>) -> anyhow::Result<()> {
    let states = [
        (1, Some(2)),
        (208, Some(2)),
        (2, Some(10)),
        (3, None),
        (4, Some(-5)),
        (5, Some(i64::MIN)),
        (6, Some(i64::MAX)),
        (7, Some(i64::MIN + 1)),
    ]
    .into_iter()
    .map(|(byte, value)| member(byte, value.map(|value| (score(), Value::I64(value)))))
    .collect::<anyhow::Result<_>>()?;
    let mut fixture = Fixture::new(storage, states).await?;
    // SELECT id FROM fixture ORDER BY score ASC/DESC, id ASC;
    fixture.check("numeric ascending", Some(vec![order(score().into(), OrderDirection::Asc)]), &[5, 7, 4, 1, 208, 2, 6, 3]).await?;
    fixture.check("numeric descending", Some(vec![order(score().into(), OrderDirection::Desc)]), &[3, 6, 2, 1, 208, 4, 7, 5]).await?;
    fixture.check("implicit ID order", None, &[1, 2, 3, 4, 5, 6, 7, 208]).await?;
    fixture
        .check("explicit descending ID", Some(vec![order(PropertyPath::id(), OrderDirection::Desc)]), &[208, 7, 6, 5, 4, 3, 2, 1])
        .await?;
    fixture.finish()
}

/// JSONB type order, exact numeric equality, containers and missing paths.
pub async fn json_ordering<SE: StorageEngine + 'static>(storage: Arc<SE>) -> anyhow::Result<()> {
    use serde_json::json;

    let values = [
        (1, json!(null)),
        (2, json!("a")),
        (3, json!(2)),
        (4, json!(10)),
        (5, json!(false)),
        (6, json!(true)),
        (7, json!([])),
        (8, json!([1])),
        (9, json!({})),
        (10, json!({"a": 1})),
        (13, json!(2.0)),
        (14, json!(u64::MAX)),
    ];
    let mut states = values
        .into_iter()
        .map(|(byte, value)| member(byte, [(document(), Value::Json(json!({"rank": value})))]))
        .collect::<anyhow::Result<Vec<_>>>()?;
    states.push(member(11, [(document(), Value::Json(json!({})))])?);
    states.push(member(12, [])?);
    let mut fixture = Fixture::new(storage, states).await?;
    let mut rank = PropertyPath::from(document());
    rank.subpath.push("rank".into());
    // SELECT id FROM fixture ORDER BY (document -> 'rank') ASC/DESC, id ASC;
    // SQL NULL (absent path) differs from JSON null; empty JSON arrays sort
    // before JSON null. Numerically equal 2 and 2.0 retain ascending ID ties.
    fixture
        .check("JSON ascending", Some(vec![order(rank.clone(), OrderDirection::Asc)]), &[7, 1, 2, 3, 13, 4, 14, 5, 6, 8, 9, 10, 11, 12])
        .await?;
    fixture
        .check("JSON descending", Some(vec![order(rank, OrderDirection::Desc)]), &[11, 12, 10, 9, 8, 6, 5, 14, 4, 3, 13, 2, 1, 7])
        .await?;
    // The same values inside an object exercise JSONB's nested comparison:
    // its special placement of empty arrays applies only at the root.
    fixture
        .check(
            "whole JSON ascending",
            Some(vec![order(document().into(), OrderDirection::Asc)]),
            &[11, 1, 2, 3, 13, 4, 14, 5, 6, 7, 8, 9, 10, 12],
        )
        .await?;
    fixture
        .check(
            "whole JSON descending",
            Some(vec![order(document().into(), OrderDirection::Desc)]),
            &[12, 10, 9, 8, 7, 6, 5, 14, 4, 3, 13, 2, 1, 11],
        )
        .await?;
    fixture.finish()
}

/// JSON path components are literal object keys or signed array indices.
pub async fn json_path_ordering<SE: StorageEngine + 'static>(storage: Arc<SE>) -> anyhow::Result<()> {
    use serde_json::json;

    let mut states = [
        (1, json!([2, 10])),
        (2, json!({"0": 10, "-1": 2, "$.x": 4})),
        (3, json!({"0": null, "-1": 10, "$.x": null})),
        (4, json!([])),
        (5, json!({"$.x": 2})),
        (6, json!({"0": 2, "-1": null, "$": {"x": 10}})),
        (8, json!([null, 2])),
    ]
    .into_iter()
    .map(|(byte, value)| member(byte, [(document(), Value::Json(value))]))
    .collect::<anyhow::Result<Vec<_>>>()?;
    states.push(member(7, [])?);
    let mut fixture = Fixture::new(storage, states).await?;
    for (component, ascending, descending) in [
        ("0", &[3, 8, 1, 6, 2, 4, 5, 7][..], &[4, 5, 7, 2, 1, 6, 3, 8][..]),
        ("-1", &[6, 2, 8, 1, 3, 4, 5, 7][..], &[4, 5, 7, 1, 3, 2, 8, 6][..]),
        ("$.x", &[3, 5, 2, 1, 4, 6, 7, 8][..], &[1, 4, 6, 7, 8, 2, 5, 3][..]),
    ] {
        let mut path = PropertyPath::from(document());
        path.subpath.push(component.into());
        // SELECT id FROM fixture ORDER BY document #> ARRAY[component], id;
        fixture.check(&format!("JSON path {component} ascending"), Some(vec![order(path.clone(), OrderDirection::Asc)]), ascending).await?;
        fixture.check(&format!("JSON path {component} descending"), Some(vec![order(path, OrderDirection::Desc)]), descending).await?;
    }
    fixture.finish()
}

/// Independent directions, null placement per part, C-collated text and ID ties.
pub async fn mixed_ordering<SE: StorageEngine + 'static>(storage: Arc<SE>) -> anyhow::Result<()> {
    let states = [
        (1, Some("a"), Some(2)),
        (208, Some("a"), Some(2)),
        (2, Some("a"), Some(10)),
        (3, Some("b"), Some(1)),
        (4, Some("b"), None),
        (5, None, Some(5)),
        (6, Some("\u{e000}"), Some(0)),
        (7, Some("\u{10000}"), Some(0)),
        (8, Some("Z"), Some(0)),
    ]
    .into_iter()
    .map(|(byte, label, number)| {
        member(
            byte,
            label.map(|label| (group(), Value::String(label.into()))).into_iter().chain(number.map(|number| (score(), Value::I64(number)))),
        )
    })
    .collect::<anyhow::Result<_>>()?;
    let mut fixture = Fixture::new(storage, states).await?;
    // SELECT id FROM fixture ORDER BY label COLLATE "C" ASC, score DESC, id ASC;
    fixture
        .check(
            "mixed ascending/descending",
            Some(vec![order(group().into(), OrderDirection::Asc), order(score().into(), OrderDirection::Desc)]),
            &[8, 2, 1, 208, 4, 3, 6, 7, 5],
        )
        .await?;
    fixture
        .check(
            "mixed descending/ascending",
            Some(vec![order(group().into(), OrderDirection::Desc), order(score().into(), OrderDirection::Asc)]),
            &[5, 7, 6, 3, 4, 1, 208, 2, 8],
        )
        .await?;
    // A declared property with no materialized column must not shift the
    // logical types assigned to the remaining ORDER BY expressions.
    fixture
        .check(
            "absent leading JSON property",
            Some(vec![
                order(absent_document().into(), OrderDirection::Asc),
                order(group().into(), OrderDirection::Asc),
                order(PropertyPath::id(), OrderDirection::Desc),
            ]),
            &[8, 208, 2, 1, 4, 3, 6, 7, 5],
        )
        .await?;
    fixture.finish()
}

fn order(path: PropertyPath, direction: OrderDirection) -> OrderByItem<Resolved> { OrderByItem { path, direction } }

fn member(byte: u8, values: impl IntoIterator<Item = (PropertyId, Value)>) -> anyhow::Result<Attested<EntityState>> {
    state(id(byte), model(), values.into_iter().chain([(active(), Value::Bool(true))]))
}

fn state(
    entity_id: EntityId,
    membership: ModelId,
    values: impl IntoIterator<Item = (PropertyId, Value)>,
) -> anyhow::Result<Attested<EntityState>> {
    let backend = LWWBackend::new();
    for (property, value) in values {
        backend.set(property, Some(value));
    }
    let event = super::commit_backend(&backend, entity_id.to_bytes()[0])?;
    Ok(Attested::opt(
        EntityState {
            entity_id,
            state: State {
                state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                memberships: [membership].into_iter().collect(),
                head: Clock::singleton(&event),
            },
        },
        None,
    ))
}

struct Fixture<SE: StorageEngine> {
    storage: Arc<SE>,
    manager: TreeManager<SE>,
    members: Vec<Attested<EntityState>>,
    failures: Vec<String>,
}

impl<SE: StorageEngine + 'static> Fixture<SE> {
    async fn new(storage: Arc<SE>, members: Vec<Attested<EntityState>>) -> anyhow::Result<Self> {
        let mut trx = storage.transaction();
        for (property, name, value_type) in [
            (score(), "score", "i64"),
            (group(), "group", "string"),
            (document(), "document", "json"),
            (active(), "active", "bool"),
            (absent_document(), "absent_document", "json"),
        ] {
            let PropertyId::EntityId(entity_id) = property else { unreachable!() };
            let definition = state(
                entity_id,
                ModelId::System(SystemModel::Property),
                [
                    (PropertyId::System(SystemProperty::Name), Value::String(name.into())),
                    (PropertyId::System(SystemProperty::Backend), Value::String("lww".into())),
                    (PropertyId::System(SystemProperty::ValueType), Value::String(value_type.into())),
                ],
            )?;
            trx.set_state(&Clock::default(), &definition).await?;
        }
        // Insertion order is deliberately unrelated to the expected order.
        for member in members.iter().rev() {
            trx.set_state(&Clock::default(), member).await?;
        }
        let excluded =
            state(id(250), model(), [(score(), Value::I64(0)), (group(), Value::String("a".into())), (active(), Value::Bool(false))])?;
        trx.set_state(&Clock::default(), &excluded).await?;
        trx.commit().await?.committed()?;
        let manager = TreeManager::<_, Ristretto255>::new(
            storage.clone(),
            Arc::new(tokio::sync::Mutex::new(())),
            CacheSettings { bytes: 1 << 20 },
            0,
        );
        Ok(Self { storage, manager, members, failures: Vec::new() })
    }

    async fn check(&mut self, name: &str, order_by: Option<Vec<OrderByItem<Resolved>>>, expected: &[u8]) -> anyhow::Result<()> {
        let expected: Vec<_> = expected.iter().copied().map(id).collect();
        let active = Predicate::Comparison {
            left: Box::new(Expr::Path(active().into())),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::Bool(true))),
        };
        let selection = Selection { predicate: active.clone(), order_by, limit: None };
        self.record(
            format!("{name}: independent full digest tree"),
            self.manager.fixture_order(&selection, &self.members).await.map_err(Into::into),
            &expected,
        );
        self.record(
            format!("{name}: independent Reactor result set"),
            reactor_order(&self.members, selection.order_by.as_deref(), None),
            &expected,
        );

        // The explicit membership conjunct selects an indexed materialization.
        // Without it, the same fixture uses canonical-state candidate retrieval.
        // The active predicate excludes both the distractor and catalog rows.
        for indexed in [false, true] {
            let mut selection = selection.clone();
            if indexed {
                selection.predicate = Predicate::And(Box::new(Predicate::MemberOf(model())), Box::new(active.clone()));
            }
            let route = if indexed { "materialization" } else { "canonical" };
            let mut unlimited = selection.clone();
            // Unlimited queries do not promise an implicit ID tie-breaker.
            // Supply it explicitly for the full-order check.
            if !unlimited.order_by.as_deref().unwrap_or_default().iter().any(|order| order.path == PropertyPath::id()) {
                unlimited.order_by.get_or_insert_with(Vec::new).push(order(PropertyPath::id(), OrderDirection::Asc));
            }
            let fetched = self.storage.fetch_states(&unlimited).await.map(|states| ids(&states)).map_err(Into::into);
            self.record(format!("{name}/{route}: unlimited fetch"), fetched, &expected);

            for limit in 0..=expected.len() + 1 {
                selection.limit = Some(limit as u64);
                let prefix = &expected[..expected.len().min(limit)];
                if !indexed {
                    self.record(
                        format!("{name}/LIMIT {limit}: Reactor result set"),
                        reactor_order(&self.members, selection.order_by.as_deref(), Some(limit)),
                        prefix,
                    );
                }
                let fetched = self.storage.fetch_states(&selection).await.map(|states| ids(&states)).map_err(Into::into);
                self.record(format!("{name}/{route}/LIMIT {limit}: fetch"), fetched, prefix);
                let snapshot = snapshot_order(self.storage.as_ref(), &selection).await;
                self.record(format!("{name}/{route}/LIMIT {limit}: snapshot"), snapshot, prefix);
                let managed = async {
                    let tree = self.manager.open(selection.clone()).await?;
                    Ok(tree.view().range(..)?.map_ok(|leaf| leaf.id.0).try_collect::<Vec<_>>().await?)
                }
                .await;
                self.record(format!("{name}/{route}/LIMIT {limit}: managed tree"), managed, prefix);
            }
        }
        Ok(())
    }

    fn record(&mut self, label: String, result: anyhow::Result<Vec<EntityId>>, expected: &[EntityId]) {
        match result {
            Ok(actual) if actual == expected => {}
            Ok(actual) => self.failures.push(format!("{label}: expected {:?}, got {:?}", labels(expected), labels(&actual))),
            Err(error) => self.failures.push(format!("{label}: {error:#}")),
        }
    }

    fn finish(self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.failures.is_empty(),
            "{} ordering disagreements; first 16:\n{}",
            self.failures.len(),
            self.failures.iter().take(16).cloned().collect::<Vec<_>>().join("\n")
        );
        Ok(())
    }
}

fn ids(states: &[Attested<EntityState>]) -> Vec<EntityId> { states.iter().map(|state| state.payload.entity_id).collect() }
fn labels(ids: &[EntityId]) -> Vec<u8> { ids.iter().map(|id| id.to_bytes()[0]).collect() }

#[derive(Clone)]
struct ReactorEntity {
    entity: Arc<TemporaryEntity>,
    memberships: BTreeSet<ModelId>,
}

impl std::fmt::Debug for ReactorEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.entity.id().fmt(f) }
}

impl AbstractEntity for ReactorEntity {
    fn memberships(&self) -> BTreeSet<ModelId> { self.memberships.clone() }
    fn id(&self) -> EntityId { self.entity.id() }
    fn value(&self, property: &PropertyId) -> Option<Value> { self.entity.value(property) }
}

fn reactor_order(
    states: &[Attested<EntityState>],
    order_by: Option<&[OrderByItem<Resolved>]>,
    limit: Option<usize>,
) -> anyhow::Result<Vec<EntityId>> {
    let resultset = EntityResultSet::empty();
    for state in states {
        resultset.write().add(ReactorEntity {
            entity: Arc::new(TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?),
            memberships: state.payload.state.memberships.clone(),
        });
    }
    let key_spec = order_by.map(|order| crate::reactor::build_key_spec_from_selection(order, &resultset)).transpose()?;
    resultset.order_by(key_spec);
    resultset.limit(limit);
    Ok(resultset.keys().collect())
}

async fn snapshot_order(storage: &impl StorageEngine, selection: &Selection<Resolved>) -> anyhow::Result<Vec<EntityId>> {
    let mut snapshot = storage.state_snapshot(StateScan::Selection(selection)).await?;
    let mut found = Vec::new();
    loop {
        let page = snapshot.next_page(NonZeroUsize::new(2).unwrap()).await?;
        anyhow::ensure!(page.len() <= 2, "snapshot exceeded its page limit");
        if page.is_empty() {
            break;
        }
        found.extend(ids(&page));
    }
    snapshot.finish().await?;
    Ok(found)
}
