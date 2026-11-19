//! Digest-tree adapter and manager integration over canonical Sled storage.

use std::collections::BTreeMap;
use std::sync::Arc;

use ankql::ast::{ComparisonOperator, Expr, OrderByItem, OrderDirection, Predicate, Selection};
use ankurah::core::sync::conformance::fixtures::{self, assert_holds, commit, created, item, stores};
use ankurah::core::sync::conformance::{self, TestDatabase};
use ankurah::core::{
    property::backend::{LWWBackend, PropertyBackend},
    storage::{StorageEngine, StorageTransaction},
    sync::{CacheSettings, TreeId, TreeManager},
    value::{Value, ValueType},
};
use ankurah::proto::{Attested, Clock, EntityId, EntityState, ModelId, PropertyId, State, StateBuffers, SystemModel, SystemProperty};
use ankurah_digest_tree::{Ristretto255, Span};
use ankurah_storage_sled::SledStorageEngine;
use futures_util::TryStreamExt;

type Tree = fixtures::Tree<SledStorageEngine, Ristretto255>;

#[tokio::test]
async fn a_trees_commits_outlive_its_databases_closing() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;

    let digest = {
        let database = Database::open_in(directory.path())?;
        let engine = database.engine();
        let trees = stores(engine);
        let tree: Tree = created(&trees, engine, "albums", [item!(1, 10), item!(2, 20)]).await?;
        commit(&trees, engine, &tree, |changes| changes.remove(item!(1, 10))).await?;

        tree.view().span_digest(Span::all()).await?.digest
    };

    let reopened = Database::open_in(directory.path())?;
    let engine = reopened.engine();
    let trees = stores(engine);
    let tree: Tree = trees.open(&TreeId::new("albums")).await?;
    assert_eq!(tree.view().span_digest(Span::all()).await?.digest, digest);

    commit(&trees, engine, &tree, |changes| changes.insert(item!(3, 30))).await?;
    assert_holds(&tree, &[item!(2, 20), item!(3, 30)]).await
}

#[tokio::test]
async fn a_manager_builds_maintains_and_resumes_from_canonical_sled_states() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let model = ModelId::EntityId(EntityId::from_bytes([80; 32]));
    let score = PropertyId::EntityId(EntityId::from_bytes([90; 32]));
    let active = PropertyId::EntityId(EntityId::from_bytes([91; 32]));
    let definition = |id, name: &str, value_type: &str| {
        canonical_state(
            id,
            ModelId::System(SystemModel::Property),
            [
                (PropertyId::System(SystemProperty::Name), Value::String(name.into())),
                (PropertyId::System(SystemProperty::Backend), Value::String("lww".into())),
                (PropertyId::System(SystemProperty::ValueType), Value::String(value_type.into())),
            ],
            90,
        )
    };
    let score_definition = definition(EntityId::from_bytes([90; 32]), "score", "i64")?;
    let active_definition = definition(EntityId::from_bytes([91; 32]), "active", "bool")?;
    let entity = |id, number, included, seed| {
        canonical_state(EntityId::from_bytes([id; 32]), model, [(score, Value::I64(number)), (active, Value::Bool(included))], seed)
    };
    let first = entity(1, 30, true, 1)?;
    let second = entity(2, 10, true, 2)?;
    let third = entity(3, 20, false, 3)?;
    let moved = entity(1, 5, true, 11)?;
    let entered = entity(3, 20, true, 13)?;
    let while_absent = entity(2, 1, true, 22)?;
    let selection = Selection {
        predicate: Predicate::Comparison {
            left: Box::new(Expr::Path(active.into())),
            operator: ComparisonOperator::Equal,
            right: Box::new(Expr::Literal(Value::Bool(true))),
        },
        order_by: Some(vec![OrderByItem { path: score.into(), direction: OrderDirection::Asc }]),
        limit: Some(1),
    }
    .and_member_of(model);

    let tree_id = {
        let database = Database::open_in(directory.path())?;
        let storage = database.engine();
        let mut trx = storage.transaction();
        for state in [&score_definition, &active_definition, &first, &second, &third] {
            trx.set_state(&Clock::default(), state).await?;
        }
        trx.commit().await?.committed()?;

        let publication = Arc::new(tokio::sync::Mutex::new(()));
        let manager = TreeManager::<_, Ristretto255>::new(Arc::clone(storage), Arc::clone(&publication), CacheSettings { bytes: 4096 }, 1);
        let tree = manager.open(selection.clone()).await?;
        let tree_id = tree.store().id().clone();
        assert_eq!(tree.key_spec().keyparts[0].value_type, ValueType::I64);
        let members: Vec<_> = tree.view().range(..)?.map_ok(|leaf| (leaf.id.0, leaf.payload.0)).try_collect().await?;
        assert_eq!(
            members,
            vec![(second.payload.entity_id, second.payload.state.head.clone())],
            "the tree applies LIMIT using the declared numeric property"
        );

        {
            let _publication = publication.lock().await;
            let mut trx = storage.transaction();
            for (before, after) in [(&first, &moved), (&third, &entered)] {
                trx.set_state(&before.payload.state.head, after).await?;
            }
            manager.invalidate();
            trx.commit().await?.committed()?;
        }
        manager.maintain().await?;
        let members: Vec<_> = tree.view().range(..)?.map_ok(|leaf| (leaf.id.0, leaf.payload.0)).try_collect().await?;
        assert_eq!(members, vec![(moved.payload.entity_id, moved.payload.state.head.clone())]);
        drop((tree, manager));

        // With no resident tree, canonical storage advances without maintenance.
        // The next manager must continue from the persisted cursor.
        let mut trx = storage.transaction();
        trx.set_state(&second.payload.state.head, &while_absent).await?;
        trx.commit().await?.committed()?;
        tree_id
    };

    let reopened = Database::open_in(directory.path())?;
    let manager = TreeManager::<_, Ristretto255>::new(
        Arc::clone(reopened.engine()),
        Arc::new(tokio::sync::Mutex::new(())),
        CacheSettings { bytes: 4096 },
        1,
    );
    let tree = manager.open(selection).await?;
    assert_eq!(tree.store().id(), &tree_id);
    let members: Vec<_> = tree.view().range(..)?.map_ok(|leaf| (leaf.id.0, leaf.payload.0)).try_collect().await?;
    assert_eq!(members, vec![(while_absent.payload.entity_id, while_absent.payload.state.head)]);
    assert_eq!(tree.audit().await?.members, 1);
    Ok(())
}

fn canonical_state(
    id: EntityId,
    model: ModelId,
    values: impl IntoIterator<Item = (PropertyId, Value)>,
    seed: u8,
) -> anyhow::Result<Attested<EntityState>> {
    let backend = LWWBackend::new();
    for (property, value) in values {
        backend.set(property, Some(value));
    }
    let event = ankurah::core::test_helpers::commit_backend(&backend, seed)?;
    Ok(Attested::opt(
        EntityState {
            entity_id: id,
            state: State {
                state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                memberships: [model].into_iter().collect(),
                head: Clock::singleton(&event),
            },
        },
        None,
    ))
}

#[tokio::test]
async fn entity_records_and_several_trees_commit_together() -> anyhow::Result<()> {
    conformance::several_trees_and_entity_records_land_in_one_storage_transaction_or_none_do::<Ristretto255, _>(&Database::start()?).await
}

#[tokio::test]
async fn rebuilding_preserves_live_handles_and_removes_obsolete_rows() -> anyhow::Result<()> {
    conformance::a_rebuild_preserves_handles_and_removes_obsolete_rows::<Ristretto255, _>(&Database::start()?).await
}

#[tokio::test]
async fn later_range_batches_see_intervening_commits() -> anyhow::Result<()> {
    conformance::a_range_reads_later_batches_see_a_commit_made_between_them::<Ristretto255, _>(&Database::start()?).await
}

#[tokio::test]
async fn a_capture_survives_flushes_and_other_managers_commits() -> anyhow::Result<()> {
    conformance::a_capture_survives_flushes_and_other_managers_commits::<Ristretto255, _>(&Database::start()?).await
}

#[tokio::test]
async fn rolling_back_a_refusal_preserves_the_tree() -> anyhow::Result<()> {
    conformance::a_refusal_whose_transaction_is_dropped_leaves_the_tree_valid::<Ristretto255, _>(&Database::start()?).await
}

struct Database(Arc<SledStorageEngine>);

impl Database {
    fn start() -> anyhow::Result<Self> { Ok(Self(Arc::new(SledStorageEngine::new_test()?))) }

    fn open_in(path: &std::path::Path) -> anyhow::Result<Self> { Ok(Self(Arc::new(SledStorageEngine::with_path(path.to_path_buf())?))) }

    fn engine(&self) -> &Arc<SledStorageEngine> { &self.0 }
}

impl TestDatabase for Database {
    type Engine = SledStorageEngine;

    async fn engine(&self) -> anyhow::Result<Arc<SledStorageEngine>> { Ok(Arc::clone(&self.0)) }

    fn keeps_a_reads_state_across_commits(&self) -> bool { false }
}
