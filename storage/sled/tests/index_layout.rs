//! An index whose tree was written under another key layout is started over
//! when the database opens: the tree is dropped, the record marks the index
//! not built under the current layout, and the first use rebuilds it.

use std::collections::{BTreeMap, BTreeSet};

use ankql::ast::{ComparisonOperator, Expr, Predicate, Selection};
use ankurah_core::{
    indexing::KeySpec,
    property::backend::{LWWBackend, PropertyBackend},
    storage::{StorageCommitOutcome, StorageEngine, StorageTransaction},
    value::Value,
};
use ankurah_proto::{Attested, Clock, EntityId, EntityState, EventId, ModelId, PropertyId, State, StateBuffers};
use ankurah_storage_sled::{
    index::{BuildStatus, IndexRecord, KEY_LAYOUT_VERSION},
    SledStorageEngine,
};

/// The record shape written before index keys carried a layout version.
#[derive(serde::Serialize)]
struct UnversionedIndexRecord {
    id: u32,
    name: String,
    spec: KeySpec<String>,
    created_at_unix_ms: i64,
    build_status: BuildStatus,
}

#[tokio::test]
async fn index_recorded_without_a_layout_version_is_rebuilt_on_open() -> anyhow::Result<()> {
    stale_index_is_rebuilt_on_open(|record| {
        let IndexRecord { id, name, spec, created_at_unix_ms, build_status, .. } = record;
        bincode::serialize(&UnversionedIndexRecord { id, name, spec, created_at_unix_ms, build_status }).unwrap()
    })
    .await
}

#[tokio::test]
async fn index_recorded_under_an_older_layout_version_is_rebuilt_on_open() -> anyhow::Result<()> {
    stale_index_is_rebuilt_on_open(|record| {
        bincode::serialize(&IndexRecord { key_layout_version: KEY_LAYOUT_VERSION - 1, ..record }).unwrap()
    })
    .await
}

/// Build an index through a fetch, rewrite its record with `stale_record`
/// and plant a foreign key in its tree, reopen, and expect the first fetch
/// to serve correct results from a tree holding only the rebuilt keys.
async fn stale_index_is_rebuilt_on_open(stale_record: impl Fn(IndexRecord) -> Vec<u8>) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let model = ModelId::EntityId(EntityId::from_bytes([101; 32]));
    let name = PropertyId::EntityId(EntityId::from_bytes([201; 32]));
    let names = ["a", "b"];

    {
        let engine = SledStorageEngine::with_path(dir.path().to_path_buf())?;
        for (position, value) in names.iter().enumerate() {
            insert(&engine, model, name, position as u8, value).await?;
        }
        assert_eq!(names_matching(&engine, model, name, &names, "a").await?, BTreeSet::from(["a"]));
    }

    let planted_key = vec![0xFF; 40];
    let (index_id, real_keys) = {
        let db = sled::open(dir.path().join("sled"))?;
        let config = db.open_tree("index_config")?;
        let (key, bytes) = config.iter().next().expect("the fetch recorded an index")?;
        let record: IndexRecord = bincode::deserialize(&bytes)?;
        assert_eq!(record.key_layout_version, KEY_LAYOUT_VERSION);
        let index_id = u32::from_be_bytes(key.as_ref().try_into()?);
        config.insert(key, stale_record(record))?;
        let tree = db.open_tree(format!("index_{index_id}"))?;
        let real_keys: BTreeSet<Vec<u8>> = tree.iter().keys().map(|key| key.map(|key| key.to_vec())).collect::<Result<_, _>>()?;
        assert_eq!(real_keys.len(), names.len());
        tree.insert(planted_key.clone(), &[])?;
        db.flush()?;
        (index_id, real_keys)
    };

    let engine = SledStorageEngine::with_path(dir.path().to_path_buf())?;
    assert_eq!(names_matching(&engine, model, name, &names, "a").await?, BTreeSet::from(["a"]));
    assert_eq!(names_matching(&engine, model, name, &names, "b").await?, BTreeSet::from(["b"]));
    let database = engine.database.lock().unwrap().clone();
    let index = database.index_manager.indexes.read().unwrap()[&index_id].clone();
    let keys: BTreeSet<Vec<u8>> = index.tree().iter().keys().map(|key| key.map(|key| key.to_vec())).collect::<Result<_, _>>()?;
    assert_eq!(keys, real_keys, "the rebuilt tree holds exactly the keys of the stored entities");
    assert!(!keys.contains(&planted_key));
    let record: IndexRecord = bincode::deserialize(&database.index_manager.index_config_tree.get(index_id.to_be_bytes())?.unwrap())?;
    assert_eq!((record.key_layout_version, record.build_status), (KEY_LAYOUT_VERSION, BuildStatus::Ready));
    Ok(())
}

/// The stored names, by entity id position, whose `name` equals `literal`.
async fn names_matching<'a>(
    engine: &SledStorageEngine,
    model: ModelId,
    name: PropertyId,
    names: &[&'a str],
    literal: &str,
) -> anyhow::Result<BTreeSet<&'a str>> {
    let predicate = Predicate::Comparison {
        left: Box::new(Expr::Path(name.into())),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::String(literal.into()))),
    };
    let selection = Selection { predicate, order_by: None, limit: None }.and_member_of(model);
    let states = engine.fetch_states(&selection).await?;
    Ok(states.iter().map(|state| names[state.payload.entity_id.to_bytes()[0] as usize]).collect())
}

async fn insert(engine: &SledStorageEngine, model: ModelId, property: PropertyId, byte: u8, value: &str) -> anyhow::Result<()> {
    let backend = LWWBackend::new();
    backend.set(property, Some(Value::String(value.into())));
    let event = EventId::from_bytes([byte; 32]);
    if let Some(operations) = backend.to_operations()? {
        backend.apply_operations_with_event(&operations, event.clone())?;
    }
    let state = Attested::opt(
        EntityState {
            entity_id: EntityId::from_bytes([byte; 32]),
            state: State {
                state_buffers: StateBuffers(BTreeMap::from([("lww".into(), backend.to_state_buffer()?)])),
                memberships: BTreeSet::from([model]),
                head: Clock::genesis(event),
            },
        },
        None,
    );
    let mut transaction = engine.transaction();
    transaction.set_state(&Clock::default(), &state).await?;
    assert!(matches!(transaction.commit().await?, StorageCommitOutcome::Committed(_)));
    Ok(())
}
