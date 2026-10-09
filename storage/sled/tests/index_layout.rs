//! An index whose tree holds keys of another layout is started over when
//! the database opens: the tree is dropped, the record marks the index not
//! built under the current layout, and the first use rebuilds it. Each
//! case below leaves the raw trees as an older engine or an interrupted
//! open would have, reopens, and expects nothing served from the stale tree.

mod support;

use std::collections::BTreeSet;
use std::path::Path;

use ankql::ast::{ComparisonOperator, Expr, Predicate, Selection};
use ankurah_core::{indexing::KeySpec, storage::StorageEngine, value::Value};
use ankurah_proto::PropertyId;
use ankurah_storage_common::{Endpoint, KeyBoundComponent, KeyBounds};
use ankurah_storage_sled::{
    index::{BuildStatus, IndexRecord, KEY_LAYOUT_VERSION},
    planner_integration::key_bounds_to_sled_range,
    SledStorageEngine,
};
use support::{entity_id, index_specs, insert, model, property};

/// The stored names, each under `entity_id(byte)`.
const ROWS: [(u8, &str); 2] = [(1, "a"), (2, "b")];

fn name() -> PropertyId { property(201) }

/// The record shape written before index keys carried a layout version.
#[derive(serde::Serialize)]
struct UnversionedIndexRecord {
    id: u32,
    name: String,
    spec: KeySpec<String>,
    created_at_unix_ms: i64,
    build_status: BuildStatus,
}

fn unversioned(record: IndexRecord) -> Vec<u8> {
    let IndexRecord { id, name, spec, created_at_unix_ms, build_status, .. } = record;
    bincode::serialize(&UnversionedIndexRecord { id, name, spec, created_at_unix_ms, build_status }).unwrap()
}

/// The framing index keys had before the layout version: 0x00 escaped as
/// 0x00 0xFF, then a single 0x00 terminator.
fn old_framing(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 1);
    for &b in payload {
        out.push(b);
        if b == 0x00 {
            out.push(0xFF);
        }
    }
    out.push(0x00);
    out
}

/// The key the old layout gave an entity under `spec`, whose parts are the
/// membership column and the name, both ascending strings.
fn old_layout_key(spec: &KeySpec<String>, byte: u8, name_value: &str) -> Vec<u8> {
    assert_eq!(spec.keyparts.len(), 2, "{spec:?}");
    let mut key = old_framing(model().to_string().as_bytes());
    key.extend(old_framing(name_value.as_bytes()));
    key.extend(entity_id(byte).to_bytes());
    key
}

/// The bounds a fetch for `name = literal` carries, membership first.
fn equality_bounds(spec: &KeySpec<String>, literal: &str) -> KeyBounds {
    let pin = |column: &str, value: String| KeyBoundComponent {
        column: column.to_owned(),
        low: Endpoint::incl(Value::String(value.clone())),
        high: Endpoint::incl(Value::String(value)),
    };
    KeyBounds::new(vec![pin(&spec.keyparts[0].key, model().to_string()), pin(&spec.keyparts[1].key, literal.to_owned())])
}

#[tokio::test]
async fn index_recorded_without_a_layout_version_is_rebuilt_on_open() -> anyhow::Result<()> {
    rebuilt_after(|_, config, id, record| {
        config.insert(id.to_be_bytes(), unversioned(record))?;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn index_recorded_under_an_older_layout_version_is_rebuilt_on_open() -> anyhow::Result<()> {
    rebuilt_after(|_, config, id, record| {
        config.insert(id.to_be_bytes(), bincode::serialize(&IndexRecord { key_layout_version: KEY_LAYOUT_VERSION - 1, ..record })?)?;
        Ok(())
    })
    .await
}

/// A tree written by the old layout, as a store from before the version
/// field holds it. Served as it is, it answers `name = 'b'` wrongly: its key
/// for "b" lies outside the range the current conversion scans.
#[tokio::test]
async fn index_holding_old_layout_keys_is_not_served_before_its_rebuild() -> anyhow::Result<()> {
    rebuilt_after(|db, config, id, record| {
        let stale_b = old_layout_key(&record.spec, 2, "b");
        let range = key_bounds_to_sled_range(&equality_bounds(&record.spec, "b"), &record.spec)?;
        let end = range.end.expect("an equality has a tight end");
        assert!(!(range.start <= stale_b && stale_b < end), "the old key would have to lie outside the scanned range");

        let tree = db.open_tree(format!("index_{id}"))?;
        tree.clear()?;
        for (byte, name_value) in ROWS {
            tree.insert(old_layout_key(&record.spec, byte, name_value), &[])?;
        }
        config.insert(id.to_be_bytes(), unversioned(record))?;
        Ok(())
    })
    .await
}

/// The open was interrupted after dropping the tree and before rewriting
/// the record: the old layout is still recorded, so the drop repeats and the
/// rebuild proceeds.
#[tokio::test]
async fn index_left_between_the_drop_and_the_record_rewrite_is_rebuilt_on_open() -> anyhow::Result<()> {
    rebuilt_after(|db, config, id, record| {
        config.insert(id.to_be_bytes(), unversioned(record))?;
        assert!(db.drop_tree(format!("index_{id}"))?);
        Ok(())
    })
    .await
}

/// Build a store whose index is current, hand its raw config tree, index id
/// and record to `tamper`, reopen, and expect: an empty, not-built tree
/// right after the open, so nothing stale is served; correct fetches on
/// first use; and a tree holding exactly the keys the current layout gives
/// the stored entities, recorded Ready under the current version.
async fn rebuilt_after(tamper: impl Fn(&sled::Db, &sled::Tree, u32, IndexRecord) -> anyhow::Result<()>) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let (index_id, rebuilt_keys) = store_with_a_built_index(dir.path()).await?;

    {
        let db = sled::open(dir.path().join("sled"))?;
        let config = db.open_tree("index_config")?;
        let record: IndexRecord = bincode::deserialize(&config.get(index_id.to_be_bytes())?.expect("the fetch recorded the index"))?;
        assert_eq!(record.key_layout_version, KEY_LAYOUT_VERSION);
        tamper(&db, &config, index_id, record)?;
        db.flush()?;
    }

    let engine = SledStorageEngine::with_path(dir.path().to_path_buf())?;
    let database = engine.database.lock().unwrap().clone();
    let index = database.index_manager.indexes.read().unwrap()[&index_id].clone();
    assert!(index.tree().is_empty(), "the stale tree must be gone before anything is served");
    assert_eq!(index.status(), BuildStatus::NotBuilt);
    let record: IndexRecord = bincode::deserialize(&database.index_manager.index_config_tree.get(index_id.to_be_bytes())?.unwrap())?;
    assert_eq!((record.key_layout_version, record.build_status), (KEY_LAYOUT_VERSION, BuildStatus::NotBuilt));

    for (byte, name_value) in ROWS {
        assert_eq!(ids_named(&engine, name_value).await?, vec![byte]);
    }
    let keys: BTreeSet<Vec<u8>> = index.tree().iter().keys().map(|key| key.map(|key| key.to_vec())).collect::<Result<_, _>>()?;
    assert_eq!(keys, rebuilt_keys, "the rebuilt tree holds exactly the current-layout keys of the stored entities");
    let record: IndexRecord = bincode::deserialize(&database.index_manager.index_config_tree.get(index_id.to_be_bytes())?.unwrap())?;
    assert_eq!((record.key_layout_version, record.build_status), (KEY_LAYOUT_VERSION, BuildStatus::Ready));
    assert_eq!(index_specs(&engine).len(), 1, "the rebuilt index served the fetches; no second index was created");
    Ok(())
}

/// Open a store at `dir`, store `ROWS`, and build the name index through a
/// fetch; return the index id and the keys the current layout gave the rows.
async fn store_with_a_built_index(dir: &Path) -> anyhow::Result<(u32, BTreeSet<Vec<u8>>)> {
    let engine = SledStorageEngine::with_path(dir.to_path_buf())?;
    for (byte, name_value) in ROWS {
        insert(&engine, model(), byte, &[(name(), Value::String(name_value.into()))]).await?;
    }
    assert_eq!(ids_named(&engine, "a").await?, vec![1]);
    let database = engine.database.lock().unwrap().clone();
    let indexes = database.index_manager.indexes.read().unwrap();
    let (index_id, index) = indexes.iter().next().expect("the fetch built an index");
    let keys: BTreeSet<Vec<u8>> = index.tree().iter().keys().map(|key| key.map(|key| key.to_vec())).collect::<Result<_, _>>()?;
    assert_eq!(keys.len(), ROWS.len());
    Ok((*index_id, keys))
}

/// Entity id bytes of the stored entities whose `name` equals `literal`.
async fn ids_named(engine: &SledStorageEngine, literal: &str) -> anyhow::Result<Vec<u8>> {
    let predicate = Predicate::Comparison {
        left: Box::new(Expr::Path(name().into())),
        operator: ComparisonOperator::Equal,
        right: Box::new(Expr::Literal(Value::String(literal.into()))),
    };
    let selection = Selection { predicate, order_by: None, limit: None }.and_member_of(model());
    Ok(engine.fetch_states(&selection).await?.iter().map(|state| state.payload.entity_id.to_bytes()[0]).collect())
}
