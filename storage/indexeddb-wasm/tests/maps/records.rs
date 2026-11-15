//! Records put or removed through the browser's own API: a read that meets
//! a damaged one fails with the engine's `MapError::Damaged`, and a lock's
//! version at the last a lock keeps fails the next commit under it. A
//! database whose maps hold its only data, and whose record of its protocol
//! version the browser's API removed, is refused when opened again.

use std::ops::Bound;

use ankurah::core::error::{MutationError, RetrievalError};
use ankurah::core::storage::{LockName, MapId, MapRead, MapRows, MapSnapshot, MapWrite, ScanDirection, StorageEngine, StorageTransaction};
use ankurah::core::test_helpers::map_conformance::fixtures::entity;
use ankurah::proto::Clock;
use ankurah_storage_indexeddb_wasm::{IndexedDBStorageEngine, MapError};
use js_sys::{Array, Number, Promise, Uint8Array};
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{IdbObjectStore, IdbRequest, IdbTransactionMode};

use crate::thrown;

/// The engine's object store of every map's entries.
const MAPS: &str = "_ankurah_map";

/// The engine's object store of every lock name's version, and the reset
/// epoch.
const LOCK_VERSIONS: &str = "_ankurah_map_lock_version";

/// The engine's object store of the record of the database's protocol
/// version.
const META: &str = "_ankurah_meta";

/// The value IndexedDB keeps these bytes as: `bytes!(b"m")`.
macro_rules! bytes {
    ($bytes:expr) => {
        JsValue::from(Uint8Array::from(&$bytes[..]))
    };
}

/// A key of three parts, the map's name, a key and a number, lies among the
/// map's entries, and fails a range read over them, either way, as damaged.
#[wasm_bindgen_test]
async fn a_map_entrys_key_of_three_parts_fails_a_range_read_either_way_as_damaged() -> anyhow::Result<()> {
    let database = Database::new().await?;
    database.put(MAPS, &Array::of3(&bytes!(b"m"), &bytes!(b"b"), &JsValue::from(0)).into(), &bytes!(b"\x01")).await?;

    for direction in [ScanDirection::Forward, ScanDirection::Reverse] {
        let read = MapRead::Range { map: MapId::new("m"), start: Bound::Unbounded, end: Bound::Unbounded, direction, limit: usize::MAX };
        let failure = database.engine.map_snapshot(&[]).await?.map_read(vec![read]).await.expect_err("a range over a damaged key");
        assert!(matches!(read_failure(&failure), Some(MapError::Damaged { .. })), "{direction:?}: {failure}");
    }

    database.delete().await
}

/// A reset epoch is a ULID's sixteen bytes; an epoch of any other length
/// fails the read that notes it as damaged, the empty one among them, which
/// no reset puts.
#[wasm_bindgen_test]
async fn a_reset_epoch_of_other_than_sixteen_bytes_fails_a_read_as_damaged() -> anyhow::Result<()> {
    let database = Database::new().await?;

    for length in [0, 15, 17] {
        database.put(LOCK_VERSIONS, &JsValue::from_str("reset_epoch"), &bytes!(vec![0; length])).await?;
        let failure = database.engine.map_snapshot(&[]).await?.map_read(Vec::new()).await.expect_err("a read of a damaged epoch");
        assert!(matches!(read_failure(&failure), Some(MapError::Damaged { .. })), "{length} bytes: {failure}");
    }

    database.delete().await
}

/// A lock's version that isn't a number fails every transaction under its
/// name as damaged, until a reset, which clears every lock's version with the
/// maps.
#[wasm_bindgen_test]
async fn a_damaged_lock_version_fails_transactions_under_its_name_until_a_reset_clears_it() -> anyhow::Result<()> {
    let database = Database::new().await?;
    database.put(LOCK_VERSIONS, &bytes!(b"l"), &JsValue::from_str("damaged")).await?;

    let mut trx = database.engine.transaction();
    let failure = trx.map_lock_and_read(&[LockName::new("l")], Vec::new()).await.expect_err("a lock whose version is damaged");
    assert!(matches!(call_failure(&failure), Some(MapError::Damaged { .. })), "{failure}");
    drop(trx);

    database.engine.delete_all().await?;
    let mut trx = database.engine.transaction();
    trx.map_lock_and_read(&[LockName::new("l")], Vec::new()).await?;
    trx.commit().await?.committed()?;

    database.delete().await
}

/// A lock's version is kept as a JavaScript number, whose last exact integer
/// is the last version a lock keeps. A commit under a lock one below it
/// raises it to the last, which reads back; the next commit under it is
/// refused, and lands nothing: not its entity's record, nor the raised
/// version of the name it took before, nor its map writes.
#[wasm_bindgen_test]
async fn a_commit_under_a_lock_at_its_last_version_is_refused_and_lands_nothing() -> anyhow::Result<()> {
    let database = Database::new().await?;
    let (ordinary, exhausted, map) = (LockName::new("k"), LockName::new("l"), MapId::new("m"));
    database.put(LOCK_VERSIONS, &bytes!(b"l"), &JsValue::from_f64(Number::MAX_SAFE_INTEGER - 1.0)).await?;

    let mut trx = database.engine.transaction();
    trx.map_lock_and_read(&[exhausted.clone()], Vec::new()).await?;
    trx.map_write(vec![MapWrite::Put { map: map.clone(), key: b"a".to_vec(), value: vec![1] }]).await?;
    trx.commit().await?.committed()?;

    let (state, _) = entity!(1);
    let mut trx = database.engine.transaction();
    trx.set_state(&Clock::default(), &state).await?;
    trx.map_lock_and_read(&[ordinary, exhausted.clone()], Vec::new()).await?;
    trx.map_write(vec![MapWrite::Put { map: map.clone(), key: b"b".to_vec(), value: vec![2] }]).await?;
    let failure = trx.commit().await.expect_err("a commit under a lock at its last version");
    assert!(matches!(call_failure(&failure), Some(MapError::LastVersion { .. })), "{failure}");

    let entity = database.engine.get_state(state.payload.entity_id).await;
    assert!(matches!(entity, Err(RetrievalError::EntityNotFound(_))), "{:?}", entity.map(|_| ()));
    assert!(database.get(LOCK_VERSIONS, &bytes!(b"k")).await?.is_undefined(), "the name taken before is at no version");

    let get = MapRead::Get { map, keys: vec![b"a".to_vec(), b"b".to_vec()] };
    let rows = database.engine.map_snapshot(&[exhausted]).await?.map_read(vec![get]).await?;
    assert_eq!(rows, [MapRows::Got(vec![Some(vec![1]), None])]);

    database.delete().await
}

/// A database whose maps hold its only data holds ankurah's data all the
/// same: with its record of its protocol version removed, it is refused when
/// opened again, as any database with data and no record is.
#[wasm_bindgen_test]
async fn a_database_whose_only_data_is_in_its_maps_and_without_its_protocol_version_record_is_refused() -> anyhow::Result<()> {
    let database = Database::new().await?;
    let mut trx = database.engine.transaction();
    trx.map_write(vec![MapWrite::Put { map: MapId::new("m"), key: b"a".to_vec(), value: vec![1] }]).await?;
    trx.commit().await?.committed()?;

    database.remove(META, &JsValue::from_str("protocol_version")).await?;

    let Err(refusal) = IndexedDBStorageEngine::open(&database.name).await else {
        anyhow::bail!("a database holding maps and no record of its protocol version opened");
    };
    assert!(refusal.to_string().contains("no recorded protocol version"), "{refusal}");

    database.delete().await
}

/// An IndexedDB database of the test's own, and an engine on it whose first
/// map call has made the maps' object stores.
struct Database {
    name: String,
    engine: IndexedDBStorageEngine,
}

impl Database {
    async fn new() -> anyhow::Result<Self> {
        let name = format!("map_records_{}", ulid::Ulid::new());
        let engine = IndexedDBStorageEngine::open(&name).await?;
        engine.map_snapshot(&[]).await?.map_read(Vec::new()).await?;

        Ok(Self { name, engine })
    }

    /// Put `value` under `key` in the object store `store`, through the
    /// engine's connection but not the engine.
    async fn put(&self, store: &str, key: &JsValue, value: &JsValue) -> anyhow::Result<()> {
        self.request(store, |store| store.put_with_key(value, key)).await?;
        Ok(())
    }

    /// Remove `key` from the object store `store`, through the engine's
    /// connection but not the engine.
    async fn remove(&self, store: &str, key: &JsValue) -> anyhow::Result<()> {
        self.request(store, |store| store.delete(key)).await?;
        Ok(())
    }

    /// What `key` holds in the object store `store`, through the engine's
    /// connection but not the engine.
    async fn get(&self, store: &str, key: &JsValue) -> anyhow::Result<JsValue> { self.request(store, |store| store.get(key)).await }

    /// Make the request `request` makes of the object store `store` in a
    /// readwrite transaction of its own on the engine's connection: its
    /// result, once the transaction has committed.
    async fn request(&self, store: &str, request: impl FnOnce(&IdbObjectStore) -> Result<IdbRequest, JsValue>) -> anyhow::Result<JsValue> {
        let connection = self.engine.db.get_connection().await;
        let transaction = connection.transaction_with_str_and_mode(store, IdbTransactionMode::Readwrite).map_err(thrown)?;
        let complete = Promise::new(&mut |resolve, reject| {
            transaction.set_oncomplete(Some(&resolve));
            transaction.set_onabort(Some(&reject));
        });

        let request = request(&transaction.object_store(store).map_err(thrown)?).map_err(thrown)?;
        JsFuture::from(complete).await.map_err(thrown)?;
        request.result().map_err(thrown)
    }

    async fn delete(self) -> anyhow::Result<()> { IndexedDBStorageEngine::cleanup(&self.name).await }
}

/// The engine's map failure a snapshot's read failed with, if it failed with
/// one.
fn read_failure(error: &RetrievalError) -> Option<&MapError> {
    match error {
        RetrievalError::MutationError(cause) => call_failure(cause),
        _ => None,
    }
}

/// The engine's map failure a map call or a commit failed with, if it failed
/// with one.
fn call_failure(error: &MutationError) -> Option<&MapError> {
    match error {
        MutationError::General(error) => error.downcast_ref(),
        _ => None,
    }
}
