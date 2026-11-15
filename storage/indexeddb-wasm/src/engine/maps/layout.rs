//! Where the maps and the lock versions live in the engine's IndexedDB
//! database, and the keys and values they are kept under.
//!
//! A map's entry is keyed by an array of two binary keys, its map's name and
//! its own key. IndexedDB orders binary keys by their bytes, unsigned, and
//! arrays by their items in turn, so a map's entries lie together in key
//! order, the empty key first. An array sorts after every binary key, so the
//! map's name and an empty array bound every key of the map from above.
//!
//! A lock's version is keyed by the lock's name as a binary key, and the
//! reset epoch by a key of text, which no binary key equals. Each reset puts
//! an epoch the database never held before, a ULID's bytes, so that a reset
//! moves the epoch without reading it first.

use std::ops::Bound;

use ankurah_core::storage::{LockName, MapId};
use js_sys::{Array, Uint8Array};
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{IdbDatabase, IdbKeyRange};

use super::MapError;

/// The object store of every map's entries.
pub(super) const MAPS: &str = "_ankurah_map";

/// The object store of every lock name's version, and the reset epoch.
pub(super) const LOCK_VERSIONS: &str = "_ankurah_map_lock_version";

/// The maps' object stores, which every transaction over the maps covers.
pub(crate) const STORES: [&str; 2] = [MAPS, LOCK_VERSIONS];

/// The key of the reset epoch in [`LOCK_VERSIONS`].
const RESET_EPOCH: &str = "reset_epoch";

/// Make the maps' object stores, in an upgrade of `db`.
pub(crate) fn create_object_stores(db: &IdbDatabase) -> Result<(), JsValue> {
    for store in STORES {
        db.create_object_store(store)?;
    }

    Ok(())
}

/// Whether `db` has the maps' object stores.
pub(crate) fn has_object_stores(db: &IdbDatabase) -> bool {
    let names = db.object_store_names();
    STORES.iter().all(|store| names.contains(store))
}

/// The key of the entry under `key` in `map`.
pub(super) fn entry_key(map: &MapId, key: &[u8]) -> JsValue { Array::of2(&bytes(map.as_bytes()), &bytes(key)).into() }

/// The keys of the entries of `map` between the bounds, which hold some key.
pub(super) fn entry_range(map: &MapId, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<IdbKeyRange, MapError> {
    let (lower, lower_open) = match start {
        Bound::Included(key) => (entry_key(map, key), false),
        Bound::Excluded(key) => (entry_key(map, key), true),
        Bound::Unbounded => (entry_key(map, &[]), false),
    };

    let (upper, upper_open) = match end {
        Bound::Included(key) => (entry_key(map, key), false),
        Bound::Excluded(key) => (entry_key(map, key), true),
        Bound::Unbounded => (Array::of2(&bytes(map.as_bytes()), &Array::new()).into(), true),
    };

    IdbKeyRange::bound_with_lower_open_and_upper_open(&lower, &upper, lower_open, upper_open).map_err(MapError::thrown)
}

/// The keys of every entry of `map`.
pub(super) fn whole_map(map: &MapId) -> Result<IdbKeyRange, MapError> { entry_range(map, Bound::Unbounded, Bound::Unbounded) }

/// The bytes of a map entry's key, as a read of the entry found the entry's
/// key: the second of its two items, both binary, which IndexedDB gives back
/// as array buffers.
pub(super) fn key_of(entry_key: &JsValue) -> Result<Vec<u8>, MapError> {
    let items = entry_key.dyn_ref::<Array>().filter(|items| items.length() == 2 && is_bytes(&items.get(0)));
    items.and_then(|items| bytes_of(&items.get(1))).ok_or(MapError::Damaged { what: "map entry's key" })
}

/// The bytes of a map entry's value, as a read of it found it.
pub(super) fn value_of(value: &JsValue) -> Result<Vec<u8>, MapError> {
    bytes_of(value).ok_or(MapError::Damaged { what: "map entry's value" })
}

/// The key of the version of the lock `name`.
pub(super) fn version_key(name: &LockName) -> JsValue { bytes(name.as_bytes()) }

/// The key of the reset epoch.
pub(super) fn epoch_key() -> JsValue { JsValue::from_str(RESET_EPOCH) }

/// The reset epoch as a read of it found it: no bytes where no reset has
/// put one, and otherwise the sixteen of the ULID a reset put.
pub(super) fn epoch_of(value: &JsValue) -> Result<Vec<u8>, MapError> {
    if value.is_undefined() {
        return Ok(Vec::new());
    }

    let epoch = view_of(value).filter(|epoch| epoch.length() == 16).ok_or(MapError::Damaged { what: "reset epoch" })?;
    Ok(epoch.to_vec())
}

/// An epoch the database never held before, for a reset to put.
pub(super) fn new_epoch() -> JsValue { bytes(&ulid::Ulid::new().to_bytes()) }

/// The last version a lock keeps: the largest integer the JavaScript number
/// a version is kept as holds exactly, as it holds every integer below it.
const LAST_VERSION: u64 = js_sys::Number::MAX_SAFE_INTEGER as u64;

/// A lock's version, as a read of it found it: 0 where there is none.
pub(super) fn version_of(value: &JsValue) -> Result<u64, MapError> {
    if value.is_undefined() {
        return Ok(0);
    }

    let number = value.as_f64().ok_or(MapError::Damaged { what: "lock version" })?;
    match number.fract() == 0.0 && (0.0..=LAST_VERSION as f64).contains(&number) {
        true => Ok(number as u64),
        false => Err(MapError::Damaged { what: "lock version" }),
    }
}

/// The value the version after `version` is kept as: none after the last.
pub(super) fn next_version_value(version: u64) -> Option<JsValue> {
    (version < LAST_VERSION).then(|| JsValue::from_f64((version + 1) as f64))
}

/// The value bytes are kept as.
pub(super) fn bytes(bytes: &[u8]) -> JsValue { Uint8Array::from(bytes).into() }

/// Whether `value` holds bytes.
fn is_bytes(value: &JsValue) -> bool { value.is_instance_of::<Uint8Array>() || value.is_instance_of::<js_sys::ArrayBuffer>() }

/// The bytes `value` holds, if it holds bytes.
fn bytes_of(value: &JsValue) -> Option<Vec<u8>> { view_of(value).map(|view| view.to_vec()) }

/// A view of the bytes `value` holds, if it holds bytes, which copies none
/// of them.
fn view_of(value: &JsValue) -> Option<Uint8Array> {
    value.dyn_ref::<Uint8Array>().cloned().or_else(|| value.dyn_ref::<js_sys::ArrayBuffer>().map(|buffer| Uint8Array::new(buffer)))
}
