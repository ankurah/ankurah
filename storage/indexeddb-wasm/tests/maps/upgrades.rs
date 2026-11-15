//! Upgrades of a database by several tabs: a tab's first map call
//! upgrades it to make the maps' object stores, unless another tab's has,
//! and a tab's index takes an upgrade of its own. An upgrade closes every
//! other tab's connection, which each tab opens again before it looks at
//! what the database has. Where the Web Locks API is missing, nothing orders
//! the tabs' upgrades, and a first map call that loses the database's next
//! version to another tab upgrades the database once more.

use ankurah::core::indexing::{IndexKeyPart, KeySpec};
use ankurah::core::storage::{MapId, MapRead, MapRows, MapSnapshot, MapWrite, StorageEngine, StorageTransaction};
use ankurah::core::value::ValueType;
use ankurah_storage_indexeddb_wasm::IndexedDBStorageEngine;
use anyhow::Context;
use js_sys::{Object, Reflect};
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{IdbDatabase, Navigator};

use crate::thrown;

/// Two tabs make their first map calls at once on a database that has no
/// maps' object stores yet: one upgrades the database to make them, and the
/// other, waiting its turn, finds them made.
#[wasm_bindgen_test]
async fn two_tabs_first_map_calls_at_once_on_a_database_without_the_maps_stores_both_land() -> anyhow::Result<()> {
    let tabs = Tabs::open().await?;

    futures::try_join!(put(&tabs.here, "a", 1), put(&tabs.there, "b", 2))?;

    assert_eq!(got(&tabs.here, &["a", "b"]).await?, [MapRows::Got(vec![Some(vec![1]), Some(vec![2])])]);
    tabs.delete().await
}

/// A tab's first map call waits its turn to upgrade the database while
/// another tab upgrades it for an index: the upgrade closes the first tab's
/// connection, which the tab opens again, at the database's new version,
/// before it upgrades the database once more for the maps' object stores.
#[wasm_bindgen_test]
async fn a_first_map_call_waiting_while_another_tab_upgrades_the_database_for_an_index_upgrades_it_after() -> anyhow::Result<()> {
    let tabs = Tabs::open().await?;

    futures::try_join!(add_an_index(&tabs.there), put(&tabs.here, "a", 1))?;

    assert_eq!(got(&tabs.here, &["a"]).await?, [MapRows::Got(vec![Some(vec![1])])]);
    tabs.delete().await
}

/// A tab waits its turn to upgrade the database for an index while another
/// tab's first map call upgrades it for the maps' object stores: the upgrade
/// closes the first tab's connection, which the tab opens again, at the
/// database's new version, before it looks for the index and upgrades the
/// database once more.
#[wasm_bindgen_test]
async fn an_index_upgrade_waiting_while_another_tabs_first_map_call_upgrades_the_database_upgrades_it_after() -> anyhow::Result<()> {
    let tabs = Tabs::open().await?;

    futures::try_join!(put(&tabs.here, "a", 1), add_an_index(&tabs.there))?;

    assert_eq!(got(&tabs.there, &["a"]).await?, [MapRows::Got(vec![Some(vec![1])])]);
    tabs.delete().await
}

/// A tab's first map call upgrades the database, which closes another tab's
/// connection; that tab's next upgrade for an index opens it again before it
/// looks for the index.
#[wasm_bindgen_test]
async fn an_index_upgrade_after_another_tabs_first_map_call_upgraded_the_database_upgrades_it() -> anyhow::Result<()> {
    let tabs = Tabs::open().await?;

    put(&tabs.here, "a", 1).await?;
    add_an_index(&tabs.there).await?;

    assert_eq!(got(&tabs.there, &["a"]).await?, [MapRows::Got(vec![Some(vec![1])])]);
    tabs.delete().await
}

/// Where the Web Locks API is missing, as outside a secure context, nothing
/// makes a tab's first map call wait its turn to upgrade the database:
/// another tab's upgrade takes the next version first, the tab's own open
/// then finds that version without the maps' object stores, and the tab
/// upgrades the database once more to make them.
#[wasm_bindgen_test]
async fn without_web_locks_a_first_map_call_that_loses_the_next_version_upgrades_the_database_once_more() -> anyhow::Result<()> {
    let name = format!("map_upgrades_{}", ulid::Ulid::new());
    let tab = IndexedDBStorageEngine::open(&name).await?;
    let version = tab.db.get_connection().await.version() as u32;

    {
        let _hidden = WebLocksHidden::hide()?;

        // Both opens are asked for in one turn of the event loop, the other
        // tab's first, so the tab's own finds the version it asked for taken.
        upgrade_for_something_else(&name, version + 1)?;
        put(&tab, "a", 1).await?;
    }

    assert_eq!(tab.db.get_connection().await.version() as u32, version + 2, "the tab upgraded the database after the other tab had");
    assert_eq!(got(&tab, &["a"]).await?, [MapRows::Got(vec![Some(vec![1])])]);
    IndexedDBStorageEngine::cleanup(&name).await
}

/// Two engines on an IndexedDB database of the test's own, as two tabs have,
/// under a name no other test's has: the database has no maps' object stores
/// until a map call makes them.
struct Tabs {
    name: String,
    here: IndexedDBStorageEngine,
    there: IndexedDBStorageEngine,
}

impl Tabs {
    async fn open() -> anyhow::Result<Self> {
        let name = format!("map_upgrades_{}", ulid::Ulid::new());
        let (here, there) = (IndexedDBStorageEngine::open(&name).await?, IndexedDBStorageEngine::open(&name).await?);

        Ok(Self { name, here, there })
    }

    async fn delete(self) -> anyhow::Result<()> { IndexedDBStorageEngine::cleanup(&self.name).await }
}

/// Put `value` under `key` in the map `m`, in a storage transaction of its
/// own on `tab`, and commit it.
async fn put(tab: &IndexedDBStorageEngine, key: &str, value: u8) -> anyhow::Result<()> {
    let mut trx = tab.transaction();
    trx.map_write(vec![MapWrite::Put { map: MapId::new("m"), key: key.into(), value: vec![value] }]).await?;
    trx.commit().await?.committed()?;
    Ok(())
}

/// What the map `m` holds under each of `keys`, read in a snapshot on `tab`.
async fn got(tab: &IndexedDBStorageEngine, keys: &[&str]) -> anyhow::Result<Vec<MapRows>> {
    let get = MapRead::Get { map: MapId::new("m"), keys: keys.iter().map(|key| key.as_bytes().to_vec()).collect() };
    Ok(tab.map_snapshot(&[]).await?.map_read(vec![get]).await?)
}

/// Make an index of a name through `tab`, as an indexed query does, which
/// upgrades the database unless the index is made.
async fn add_an_index(tab: &IndexedDBStorageEngine) -> anyhow::Result<()> {
    let index = KeySpec::new(vec![IndexKeyPart::asc("name", ValueType::String)]);
    Ok(tab.db.assure_index_exists(&index).await?)
}

/// The Web Locks API, hidden from the page until this is dropped: what a page
/// outside a secure context finds.
struct WebLocksHidden {
    navigator: Navigator,
}

impl WebLocksHidden {
    fn hide() -> anyhow::Result<Self> {
        let navigator = web_sys::window().context("the test runs in a window")?.navigator();

        // A property of the page's own, with no value, in front of the
        // browser's.
        let no_value = Object::new();
        Reflect::set(&no_value, &"configurable".into(), &JsValue::TRUE).map_err(thrown)?;
        Object::define_property(&navigator, &"locks".into(), &no_value);

        Ok(Self { navigator })
    }
}

impl Drop for WebLocksHidden {
    fn drop(&mut self) {
        // Without the page's own property, the browser's shows again.
        let _ = Reflect::delete_property(&self.navigator, &"locks".into());
    }
}

/// Ask IndexedDB to upgrade the database `name` to `version` and make
/// nothing, as another tab's upgrade for something of its own does. The
/// connection it opens closes when a later upgrade asks it to.
fn upgrade_for_something_else(name: &str, version: u32) -> anyhow::Result<()> {
    let window = web_sys::window().context("the test runs in a window")?;
    let request = window.indexed_db().map_err(thrown)?.context("the window has IndexedDB")?.open_with_u32(name, version).map_err(thrown)?;

    let opened = request.clone();
    let close_when_asked = Closure::<dyn FnMut()>::new(move || {
        let connection: IdbDatabase = opened.result().expect("the database the request opened").unchecked_into();

        let closing = connection.clone();
        let close = Closure::<dyn FnMut()>::new(move || closing.close());
        connection.set_onversionchange(Some(close.as_ref().unchecked_ref()));
        close.forget();
    });
    request.set_onsuccess(Some(close_when_asked.as_ref().unchecked_ref()));
    close_when_asked.forget();

    Ok(())
}
