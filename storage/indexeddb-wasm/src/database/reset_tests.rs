use std::future::Future;

use ankurah_core::{
    indexing::{IndexKeyPart, KeySpec},
    storage::{StorageEngine, StorageTransaction},
    value::ValueType,
};
use futures::{
    channel::oneshot,
    future::{select, Either},
    FutureExt,
};
use js_sys::{Object, Promise, Reflect};
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;

use super::{Database, NavigatorLock};
use crate::IndexedDBStorageEngine;

#[wasm_bindgen_test]
async fn reset_drains_a_cancelled_queued_upgrade() -> anyhow::Result<()> {
    let name = format!("reset_queued_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    let (state, _) = ankurah_core::test_helpers::map_conformance::fixtures::entity!(9);
    let mut transaction = engine.transaction();
    transaction.set_state(&ankurah_proto::Clock::default(), &state).await?;
    transaction.commit().await?.committed()?;
    let lock_name = format!("ankurah-idb-upgrade-{name}");
    let (started, entered) = oneshot::channel();
    let (release, blocked) = oneshot::channel();
    let mut holder = Box::pin(NavigatorLock::with(&lock_name, async move || {
        let _ = started.send(());
        let _ = blocked.await;
        Ok(())
    }));
    assert!(holder.as_mut().now_or_never().is_none());
    deadline(entered).await?;

    let spec = index();
    let mut upgrade = Box::pin(engine.db.assure_index_exists(&spec));
    assert!(upgrade.as_mut().now_or_never().is_none());
    drop(upgrade);
    let mut reset = Box::pin(engine.delete_all());
    let waited = reset.as_mut().now_or_never().is_none();
    // A native read queues behind any clear reset already submitted. Seeing
    // the sentinel proves reset stayed at the upgrade fence, rather than
    // merely returning Pending while its native clear ran ahead of the queue.
    let retained = deadline(engine.get_state(state.payload.entity_id)).await;
    let _ = release.send(());
    assert!(waited, "the queued browser callback owns reset protection after its caller drops");
    assert_eq!(retained?.payload, state.payload);
    deadline(holder).await?;
    deadline(reset).await?;
    assert!(engine.get_state(state.payload.entity_id).await.is_err());
    assert!(has_index(&engine.db, &spec).await);
    IndexedDBStorageEngine::cleanup(&name).await
}

#[wasm_bindgen_test]
async fn reset_drains_cancelled_running_upgrades_with_and_without_web_locks() -> anyhow::Result<()> {
    for fallback in [false, true] {
        let name = format!("reset_running_{}", ulid::Ulid::new());
        let engine = IndexedDBStorageEngine::open(&name).await?;
        let blocker = Database::open(&name).await?;
        let connection = blocker.get_connection().await;
        // This connection deliberately stays open when the browser requests
        // an upgrade. Its versionchange event proves the native open started.
        let (started, entered) = oneshot::channel();
        let blocking = Closure::once(move || {
            let _ = started.send(());
        });
        connection.set_onversionchange(Some(blocking.as_ref().unchecked_ref()));
        let _hidden = fallback.then(|| LocksOverride::new(JsValue::UNDEFINED));
        let spec = index();
        let mut upgrade = Box::pin(engine.db.assure_index_exists(&spec));
        assert!(upgrade.as_mut().now_or_never().is_none());
        deadline(entered).await?;
        drop(upgrade);
        let mut reset = Box::pin(engine.delete_all());
        let waited = reset.as_mut().now_or_never().is_none();
        connection.close();
        assert!(waited, "an active native upgrade keeps protection (fallback={fallback})");
        deadline(reset).await?;
        assert!(has_index(&engine.db, &spec).await);
        IndexedDBStorageEngine::cleanup(&name).await?;
    }
    Ok(())
}

#[wasm_bindgen_test]
async fn rejected_upgrade_request_releases_its_reset_guard() -> anyhow::Result<()> {
    let name = format!("reset_rejected_{}", ulid::Ulid::new());
    let engine = IndexedDBStorageEngine::open(&name).await?;
    let locks = Object::new();
    let reject = Closure::wrap(Box::new(|| Promise::reject(&JsValue::from_str("refused for this test"))) as Box<dyn FnMut() -> Promise>);
    Reflect::set(&locks, &"request".into(), reject.as_ref()).unwrap();
    let _override = LocksOverride::new(locks.into());
    assert!(engine.db.assure_index_exists(&index()).await.is_err());
    deadline(engine.delete_all()).await?;
    IndexedDBStorageEngine::cleanup(&name).await
}

fn index() -> KeySpec<String> { KeySpec::new(vec![IndexKeyPart::asc("reset_test", ValueType::String)]) }

async fn has_index(db: &Database, spec: &KeySpec<String>) -> bool {
    let connection = db.get_connection().await;
    let transaction = connection.transaction_with_str("materializations").unwrap();
    transaction.object_store("materializations").unwrap().index_names().contains(&spec.name_with("", "__"))
}

async fn deadline<T>(future: impl Future<Output = T>) -> T {
    match select(Box::pin(future), Box::pin(gloo_timers::future::TimeoutFuture::new(10_000))).await {
        Either::Left((value, _)) => value,
        Either::Right(_) => panic!("reset/upgrade did not finish after its test gate opened"),
    }
}

struct LocksOverride(web_sys::Navigator);

impl LocksOverride {
    fn new(value: JsValue) -> Self {
        let navigator = web_sys::window().unwrap().navigator();
        let descriptor = Object::new();
        Reflect::set(&descriptor, &"configurable".into(), &JsValue::TRUE).unwrap();
        Reflect::set(&descriptor, &"value".into(), &value).unwrap();
        Object::define_property(&navigator, &"locks".into(), &descriptor);
        Self(navigator)
    }
}

impl Drop for LocksOverride {
    fn drop(&mut self) { let _ = Reflect::delete_property(&self.0, &"locks".into()); }
}
