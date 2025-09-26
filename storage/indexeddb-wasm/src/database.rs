use crate::util::navigator_lock::NavigatorLock;
use ankurah_core::indexing::KeySpec;
use ankurah_core::{error::RetrievalError, notice_info, util::safeset::SafeSet};
use anyhow::{anyhow, Result};
use send_wrapper::SendWrapper;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tracing::warn;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{prelude::*, JsCast};
use web_sys::{window, IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbVersionChangeEvent};

use crate::util::{cb_future::CBFuture, cb_race::CBRace, require::WBGRequire};

#[derive(Debug, Clone)]
pub struct Database(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    connection: Arc<tokio::sync::Mutex<Connection>>,
    db_name: String,
    // _callbacks: SendWrapper<Vec<Box<dyn Any>>>,
    /// Cache of existing index names to avoid repeated checks
    index_cache: SafeSet<String>,
}

#[derive(Debug)]
pub struct Connection {
    db: SendWrapper<IdbDatabase>,
    // _callbacks: SendWrapper<Vec<Box<dyn Any>>>,
    /// Keep onversionchange handler alive for the lifetime of the connection
    onversionchange: Option<SendWrapper<Closure<dyn FnMut(IdbVersionChangeEvent)>>>,
    /// Mark connection as stale when a versionchange occurs; triggers lazy reopen
    stale: Arc<AtomicBool>,
}

impl Database {
    pub async fn open(db_name: &str) -> Result<Self, RetrievalError> {
        let connection = Connection::open(db_name).await?;

        Ok(Self(Arc::new(Inner {
            connection: Arc::new(tokio::sync::Mutex::new(connection)),
            db_name: db_name.to_string(),
            // _callbacks: SendWrapper::new(Vec::new()), // Callbacks are now stored in Connection
            index_cache: SafeSet::new(),
        })))
    }

    /// Get a clone of the current database connection; lazily re-open if stale
    pub async fn get_connection(&self) -> SendWrapper<IdbDatabase> {
        let mut conn = self.0.connection.lock().await;
        if conn.is_stale() {
            let reopened = Connection::open(&self.0.db_name).await.expect("reopen IndexedDB after versionchange");
            *conn = reopened;
        }
        conn.db.clone()
    }

    /// Close the database connection
    pub async fn close(&self) { self.0.connection.lock().await.close(); }

    /// Ensure an index exists, creating it if necessary via database version upgrade
    pub async fn assure_index_exists(&self, index_spec: &KeySpec) -> Result<(), RetrievalError> {
        let name = index_spec.name_with("", "__");
        if self.0.index_cache.contains(&name) {
            return Ok(());
        }
        if self.0.connection.lock().await.has_index(&name)? {
            self.0.index_cache.insert(name);
            return Ok(());
        }

        let lock_name = format!("ankurah-idb-upgrade-{}", self.0.db_name);
        let inner = self.0.clone();
        let spec = index_spec.clone();

        NavigatorLock::with(&lock_name, async move || {
            let name = spec.name_with("", "__");
            if inner.index_cache.contains(&name) {
                return Ok(());
            }
            let mut connection = inner.connection.lock().await;
            if connection.has_index(&name)? {
                inner.index_cache.insert(name);
                return Ok(());
            }
            let current_version = connection.version();
            connection.close();
            let new_connection = Connection::open_with_index(&inner.db_name, current_version + 1, spec.clone()).await?;
            *connection = new_connection;
            inner.index_cache.insert(name);
            Ok(())
        })
        .await?;

        Ok(())
    }

    /// Get database name
    pub fn name(&self) -> &str { &self.0.db_name }

    /// Cleanup database (delete it entirely)
    pub async fn cleanup(db_name: &str) -> anyhow::Result<()> {
        let request = {
            let window = window().require("get window")?;
            let idb: IdbFactory = window.indexed_db().require("get indexeddb")?;
            idb.delete_database(db_name).require("delete database")?
        };
        CBFuture::new(request, &["success", "blocked"], "error").await.require("await delete request")?;
        Ok(())
    }
}

impl Connection {
    pub fn is_stale(&self) -> bool { self.stale.load(Ordering::SeqCst) }

    pub fn version(&self) -> u32 { self.db.version() as u32 }

    /// Check whether the `entities` store already has an index with the given name
    pub fn has_index(&self, index_name: &str) -> Result<bool, RetrievalError> {
        let tx = self.db.transaction_with_str_and_mode("entities", web_sys::IdbTransactionMode::Readonly).require("get transaction")?;
        let store = tx.object_store("entities").require("get object store")?;
        Ok(store.index_names().contains(index_name))
    }

    /// Close the database connection
    pub fn close(&self) {
        // Remove handler and close DB
        self.db.set_onversionchange(None);
        self.db.close();
        // Drop closure by clearing the field (if we had &mut self); retained for lifetime otherwise
    }

    /// Open or create a new database connection with default schema
    pub async fn open(db_name: &str) -> Result<Self, RetrievalError> {
        notice_info!("Database.open({})", db_name);
        if db_name.is_empty() {
            return Err(anyhow!("Database name cannot be empty").into());
        }
        Self::new(db_name, None, move |event: IdbVersionChangeEvent| -> Result<(), RetrievalError> {
            let open_request: IdbOpenDbRequest = event.target().require("get event target")?.unchecked_into();
            let transaction = open_request.transaction().require("get upgrade transaction")?;

            if let Err(_) = transaction.object_store("entities") {
                let db: IdbDatabase = transaction.db();
                let store = db.create_object_store("entities").require("create entities store")?;

                let key_path = js_sys::Array::of2(&"__collection".into(), &"id".into());
                store.create_index_with_str_sequence("__collection__id", &key_path).require("create collection index")?;

                let events_store = db.create_object_store("events").require("create events store")?;
                events_store.create_index_with_str("by_entity_id", "__entity_id").require("create entity_id index")?;
            }
            Ok(())
        })
        .await
    }

    /// Open database connection with a specific index to be created
    pub async fn open_with_index(db_name: &str, version: u32, index_spec: KeySpec) -> Result<Self, RetrievalError> {
        let index_name = index_spec.name_with("", "__");
        notice_info!("creating index {db_name}.entities.{index_name} -> {:?}", index_spec.keyparts);

        Self::new(db_name, Some(version), move |event: IdbVersionChangeEvent| -> Result<(), RetrievalError> {
            let open_request: IdbOpenDbRequest = event.target().require("get event target")?.unchecked_into();
            let transaction = open_request.transaction().require("get upgrade transaction")?;
            let store = transaction.object_store("entities").require("get entities store during upgrade")?;
            let key_path: Vec<JsValue> = index_spec.keyparts.iter().map(|kp| (&kp.column).into()).collect();
            store.create_index_with_str_sequence(&index_name, &key_path.into()).require("create index")?;
            Ok(())
        })
        .await
    }

    async fn new<F>(db_name: &str, version: Option<u32>, onupgradeneeded: F) -> Result<Self, RetrievalError>
    where F: 'static + Fn(IdbVersionChangeEvent) -> Result<(), RetrievalError> {
        let open_request = SendWrapper::new({
            let window = window().require("get window")?;
            let idb: IdbFactory = window.indexed_db().require("get indexeddb")?;
            match version {
                Some(v) => idb.open_with_u32(db_name, v).require("open database")?,
                None => idb.open(db_name).require("open database")?,
            }
        });

        let race = CBRace::new();
        let closure = SendWrapper::new(race.wrap(onupgradeneeded));
        open_request.set_onupgradeneeded(Some(closure.as_ref().unchecked_ref()));

        CBFuture::new(&*open_request, "success", "error").await.require("IndexedDB open failed")?;
        race.take_err()??;

        let db = open_request.result().require("get database result")?.unchecked_into::<IdbDatabase>();

        let stale = Arc::new(AtomicBool::new(false));
        let stale_for_cb = stale.clone();
        let onversionchange = Closure::wrap(Box::new(move |event: IdbVersionChangeEvent| {
            stale_for_cb.store(true, Ordering::SeqCst);
            if let Some(target) = event.target() {
                warn!("Version change event received - closing database");
                let db: IdbDatabase = target.unchecked_into();
                db.close();
            }
        }) as Box<dyn FnMut(IdbVersionChangeEvent)>);
        db.set_onversionchange(Some(onversionchange.as_ref().unchecked_ref()));
        Ok(Self { db: SendWrapper::new(db), onversionchange: Some(SendWrapper::new(onversionchange)), stale })
    }
}
