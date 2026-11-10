use crate::util::navigator_lock::NavigatorLock;
use ankurah_core::indexing::KeySpec;
use ankurah_core::{error::RetrievalError, notice_info, storage::StoreFormatError, util::safeset::SafeSet};
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

/// The format of the stores this engine writes, recorded in each store and checked on every open.
/// Bump it when this engine's stored layout, or the encoding of anything it stores, changes
/// incompatibly.
pub const STORE_FORMAT_VERSION: u32 = 1;
/// Key of the store format record in the `_ankurah_meta` object store.
const STORE_FORMAT_VERSION_KEY: &str = "store_format_version";

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
    _onversionchange: Option<SendWrapper<Closure<dyn FnMut(IdbVersionChangeEvent)>>>,
    /// Mark connection as stale when a versionchange occurs; triggers lazy reopen
    stale: Arc<AtomicBool>,
}

impl Database {
    /// Open the database `db_name`, refusing one of another format with a [`StoreFormatError`].
    pub async fn open(db_name: &str) -> anyhow::Result<Self> {
        let connection = Connection::open(db_name).await?;

        let database = Self(Arc::new(Inner {
            connection: Arc::new(tokio::sync::Mutex::new(connection)),
            db_name: db_name.to_string(),
            // _callbacks: SendWrapper::new(Vec::new()), // Callbacks are now stored in Connection
            index_cache: SafeSet::new(),
        }));
        // A storage failure while checking, then the refusal of a store of another format.
        database.check_store_format().await??;
        Ok(database)
    }

    /// Check the store format record (`_ankurah_meta` store, key
    /// `store_format_version`) against [`STORE_FORMAT_VERSION`], once per open:
    /// - no record and no existing ankurah data: write the record and proceed
    ///   (a fresh database created by [`Connection::open`] was already recorded
    ///   inside its creation upgrade; this covers databases whose creation was
    ///   interrupted before the record landed);
    /// - record present and equal: proceed;
    /// - record present and different or unreadable: refuse;
    /// - existing ankurah data but no record: refuse (a store from before the
    ///   record existed).
    ///
    /// The refusal paths write nothing.
    async fn check_store_format(&self) -> Result<Result<(), StoreFormatError>, RetrievalError> {
        let mut connection = self.0.connection.lock().await;

        // A database from before the `_ankurah_meta` store existed carries no record at
        // all: with ankurah data present that is a store from before format records (refuse before
        // creating anything); an empty one gets the store via a versioned
        // upgrade and falls through to be recorded below.
        if !connection.db.object_store_names().contains("_ankurah_meta") {
            if has_ankurah_data(&connection.db).await? {
                return Ok(Err(self.store_format_refusal(None)));
            }
            let current_version = connection.version();
            connection.close();
            *connection = Connection::open_with_meta_store(&self.0.db_name, current_version + 1).await?;
        }

        let db = connection.db.clone();
        SendWrapper::new(async move {
            let transaction = db.transaction_with_str("_ankurah_meta").require("create meta transaction")?;
            let store = transaction.object_store("_ankurah_meta").require("get meta store")?;
            let request = store.get(&JsValue::from_str(STORE_FORMAT_VERSION_KEY)).require("get store_format_version")?;
            CBFuture::new(&request, "success", "error").await.require("await store_format_version get")?;
            let found = request.result().require("store_format_version result")?;

            if found.is_undefined() || found.is_null() {
                // No record. Existing ankurah data makes this a store from before format records;
                // an empty database is recorded here and proceeds.
                if has_ankurah_data(&db).await? {
                    return Ok(Err(self.store_format_refusal(None)));
                }
                write_store_format_version(&db).await?;
                return Ok(Ok(()));
            }

            Ok(match found.as_f64() {
                Some(version) if version == f64::from(STORE_FORMAT_VERSION) => Ok(()),
                _ => Err(self.store_format_refusal(Some(found))),
            })
        })
        .await
    }

    /// The refusal of a store of another format. `found` is the raw stored
    /// record; `None` means the database carries ankurah data but no record at
    /// all (a store from before format records).
    fn store_format_refusal(&self, found: Option<JsValue>) -> StoreFormatError {
        let expected = STORE_FORMAT_VERSION;
        let refusal = match found {
            None => StoreFormatError::Unversioned { expected },
            Some(record) => match record.as_f64() {
                Some(version) if version.fract() == 0.0 && (0.0..=f64::from(u32::MAX)).contains(&version) => {
                    StoreFormatError::Mismatch { found: version as u32, expected }
                }
                _ => StoreFormatError::UnreadableRecord { record: format!("{record:?}"), expected },
            },
        };
        warn!(
            "Refusing IndexedDB database {:?} (delete it, e.g. via IndexedDBStorageEngine::cleanup, to proceed): {refusal}",
            self.0.db_name
        );
        refusal
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
    pub async fn assure_index_exists(&self, index_spec: &KeySpec<String>) -> Result<(), RetrievalError> {
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

/// Whether any ankurah object store (beyond `_ankurah_meta`) already holds content:
/// the engine-natural detection of an existing database, distinguishing a
/// fresh store (record and proceed) from one predating the version record
/// (refuse).
async fn has_ankurah_data(db: &IdbDatabase) -> Result<bool, RetrievalError> {
    let db = SendWrapper::new(db);
    SendWrapper::new(async move {
        for store_name in ["entities", "materializations", "events", "entity_models", "property_columns", "model_registrations"] {
            if !db.object_store_names().contains(store_name) {
                continue;
            }
            let transaction = db.transaction_with_str(store_name).require("create count transaction")?;
            let store = transaction.object_store(store_name).require("get store for count")?;
            let request = store.count().require("count store")?;
            CBFuture::new(&request, "success", "error").await.require("await count")?;
            if request.result().require("count result")?.as_f64().unwrap_or(0.0) > 0.0 {
                return Ok(true);
            }
        }
        Ok(false)
    })
    .await
}

/// Write the [`STORE_FORMAT_VERSION`] record into the `_ankurah_meta` store. Only ever
/// called on a database with no record and no ankurah data; fresh databases
/// are recorded inside their creation upgrade transaction instead.
async fn write_store_format_version(db: &IdbDatabase) -> Result<(), RetrievalError> {
    let db = SendWrapper::new(db);
    SendWrapper::new(async move {
        let transaction = db
            .transaction_with_str_and_mode("_ankurah_meta", web_sys::IdbTransactionMode::Readwrite)
            .require("create meta write transaction")?;
        let store = transaction.object_store("_ankurah_meta").require("get meta store")?;
        let request = store
            .put_with_key(&JsValue::from(STORE_FORMAT_VERSION), &JsValue::from_str(STORE_FORMAT_VERSION_KEY))
            .require("record store_format_version")?;
        CBFuture::new(&request, "success", "error").await.require("await store_format_version put")?;
        CBFuture::new(&transaction, "complete", "error").await.require("complete meta write transaction")?;
        Ok(())
    })
    .await
}

impl Connection {
    pub fn is_stale(&self) -> bool { self.stale.load(Ordering::SeqCst) }

    pub fn version(&self) -> u32 { self.db.version() as u32 }

    /// Check whether the model-materialization store already has an index with
    /// the given name.
    pub fn has_index(&self, index_name: &str) -> Result<bool, RetrievalError> {
        let tx =
            self.db.transaction_with_str_and_mode("materializations", web_sys::IdbTransactionMode::Readonly).require("get transaction")?;
        let store = tx.object_store("materializations").require("get object store")?;
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
                db.create_object_store("entities").require("create entities store")?;

                let materializations = db.create_object_store("materializations").require("create materializations store")?;
                let key_path = js_sys::Array::of2(&"__materialization".into(), &"id".into());
                materializations
                    .create_index_with_str_sequence("__materialization__id", &key_path)
                    .require("create materialization index")?;

                let events_store = db.create_object_store("events").require("create events store")?;
                events_store.create_index_with_str("by_entity_id", "__entity_id").require("create entity_id index")?;
                db.create_object_store("entity_models").require("create entity-model association store")?;

                // Engine-owned durable id-to-field map. Out-of-line string keys
                // `{serialized model id}\0{serialized property id}` -> assigned
                // field name.
                // Created here alongside the canonical and materialization
                // stores. This unreleased layout requires no migration or
                // database-version bump.
                db.create_object_store("property_columns").require("create property_columns store")?;
                db.create_object_store("model_registrations").require("create model registrations store")?;

                // Store format record: `_ankurah_meta` carries
                // `store_format_version` = STORE_FORMAT_VERSION, written inside the
                // creation upgrade transaction so a fresh database is recorded
                // atomically with its stores; `Database::open` checks the record
                // on every open.
                let meta_store = db.create_object_store("_ankurah_meta").require("create meta store")?;
                meta_store
                    .put_with_key(&JsValue::from(STORE_FORMAT_VERSION), &JsValue::from_str(STORE_FORMAT_VERSION_KEY))
                    .require("record store_format_version")?;
            }
            Ok(())
        })
        .await
    }

    /// Open the database at a bumped version, creating the empty
    /// `_ankurah_meta` store. This is the upgrade path for a database created
    /// before the store existed. It is only reachable when the database holds
    /// no ankurah data: a data-bearing store without a record is refused
    /// before this runs, and the caller records the version immediately after.
    pub async fn open_with_meta_store(db_name: &str, version: u32) -> Result<Self, RetrievalError> {
        notice_info!("creating meta store for {db_name}");
        Self::new(db_name, Some(version), move |event: IdbVersionChangeEvent| -> Result<(), RetrievalError> {
            let open_request: IdbOpenDbRequest = event.target().require("get event target")?.unchecked_into();
            let transaction = open_request.transaction().require("get upgrade transaction")?;
            if transaction.object_store("_ankurah_meta").is_err() {
                transaction.db().create_object_store("_ankurah_meta").require("create meta store")?;
            }
            Ok(())
        })
        .await
    }

    /// Open database connection with a specific index to be created
    pub async fn open_with_index(db_name: &str, version: u32, index_spec: KeySpec<String>) -> Result<Self, RetrievalError> {
        let index_name = index_spec.name_with("", "__");
        notice_info!("creating index {db_name}.materializations.{index_name} -> {:?}", index_spec.keyparts);

        Self::new(db_name, Some(version), move |event: IdbVersionChangeEvent| -> Result<(), RetrievalError> {
            let open_request: IdbOpenDbRequest = event.target().require("get event target")?.unchecked_into();
            let transaction = open_request.transaction().require("get upgrade transaction")?;
            let store = transaction.object_store("materializations").require("get materializations store during upgrade")?;
            // Use full_path() to support JSON sub-paths (e.g., "context.session_id")
            let key_path: Vec<JsValue> = index_spec.keyparts.iter().map(|kp| kp.full_path().into()).collect();
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
        Ok(Self { db: SendWrapper::new(db), _onversionchange: Some(SendWrapper::new(onversionchange)), stale })
    }
}
