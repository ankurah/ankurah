use crate::util::navigator_lock::NavigatorLock;
use ankurah_core::indexing::KeySpec;
use ankurah_core::{error::RetrievalError, notice_info, util::safeset::SafeSet};
use ankurah_proto::PROTOCOL_VERSION;
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

/// Key of the storage protocol epoch stamp in the `meta` object store.
const PROTOCOL_VERSION_KEY: &str = "protocol_version";

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
    pub async fn open(db_name: &str) -> Result<Self, RetrievalError> {
        let connection = Connection::open(db_name).await?;

        let database = Self(Arc::new(Inner {
            connection: Arc::new(tokio::sync::Mutex::new(connection)),
            db_name: db_name.to_string(),
            // _callbacks: SendWrapper::new(Vec::new()), // Callbacks are now stored in Connection
            index_cache: SafeSet::new(),
        }));
        database.check_protocol_version().await?;
        Ok(database)
    }

    /// Check the storage protocol epoch stamp (`meta` store, key
    /// `protocol_version`) against [`PROTOCOL_VERSION`], once per open:
    /// - no stamp and no existing ankurah data: write the stamp and proceed
    ///   (a fresh database created by [`Connection::open`] was already stamped
    ///   inside its creation upgrade; this covers databases whose creation was
    ///   interrupted before the stamp landed);
    /// - stamp present and equal: proceed;
    /// - stamp present and different: refuse;
    /// - existing ankurah data but no stamp: refuse (a store from before the
    ///   stamp existed).
    ///
    /// The refusal paths write nothing.
    async fn check_protocol_version(&self) -> Result<(), RetrievalError> {
        let mut connection = self.0.connection.lock().await;

        // A database from before the `meta` store existed carries no stamp at
        // all: with ankurah data present that is a pre-v3 store (refuse before
        // creating anything); an empty one gets the store via a versioned
        // upgrade and falls through to be stamped below.
        if !connection.db.object_store_names().contains("meta") {
            if has_ankurah_data(&connection.db).await? {
                return Err(self.protocol_version_error(None));
            }
            let current_version = connection.version();
            connection.close();
            *connection = Connection::open_with_meta_store(&self.0.db_name, current_version + 1).await?;
        }

        let db = connection.db.clone();
        SendWrapper::new(async move {
            let transaction = db.transaction_with_str("meta").require("create meta transaction")?;
            let store = transaction.object_store("meta").require("get meta store")?;
            let request = store.get(&JsValue::from_str(PROTOCOL_VERSION_KEY)).require("get protocol_version")?;
            CBFuture::new(&request, "success", "error").await.require("await protocol_version get")?;
            let found = request.result().require("protocol_version result")?;

            if found.is_undefined() || found.is_null() {
                // No stamp. Existing ankurah data makes this a pre-v3 store;
                // an empty database is stamped here and proceeds.
                if has_ankurah_data(&db).await? {
                    return Err(self.protocol_version_error(None));
                }
                write_protocol_stamp(&db).await?;
                return Ok(());
            }

            match found.as_f64() {
                Some(version) if version == f64::from(PROTOCOL_VERSION) => Ok(()),
                _ => Err(self.protocol_version_error(Some(found))),
            }
        })
        .await
    }

    /// The refusal for a protocol-version mismatch. `found` is the raw stored
    /// stamp; `None` means the database carries ankurah data but no stamp at
    /// all (a store from before protocol v3).
    fn protocol_version_error(&self, found: Option<JsValue>) -> RetrievalError {
        let found = match found {
            None => "no protocol version stamp (a store from before protocol v3)".to_string(),
            Some(value) => match value.as_f64() {
                Some(version) => format!("protocol version {}", version),
                None => format!("an unreadable protocol version stamp ({:?})", value),
            },
        };
        RetrievalError::StorageError(
            format!(
                "IndexedDB database {:?} carries {}, but this build requires protocol version {}; reset your dev database (delete the IndexedDB database, e.g. via IndexedDBStorageEngine::cleanup) to proceed",
                self.0.db_name, found, PROTOCOL_VERSION
            )
            .into(),
        )
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

/// Whether any ankurah object store (beyond `meta`) already holds content:
/// the engine-natural detection of an existing database, distinguishing a
/// fresh store (stamp and proceed) from one predating the protocol stamp
/// (refuse).
async fn has_ankurah_data(db: &IdbDatabase) -> Result<bool, RetrievalError> {
    let db = SendWrapper::new(db);
    SendWrapper::new(async move {
        for store_name in ["entities", "events", "property_columns"] {
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

/// Write the [`PROTOCOL_VERSION`] stamp into the `meta` store. Only ever
/// called on a database with no stamp and no ankurah data; fresh databases
/// are stamped inside their creation upgrade transaction instead.
async fn write_protocol_stamp(db: &IdbDatabase) -> Result<(), RetrievalError> {
    let db = SendWrapper::new(db);
    SendWrapper::new(async move {
        let transaction =
            db.transaction_with_str_and_mode("meta", web_sys::IdbTransactionMode::Readwrite).require("create meta write transaction")?;
        let store = transaction.object_store("meta").require("get meta store")?;
        let request = store
            .put_with_key(&JsValue::from(PROTOCOL_VERSION), &JsValue::from_str(PROTOCOL_VERSION_KEY))
            .require("stamp protocol_version")?;
        CBFuture::new(&request, "success", "error").await.require("await protocol_version put")?;
        CBFuture::new(&transaction, "complete", "error").await.require("complete meta write transaction")?;
        Ok(())
    })
    .await
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

                // Engine-owned durable id-to-field map. Out-of-line string keys
                // `{collection}\0{property id base64}` -> assigned field name.
                // Created here alongside `entities` and `events` on the fresh
                // database; the clean-start assumption is ratified, so no
                // versioned migration (and thus no DB version bump) is needed
                // for pre-existing databases.
                db.create_object_store("property_columns").require("create property_columns store")?;

                // Storage protocol epoch stamp: `meta` carries
                // `protocol_version` = PROTOCOL_VERSION, written inside the
                // creation upgrade transaction so a fresh database is stamped
                // atomically with its stores; `Database::open` checks the stamp
                // on every open.
                let meta_store = db.create_object_store("meta").require("create meta store")?;
                meta_store
                    .put_with_key(&JsValue::from(PROTOCOL_VERSION), &JsValue::from_str(PROTOCOL_VERSION_KEY))
                    .require("stamp protocol_version")?;
            }
            Ok(())
        })
        .await
    }

    /// Open the database at a bumped version, creating the (empty) `meta`
    /// store: the upgrade path for a database created before the store
    /// existed. Only reachable when the database holds no ankurah data -- a
    /// data-bearing store without a stamp is refused before this runs -- and
    /// the caller stamps `meta` right after.
    pub async fn open_with_meta_store(db_name: &str, version: u32) -> Result<Self, RetrievalError> {
        notice_info!("creating meta store for {db_name}");
        Self::new(db_name, Some(version), move |event: IdbVersionChangeEvent| -> Result<(), RetrievalError> {
            let open_request: IdbOpenDbRequest = event.target().require("get event target")?.unchecked_into();
            let transaction = open_request.transaction().require("get upgrade transaction")?;
            if transaction.object_store("meta").is_err() {
                transaction.db().create_object_store("meta").require("create meta store")?;
            }
            Ok(())
        })
        .await
    }

    /// Open database connection with a specific index to be created
    pub async fn open_with_index(db_name: &str, version: u32, index_spec: KeySpec<String>) -> Result<Self, RetrievalError> {
        let index_name = index_spec.name_with("", "__");
        notice_info!("creating index {db_name}.entities.{index_name} -> {:?}", index_spec.keyparts);

        Self::new(db_name, Some(version), move |event: IdbVersionChangeEvent| -> Result<(), RetrievalError> {
            let open_request: IdbOpenDbRequest = event.target().require("get event target")?.unchecked_into();
            let transaction = open_request.transaction().require("get upgrade transaction")?;
            let store = transaction.object_store("entities").require("get entities store during upgrade")?;
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
