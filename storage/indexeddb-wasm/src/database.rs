use ankurah_core::indexing::KeySpec;
use ankurah_core::{error::RetrievalError, notice_info, util::safeset::SafeSet};
use anyhow::{anyhow, Result};
use send_wrapper::SendWrapper;
use std::sync::Arc;
use wasm_bindgen::{prelude::*, JsCast};
use web_sys::{window, IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbVersionChangeEvent};

use crate::{cb_future::CBFuture, cb_race::CBRace, require::Require};

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

    /// Get a clone of the current database connection
    pub async fn get_connection(&self) -> SendWrapper<IdbDatabase> { self.0.connection.lock().await.db.clone() }

    /// Close the database connection
    pub async fn close(&self) { self.0.connection.lock().await.close(); }

    /// Ensure an index exists, creating it if necessary via database version upgrade
    pub async fn assure_index_exists(&self, index_spec: &KeySpec) -> Result<(), RetrievalError> {
        let name = index_spec.name_with("", "__");

        // Check cache first
        if self.0.index_cache.contains(&name) {
            return Ok(());
        }

        let mut connection_guard = self.0.connection.lock().await;

        // Check if index already exists in database
        if connection_guard
            .db
            .transaction_with_str_and_mode("entities", web_sys::IdbTransactionMode::Readonly)
            .require("get transaction")?
            .object_store("entities")
            .require("get object store")?
            .index_names()
            .contains(name.as_str())
        {
            self.0.index_cache.insert(name);
            return Ok(());
        }

        // Index doesn't exist, need to create it via version upgrade
        let current_version = connection_guard.db.version() as u32;
        connection_guard.db.close();

        let new_connection = Connection::open_with_index(&self.0.db_name, current_version + 1, index_spec.clone()).await?;
        *connection_guard = new_connection;

        // Add to cache
        self.0.index_cache.insert(name);

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
    /// Close the database connection
    pub fn close(&self) { self.db.close(); }

    /// Open or create a new database connection with default schema
    pub async fn open(db_name: &str) -> Result<Self, RetrievalError> {
        notice_info!("Database.open({})", db_name);
        if db_name.is_empty() {
            return Err(anyhow!("Database name cannot be empty").into());
        }

        let open_request = SendWrapper::new({
            let window = window().require("get window")?;
            let idb: IdbFactory = window.indexed_db().require("get indexeddb")?;
            idb.open(db_name).require("open database")? // open the current version of the database
        });

        let onupgradeneeded = move |event: IdbVersionChangeEvent| -> Result<(), RetrievalError> {
            let open_request: IdbOpenDbRequest = event.target().require("get event target")?.unchecked_into();
            let transaction = open_request.transaction().require("get upgrade transaction")?;

            // Create entities store if it doesn't exist
            if let Err(_) = transaction.object_store("entities") {
                // TODO check if this is the error type we are expecting
                let db: IdbDatabase = transaction.db();
                let store = db.create_object_store("entities").require("create entities store")?;

                let key_path = js_sys::Array::of2(&"__collection".into(), &"id".into());
                store.create_index_with_str_sequence("__collection__id", &key_path).require("create collection index")?;

                let events_store = db.create_object_store("events").require("create events store")?;
                events_store.create_index_with_str("by_entity_id", "__entity_id").require("create entity_id index")?;
            }
            Ok(())
        };

        let race = CBRace::new();
        let closure = SendWrapper::new(race.wrap(onupgradeneeded));
        open_request.set_onupgradeneeded(Some(closure.as_ref().unchecked_ref()));

        CBFuture::new(&*open_request, "success", "error").await.require("IndexedDB open failed")?;

        race.take_err()??; // if the callback was called and returned an error, bail out with that error

        // Get the database from the request result
        let db = open_request.result().require("get database result")?.unchecked_into::<IdbDatabase>();
        Ok(Self { db: SendWrapper::new(db) })
    }

    /// Open database connection with a specific index to be created
    pub async fn open_with_index(db_name: &str, version: u32, index_spec: KeySpec) -> Result<Self, RetrievalError> {
        let index_name = index_spec.name_with("", "__");

        let open_request = SendWrapper::new({
            let window = window().require("get window")?;
            let idb: IdbFactory = window.indexed_db().require("get indexeddb")?;
            idb.open_with_u32(db_name, version).require("open database")?
        });

        let onupgradeneeded = move |event: IdbVersionChangeEvent| -> Result<(), RetrievalError> {
            let open_request: IdbOpenDbRequest = event.target().require("get event target")?.unchecked_into();
            let transaction = open_request.transaction().require("get upgrade transaction")?;
            let store = transaction.object_store("entities").require("get entities store during upgrade")?;
            let key_path: Vec<JsValue> = index_spec.keyparts.iter().map(|kp| (&kp.column).into()).collect();
            store.create_index_with_str_sequence(&index_name, &key_path.into()).require("create index")?;
            Ok(())
        };

        let race = CBRace::new();
        let closure = SendWrapper::new(race.wrap(onupgradeneeded));
        open_request.set_onupgradeneeded(Some(closure.as_ref().unchecked_ref()));

        CBFuture::new(&*open_request, "success", "error").await.require("IndexedDB open failed")?;

        race.take_err()??;
        // Get the database from the request result
        let db = open_request.result().require("get database result")?.unchecked_into::<IdbDatabase>();
        // don't need to store callbacks, because they should be mutually exclusive
        Ok(Self { db: SendWrapper::new(db) })
    }
}
