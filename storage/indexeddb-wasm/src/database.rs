use ankurah_core::{error::RetrievalError, notice_info, util::safeset::SafeSet};
use ankurah_storage_common::index_spec::KeySpec;
use anyhow::Result;
use js_sys::Function;
use send_wrapper::SendWrapper;
use std::{any::Any, sync::Arc};
use wasm_bindgen::{prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use web_sys::{IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest, IdbVersionChangeEvent};

use crate::cb_future::CBFuture;

#[derive(Debug, Clone)]
pub struct Database(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    connection: Arc<tokio::sync::Mutex<Connection>>,
    db_name: String,
    _callbacks: SendWrapper<Vec<Box<dyn Any>>>,
    /// Cache of existing index names to avoid repeated checks
    index_cache: SafeSet<String>,
}

#[derive(Debug)]
struct Connection {
    db: SendWrapper<IdbDatabase>,
    _callbacks: SendWrapper<Vec<Box<dyn Any>>>,
}

impl Database {
    pub async fn open(db_name: &str) -> anyhow::Result<Self> {
        let connection = Connection::open(db_name, 1).await?;

        Ok(Self(Arc::new(Inner {
            connection: Arc::new(tokio::sync::Mutex::new(connection)),
            db_name: db_name.to_string(),
            _callbacks: SendWrapper::new(Vec::new()), // Callbacks are now stored in Connection
            index_cache: SafeSet::new(),
        })))
    }

    /// Get a clone of the current database connection
    pub async fn get_connection(&self) -> SendWrapper<IdbDatabase> { self.0.connection.lock().await.db.clone() }

    /// Ensure an index exists, creating it if necessary via database version upgrade
    pub async fn assure_index_exists(&self, index_spec: &KeySpec) -> Result<(), RetrievalError> {
        let name = index_spec.name_with("", "__");
        tracing::info!("assure_index_exists: Starting for index {}", &name);

        // Check cache first
        if self.0.index_cache.contains(&name) {
            tracing::debug!("assure_index_exists: Index {} found in cache, returning", &name);
            return Ok(());
        }

        let mut connection_guard = self.0.connection.lock().await;
        tracing::info!("assure_index_exists: Acquired connection lock");

        // Double-check cache after acquiring lock (in case another thread added it)
        if self.0.index_cache.contains(&name) {
            tracing::debug!("assure_index_exists: Index {} found in cache after lock, returning", &name);
            return Ok(());
        }

        // Check if index already exists in database
        tracing::info!("assure_index_exists: Checking if index exists in database");
        if self.index_exists(&connection_guard.db, &name)? {
            tracing::info!("assure_index_exists: Index already exists in database, adding to cache");
            // Add to cache
            self.0.index_cache.insert(name);
            return Ok(());
        }

        tracing::info!("Creating index: {} for keyparts: {:?}", &name, index_spec.keyparts);

        // Get current version before closing
        let current_version = connection_guard.db.version() as u32;
        tracing::info!("assure_index_exists: Current version: {}", current_version);

        // Close current connection
        tracing::info!("assure_index_exists: Closing current connection");
        connection_guard.db.close();

        // Create new connection with index
        tracing::info!("assure_index_exists: Creating new connection with version {}", current_version + 1);
        let new_connection = Connection::open_with_index(&self.0.db_name, current_version + 1, index_spec).await?;
        tracing::info!("assure_index_exists: New connection created successfully");

        // Replace the entire connection
        tracing::info!("assure_index_exists: Replacing connection");
        *connection_guard = new_connection;

        tracing::info!("Successfully created index: {}", &name);

        // Add to cache
        self.0.index_cache.insert(name);

        Ok(())
    }

    /// Check if an index exists in the entities object store
    fn index_exists(&self, db: &IdbDatabase, index_name: &str) -> Result<bool, RetrievalError> {
        // Check if index exists by trying to access the index directly
        // If the object store doesn't exist, the index definitely doesn't exist
        let transaction = match db.transaction_with_str_and_mode("albums", web_sys::IdbTransactionMode::Readonly) {
            Ok(tx) => tx,
            Err(_) => {
                // Object store doesn't exist, so index doesn't exist either
                tracing::debug!("Object store 'albums' doesn't exist, so index {} doesn't exist", index_name);
                return Ok(false);
            }
        };

        let store = match transaction.object_store("albums") {
            Ok(store) => store,
            Err(_) => {
                // Object store doesn't exist, so index doesn't exist either
                tracing::debug!("Failed to get object store 'albums', so index {} doesn't exist", index_name);
                return Ok(false);
            }
        };

        // Try to access the index - if it exists, this will succeed; if not, it will throw
        match store.index(index_name) {
            Ok(_) => {
                tracing::debug!("Index {} exists", index_name);
                Ok(true)
            }
            Err(_) => {
                tracing::debug!("Index {} doesn't exist", index_name);
                Ok(false)
            }
        }
    }

    /// Get database name
    pub fn name(&self) -> &str { &self.0.db_name }

    /// Cleanup database (delete it entirely)
    pub async fn cleanup(db_name: &str) -> anyhow::Result<()> {
        let window = web_sys::window().ok_or_else(|| anyhow::anyhow!("No window found"))?;
        let idb = window
            .indexed_db()
            .map_err(|e| anyhow::anyhow!("IndexedDB error: {:?}", e))?
            .ok_or_else(|| anyhow::anyhow!("IndexedDB not available"))?;

        let delete_request = idb.delete_database(db_name).map_err(|e| anyhow::anyhow!("Failed to delete database: {:?}", e))?;

        let future = CBFuture::new(&delete_request, &["success", "blocked"], "error");
        future.await.map_err(|e| anyhow::anyhow!("Failed to delete database: {:?}", e))?;

        Ok(())
    }
}

impl Connection {
    /// Open or create a new database connection with default schema
    pub async fn open(db_name: &str, _version: u32) -> anyhow::Result<Self> {
        notice_info!("Database.open({})", db_name);

        // Validate database name
        if db_name.is_empty() {
            return Err(anyhow::anyhow!("Database name cannot be empty"));
        }

        let window = web_sys::window().ok_or_else(|| anyhow::anyhow!("No window found"))?;
        let idb: IdbFactory = window
            .indexed_db()
            .map_err(|e| anyhow::anyhow!("IndexedDB error: {:?}", e))?
            .ok_or_else(|| anyhow::anyhow!("IndexedDB not available"))?;

        let open_request: IdbOpenDbRequest = idb.open_with_u32(db_name, 1).map_err(|e| anyhow::anyhow!("Failed to open DB: {:?}", e))?;

        let mut callbacks: Vec<Box<dyn Any>> = Vec::new();
        let promise = js_sys::Promise::new(&mut |resolve: Function, reject: Function| {
            let onupgradeneeded = Closure::wrap(Box::new(move |event: IdbVersionChangeEvent| {
                let target: IdbRequest = event.target().unwrap().unchecked_into();
                let db: SendWrapper<IdbDatabase> = SendWrapper::new(target.result().unwrap().unchecked_into());

                // Create entities store
                let store = match db.create_object_store("entities") {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::warn!("Error creating store (may already exist): {:?}", e);
                        return;
                    }
                };

                // Create default indexes
                // Default index on collection + id (IndexedDB naming: __collection__id)
                if let Err(e) =
                    store.create_index_with_str_sequence("__collection__id", &js_sys::Array::of2(&"__collection".into(), &"id".into()))
                {
                    tracing::error!("Failed to create collection+id index: {:?}", e);
                }

                // Create events store
                let events_store = match db.create_object_store("events") {
                    Ok(store) => store,
                    Err(e) => {
                        tracing::warn!("Error creating events store (may already exist): {:?}", e);
                        return;
                    }
                };

                // Create index on entity_id field for efficient event lookups
                if let Err(e) = events_store.create_index_with_str("by_entity_id", "__entity_id") {
                    tracing::error!("Failed to create entity_id index: {:?}", e);
                }
            }) as Box<dyn FnMut(_)>);

            let onsuccess = Closure::wrap(Box::new(move |event: web_sys::Event| {
                let target: IdbRequest = event.target().unwrap().unchecked_into();
                let db: IdbDatabase = target.result().unwrap().unchecked_into();
                resolve.call1(&JsValue::NULL, &JsValue::from(db)).unwrap();
            }) as Box<dyn FnMut(_)>);

            let onerror = Closure::wrap(Box::new(move |event: web_sys::Event| {
                let target: IdbRequest = event.target().unwrap().unchecked_into();
                let error = target.error().unwrap();
                reject.call1(&JsValue::NULL, &error.into()).unwrap();
            }) as Box<dyn FnMut(_)>);

            open_request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));
            open_request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
            open_request.set_onerror(Some(onerror.as_ref().unchecked_ref()));

            // Keep closures alive
            callbacks.push(Box::new(onupgradeneeded));
            callbacks.push(Box::new(onsuccess));
            callbacks.push(Box::new(onerror));
        });

        let db = SendWrapper::new(
            JsFuture::from(promise).await.map_err(|e| anyhow::anyhow!("Failed to open database: {:?}", e))?.unchecked_into::<IdbDatabase>(),
        );

        Ok(Self { db, _callbacks: SendWrapper::new(callbacks) })
    }

    /// Open database connection with a specific index to be created
    pub async fn open_with_index(db_name: &str, version: u32, index_spec: &KeySpec) -> Result<Self, RetrievalError> {
        tracing::info!("Connection::open_with_index: Starting for {} v{}", db_name, version);
        let index_spec_clone = index_spec.clone();

        SendWrapper::new(async move {
            tracing::info!("Connection::open_with_index: Inside SendWrapper");
            let window = web_sys::window().ok_or_else(|| RetrievalError::StorageError(anyhow::anyhow!("No window found").into()))?;

            let idb: IdbFactory = window
                .indexed_db()
                .map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("IndexedDB error: {:?}", e).into()))?
                .ok_or_else(|| RetrievalError::StorageError(anyhow::anyhow!("IndexedDB not available").into()))?;

            let open_request: IdbOpenDbRequest = idb
                .open_with_u32(db_name, version)
                .map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("Failed to open DB: {:?}", e).into()))?;

            tracing::info!("Connection::open_with_index: Created open request");

            let mut callbacks: Vec<Box<dyn Any>> = Vec::new();
            let promise = js_sys::Promise::new(&mut |resolve: Function, reject: Function| {
                tracing::info!("Connection::open_with_index: Setting up promise callbacks");
                let index_spec_for_closure = index_spec_clone.clone();
                // Set up upgrade handler
                let onupgradeneeded = Closure::wrap(Box::new(move |event: IdbVersionChangeEvent| {
                    tracing::info!("Connection::open_with_index: Upgrade needed event triggered");
                    let target: IdbRequest = event.target().unwrap().unchecked_into();
                    let _db: IdbDatabase = target.result().unwrap().unchecked_into();

                    // During upgrade, get the upgrade transaction
                    let open_request: IdbOpenDbRequest = target.unchecked_into();
                    let transaction = match open_request.transaction() {
                        Some(tx) => tx,
                        None => {
                            tracing::error!("Failed to get upgrade transaction");
                            return;
                        }
                    };

                    let store = match transaction.object_store("entities") {
                        Ok(store) => store,
                        Err(e) => {
                            tracing::error!("Failed to get entities store during upgrade: {:?}", e);
                            return;
                        }
                    };

                    // Create the index
                    let key_path: js_sys::Array = js_sys::Array::new();
                    for field in &index_spec_for_closure.keyparts {
                        key_path.push(&JsValue::from_str(&field.column));
                    }

                    tracing::info!("Connection::open_with_index: Creating index with key_path");
                    match store.create_index_with_str_sequence(&index_spec_for_closure.name_with("", "__"), &key_path) {
                        Ok(_) => {
                            tracing::info!("Successfully created index in upgrade handler: {}", index_spec_for_closure.name_with("", "__"));
                        }
                        Err(e) => {
                            // Check if it's a "ConstraintError" indicating the index already exists
                            let error_string = format!("{:?}", e);
                            if error_string.contains("ConstraintError") && error_string.contains("already exists") {
                                tracing::debug!("Index {} already exists, skipping creation", index_spec_for_closure.name_with("", "__"));
                            } else {
                                tracing::error!("Failed to create index {}: {:?}", index_spec_for_closure.name_with("", "__"), e);
                            }
                        }
                    }
                }) as Box<dyn FnMut(_)>);

                let onsuccess = Closure::wrap(Box::new(move |event: web_sys::Event| {
                    tracing::info!("Connection::open_with_index: Success event triggered");
                    let target: IdbRequest = event.target().unwrap().unchecked_into();
                    let db: IdbDatabase = target.result().unwrap().unchecked_into();
                    resolve.call1(&JsValue::NULL, &JsValue::from(db)).unwrap();
                }) as Box<dyn FnMut(_)>);

                let onerror = Closure::wrap(Box::new(move |event: web_sys::Event| {
                    tracing::error!("Connection::open_with_index: Error event triggered");
                    let target: IdbRequest = event.target().unwrap().unchecked_into();
                    let error = target.error().unwrap();
                    reject.call1(&JsValue::NULL, &error.into()).unwrap();
                }) as Box<dyn FnMut(_)>);

                tracing::info!("Connection::open_with_index: Setting event handlers");
                open_request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));
                open_request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
                open_request.set_onerror(Some(onerror.as_ref().unchecked_ref()));
                tracing::info!("Connection::open_with_index: Event handlers set");

                // Keep closures alive
                callbacks.push(Box::new(onupgradeneeded));
                callbacks.push(Box::new(onsuccess));
                callbacks.push(Box::new(onerror));
            });

            tracing::info!("Connection::open_with_index: Waiting for promise to resolve");
            let db = SendWrapper::new(
                JsFuture::from(promise)
                    .await
                    .map_err(|e| RetrievalError::StorageError(anyhow::anyhow!("Database upgrade failed: {:?}", e).into()))?
                    .unchecked_into::<IdbDatabase>(),
            );
            tracing::info!("Connection::open_with_index: Promise resolved successfully");

            Ok(Self { db, _callbacks: SendWrapper::new(callbacks) })
        })
        .await
    }
}
