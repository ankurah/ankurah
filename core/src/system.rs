use ankurah_proto::{self as proto, Clock, CollectionId};
use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, RwLock};
use tokio::sync::Notify;
use tracing::error;

use crate::collectionset::CollectionSet;
use crate::entity::{Entity, WeakEntitySet};
use crate::error::RetrievalError;
use crate::property::{backend::LWWBackend, PropertyValue};
use crate::property::{Property, PropertyError};
use crate::storage::{StorageCollectionWrapper, StorageEngine};

pub const SYSTEM_COLLECTION_ID: &str = "_ankurah_system";
pub const PROTECTED_COLLECTIONS: &[&str] = &[SYSTEM_COLLECTION_ID];

/// System catalog manager for storing various metadata about the system
/// * root clock
/// * valid collections (TODO)
/// * property definitions (TODO)

pub struct SystemManager<SE>(Arc<Inner<SE>>);
impl<SE> Clone for SystemManager<SE> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct Inner<SE> {
    collectionset: CollectionSet<SE>,
    collection_map: RwLock<BTreeMap<CollectionId, Entity>>,
    entities: WeakEntitySet,
    durable: bool,
    root: RwLock<Option<Clock>>,
    items: RwLock<Vec<Entity>>,
    loaded: OnceLock<()>,
    loading: Notify,
}

impl<SE> SystemManager<SE>
where SE: StorageEngine + Send + Sync + 'static
{
    pub(crate) fn new(collections: CollectionSet<SE>, entities: WeakEntitySet, durable: bool) -> Self {
        let me = Self(Arc::new(Inner {
            collectionset: collections,
            entities,
            durable,
            items: RwLock::new(Vec::new()),
            root: RwLock::new(None),
            loaded: OnceLock::new(),
            loading: Notify::new(),
            collection_map: RwLock::new(BTreeMap::new()),
        }));
        {
            let me = me.clone();
            crate::task::spawn(async move {
                if let Err(e) = me.load_system_catalog().await {
                    error!("Failed to load system catalog: {}", e);
                }
            });
        }
        me
    }

    pub fn root(&self) -> Option<Clock> { self.0.root.read().unwrap().clone() }
    pub fn items(&self) -> Vec<Entity> { self.0.items.read().unwrap().clone() }

    /// get an existing collection if it's defined in the system catalog, else insert a SysItem::Collection
    /// then return collections.get to get the StorageCollectionWrapper
    pub async fn collection(&self, id: &CollectionId) -> Result<StorageCollectionWrapper, RetrievalError> {
        self.wait_loaded().await;

        // // Check if collection exists in system catalog
        // let collection_exists = {
        //     let state = self.0.state.lock().unwrap();
        //     state.items.iter().any(|item| if let proto::sys::Item::Collection { name } = item { name == id.as_str() } else { false })
        // };

        // // If collection doesn't exist, create it in system catalog
        // if !collection_exists {
        //     let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        //     let storage = self.0.collectionset.get(&collection_id).await?;
        //     let entity = self.0.entities.create(collection_id.clone());

        //     // Initialize the LWW backend and set the Collection value
        //     let lww_backend = Arc::new(LWWBackend::new());
        //     lww_backend.set(
        //         item.into(),
        //         Some(PropertyValue::Object(bincode::serialize(&proto::sys::Item::Collection { name: id.as_str().to_string() })?)),
        //     );

        //     // Create an event for the collection
        //     let event = Event {
        //         id: ID::new(),
        //         entity_id: entity.id.clone(),
        //         collection: collection_id,
        //         operations: BTreeMap::from([("lww".into(), lww_backend.to_operations()?)]),
        //         parent: Clock::default(),
        //     };

        //     entity.apply_event(&event)?;
        //     storage.add_event(&event).await?;
        //     let state =
        //         State { state_buffers: BTreeMap::from([("lww".into(), lww_backend.to_state_buffer()?)]), head: Clock::new([event.id]) };
        //     storage.set_state(entity.id.clone(), &state).await?;

        //     // Update our system state
        //     let mut state = self.0.state.lock().unwrap();
        //     state.items.push(proto::sys::Item::Collection { name: id.as_str().to_string() });
        // }

        // Return the collection wrapper
        self.0.collectionset.get(id).await
    }

    /// Creates a new system root. This should only be called once per system.
    /// Returns an error if a system root already exists.
    pub async fn initialize(&self) -> Result<()> {
        if !self.0.durable {
            return Err(anyhow!("Only durable nodes can create a new system"));
        }

        // Wait for local system catalog to be loaded
        self.wait_loaded().await;

        {
            let items = self.0.items.read().unwrap();
            if !items.is_empty() {
                return Err(anyhow!("System root already exists"));
            }
        }

        // TODO - see if we can use the Model derive macro for a SysCatalogItem model rather than doing this manually
        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;

        let system_entity = self.0.entities.create(collection_id.clone());

        let lww_backend = system_entity.backends().get::<LWWBackend>().expect("LWW Backend should exist");
        lww_backend.set("item".into(), proto::sys::Item::SysRoot.into_value()?);

        let event = system_entity.commit()?.ok_or(anyhow!("Expected event"))?;
        system_entity.apply_event(&storage, &event).await?;

        let root = Clock::new([event.id()]);

        storage.add_event(&event.into()).await?;
        storage.set_state(system_entity.id.clone(), &system_entity.to_state()?).await?;

        // Update our system state
        let mut items = self.0.items.write().unwrap();
        items.push(system_entity);
        *self.0.root.write().unwrap() = Some(root);

        Ok(())
    }

    /// Joins an existing system. This should only be called by ephemeral nodes.
    pub async fn join_system(&self, clock: Clock) -> Result<()> {
        // If node is durable, fail - durable nodes should not join an existing system
        // if self.0.durable {
        //     return Err(anyhow!("Durable nodes cannot join an existing system"));
        // }

        // // Wait for system catalog to be loaded
        // self.0.loading.notified().await;

        // let collection_id = CollectionId::from(SYSTEM_COLLECTION_ID);
        // let storage = self.0.collectionset.get(&collection_id).await?;

        // let entity = self.0.entities.create(collection_id.clone());

        // // Initialize the LWW backend and set the SysRoot value with the provided clock
        // let lww_backend = Arc::new(LWWBackend::new());
        // let clock = clock.clone(); // Clone before serializing
        // lww_backend.set(item.into(), Some(PropertyValue::Object(bincode::serialize(&proto::sys::Item::SysRoot(clock.clone()))?)));

        // // Create an event for joining the system
        // let event = Event {
        //     id: ID::new(),
        //     entity_id: entity.id.clone(),
        //     collection: collection_id,
        //     operations: BTreeMap::from([("lww".into(), lww_backend.to_operations()?)]),
        //     parent: clock.clone(),
        // };

        // entity.apply_event(&event)?;
        // storage.add_event(&event).await?;
        // let state = State { state_buffers: BTreeMap::from([("lww".into(), lww_backend.to_state_buffer()?)]), head: Clock::new([event.id]) };
        // storage.set_state(entity.id.clone(), &state).await?;

        // // Update our system state
        // let mut state = self.0.state.lock().unwrap();
        // state.items.push(proto::sys::Item::SysRoot(clock.clone()));
        // state.root = Some(clock);

        // Ok(())
        unimplemented!()
    }

    /// Returns true if the local system catalog is loaded
    pub fn is_loaded(&self) -> bool { self.0.loaded.get().is_some() }

    /// Waits for the local system catalog to be loaded
    pub async fn wait_loaded(&self) {
        if !self.is_loaded() {
            self.0.loading.notified().await;
        }
    }

    async fn load_system_catalog(&self) -> Result<()> {
        if self.is_loaded() {
            return Err(anyhow!("System catalog already loaded"));
        }

        let collection_id = CollectionId::fixed_name(SYSTEM_COLLECTION_ID);
        let storage = self.0.collectionset.get(&collection_id).await?;

        let mut entities = Vec::new();
        let mut root_clock = None;

        for (id, state) in storage.fetch_states(&ankql::ast::Predicate::True).await? {
            let entity = self.0.entities.with_state(id, collection_id.clone(), state)?;
            let lww_backend = entity.backends().get::<LWWBackend>().expect("LWW Backend should exist");
            if let Some(value) = lww_backend.get(&"item".to_string()) {
                let item = proto::sys::Item::from_value(Some(value)).expect("Invalid sys item");

                if let proto::sys::Item::SysRoot = &item {
                    root_clock = Some(entity.head());
                }
                entities.push(entity);
            }
        }

        // Update our system state
        {
            let mut items = self.0.items.write().unwrap();
            items.extend(entities);
        }
        {
            let mut root = self.0.root.write().expect("Root lock poisoned");
            *root = root_clock;
        }

        // Set loaded state and notify waiters
        self.0.loaded.set(()).expect("Loading flag already set");
        self.0.loading.notify_waiters();
        Ok(())
    }
}

impl Property for proto::sys::Item {
    fn into_value(&self) -> std::result::Result<Option<PropertyValue>, crate::property::PropertyError> {
        Ok(Some(PropertyValue::String(
            serde_json::to_string(self).map_err(|_| PropertyError::InvalidValue { value: "".to_string(), ty: "sys::Item".to_string() })?,
        )))
    }

    fn from_value(value: Option<PropertyValue>) -> std::result::Result<Self, crate::property::PropertyError> {
        if let Some(PropertyValue::String(string)) = value {
            let item: proto::sys::Item = serde_json::from_str(&string)
                .map_err(|_| PropertyError::InvalidValue { value: "".to_string(), ty: "sys::Item".to_string() })?;
            Ok(item)
        } else {
            Err(PropertyError::InvalidValue { value: "".to_string(), ty: "sys::Item".to_string() })
        }
    }
}
