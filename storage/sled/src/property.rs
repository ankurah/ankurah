use crate::error::sled_error;
use ankurah_core::error::RetrievalError;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct PropertyManager(Arc<Inner>);

struct Inner {
    property_config_tree: sled::Tree,
    properties: RwLock<HashMap<String, u32>>,
    next_property_id: AtomicU32,
}

impl PropertyManager {
    pub fn open(property_config_tree: sled::Tree) -> anyhow::Result<Self> {
        // Scan property_config_tree to find max property ID
        let mut max_property_id = 0u32;
        for (_key, value_bytes) in property_config_tree.iter().flatten() {
            if value_bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&value_bytes);
                let id = u32::from_be_bytes(arr);
                max_property_id = max_property_id.max(id);
            }
        }

        Ok(Self(Arc::new(Inner {
            property_config_tree,
            properties: RwLock::new(HashMap::new()),
            next_property_id: AtomicU32::new(max_property_id + 1),
        })))
    }

    /// Gets or creates a property ID for the given property name
    pub fn get_property_id(&self, name: &str) -> Result<u32, RetrievalError> {
        // FIXME - I think we need a mutex here to avoid race conditions creating conflicting
        // registrations for the same property
        match self.0.property_config_tree.get(name.as_bytes()).map_err(sled_error)? {
            Some(ivec) => {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&ivec);
                Ok(u32::from_be_bytes(arr))
            }
            None => {
                let new_id = self.0.next_property_id.fetch_add(1, Ordering::Relaxed);
                self.0.property_config_tree.insert(name.as_bytes(), &new_id.to_be_bytes()).map_err(sled_error)?;
                Ok(new_id)
            }
        }
    }

    /// Gets the property name for the given property ID
    pub fn get_property_name(&self, id: u32) -> Option<String> {
        // Scan the property_config_tree to find the name for this ID
        for (key_bytes, value_bytes) in self.0.property_config_tree.iter().flatten() {
            if value_bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&value_bytes);
                let property_id = u32::from_be_bytes(arr);
                if property_id == id {
                    return String::from_utf8(key_bytes.to_vec()).ok();
                }
            }
        }
        None
    }
}
