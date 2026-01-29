use crate::error::sled_error;
use ankurah_core::error::StorageError;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct PropertyManager(Arc<Inner>);

struct Inner {
    property_config_tree: sled::Tree,
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

        Ok(Self(Arc::new(Inner { property_config_tree, next_property_id: AtomicU32::new(max_property_id + 1) })))
    }

    /// Gets or creates a property ID for the given property name.
    /// Uses compare_and_swap for atomic insert-if-absent to avoid race conditions.
    pub fn get_property_id(&self, name: &str) -> Result<u32, StorageError> {
        // Fast path: check if already exists
        if let Some(ivec) = self.0.property_config_tree.get(name.as_bytes()).map_err(sled_error)? {
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&ivec);
            return Ok(u32::from_be_bytes(arr));
        }

        // Slow path: try to insert atomically
        loop {
            let new_id = self.0.next_property_id.fetch_add(1, Ordering::Relaxed);
            let new_id_bytes = new_id.to_be_bytes();

            // Atomically insert only if key doesn't exist (expected = None)
            match self.0.property_config_tree.compare_and_swap(name.as_bytes(), None::<&[u8]>, Some(&new_id_bytes[..])) {
                Ok(Ok(())) => {
                    // Successfully inserted our new ID
                    return Ok(new_id);
                }
                Ok(Err(cas_error)) => {
                    // Another thread inserted first - use their value
                    let existing = cas_error.current.expect("CAS failed but no current value");
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(&existing);
                    return Ok(u32::from_be_bytes(arr));
                }
                Err(e) => {
                    return Err(sled_error(e));
                }
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
