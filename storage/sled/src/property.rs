use crate::error::sled_error;
use ankql::ast::PropertyId;
use ankurah_core::error::RetrievalError;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

#[derive(Clone)]
/// Database-wide durable mapping between property identities and compact sled slots.
pub struct PropertyManager(Arc<Inner>);

struct Inner {
    /// Engine-owned durable bijection between a [`PropertyId`] and the compact
    /// numeric slot used in every sled materialization. Both directions live in
    /// one tree so first assignment is atomic.
    property_slots_tree: sled::Tree,
    next_slot: AtomicU32,
}

const PROPERTY_PREFIX: &[u8] = b"property\0";
const SLOT_PREFIX: &[u8] = b"slot\0";
const SLOT_COLUMN_PREFIX: &str = "slot_";

fn property_key(id: &PropertyId) -> Vec<u8> {
    let encoded = bincode::serialize(id).expect("PropertyId always serializes");
    let mut key = Vec::with_capacity(PROPERTY_PREFIX.len() + encoded.len());
    key.extend_from_slice(PROPERTY_PREFIX);
    key.extend_from_slice(&encoded);
    key
}

fn slot_key(slot: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(SLOT_PREFIX.len() + std::mem::size_of::<u32>());
    key.extend_from_slice(SLOT_PREFIX);
    key.extend_from_slice(&slot.to_be_bytes());
    key
}

fn decode_slot(bytes: &[u8]) -> Result<u32, RetrievalError> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| RetrievalError::Other(format!("corrupt sled property slot: expected 4 bytes, found {}", bytes.len())))?;
    Ok(u32::from_be_bytes(bytes))
}

/// The storage-common planner currently represents an engine's physical
/// property address as a string. Sled's actual address is a numeric slot, so
/// this is only a lossless transient encoding at the planner boundary; it is
/// not another durable PropertyId-to-name registry.
pub(crate) fn planner_column_for_slot(slot: u32) -> String { format!("{SLOT_COLUMN_PREFIX}{slot}") }

/// Decode the transient planner representation produced by
/// [`planner_column_for_slot`], rejecting logical names and malformed slots.
pub(crate) fn slot_from_planner_column(column: &str) -> Option<u32> { column.strip_prefix(SLOT_COLUMN_PREFIX)?.parse().ok() }

impl PropertyManager {
    /// Open the mapping tree and initialize allocation after its highest assigned slot.
    pub fn open(property_slots_tree: sled::Tree) -> anyhow::Result<Self> {
        let mut max_slot = 0u32;
        for entry in property_slots_tree.scan_prefix(PROPERTY_PREFIX) {
            let (_, value) = entry?;
            max_slot = max_slot.max(decode_slot(&value).map_err(|error| anyhow::anyhow!(error.to_string()))?);
        }

        Ok(Self(Arc::new(Inner { property_slots_tree, next_slot: AtomicU32::new(max_slot.saturating_add(1)) })))
    }

    /// Return the already-assigned numeric slot for `property`, if this store
    /// has ever materialized it. This is the read-only query-planning path.
    pub fn slot_of(&self, property: &PropertyId) -> Result<Option<u32>, RetrievalError> {
        self.0.property_slots_tree.get(property_key(property)).map_err(sled_error)?.map(|bytes| decode_slot(&bytes)).transpose()
    }

    /// Get or atomically assign the database-wide numeric slot for a property.
    /// Gaps are harmless: losing a race may consume a candidate, but an assigned
    /// slot is durable and is never reused for another identity.
    pub fn slot_for(&self, property: &PropertyId) -> Result<u32, RetrievalError> {
        if property == &PropertyId::Id {
            return Err(RetrievalError::Other(
                "the id pseudo-property is an entity key and is never assigned a sled property slot".to_owned(),
            ));
        }
        if let Some(slot) = self.slot_of(property)? {
            return Ok(slot);
        }

        let property_key = property_key(property);
        let property_bytes =
            bincode::serialize(property).map_err(|error| RetrievalError::Other(format!("could not serialize property id: {error}")))?;

        loop {
            let candidate = self.0.next_slot.fetch_add(1, Ordering::Relaxed);
            let candidate_bytes = candidate.to_be_bytes();
            let slot_key = slot_key(candidate);
            let result = self.0.property_slots_tree.transaction(|tree| {
                if let Some(existing) = tree.get(property_key.as_slice())? {
                    let bytes: [u8; 4] = existing.as_ref().try_into().map_err(|_| {
                        sled::transaction::ConflictableTransactionError::Abort("corrupt PropertyId-to-slot registration".to_owned())
                    })?;
                    return Ok(u32::from_be_bytes(bytes));
                }
                if tree.get(slot_key.as_slice())?.is_some() {
                    return Err(sled::transaction::ConflictableTransactionError::Abort("slot-taken".to_owned()));
                }
                tree.insert(property_key.as_slice(), candidate_bytes.as_slice())?;
                tree.insert(slot_key.as_slice(), property_bytes.as_slice())?;
                Ok(candidate)
            });

            match result {
                Ok(slot) => return Ok(slot),
                Err(sled::transaction::TransactionError::Abort(message)) if message == "slot-taken" => continue,
                Err(sled::transaction::TransactionError::Abort(message)) => return Err(RetrievalError::Other(message)),
                Err(sled::transaction::TransactionError::Storage(error)) => return Err(sled_error(error)),
            }
        }
    }

    /// Reverse a materialized numeric slot to its durable property identity.
    pub fn property_for_slot(&self, slot: u32) -> Result<Option<PropertyId>, RetrievalError> {
        self.0
            .property_slots_tree
            .get(slot_key(slot))
            .map_err(sled_error)?
            .map(|bytes| {
                bincode::deserialize(&bytes)
                    .map_err(|error| RetrievalError::Other(format!("corrupt sled slot-to-PropertyId registration: {error}")))
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankql::ast::SystemProperty;
    use ankurah_core::EntityId;

    fn manager() -> PropertyManager {
        let db = sled::Config::new().temporary(true).open().unwrap();
        PropertyManager::open(db.open_tree("property_slots").unwrap()).unwrap()
    }

    fn registered(first_byte: u8) -> PropertyId {
        let mut bytes = [0u8; 16];
        bytes[0] = first_byte;
        PropertyId::EntityId(EntityId::from_bytes(bytes))
    }

    #[test]
    fn property_id_and_numeric_slot_roundtrip() {
        let manager = manager();
        let system = PropertyId::System(SystemProperty::Name);
        let registered = registered(0x11);

        let system_slot = manager.slot_for(&system).unwrap();
        let registered_slot = manager.slot_for(&registered).unwrap();
        assert_ne!(system_slot, registered_slot);
        assert_eq!(manager.slot_for(&system).unwrap(), system_slot);
        assert_eq!(manager.slot_of(&system).unwrap(), Some(system_slot));
        assert_eq!(manager.property_for_slot(system_slot).unwrap(), Some(system));
        assert_eq!(manager.property_for_slot(registered_slot).unwrap(), Some(registered));
    }

    #[test]
    fn planner_column_is_only_a_lossless_slot_encoding() {
        for slot in [0, 1, 42, u32::MAX] {
            assert_eq!(slot_from_planner_column(&planner_column_for_slot(slot)), Some(slot));
        }
        assert_eq!(slot_from_planner_column("id"), None);
        assert_eq!(slot_from_planner_column("name"), None);
    }
}
