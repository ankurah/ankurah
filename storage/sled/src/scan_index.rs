use crate::{
    error::IndexError,
    planner_integration::{key_bounds_to_sled_range, SledRangeBounds},
};
use ankurah_core::{error::RetrievalError, EntityId};
use ankurah_storage_common::{IndexSpecMatch, KeyBounds, ScanDirection};

use crate::index::Index;

/// Scanner over a sled index tree yielding EntityId directly
pub struct SledIndexScanner<'a> {
    pub index: &'a Index,
    pub bounds: &'a KeyBounds,
    pub direction: ScanDirection,
    pub match_type: IndexSpecMatch,
    pub prefix_guard_disabled: bool,
    // Iterator state
    iter: SledIndexIter,
    eq_prefix_bytes: Vec<u8>,
    use_prefix_guard: bool,
}

enum SledIndexIter {
    Forward(sled::Iter),
    Reverse(std::iter::Rev<sled::Iter>),
}

impl Iterator for SledIndexIter {
    type Item = Result<(sled::IVec, sled::IVec), sled::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SledIndexIter::Forward(iter) => iter.next(),
            SledIndexIter::Reverse(iter) => iter.next(),
        }
    }
}

impl<'a> SledIndexScanner<'a> {
    pub fn new(
        index: &'a Index,
        bounds: &'a KeyBounds,
        direction: ScanDirection,
        match_type: IndexSpecMatch,
        prefix_guard_disabled: bool,
    ) -> Result<Self, IndexError> {
        // Setup iterator immediately in constructor
        let SledRangeBounds { start: start_full, end: end_full_opt, upper_open_ended, eq_prefix_guard: eq_prefix_bytes } =
            key_bounds_to_sled_range(bounds, index.spec())?;

        let use_prefix_guard = upper_open_ended && !eq_prefix_bytes.is_empty() && !prefix_guard_disabled;

        let effective_direction = match match_type {
            IndexSpecMatch::Match => direction,
            IndexSpecMatch::Inverse => match direction {
                ScanDirection::Forward => ScanDirection::Reverse,
                ScanDirection::Reverse => ScanDirection::Forward,
            },
        };

        let iter = match effective_direction {
            ScanDirection::Forward => match &end_full_opt {
                Some(end_full) => SledIndexIter::Forward(index.tree().range(start_full.clone()..end_full.clone())),
                None => SledIndexIter::Forward(index.tree().range(start_full.clone()..)),
            },
            ScanDirection::Reverse => match &end_full_opt {
                Some(end_full) => SledIndexIter::Reverse(index.tree().range(start_full.clone()..end_full.clone()).rev()),
                None => SledIndexIter::Reverse(index.tree().range(start_full.clone()..).rev()),
            },
        };

        Ok(Self { index, bounds, direction, match_type, prefix_guard_disabled, iter, eq_prefix_bytes, use_prefix_guard })
    }

    /// Get the effective scan direction, accounting for index match type
    pub fn effective_scan_direction(&self) -> ScanDirection {
        match self.match_type {
            IndexSpecMatch::Match => self.direction,
            IndexSpecMatch::Inverse => match self.direction {
                ScanDirection::Forward => ScanDirection::Reverse,
                ScanDirection::Reverse => ScanDirection::Forward,
            },
        }
    }
}

impl<'a> Iterator for SledIndexScanner<'a> {
    type Item = Result<EntityId, RetrievalError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next item from iterator
        loop {
            let (key_bytes, _value_bytes) = match self.iter.next()? {
                Ok(kv) => kv,
                Err(e) => return Some(Err(RetrievalError::StorageError(e.to_string().into()))),
            };

            // Apply prefix guard if needed
            if self.use_prefix_guard && !self.eq_prefix_bytes.is_empty() {
                if !key_bytes.starts_with(&self.eq_prefix_bytes) {
                    return None; // End of prefix range
                }
            }

            // Decode EntityId from key suffix
            match decode_entity_id_from_index_key(&key_bytes) {
                Ok(entity_id) => return Some(Ok(entity_id)),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// EntityIdStream is automatically implemented via blanket impl since our scanners emit Result<EntityId, RetrievalError>

// TODO: Add extract_ids method for streams that need to convert back to EntityId streams

/// Decode EntityId from index key suffix (last 16 bytes)
pub fn decode_entity_id_from_index_key(key: &[u8]) -> Result<EntityId, RetrievalError> {
    if key.len() < 1 + 16 {
        return Err(RetrievalError::StorageError("index key too short".into()));
    }
    let eid_bytes: [u8; 16] =
        key[key.len() - 16..].try_into().map_err(|_| RetrievalError::StorageError("invalid entity id suffix".into()))?;
    Ok(EntityId::from_bytes(eid_bytes))
}
