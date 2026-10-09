use crate::{
    error::IndexError,
    planner_integration::{key_bounds_to_sled_range, SledRangeBounds},
};
use ankurah_core::indexing::IndexSpecMatch;
use ankurah_core::{error::RetrievalError, EntityId};
use ankurah_storage_common::{KeyBounds, ScanDirection};
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::index::Index;

/// Scanner over a sled index tree yielding EntityId directly
pub struct SledIndexScanner<'a> {
    pub index: &'a Index,
    pub bounds: &'a KeyBounds,
    pub direction: ScanDirection,
    pub match_type: IndexSpecMatch,
    // Iterator state
    iter: SledIndexIter,
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
    pub fn new(index: &'a Index, bounds: &'a KeyBounds, direction: ScanDirection, match_type: IndexSpecMatch) -> Result<Self, IndexError> {
        // The range covers exactly the matching tuples' keys (see
        // `key_bounds_to_sled_range`), so the scan needs no guard of its own.
        let SledRangeBounds { start, end } = key_bounds_to_sled_range(bounds, index.spec())?;

        let effective_direction = match match_type {
            IndexSpecMatch::Match => direction,
            IndexSpecMatch::Inverse => match direction {
                ScanDirection::Forward => ScanDirection::Reverse,
                ScanDirection::Reverse => ScanDirection::Forward,
            },
        };

        let iter = match effective_direction {
            ScanDirection::Forward => match &end {
                Some(end) => SledIndexIter::Forward(index.tree().range(start.clone()..end.clone())),
                None => SledIndexIter::Forward(index.tree().range(start.clone()..)),
            },
            ScanDirection::Reverse => match &end {
                Some(end) => SledIndexIter::Reverse(index.tree().range(start.clone()..end.clone()).rev()),
                None => SledIndexIter::Reverse(index.tree().range(start.clone()..).rev()),
            },
        };

        Ok(Self { index, bounds, direction, match_type, iter })
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

impl Unpin for SledIndexScanner<'_> {}

impl Stream for SledIndexScanner<'_> {
    type Item = Result<EntityId, RetrievalError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Synchronous implementation - always returns Ready
        let (key_bytes, _value_bytes) = match self.iter.next() {
            Some(Ok(kv)) => kv,
            Some(Err(e)) => return Poll::Ready(Some(Err(RetrievalError::storage(e.to_string())))),
            None => return Poll::Ready(None),
        };

        // Decode EntityId from key suffix
        Poll::Ready(Some(decode_entity_id_from_index_key(&key_bytes)))
    }
}

// EntityIdStream is automatically implemented via blanket impl since our scanners emit Result<EntityId, RetrievalError>

// TODO: Add extract_ids method for streams that need to convert back to EntityId streams

/// Decode the EntityId that an index key carries as its fixed-width suffix.
pub fn decode_entity_id_from_index_key(key: &[u8]) -> Result<EntityId, RetrievalError> {
    if key.len() < 1 + EntityId::BYTE_LEN {
        return Err(RetrievalError::storage("index key too short"));
    }
    let eid_bytes: [u8; EntityId::BYTE_LEN] =
        key[key.len() - EntityId::BYTE_LEN..].try_into().map_err(|_| RetrievalError::storage("invalid entity id suffix"))?;
    Ok(EntityId::from_bytes(eid_bytes))
}
