//! Deltas too large for one frame travel as chunks of their encoded content.
//! A responder splits the encoding without copying it; a requester reassembles
//! the chunks in order and decodes the content once the last one arrives.

use anyhow::{ensure, Context};
use bincode::Options;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{state_sync::limits::MAX_DELTA_BYTES, DeltaContent, EntityId};

/// One fragment of an entity's encoded DeltaContent. Repairs and live delivery
/// split deltas the same way: offsets start at zero and are contiguous, and `last`
/// marks the final fragment.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityDeltaChunk {
    pub entity_id: EntityId,
    pub offset: u64,
    pub bytes: Bytes,
    pub last: bool,
}

impl DeltaContent {
    /// This content as the bytes its chunks carry.
    pub fn encode_payload(&self) -> bincode::Result<Bytes> { Ok(bincode::serialize(self)?.into()) }

    fn decode_payload(payload: &[u8]) -> bincode::Result<Self> {
        bincode::DefaultOptions::new().with_fixint_encoding().with_limit(payload.len() as u64).reject_trailing_bytes().deserialize(payload)
    }
}

impl EntityDeltaChunk {
    /// Splits an encoded payload into chunks of at most `chunk_bytes`, each sharing
    /// the payload's buffer.
    pub fn split(entity_id: EntityId, payload: Bytes, chunk_bytes: usize) -> DeltaChunks {
        debug_assert!(!payload.is_empty() && chunk_bytes > 0, "an encoded delta has bytes to split");
        DeltaChunks { entity_id, payload, offset: 0, chunk_bytes }
    }
}

/// The chunks of one encoded delta that remain to be sent, in order.
#[derive(Debug, Clone)]
pub struct DeltaChunks {
    entity_id: EntityId,
    payload: Bytes,
    offset: usize,
    chunk_bytes: usize,
}

impl Iterator for DeltaChunks {
    type Item = EntityDeltaChunk;

    fn next(&mut self) -> Option<EntityDeltaChunk> {
        if self.offset == self.payload.len() {
            return None;
        }
        let end = (self.offset + self.chunk_bytes).min(self.payload.len());
        let chunk = EntityDeltaChunk {
            entity_id: self.entity_id,
            offset: self.offset as u64,
            bytes: self.payload.slice(self.offset..end),
            last: end == self.payload.len(),
        };
        self.offset = end;
        Some(chunk)
    }
}

/// Reassembles one entity's delta from its chunks.
#[derive(Debug)]
pub struct DeltaAssembler {
    entity_id: EntityId,
    payload: Vec<u8>,
}

impl DeltaAssembler {
    pub fn new(entity_id: EntityId) -> Self { Self { entity_id, payload: Vec::new() } }

    /// The bytes received so far.
    pub fn len(&self) -> usize { self.payload.len() }

    pub fn is_empty(&self) -> bool { self.payload.is_empty() }

    /// Appends the next chunk: it must belong to this entity, carry bytes, and start
    /// where the previous one ended, keeping the delta within `MAX_DELTA_BYTES`.
    /// Returns the decoded content once the last chunk arrives.
    pub fn push(&mut self, chunk: EntityDeltaChunk) -> anyhow::Result<Option<DeltaContent>> {
        ensure!(chunk.entity_id == self.entity_id, "delta chunk belongs to another entity");
        ensure!(!chunk.bytes.is_empty(), "empty delta chunk");
        ensure!(chunk.offset == self.payload.len() as u64, "delta chunk out of order");
        ensure!(self.payload.len() + chunk.bytes.len() <= MAX_DELTA_BYTES, "delta exceeds its size limit");
        self.payload.extend_from_slice(&chunk.bytes);
        if !chunk.last {
            return Ok(None);
        }
        let content = DeltaContent::decode_payload(&std::mem::take(&mut self.payload)).context("undecodable delta")?;
        Ok(Some(content))
    }
}

#[cfg(test)]
mod tests;
