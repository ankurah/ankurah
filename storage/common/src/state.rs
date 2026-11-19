//! Continuation tokens for engines whose canonical writes share a transactional counter.
//!
//! Every writer sharing storage must maintain this counter and its latest-change
//! index. Backfilling existing records permits an upgrade; it does not permit
//! older writers that omit those records to keep using the same storage.

use ankurah_core::{error::RetrievalError, storage::StateCursor};

/// A counter's value and the storage incarnation in which it was allocated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterCursor {
    pub incarnation: [u8; 16],
    pub position: u64,
}

impl CounterCursor {
    pub fn fresh() -> Self { Self { incarnation: rand::random(), position: 0 } }

    /// Encode with an engine-specific tag and a format version.
    pub fn encode(self, engine: &[u8]) -> StateCursor {
        let mut bytes = Vec::with_capacity(engine.len() + 25);
        bytes.extend_from_slice(engine);
        bytes.push(1);
        bytes.extend_from_slice(&self.incarnation);
        bytes.extend_from_slice(&self.position.to_be_bytes());
        StateCursor::new(bytes)
    }

    /// Validate a recovered token against this read boundary.
    pub fn resume(self, since: Option<&StateCursor>, engine: &[u8]) -> Result<Option<u64>, RetrievalError> {
        let Some(since) = since else { return Ok(None) };
        let previous = Self::decode(since, engine)?;
        if previous.incarnation != self.incarnation || previous.position > self.position {
            return Err(RetrievalError::InvalidStateCursor);
        }
        Ok(Some(previous.position))
    }

    pub fn decode(since: &StateCursor, engine: &[u8]) -> Result<Self, RetrievalError> {
        let bytes = since.as_bytes();
        if bytes.len() != engine.len() + 25 || !bytes.starts_with(engine) || bytes[engine.len()] != 1 {
            return Err(RetrievalError::InvalidStateCursor);
        }
        let offset = engine.len() + 1;
        let incarnation: [u8; 16] = bytes[offset..offset + 16].try_into().map_err(|_| RetrievalError::InvalidStateCursor)?;
        let position = u64::from_be_bytes(bytes[offset + 16..].try_into().map_err(|_| RetrievalError::InvalidStateCursor)?);
        Ok(Self { incarnation, position })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_is_local_to_its_engine_incarnation_and_committed_counter() {
        let current = CounterCursor { incarnation: [7; 16], position: 12 };
        assert_eq!(current.resume(Some(&current.encode(b"sled")), b"sled").unwrap(), Some(12));
        for token in [
            current.encode(b"sqlite"),
            CounterCursor { incarnation: [8; 16], ..current }.encode(b"sled"),
            CounterCursor { position: 13, ..current }.encode(b"sled"),
            StateCursor::new(vec![]),
        ] {
            assert!(matches!(current.resume(Some(&token), b"sled"), Err(RetrievalError::InvalidStateCursor)));
        }
    }
}
