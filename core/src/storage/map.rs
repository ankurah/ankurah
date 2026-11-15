//! Ordered byte maps stored alongside an engine's entity records.

use std::fmt;
use std::ops::Bound;

/// The name of an ordered map from bytes to bytes.
/// Maps need no explicit creation; an unwritten map is empty.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MapId(Vec<u8>);

impl MapId {
    /// The map named by these bytes.
    pub fn new(name: impl Into<Vec<u8>>) -> Self { Self(name.into()) }

    /// The bytes that name the map.
    pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

impl fmt::Debug for MapId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "MapId(b\"{}\")", self.0.escape_ascii()) }
}

/// Traversal direction over a map's byte-ordered keys.
pub use ankurah_core_types::ScanDirection;

/// One read of one map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapRead {
    /// The value under each of `keys`, one per key and in their order.
    Get { map: MapId, keys: Vec<Vec<u8>> },

    /// The first `limit` entries within the bounds, in `direction`, or all
    /// matching entries if fewer exist. Empty or inverted intervals return nothing.
    Range { map: MapId, start: Bound<Vec<u8>>, end: Bound<Vec<u8>>, direction: ScanDirection, limit: usize },
}

impl MapRead {
    /// The map being read.
    pub fn map(&self) -> &MapId {
        match self {
            Self::Get { map, .. } | Self::Range { map, .. } => map,
        }
    }
}

/// The answer to one [`MapRead`], of the same kind as the read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapRows {
    /// A [`MapRead::Get`]'s values, one per key asked for and in their order:
    /// `None` where the map holds no value under the key.
    Got(Vec<Option<Vec<u8>>>),

    /// A [`MapRead::Range`]'s entries, each key with its value, in the order
    /// the read ran.
    Ranged(Vec<(Vec<u8>, Vec<u8>)>),
}

/// One write to one map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapWrite {
    /// Store `value` under `key`, replacing any value there.
    Put { map: MapId, key: Vec<u8>, value: Vec<u8> },

    /// Remove any value under `key`.
    Delete { map: MapId, key: Vec<u8> },

    /// Remove all keys, including earlier writes in this transaction.
    /// Later writes remain. This may require work proportional to the map's size.
    Clear { map: MapId },
}
