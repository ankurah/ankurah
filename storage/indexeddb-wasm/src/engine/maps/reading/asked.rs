//! How one map read is made as requests in a transaction's object store of
//! the maps, and answered from them.

use std::ops::Bound;

use ankurah_core::storage::pending_writes::holds_nothing;
use ankurah_core::storage::{MapRead, MapRows, ScanDirection};
use js_sys::Array;
use wasm_bindgen::JsCast;
use web_sys::{IdbCursorDirection, IdbObjectStore};

use crate::engine::maps::layout;
use crate::engine::maps::requests::{Cursor, Request};
use crate::engine::maps::MapError;

/// The requests that answer one read.
pub(super) enum Asked {
    /// A get's request for each key, in order.
    Got(Vec<Request>),

    /// A range in key order: its entries' keys and values, read together by
    /// two requests over the same entries.
    Forward { keys: Request, values: Request },

    /// A range in reverse key order, read by a cursor, since IndexedDB reads
    /// many at once only forward.
    Reverse { cursor: Cursor, limit: usize },

    /// A range that reads nothing.
    Nothing,
}

impl Asked {
    /// Make the requests that answer `read` in `maps`, the maps' object store
    /// of a transaction.
    pub(super) fn make(maps: &IdbObjectStore, read: &MapRead) -> Result<Self, MapError> {
        match read {
            MapRead::Get { map, keys } => {
                let requests = keys.iter().map(|key| Request::made(maps.get(&layout::entry_key(map, key))));
                Ok(Asked::Got(requests.collect::<Result<_, _>>()?))
            }

            MapRead::Range { map, start, end, direction, limit } => {
                // IndexedDB refuses bounds that hold nothing, an end before
                // the empty key among them, and reads every entry for a limit
                // of none.
                let before_every_key = matches!(end, Bound::Excluded(key) if key.is_empty());
                if *limit == 0 || holds_nothing(start.as_ref(), end.as_ref()) || before_every_key {
                    return Ok(Asked::Nothing);
                }

                let range = layout::entry_range(map, start.as_ref().map(Vec::as_slice), end.as_ref().map(Vec::as_slice))?;
                match direction {
                    ScanDirection::Forward => {
                        // IndexedDB counts in 32 bits: a larger limit reads as
                        // many entries as it counts.
                        let count = u32::try_from(*limit).unwrap_or(u32::MAX);

                        Ok(Asked::Forward {
                            keys: Request::made(maps.get_all_keys_with_key_and_limit(&range, count))?,
                            values: Request::made(maps.get_all_with_key_and_limit(&range, count))?,
                        })
                    }

                    ScanDirection::Reverse => {
                        let cursor = Cursor::opened(maps.open_cursor_with_range_and_direction(&range, IdbCursorDirection::Prev))?;
                        Ok(Asked::Reverse { cursor, limit: *limit })
                    }
                }
            }
        }
    }

    /// The read's answer, once its requests have succeeded.
    pub(super) async fn answer(self) -> Result<MapRows, MapError> {
        match self {
            Asked::Got(requests) => {
                let mut values = Vec::with_capacity(requests.len());
                for request in requests {
                    let found = request.result().await?;
                    values.push(if found.is_undefined() { None } else { Some(layout::value_of(&found)?) });
                }

                Ok(MapRows::Got(values))
            }

            Asked::Forward { keys, values } => {
                let keys: Array = keys.result().await?.unchecked_into();
                let values: Array = values.result().await?.unchecked_into();

                let entries = keys.iter().zip(values.iter()).map(|(key, value)| Ok((layout::key_of(&key)?, layout::value_of(&value)?)));
                Ok(MapRows::Ranged(entries.collect::<Result<_, MapError>>()?))
            }

            Asked::Reverse { mut cursor, limit } => {
                let mut entries = Vec::new();
                while let Some(at) = cursor.next().await? {
                    let (key, value) = (at.key().map_err(MapError::thrown)?, at.value().map_err(MapError::thrown)?);
                    entries.push((layout::key_of(&key)?, layout::value_of(&value)?));

                    if entries.len() == limit {
                        break;
                    }

                    at.continue_().map_err(MapError::thrown)?;
                }

                Ok(MapRows::Ranged(entries))
            }

            Asked::Nothing => Ok(MapRows::Ranged(Vec::new())),
        }
    }
}
