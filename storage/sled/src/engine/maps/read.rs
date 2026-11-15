//! Map reads answered from what the engine has stored: the one sled tree
//! that holds every map's entries.

use ankurah_core::error::RetrievalError;
use ankurah_core::storage::{MapRead, MapRows, ScanDirection};
use sled::IVec;

use super::keys::MapPrefix;
use crate::error::sled_error;

/// Answer `reads`, one [`MapRows`] for each and in their order, from what
/// `maps`, the maps' sled tree, holds, on a thread that may block on sled.
pub(super) async fn stored(maps: &sled::Tree, reads: Vec<MapRead>) -> Result<Vec<MapRows>, RetrievalError> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }

    let maps = maps.clone();
    tokio::task::spawn_blocking(move || reads.into_iter().map(|read| answer(&maps, read)).collect::<Result<_, _>>())
        .await?
        .map_err(sled_error)
}

/// Answer one read from `maps`, blocking on sled.
fn answer(maps: &sled::Tree, read: MapRead) -> Result<MapRows, sled::Error> {
    match read {
        MapRead::Get { map, keys } => {
            let prefix = MapPrefix::of(&map);
            let values = keys.iter().map(|key| Ok(maps.get(prefix.entry(key))?.map(|value| value.to_vec())));

            Ok(MapRows::Got(values.collect::<Result<_, sled::Error>>()?))
        }

        MapRead::Range { map, start, end, direction, limit } => {
            let prefix = MapPrefix::of(&map);
            let entries = maps.range(prefix.bounds(start.as_ref().map(Vec::as_slice), end.as_ref().map(Vec::as_slice)));

            match direction {
                ScanDirection::Forward => first(&prefix, entries, limit),
                ScanDirection::Reverse => first(&prefix, entries.rev(), limit),
            }
        }
    }
}

/// The first `limit` of the map's `entries`, as a range read gives them:
/// each under its key within the map.
fn first(prefix: &MapPrefix, entries: impl Iterator<Item = sled::Result<(IVec, IVec)>>, limit: usize) -> Result<MapRows, sled::Error> {
    let entries = entries.take(limit).map(|entry| {
        let (key, value) = entry?;
        Ok((prefix.key_of(&key).to_vec(), value.to_vec()))
    });

    Ok(MapRows::Ranged(entries.collect::<Result<_, sled::Error>>()?))
}
