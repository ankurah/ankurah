//! What a digest tree keeps in the engine's maps, beside the rows the crate
//! encodes, and the bytes of each: the names of the maps and of the tree's
//! lock, the tree's entry in the trees map, and its change log.

pub(in crate::sync::storage_adapter) mod entry;
pub(in crate::sync::storage_adapter) mod history;
pub(in crate::sync::storage_adapter) mod log;
pub(in crate::sync::storage_adapter) mod names;
