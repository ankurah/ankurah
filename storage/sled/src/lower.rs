//! Which column sled reads a resolved property from.

use ankql::ast::{PropertyPath, Resolved, Selection};
use ankurah_storage_common::{lower_selection, ColumnPath, EngineColumns};

/// Rewrite a resolved selection into the columns this engine materializes
/// values under: the property id's own rendering, with the `id`
/// pseudo-property rendering as `"id"`. `set_state_blocking` keys its
/// projected values by that same rendering, and the indexes are built over
/// it, so a lowered selection names columns that exist.
///
/// This is sled's own seam, deliberately not shared with the other engines:
/// the engine-side catalog resolver replaces this rendering with the physical
/// name sled assigned the property.
pub fn lower(selection: &Selection<Resolved>) -> Selection<EngineColumns> {
    lower_selection(selection, &|path: &PropertyPath| ColumnPath::new(path.property_id().to_string(), path.subpath.clone()))
}
