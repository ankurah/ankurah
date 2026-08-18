//! Which column SQLite reads a resolved property from.

use ankql::ast::{PropertyPath, Resolved, Selection};
use ankurah_storage_common::{lower_selection, ColumnPath, EngineColumns};

/// Rewrite a resolved selection into this engine's column names: the property
/// id's own rendering, with the `id` pseudo-property rendering as `"id"`.
/// `set_state` adds a column under that same rendering, so a lowered
/// selection names columns the state table actually has.
///
/// This is SQLite's own seam, deliberately not shared with the other engines:
/// the engine-side catalog resolver replaces this rendering with the physical
/// column name this engine assigned the property.
pub fn lower(selection: &Selection<Resolved>) -> Selection<EngineColumns> {
    lower_selection(selection, &|path: &PropertyPath| ColumnPath::new(path.property_id().to_string(), path.subpath.clone()))
}
