//! Which column SQLite reads a resolved property from.

use ankql::ast::{PropertyPath, Resolved, Selection};
use ankurah_storage_common::{lower_selection, ColumnPath, EngineColumns};

pub fn lower(selection: &Selection<Resolved>) -> Selection<EngineColumns> {
    lower_selection(selection, &|path: &PropertyPath| ColumnPath::new(path.property_id().to_string(), path.subpath.clone()))
}
