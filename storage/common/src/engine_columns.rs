//! Storage-engine column paths and query lowering.

use ankql::ast::{ModelId, Selection, Stage};

/// A selection an engine can read: every path is one of its own columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineColumns;

impl Stage for EngineColumns {
    type Path = ColumnPath;
    type ModelId = ModelId;
}

/// A column in an engine's own storage, plus any JSON sub-path into the value
/// that column holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPath {
    pub column: String,
    pub subpath: Vec<String>,
}

impl ColumnPath {
    pub fn new(column: impl Into<String>, subpath: Vec<String>) -> Self { Self { column: column.into(), subpath } }

    pub fn simple(column: impl Into<String>) -> Self { Self { column: column.into(), subpath: Vec::new() } }

    pub fn is_simple(&self) -> bool { self.subpath.is_empty() }
}

impl std::fmt::Display for ColumnPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.column)?;
        for step in &self.subpath {
            write!(f, ".{}", step)?;
        }
        Ok(())
    }
}

impl ankurah_core_types::Path for ColumnPath {
    fn display_steps(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.column.as_str()).chain(self.subpath.iter().map(String::as_str))
    }
}

/// Map paths to physical columns without resolving names or applying policy.
pub fn lower_selection<S: Stage<ModelId = ModelId>>(
    selection: &Selection<S>,
    column: &impl Fn(&S::Path) -> ColumnPath,
) -> Selection<EngineColumns> {
    ankql::selection::map_references(selection, column, &|model| *model)
}
