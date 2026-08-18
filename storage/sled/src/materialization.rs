use ankurah_core::selection::filter::PathLookup;
use ankurah_proto::EntityId;
use ankurah_storage_common::filtering::HasEntityId;
use ankurah_storage_common::{ColumnPath, EngineColumns};

/// Projected property values for one entity, used for filtering and sorting.
/// Contains pre-materialized values extracted from the collection tree.
#[derive(Debug)]
pub struct ProjectedEntity {
    pub(crate) id: EntityId,
    pub(crate) map: std::collections::BTreeMap<String, ankurah_core::value::Value>,
}

impl PathLookup<EngineColumns> for ProjectedEntity {
    fn value_at(&self, path: &ColumnPath) -> Option<ankurah_core::value::Value> {
        // The primary key is not one of the materialized columns; every other
        // column is a direct lookup, since the projection map is keyed by the
        // same names the lowering wrote.
        let value =
            if path.column == "id" { ankurah_core::value::Value::EntityId(self.id) } else { self.map.get(&path.column).cloned()? };
        if path.subpath.is_empty() {
            Some(value)
        } else {
            value.extract_at_path(&path.subpath)
        }
    }
}

impl HasEntityId for ProjectedEntity {
    fn entity_id(&self) -> EntityId { self.id }
}
