use ankurah_core::{selection::filter::ValueLookup, value::Value};
use ankurah_proto::EntityId;
use ankurah_storage_common::{filtering::HasEntityId, ColumnPath, EngineColumns};

pub(crate) const MATERIALIZATION_COLUMN: &str = "__materialization";

/// One projected row, addressed by the same physical slots as its query plan.
#[derive(Debug)]
pub struct ProjectedEntity {
    pub(crate) id: EntityId,
    pub(crate) map: std::collections::BTreeMap<u32, Value>,
}

impl ValueLookup<EngineColumns> for ProjectedEntity {
    fn order_value_at(&self, path: &ColumnPath) -> Option<Value> {
        if path.subpath.is_empty() {
            return self.value_at(path);
        }
        use ankurah_core::value::ValueExt;
        let value = self.map.get(&crate::property::slot_from_planner_column(&path.column)?)?;
        value.json_order_at_path(&path.subpath).map(Value::Json)
    }

    fn value_at(&self, path: &ColumnPath) -> Option<Value> {
        let value = if path.column == "id" {
            Value::EntityId(self.id)
        } else {
            self.map.get(&crate::property::slot_from_planner_column(&path.column)?)?.clone()
        };
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
