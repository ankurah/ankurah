use crate::reactor::AbstractEntity;
use crate::value::ValueExt;
use ankurah_core_types::{PropertyPath, Value};

pub(crate) trait PropertyPathExt {
    /// Read this path from an entity, preserving JSON subvalues as `Value::Json`.
    fn extract_value<E: AbstractEntity>(&self, entity: &E) -> Option<Value>;
}

impl PropertyPathExt for PropertyPath {
    fn extract_value<E: AbstractEntity>(&self, entity: &E) -> Option<Value> {
        let value = entity.value(&self.property_id())?;
        if self.subpath.is_empty() {
            Some(value)
        } else {
            value.json_at_path(&self.subpath).map(Value::Json)
        }
    }
}
