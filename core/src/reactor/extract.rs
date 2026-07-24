//! Value extraction: how a resolved property reference reads its value off an
//! in-memory entity. The primitive is `(PropertyId, sub-path)`, deliberately
//! label-free; the AST's [`PropertyPath`] delegates to it, so there is exactly
//! one reading rule and no second path type to keep in agreement with it.

use crate::reactor::AbstractEntity;
use crate::value::Value;
use ankurah_proto::{PropertyId, PropertyPath};

/// Read the property's root value from `entity`, then walk `sub_path` into it.
/// A registered property reads its id-keyed slot only (RFC 5.4: an unwritten
/// id reads absent, it does NOT fall back to a name); the `id` pseudo-property
/// and a system property read by name. JSON sub-path values stay wrapped as
/// [`Value::Json`] to match AnkQL's JSON-subpath comparison semantics.
pub(crate) fn extract_property_value<E: AbstractEntity>(id: &PropertyId, sub_path: &[String], entity: &E) -> Option<Value> {
    let root_value = match id {
        PropertyId::EntityId(id) => E::value_by_id(entity, *id),
        // The `id` pseudo-property reads by its reserved name.
        PropertyId::Id => E::value(entity, "id"),
        PropertyId::System(property) => E::value(entity, property.as_str()),
    }?;
    if sub_path.is_empty() {
        return Some(root_value);
    }
    let json = match root_value {
        Value::Json(json) => json,
        Value::Binary(bytes) => serde_json::from_slice(&bytes).ok()?,
        _ => return None,
    };
    let mut current = &json;
    for key in sub_path {
        current = current.get(key)?;
    }
    Some(Value::Json(current.clone()))
}

/// Extraction for the resolved AST node itself.
pub(crate) trait ExtractValue {
    fn extract_value<E: AbstractEntity>(&self, entity: &E) -> Option<Value>;
}

impl ExtractValue for PropertyPath {
    fn extract_value<E: AbstractEntity>(&self, entity: &E) -> Option<Value> { extract_property_value(&self.id(), &self.subpath, entity) }
}
