//! PropertyPath represents a path to a property value, supporting both simple fields and JSON sub-paths.
//! Used by the watcher system to index and extract values for comparison.

use crate::value::Value;
use ankurah_proto::EntityId;

/// A path to a property value, supporting both simple fields and JSON sub-paths.
/// Used by the watcher system to index and extract values for comparison.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PropertyPath {
    /// The property whose value is read, in the resolution vocabulary
    /// ([`ankql::ast::PropertyId`]): a registered id reads its id-keyed slot
    /// directly (RFC 5.4: an unwritten id reads absent, it does NOT fall back
    /// to a name); the `id` pseudo-property and a system property read by
    /// name. An unresolved raw path is carried as a `System` name, matching
    /// the key-spec lowering in `build_key_spec_from_selection`: at read time
    /// "fetch by this name" IS the system read. Deliberately label-free,
    /// unlike the AST's `PropertyPath`: watcher keys must not split when the
    /// same property is referenced under different display labels.
    id: ankql::ast::PropertyId,
    /// The sub-path within the property (e.g., ["task_id"] for "context.task_id"), empty for simple fields
    sub_path: Vec<String>,
}

impl PropertyPath {
    /// Create a PropertyPath from a raw (unresolved) PathExpr: name-addressed,
    /// the first step the root property and the rest a JSON sub-path.
    pub fn from_path(path: &ankql::ast::PathExpr) -> Self {
        let steps = &path.steps;
        Self { id: ankql::ast::PropertyId::System { name: steps[0].clone() }, sub_path: steps[1..].to_vec() }
    }

    /// Create a PropertyPath from a resolved identifier: a registered id reads
    /// its id-keyed slot, a system name reads by name.
    pub fn from_identifier(identifier: &ankql::ast::PropertyPath) -> Self {
        Self { id: identifier.id(), sub_path: identifier.subpath.clone() }
    }

    /// A simple (sub-path-free) path addressed by a resolved identity.
    pub fn from_key(key: &ankql::ast::PropertyId) -> Self { Self { id: key.clone(), sub_path: Vec::new() } }

    /// A simple path addressed by name: a system field or an unresolved raw path.
    pub fn by_name(name: impl Into<String>) -> Self {
        Self { id: ankql::ast::PropertyId::System { name: name.into() }, sub_path: Vec::new() }
    }

    /// Check if this is a simple field (no sub-path)
    pub fn is_simple(&self) -> bool { self.sub_path.is_empty() }

    /// Extract the value at this path from an entity.
    /// For JSON paths, keeps the value wrapped as Value::Json to match index keys.
    pub fn extract_value<E: super::AbstractEntity>(&self, entity: &E) -> Option<Value> {
        let root_value = match &self.id {
            // A registered property reads its id-keyed slot only; an unwritten
            // id is absent (NULL), never a fallback to a name.
            ankql::ast::PropertyId::EntityId(id) => E::value_by_id(entity, EntityId::from_ulid(*id)),
            // The `id` pseudo-property reads by its reserved name.
            ankql::ast::PropertyId::Id => E::value(entity, "id"),
            ankql::ast::PropertyId::System { name } => E::value(entity, name),
        }?;
        if self.sub_path.is_empty() {
            Some(root_value)
        } else {
            // Extract nested value from JSON, keeping it wrapped as Value::Json
            // This matches how literals are stored in the comparison index after TypeResolver
            match root_value {
                Value::Json(json) => {
                    let mut current = &json;
                    for key in &self.sub_path {
                        current = current.get(key)?;
                    }
                    // Keep as Value::Json to match index keys
                    Some(Value::Json(current.clone()))
                }
                Value::Binary(bytes) => {
                    let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
                    let mut current = &json;
                    for key in &self.sub_path {
                        current = current.get(key)?;
                    }
                    // Keep as Value::Json to match index keys
                    Some(Value::Json(current.clone()))
                }
                _ => None, // Can't traverse into non-JSON types
            }
        }
    }
}

impl From<&str> for PropertyPath {
    fn from(val: &str) -> Self { PropertyPath::by_name(val) }
}
