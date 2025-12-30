//! PropertyPath represents a path to a property value, supporting both simple fields and JSON sub-paths.
//! Used by the watcher system to index and extract values for comparison.

use crate::value::Value;

/// A path to a property value, supporting both simple fields and JSON sub-paths.
/// Used by the watcher system to index and extract values for comparison.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PropertyPath {
    /// The root property name (e.g., "context" for "context.task_id")
    root: String,
    /// The sub-path within the property (e.g., ["task_id"] for "context.task_id"), empty for simple fields
    sub_path: Vec<String>,
}

impl PropertyPath {
    /// Create a PropertyPath from a PathExpr
    pub fn from_path(path: &ankql::ast::PathExpr) -> Self {
        let steps = &path.steps;
        Self { root: steps[0].clone(), sub_path: steps[1..].to_vec() }
    }

    /// Get the root property name
    pub fn root(&self) -> &str { &self.root }

    /// Check if this is a simple field (no sub-path)
    pub fn is_simple(&self) -> bool { self.sub_path.is_empty() }

    /// Extract the value at this path from an entity.
    /// For JSON paths, keeps the value wrapped as Value::Json to match index keys.
    pub fn extract_value<E: super::AbstractEntity>(&self, entity: &E) -> Option<Value> {
        let root_value = E::value(entity, &self.root)?;
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
    fn from(val: &str) -> Self { PropertyPath { root: val.to_string(), sub_path: Vec::new() } }
}
