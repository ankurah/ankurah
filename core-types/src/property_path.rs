//! Resolved property references.

use serde::{Deserialize, Serialize};

use crate::{EntityId, PropertyId, SystemProperty};

/// A durable property identity and JSON subpath. `label` is display-only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyPath {
    id: PropertyId,
    label: String,
    /// JSON path below the property.
    pub subpath: Vec<String>,
}

impl PartialEq for PropertyPath {
    fn eq(&self, other: &Self) -> bool { self.id == other.id && self.subpath == other.subpath }
}
impl Eq for PropertyPath {}
impl PartialOrd for PropertyPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for PropertyPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.id.cmp(&other.id).then_with(|| self.subpath.cmp(&other.subpath)) }
}
impl std::hash::Hash for PropertyPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.subpath.hash(state);
    }
}

impl PropertyPath {
    /// Construct a registered-property path with its display label.
    pub fn registered(id: EntityId, label: impl Into<String>, subpath: Vec<String>) -> Self {
        Self { id: PropertyId::EntityId(id), label: label.into(), subpath }
    }

    /// Construct a built-in property path.
    pub fn system(property: SystemProperty, subpath: Vec<String>) -> Self {
        Self { id: PropertyId::System(property), label: property.to_string(), subpath }
    }

    /// Construct the entity-id path.
    pub fn id() -> Self { Self { id: PropertyId::Id, label: "id".to_string(), subpath: vec![] } }

    /// Return the durable property identity.
    pub fn property_id(&self) -> PropertyId { self.id }

    /// Return whether the path names the whole property.
    pub fn is_simple(&self) -> bool { self.subpath.is_empty() }
}

impl std::fmt::Display for PropertyPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label)?;
        for step in &self.subpath {
            write!(f, ".{}", step)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_path_equality_ignores_label() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let id = EntityId::from_bytes([3u8; 32]);
        let written = PropertyPath::registered(id, "written_name", vec!["x".to_string()]);
        let renamed = PropertyPath::registered(id, "renamed", vec!["x".to_string()]);
        assert_eq!(written, renamed, "the label must not distinguish two references to one property");
        let hash = |p: &PropertyPath| {
            let mut h = DefaultHasher::new();
            p.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&written), hash(&renamed), "hash must agree with equality");
        assert_ne!(written.to_string(), renamed.to_string(), "Display still carries the written label");
        assert_ne!(written, PropertyPath::registered(id, "written_name", vec![]));
    }

    #[test]
    fn property_path_label_survives_the_wire() {
        let p = PropertyPath::registered(EntityId::from_bytes([4u8; 32]), "the_label", vec!["x".to_string()]);
        let q: PropertyPath = bincode::deserialize(&bincode::serialize(&p).unwrap()).unwrap();
        assert_eq!(p, q);
        assert_eq!(p.to_string(), q.to_string(), "the label must survive serialization even though equality ignores it");
    }
}
