//! A resolved property reference: a durable [`PropertyId`] plus any JSON
//! sub-path into that property's value.
//!
//! This lives beside [`PropertyId`] rather than in the query crate because
//! every layer downstream of name resolution addresses a property this way --
//! the query AST's resolved stage, the reactor's watcher keys, a storage
//! engine's durable property map -- and none of them needs the parser.

use serde::{Deserialize, Serialize};

use crate::{EntityId, PropertyId, SystemProperty};

/// A property reference resolved against the catalog: the `id`
/// pseudo-property, a registered property's stable entity id, or a system
/// property's durable name -- plus any JSON sub-path into the property's
/// value. A source-level name binds to one of these exactly once, where a
/// query enters the system; nothing downstream carries a name.
///
/// `label` is carried as its own field, for every arm, rather than folded
/// into one arm of `id`'s type the way it used to be. Today a `System`
/// label always equals its `PropertyId::System` name, and the `id`
/// pseudo-property's label is always the literal `"id"` -- both look
/// recoverable from `id` alone, but that is an accident of the current
/// arms, not a rule resolution should lean on: a future name -> `PropertyId`
/// resolution is not guaranteed a label derivable from the id after the
/// fact, and the source label (usable only for `Display`, never a physical
/// storage name) must survive regardless of which arm minted the id.
/// Keeping `label` next to `id`, rather than nesting `id` inside a second
/// enum shaped almost exactly like [`PropertyId`] just to bolt a label onto
/// every arm, is what makes that guarantee cheap to keep.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyPath {
    id: PropertyId,
    /// The source-level name that resolved to this id (usable only for Display)
    label: String,
    /// JSON sub-path into the property's value; empty for a plain reference.
    pub subpath: Vec<String>,
}

/// Equality, ordering, and hashing are identity + sub-path ONLY: `label` is
/// Display metadata, and two references to the same property written under
/// different names are the SAME reference. Watcher and index keys rely on
/// this to avoid splitting when a property is addressed pre- and post-rename.
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
    /// A resolved reference to a registered user-model property, addressed by
    /// its stable catalog id. `label` is the name that resolved to this id,
    /// retained for `Display` only (it must never seed a physical storage name).
    pub fn registered(id: EntityId, label: impl Into<String>, subpath: Vec<String>) -> Self {
        Self { id: PropertyId::EntityId(id), label: label.into(), subpath }
    }

    /// A resolved reference to a system/catalog property, addressed by name:
    /// the frozen bootstrap base case has no catalog entry to mint an id from.
    pub fn system(property: SystemProperty, subpath: Vec<String>) -> Self {
        Self { id: PropertyId::System(property), label: property.to_string(), subpath }
    }

    /// A resolved reference to the `id` pseudo-property (every entity's primary
    /// key), addressed by its own [`PropertyId::Id`] rather than a catalog id or
    /// a name. It takes no sub-path: resolution rejects `id.<anything>`.
    pub fn id() -> Self { Self { id: PropertyId::Id, label: "id".to_string(), subpath: vec![] } }

    /// This property's serializable, durable address (see [`PropertyId`]).
    /// A storage engine keys its durable property-to-physical map on the returned
    /// value and uses it as an opaque key (see [`PropertyId`] for the one
    /// sanctioned exception, a `System` name). When a display name is genuinely
    /// needed, pick by which name is meant: the `Display` impl on
    /// [`PropertyPath`] gives the name AS WRITTEN in the ankql statement (the
    /// resolved-from label), whereas a catalog resolver's `property_name`
    /// lookup gives the property's CURRENT name; the two can diverge after a
    /// rename.
    pub fn property_id(&self) -> PropertyId { self.id.clone() }

    /// True when there is no JSON sub-path: a plain property reference.
    pub fn is_simple(&self) -> bool { self.subpath.is_empty() }
}

impl std::fmt::Display for PropertyPath {
    /// Human-readable ONLY (never a physical storage name): the resolved-from
    /// label, then any sub-path dotted on.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label)?;
        for step in &self.subpath {
            write!(f, ".{}", step)?;
        }
        Ok(())
    }
}

/// Path construction for durable property identities: the [`PropertyId`] ->
/// [`PropertyPath`] conversion, as a method on the id. A path built from a
/// bare id has no source name, so a registered property's label falls back to
/// the id's own rendering.
pub trait PropertyIdExt {
    /// This id as a property reference, with `subpath` as its JSON sub-path.
    fn path(&self, subpath: &[String]) -> PropertyPath;
}

impl PropertyIdExt for PropertyId {
    fn path(&self, subpath: &[String]) -> PropertyPath {
        match self {
            // The id pseudo-property has no subfields (resolution rejects
            // `id.<anything>`); any subpath given here is dropped.
            PropertyId::Id => PropertyPath::id(),
            PropertyId::EntityId(id) => PropertyPath::registered(*id, id.to_string(), subpath.to_vec()),
            PropertyId::System(property) => PropertyPath::system(*property, subpath.to_vec()),
        }
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
        // A different sub-path IS a different reference.
        assert_ne!(written, PropertyPath::registered(id, "written_name", vec![]));
    }

    /// Equality ignores the label, so an equality assertion cannot catch label
    /// loss on the wire; this pins it through Display instead.
    #[test]
    fn property_path_label_survives_the_wire() {
        let p = PropertyPath::registered(EntityId::from_bytes([4u8; 32]), "the_label", vec!["x".to_string()]);
        let q: PropertyPath = bincode::deserialize(&bincode::serialize(&p).unwrap()).unwrap();
        assert_eq!(p, q);
        assert_eq!(p.to_string(), q.to_string(), "the label must survive serialization even though equality ignores it");
    }
}
