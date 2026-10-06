use ankurah_proto::{ModelId, PropertyId};

use crate::indexing::KeySpec;

/// An index a member keeps a digest tree for: which entities it files, and under which key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HashedIndex {
    /// Every entity, filed under its entity id alone: the full replica's index.
    EntityId,
    /// The members of one component, filed under their values for `key_spec`. With no key
    /// parts, the component's members filed under their entity ids.
    Component { component: ModelId, key_spec: KeySpec<PropertyId> },
}
