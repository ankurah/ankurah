use std::collections::BTreeSet;

use ankql::ast::PropertyId;
use ankurah_proto::{EntityId, ModelId};
use serde::{Deserialize, Serialize};

use crate::indexing::{encode_tuple_values_with_key_spec, IndexError, KeySpec};
use crate::selection::filter::{self, Filterable};

/// The index a digest tree serves: which entities it files, and under which
/// canonical keys. Trees register keyed by it.
///
/// A key is ankurah's engine-independent encoding of the index key
/// ([`encode_tuple_values_with_key_spec`]), so two stores on different engines
/// file an entity under the same bytes and can compare any prefix.
///
/// Each kind derives an entity's keys from the entity's own state alone and
/// files it under at most one key. Indexes whose keys follow other entities (a
/// derived group membership, a relation, the catalog) or that file an entity
/// under several keys have no kind yet; they arrive with the group-keyed index,
/// together with the transaction interface that logs their key changes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HashedIndex {
    /// Every entity, filed under its own id. Every store keeps a tree for it.
    EntityId,
    /// The members of `component`, each filed under the encoding of its values
    /// for the key parts of `key_spec`. A member lacking one of those values,
    /// and every entity outside the component, is filed under no key.
    Component { component: ModelId, key_spec: KeySpec<PropertyId> },
}

/// Why an entity's keys under a hashed index could not be derived.
#[derive(Debug, thiserror::Error)]
pub enum KeyDerivationError {
    #[error("component membership is unavailable: {0}")]
    Membership(#[from] filter::Error),
    #[error("the key does not encode: {0}")]
    Encoding(#[from] IndexError),
}

impl HashedIndex {
    /// The keys under which this index files the entity in the state `entity`
    /// describes. A commit derives them for its log rows, so that a digest
    /// tree can fold the commit without reading entity state.
    pub fn keys(&self, entity_id: EntityId, entity: &impl Filterable) -> Result<BTreeSet<Vec<u8>>, KeyDerivationError> {
        match self {
            Self::EntityId => Ok(BTreeSet::from([entity_id.to_bytes().to_vec()])),
            Self::Component { component, key_spec } => {
                if !entity.is_member_of(component)? {
                    return Ok(BTreeSet::new());
                }
                let mut values = Vec::with_capacity(key_spec.keyparts.len());
                for part in &key_spec.keyparts {
                    let value = entity.value(&part.key);
                    let value = match &part.sub_path {
                        None => value,
                        Some(path) => value.and_then(|value| value.extract_at_path(path)),
                    };
                    match value {
                        Some(value) => values.push(value),
                        None => return Ok(BTreeSet::new()),
                    }
                }
                Ok(BTreeSet::from([encode_tuple_values_with_key_spec(&values, key_spec)?]))
            }
        }
    }
}
