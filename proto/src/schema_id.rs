//! Deterministic identity derivation for the metadata catalog
//! (specs/model-property-metadata/rfc.md section 5.1).
//!
//! Any two nodes that register the same definition against the same system
//! root must mint byte-identical entity ids with no coordination, so this
//! derivation is protocol-normative: the domain tags, the length
//! prefixing, and the 16-byte truncation are all load-bearing. Changing
//! any of them is a wire-compatibility change and requires new versioned
//! domain tags (".v2"), never an in-place edit.
//!
//! Ids are scoped to the system root: two systems never mint colliding
//! catalog ids, and ids die with the system on hard_reset. The full 16
//! bytes are hash output; EntityId's nominal ulid timestamp bits carry
//! hash bytes here, and nothing may interpret them as time (truncating
//! entropy further is unsound against a name-controlling adversary).

use sha2::{Digest, Sha256};

use crate::id::EntityId;

const MODEL_TAG: &[u8] = b"ankurah.model.v1";
const PROPERTY_TAG: &[u8] = b"ankurah.property.v1";
const MEMBERSHIP_TAG: &[u8] = b"ankurah.membership.v1";

/// The minting scope for standalone declarations that belong to no model
/// (e.g. shared properties authored declaratively; RFC 5.1, 5.10). Ships
/// ahead of its consumer: the declarative DDL path (#301) is the caller;
/// it lives here because the scope constant is derivation-normative.
pub fn standalone_scope() -> EntityId { EntityId::from_bytes([0u8; 16]) }

/// id of the model definition entity for `collection`, scoped to `root`.
pub fn model_entity_id(root: &EntityId, collection: &str) -> EntityId {
    let mut h = Sha256::new();
    h.update(MODEL_TAG);
    update_field(&mut h, &root.to_bytes());
    update_field(&mut h, collection.as_bytes());
    finish(h)
}

/// id of the property definition entity derived by name.
///
/// `minting_model` is the derivation SCOPE (provenance), not ownership: it
/// keeps same-named fields of unrelated models from converging on one
/// property entity. `anchor` is the permanent derivation name (the current
/// field name unless an anchor attribute pinned an earlier one), and
/// (`backend`, `value_type`) come from the normative mapping table (RFC 4):
/// a retype derives a NEW property identity by design.
pub fn property_entity_id(root: &EntityId, minting_model: &EntityId, anchor: &str, backend: &str, value_type: &str) -> EntityId {
    let mut h = Sha256::new();
    h.update(PROPERTY_TAG);
    update_field(&mut h, &root.to_bytes());
    update_field(&mut h, &minting_model.to_bytes());
    update_field(&mut h, anchor.as_bytes());
    update_field(&mut h, backend.as_bytes());
    update_field(&mut h, value_type.as_bytes());
    finish(h)
}

/// id of the (model, property) contract-membership entity.
pub fn membership_entity_id(root: &EntityId, model: &EntityId, property: &EntityId) -> EntityId {
    let mut h = Sha256::new();
    h.update(MEMBERSHIP_TAG);
    update_field(&mut h, &root.to_bytes());
    update_field(&mut h, &model.to_bytes());
    update_field(&mut h, &property.to_bytes());
    finish(h)
}

/// Every field is length-prefixed (u64 little-endian) so no concatenation
/// of adjacent fields can encode the same bytes as a different split.
fn update_field(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u64).to_le_bytes());
    h.update(bytes);
}

fn finish(h: Sha256) -> EntityId {
    let digest = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    EntityId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> EntityId { EntityId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]) }

    /// Golden vectors: these pin the derivation byte-for-byte. If one of
    /// these fails, the change breaks id agreement with every deployed
    /// node; mint new versioned domain tags instead of editing.
    #[test]
    fn golden_vectors() {
        let model = model_entity_id(&root(), "album");
        let property = property_entity_id(&root(), &model, "name", "yrs", "string");
        let membership = membership_entity_id(&root(), &model, &property);
        assert_eq!(
            (model.to_base64(), property.to_base64(), membership.to_base64()),
            ("7EEOREMk_vNTe-6zDStY4A".into(), "ucNox8dPqT2t-3KTnEgg7g".into(), "-HZJdv0XAT0zMybc99OPlQ".into())
        );
    }

    #[test]
    fn derivations_are_deterministic_and_root_scoped() {
        let other_root = EntityId::from_bytes([2u8; 16]);
        assert_eq!(model_entity_id(&root(), "album"), model_entity_id(&root(), "album"));
        assert_ne!(model_entity_id(&root(), "album"), model_entity_id(&other_root, "album"));
        assert_ne!(model_entity_id(&root(), "album"), model_entity_id(&root(), "albums"));
    }

    /// Length prefixing must prevent boundary sliding between adjacent
    /// fields: ("ab","c") and ("a","bc") concatenate identically.
    #[test]
    fn length_prefix_prevents_boundary_sliding() {
        let model = model_entity_id(&root(), "album");
        let a = property_entity_id(&root(), &model, "ab", "c", "string");
        let b = property_entity_id(&root(), &model, "a", "bc", "string");
        assert_ne!(a, b);
    }

    /// The three kinds have distinct domain tags: identical field bytes
    /// must never collide across kinds.
    #[test]
    fn kinds_are_domain_separated() {
        let m = EntityId::from_bytes([3u8; 16]);
        let p = EntityId::from_bytes([4u8; 16]);
        assert_ne!(membership_entity_id(&root(), &m, &p), property_entity_id(&root(), &m, "", "", ""));
    }

    #[test]
    fn standalone_scope_derives_distinct_ids() {
        let model = model_entity_id(&root(), "album");
        let scoped = property_entity_id(&root(), &model, "name", "yrs", "string");
        let standalone = property_entity_id(&root(), &standalone_scope(), "name", "yrs", "string");
        assert_ne!(scoped, standalone);
    }
}
