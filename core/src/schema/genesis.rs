//! The frozen genesis encoder for catalog entities
//! (specs/model-property-metadata/rfc.md section 5.1).
//!
//! Catalog convergence rests on two nodes independently minting
//! byte-identical genesis events for the same definition. EventId is a
//! content hash over the operation bytes, so EVERY byte here is
//! identity-critical: this module is pinned, forever, to the LWW diff
//! encoding as it existed at Phase A (LWWDiff version 1 wrapping a
//! name-keyed bincode BTreeMap of scalar Values), and it must NEVER follow
//! the live LWW encoder through version bumps. That is why nothing here
//! calls into property::backend::lww: the shapes are mirrored locally and
//! pinned by golden-vector tests. If a golden test fails, the change
//! breaks genesis agreement with every deployed node; the answer is to
//! revert, not to update the vector.
//!
//! A genesis carries exactly the identity-key fields and an empty parent
//! clock. Everything else (membership `optional`, `target_model`, display
//! name changes) is follow-up events, which do not participate in
//! identity and use the node's current encoding.
//!
//! Catalog genesis is SELF-CERTIFYING (RFC 4): the entity id must equal
//! the derivation over the payload and the event id must equal the frozen
//! encoding, so [`validate_catalog_genesis`] checks both by recomputation
//! and receivers need not trust the channel.

use std::collections::BTreeMap;

use ankurah_proto::{self as proto, schema_id, CollectionId, EntityId};
use serde::{Deserialize, Serialize};

use super::{model_collection, model_property_collection, property_collection};

/// Pinned copy of the Phase A LWW diff version. Deliberately NOT the live
/// constant in property::backend::lww.
const FROZEN_LWW_DIFF_VERSION: u8 = 1;
/// Pinned copy of the LWW backend's registry name.
const FROZEN_BACKEND_KEY: &str = "lww";

/// Byte-level mirror of the live `LWWDiff` wire shape as of Phase A.
#[derive(Serialize, Deserialize)]
struct FrozenLWWDiff {
    version: u8,
    data: Vec<u8>,
}

/// Byte-level mirror of `crate::value::Value` as of Phase A. bincode
/// encodes the variant INDEX, so the unused variants exist to pin the
/// indices of the used ones (String = 5, EntityId = 6). A genesis may
/// only carry String and EntityId; the others are here so a tampered
/// payload decodes far enough to be rejected on shape rather than
/// erroring opaquely.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum FrozenValue {
    I16(i16),
    I32(i32),
    I64(i64),
    F64(f64),
    Bool(bool),
    String(String),
    EntityId(EntityId),
    Object(Vec<u8>),
    Binary(Vec<u8>),
    Json(Vec<u8>),
}

/// Genesis for a model definition entity: sets exactly `collection`.
pub fn model_genesis(root: &EntityId, collection: &str) -> proto::Event {
    let entity_id = schema_id::model_entity_id(root, collection);
    let fields = BTreeMap::from([("collection".to_string(), Some(FrozenValue::String(collection.to_string())))]);
    frozen_event(entity_id, model_collection(), &fields)
}

/// Genesis for a property definition entity: sets exactly `minted_for`,
/// `name` (the anchor), `backend`, `value_type`.
pub fn property_genesis(root: &EntityId, minting_model: &EntityId, anchor: &str, backend: &str, value_type: &str) -> proto::Event {
    let entity_id = schema_id::property_entity_id(root, minting_model, anchor, backend, value_type);
    let fields = BTreeMap::from([
        ("minted_for".to_string(), Some(FrozenValue::EntityId(*minting_model))),
        ("name".to_string(), Some(FrozenValue::String(anchor.to_string()))),
        ("backend".to_string(), Some(FrozenValue::String(backend.to_string()))),
        ("value_type".to_string(), Some(FrozenValue::String(value_type.to_string()))),
    ]);
    frozen_event(entity_id, property_collection(), &fields)
}

/// Genesis for a (model, property) membership entity: sets exactly
/// `model` and `property`.
pub fn membership_genesis(root: &EntityId, model: &EntityId, property: &EntityId) -> proto::Event {
    let entity_id = schema_id::membership_entity_id(root, model, property);
    let fields = BTreeMap::from([
        ("model".to_string(), Some(FrozenValue::EntityId(*model))),
        ("property".to_string(), Some(FrozenValue::EntityId(*property))),
    ]);
    frozen_event(entity_id, model_property_collection(), &fields)
}

fn frozen_event(entity_id: EntityId, collection: CollectionId, fields: &BTreeMap<String, Option<FrozenValue>>) -> proto::Event {
    let data = bincode::serialize(fields).expect("frozen genesis field encoding is infallible");
    let diff =
        bincode::serialize(&FrozenLWWDiff { version: FROZEN_LWW_DIFF_VERSION, data }).expect("frozen genesis diff encoding is infallible");
    let operations = proto::OperationSet(BTreeMap::from([(FROZEN_BACKEND_KEY.to_string(), vec![proto::Operation { diff }])]));
    proto::Event { collection, entity_id, operations, parent: proto::Clock::default() }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum CatalogGenesisError {
    #[error("not a catalog collection: {0}")]
    NotACatalogCollection(CollectionId),
    #[error("catalog genesis must have an empty parent clock")]
    NotGenesis,
    #[error("malformed catalog genesis operations: {0}")]
    MalformedOperations(String),
    #[error("catalog genesis field set does not match its kind: {0}")]
    WrongFields(String),
    #[error("entity id does not match the derivation over the payload (claimed {claimed}, derived {derived})")]
    EntityIdMismatch { claimed: EntityId, derived: EntityId },
    #[error("event id does not match the frozen encoding of the payload")]
    EventIdMismatch,
}

/// Validate a claimed catalog genesis by recomputation: decode the
/// payload strictly, re-derive the entity id from it, re-encode it
/// through the frozen encoder, and require both ids to match. `root` is
/// the validating node's system root.
pub fn validate_catalog_genesis(root: &EntityId, event: &proto::Event) -> Result<(), CatalogGenesisError> {
    if !event.parent.is_empty() {
        return Err(CatalogGenesisError::NotGenesis);
    }

    let fields = decode_frozen_fields(event)?;
    let field = |name: &str| fields.get(name).and_then(|v| v.as_ref());
    let expect_string = |name: &str| match field(name) {
        Some(FrozenValue::String(s)) => Ok(s.clone()),
        _ => Err(CatalogGenesisError::WrongFields(format!("expected string field '{name}'"))),
    };
    let expect_entity_id = |name: &str| match field(name) {
        Some(FrozenValue::EntityId(id)) => Ok(*id),
        _ => Err(CatalogGenesisError::WrongFields(format!("expected entity id field '{name}'"))),
    };
    let expect_field_count = |n: usize| {
        if fields.len() == n {
            Ok(())
        } else {
            Err(CatalogGenesisError::WrongFields(format!("expected {n} fields, found {}", fields.len())))
        }
    };

    let reconstructed = if event.collection == model_collection() {
        expect_field_count(1)?;
        model_genesis(root, &expect_string("collection")?)
    } else if event.collection == property_collection() {
        expect_field_count(4)?;
        property_genesis(
            root,
            &expect_entity_id("minted_for")?,
            &expect_string("name")?,
            &expect_string("backend")?,
            &expect_string("value_type")?,
        )
    } else if event.collection == model_property_collection() {
        expect_field_count(2)?;
        membership_genesis(root, &expect_entity_id("model")?, &expect_entity_id("property")?)
    } else {
        return Err(CatalogGenesisError::NotACatalogCollection(event.collection.clone()));
    };

    if reconstructed.entity_id != event.entity_id {
        return Err(CatalogGenesisError::EntityIdMismatch { claimed: event.entity_id, derived: reconstructed.entity_id });
    }
    // The event id hashes the operation bytes and the parent, so this
    // catches any encoding drift, including trailing bytes a lenient
    // decode would otherwise ignore.
    if reconstructed.id() != event.id() {
        return Err(CatalogGenesisError::EventIdMismatch);
    }
    Ok(())
}

fn decode_frozen_fields(event: &proto::Event) -> Result<BTreeMap<String, Option<FrozenValue>>, CatalogGenesisError> {
    let malformed = |reason: &str| CatalogGenesisError::MalformedOperations(reason.to_string());
    if event.operations.0.len() != 1 {
        return Err(malformed("expected exactly one backend"));
    }
    let ops = event.operations.0.get(FROZEN_BACKEND_KEY).ok_or_else(|| malformed("expected the lww backend"))?;
    let [op] = ops.as_slice() else {
        return Err(malformed("expected exactly one operation"));
    };
    let diff: FrozenLWWDiff = bincode::deserialize(&op.diff).map_err(|_| malformed("undecodable diff"))?;
    if diff.version != FROZEN_LWW_DIFF_VERSION {
        return Err(malformed("diff version is not the frozen version 1"));
    }
    bincode::deserialize(&diff.data).map_err(|_| malformed("undecodable field map"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::property::backend::{LWWBackend, PropertyBackend};
    use crate::value::Value;

    fn root() -> EntityId { EntityId::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]) }

    fn all_three() -> (proto::Event, proto::Event, proto::Event) {
        let model = model_genesis(&root(), "album");
        let property = property_genesis(&root(), &model.entity_id, "name", "yrs", "string");
        let membership = membership_genesis(&root(), &model.entity_id, &property.entity_id);
        (model, property, membership)
    }

    /// Pins the frozen encoding byte-for-byte (the event id hashes the
    /// operation bytes). A failure here means genesis agreement with
    /// every deployed node is broken: revert the change, do not update
    /// the vector.
    #[test]
    fn golden_vectors() {
        let (model, property, membership) = all_three();
        assert_eq!(
            (model.id().to_base64(), property.id().to_base64(), membership.id().to_base64()),
            (
                "niAyoeZc3-5IfwZBuuAY8aZ-SG03FPlYIPLr62OGThc".into(),
                "np0NHoScvybAnmlILtt53D7Tf46XdV8t9LH23mYS5Q0".into(),
                "8bcRwZQ0u0Qv9CdB4AzEcCJt78IWcH42DGY0DRfCXXU".into()
            )
        );
    }

    #[test]
    fn genesis_is_deterministic() {
        let (m1, p1, ms1) = all_three();
        let (m2, p2, ms2) = all_three();
        assert_eq!((m1.id(), p1.id(), ms1.id()), (m2.id(), p2.id(), ms2.id()));
    }

    /// The live LWW backend must read a frozen genesis exactly as written:
    /// this catches drift in the live decoder, the Value enum layout, or
    /// the frozen mirror, whichever moves.
    #[test]
    fn live_backend_reads_frozen_genesis() {
        let (model, property, _) = all_three();

        let backend = LWWBackend::new();
        backend.apply_operations(model.operations.0.get("lww").unwrap()).unwrap();
        assert_eq!(backend.property_values().get("collection"), Some(&Some(Value::String("album".into()))));

        let backend = LWWBackend::new();
        backend.apply_operations(property.operations.0.get("lww").unwrap()).unwrap();
        let values = backend.property_values();
        assert_eq!(values.get("minted_for"), Some(&Some(Value::EntityId(model.entity_id))));
        assert_eq!(values.get("name"), Some(&Some(Value::String("name".into()))));
        assert_eq!(values.get("backend"), Some(&Some(Value::String("yrs".into()))));
        assert_eq!(values.get("value_type"), Some(&Some(Value::String("string".into()))));
    }

    #[test]
    fn validation_accepts_own_output() {
        let (model, property, membership) = all_three();
        assert_eq!(validate_catalog_genesis(&root(), &model), Ok(()));
        assert_eq!(validate_catalog_genesis(&root(), &property), Ok(()));
        assert_eq!(validate_catalog_genesis(&root(), &membership), Ok(()));
    }

    #[test]
    fn validation_rejects_tampering() {
        let (model, property, _) = all_three();

        // Payload value swapped: derivation no longer matches the claimed id.
        let mut tampered = model.clone();
        tampered.operations = model_genesis(&root(), "albums").operations;
        assert!(matches!(validate_catalog_genesis(&root(), &tampered), Err(CatalogGenesisError::EntityIdMismatch { .. })));

        // Wrong root: same payload, different derivation scope.
        let other_root = EntityId::from_bytes([9u8; 16]);
        assert!(matches!(validate_catalog_genesis(&other_root, &model), Err(CatalogGenesisError::EntityIdMismatch { .. })));

        // Non-empty parent is not a genesis.
        let mut tampered = model.clone();
        tampered.parent = proto::Clock::new([model.id()]);
        assert_eq!(validate_catalog_genesis(&root(), &tampered), Err(CatalogGenesisError::NotGenesis));

        // Trailing junk decodes leniently but re-encodes canonically.
        let mut tampered = model.clone();
        let op = &mut tampered.operations.0.get_mut("lww").unwrap()[0];
        let mut diff: FrozenLWWDiff = bincode::deserialize(&op.diff).unwrap();
        diff.data.extend_from_slice(&[0xFF]);
        op.diff = bincode::serialize(&diff).unwrap();
        assert!(matches!(
            validate_catalog_genesis(&root(), &tampered),
            Err(CatalogGenesisError::EventIdMismatch) | Err(CatalogGenesisError::MalformedOperations(_))
        ));

        // A field set from the wrong kind.
        let mut tampered = property.clone();
        tampered.operations = model.operations.clone();
        assert!(matches!(validate_catalog_genesis(&root(), &tampered), Err(CatalogGenesisError::WrongFields(_))));

        // Extra backend entry.
        let mut tampered = model.clone();
        tampered.operations.0.insert("yrs".to_string(), vec![]);
        assert!(matches!(validate_catalog_genesis(&root(), &tampered), Err(CatalogGenesisError::MalformedOperations(_))));

        // Not a catalog collection at all.
        let mut tampered = model.clone();
        tampered.collection = CollectionId::fixed_name("album");
        assert!(matches!(validate_catalog_genesis(&root(), &tampered), Err(CatalogGenesisError::NotACatalogCollection(_))));
    }
}
