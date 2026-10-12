//! Fixtures for the API tests: a resolved `selection!`, the property types,
//! the codec, and a tree filled with a few entities.
#![allow(dead_code)]

use ankql::ast::{Parsed, Resolved, Selection};
use ankurah_core_types::{EntityId, ModelId, PropertyId, PropertyPath, Value, ValueType};
use ankurah_digest_tree::digest::HeadHash;
use ankurah_digest_tree::{DigestTree, KeyCodec, LeafChange, LeafData, PropertyTypes, Ristretto255, TreeSpec};
use std::collections::HashMap;

/// `selection!` bound to the fixture's properties: project, foo and bar are properties 1, 2 and 3.
#[macro_export]
macro_rules! selection {
    ($($tokens:tt)*) => { $crate::common::resolve(ankurah_derive::selection!($($tokens)*)) };
}

pub fn property(name: &str) -> PropertyId {
    PropertyId::EntityId(EntityId::from_bytes([match name { "project" => 1, "foo" => 2, "bar" => 3, other => panic!("no property {other}") }; 32]))
}

pub fn resolve(selection: Selection<Parsed>) -> Selection<Resolved> {
    let model = ModelId::EntityId(EntityId::from_bytes([0x77; 32]));
    ankql::selection::map_references(
        &selection,
        &|path| PropertyPath::registered(property_entity(&path.steps[0]), &path.steps[0], path.steps[1..].to_vec()),
        &|_| model,
    )
}

fn property_entity(name: &str) -> EntityId {
    match property(name) {
        PropertyId::EntityId(id) => id,
        other => panic!("{other:?}"),
    }
}

/// The value type of each property: project a string, foo and bar numbers.
pub fn types() -> impl PropertyTypes {
    HashMap::from([(property("project"), ValueType::String), (property("foo"), ValueType::I64), (property("bar"), ValueType::I64)])
}

/// The canonical encoding, as the crate is handed it.
pub fn codec() -> impl KeyCodec { ankurah_digest_tree::CanonicalCodec }

/// Four acme entities with foo 9, 10, 12, 15, each at its own head.
/// All satisfy the example's admission predicate, project = 'acme' AND foo > 7.
pub async fn populated_tree(selection: &Selection<Resolved>) -> DigestTree<Ristretto255> {
    let tree = DigestTree::new(TreeSpec::for_selection(selection, &types()).unwrap());
    let codec = codec();
    let mut tx = tree.begin();
    for (n, (project, foo)) in [("acme", 9), ("acme", 10), ("acme", 12), ("acme", 15)].into_iter().enumerate() {
        let key = codec.encode_prefix(tree.spec().key_spec(), &[Value::String(project.into()), Value::I64(foo)]).unwrap();
        let leaf = LeafData { key, entity_id: entity(n as u8), head: HeadHash::of([[0x80 + n as u8; 32]]) };
        tx.apply_change(LeafChange { before: None, after: Some(leaf) }).await.unwrap();
    }
    tx.commit().await.unwrap();
    tree
}

pub fn entity(n: u8) -> EntityId { EntityId::from_bytes([n; 32]) }
