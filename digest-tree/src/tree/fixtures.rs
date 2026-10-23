//! Fixtures the tree's unit tests share: entities, leaf rows, prefixes, an
//! empty tree, and the macro that runs a test in each group.

use ankql::ast::Predicate;
use ankurah_core_types::{EntityId, KeySpec};

use std::fmt;
use std::marker::PhantomData;

use crate::digest::{DecodeError, DigestGroup, HeadHash, Leaf, Ristretto255};
use crate::store::EncodedLeaf;
use crate::{DigestTree, LeafRow, NodePrefix, TreeSpec};

pub(crate) fn entity(n: u8) -> EntityId { EntityId::from_bytes([n; 32]) }

/// The leaf row of `entity` under `key`, with a head of its own.
pub(crate) fn row<G: DigestGroup>(key: &[u8], entity: EntityId) -> LeafRow<G> {
    let head = HeadHash::of([entity.to_bytes()]);
    LeafRow { key: key.to_vec(), entity_id: entity, head, element: Leaf::new(key, entity, head).element() }
}

/// `row` with its address and value encoded, as a write stores it.
pub(crate) fn encoded<G: DigestGroup>(row: LeafRow<G>) -> EncodedLeaf<G> { EncodedLeaf::new(row.address(), row) }

/// The prefix spelled by `text`, one character per bit.
pub(crate) fn prefix(text: &str) -> NodePrefix { text.chars().fold(NodePrefix::root(), |prefix, bit| prefix.child(bit == '1')) }

/// An empty tree. The tree files whatever addresses it is given, so its
/// spec plays no part in these tests.
pub(crate) fn empty_tree<G: DigestGroup>() -> DigestTree<G> { DigestTree::new(TreeSpec::new(KeySpec::new(vec![]), Predicate::True)) }

/// Runs each named test, generic over the group, once in each group, so
/// that both pass the same suite.
macro_rules! in_each_group {
    ($($test:ident),* $(,)?) => {
        mod ristretto255 {
            $(#[tokio::test]
            async fn $test() { super::$test::<crate::digest::Ristretto255>().await })*
        }
        mod lthash16 {
            $(#[tokio::test]
            async fn $test() { super::$test::<crate::digest::LtHash16>().await })*
        }
    };
}
pub(crate) use in_each_group;

/// What a test counts or faults in a [`Hooked`] group: each hook runs before
/// the ristretto255 operation it names, and does nothing unless a test says.
pub(crate) trait Hook: Copy + Eq + fmt::Debug + Send + Sync + 'static {
    fn add() {}
    fn hash_leaf(_encoded_leaf: &[u8]) {}
    fn encode() {}
    fn decode() {}
}

/// Ristretto255 with a hook before some of its operations, to count or fault
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Hooked<H: Hook>(PhantomData<H>);

impl<H: Hook> DigestGroup for Hooked<H> {
    type Element = <Ristretto255 as DigestGroup>::Element;
    const ROW_VERSION: u8 = Ristretto255::ROW_VERSION;
    const ELEMENT_LEN: usize = Ristretto255::ELEMENT_LEN;
    fn zero() -> Self::Element { Ristretto255::zero() }
    fn add(sum: &mut Self::Element, other: &Self::Element) {
        H::add();
        Ristretto255::add(sum, other);
    }
    fn sub(sum: &mut Self::Element, other: &Self::Element) { Ristretto255::sub(sum, other); }
    fn neg(element: &mut Self::Element) { Ristretto255::neg(element); }
    fn hash_leaf(encoded_leaf: &[u8]) -> Self::Element {
        H::hash_leaf(encoded_leaf);
        Ristretto255::hash_leaf(encoded_leaf)
    }
    fn encode(element: &Self::Element, out: &mut [u8]) {
        H::encode();
        Ristretto255::encode(element, out);
    }
    fn decode(bytes: &[u8]) -> Result<Self::Element, DecodeError> {
        H::decode();
        Ristretto255::decode(bytes)
    }
}
