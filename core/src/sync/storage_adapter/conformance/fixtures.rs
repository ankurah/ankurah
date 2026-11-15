//! The trees of items the suite's scenarios keep, and what they do with
//! them: the items and their key spec, creating a tree and committing changes
//! to it, checking what it holds, and finding the storage adapter's own error in
//! a failure. An engine's own stories keep and check trees the same way.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use ankurah_core_types::key_spec::{IndexKeyPart, KeySpec};
use ankurah_core_types::{Value, ValueType};
use ankurah_digest_tree::test_support::{member_digest, sum_digests};
use ankurah_digest_tree::{DigestGroup, DigestTree, Encode, Member, MemberId, Payload, RowKey, Span, StoreError};
use futures::TryStreamExt;

use crate::error::{MutationError, RetrievalError};
use crate::storage::{MapRead, MapRows, MapSnapshot, ScanDirection, StorageEngine, StorageTransaction};
use crate::sync::storage_adapter::cache::admits;
use crate::sync::{CacheSettings, EngineAdapter, Error, TreeChange, TreeId};

mod test_stores;
pub use test_stores::TestStores;

/// The one property the trees key by: an item's number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Number;

impl Encode for Number {
    fn encode(&self) -> Vec<u8> { Vec::new() }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        match bytes {
            [] => Ok(Number),
            _ => Err(StoreError::permanent("bytes that are no property")),
        }
    }
}

/// An item's id: eight bytes, big-endian.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id(pub u64);

impl MemberId for Id {
    fn to_bytes(&self) -> Cow<'_, [u8]> { Cow::Owned(self.0.to_be_bytes().to_vec()) }

    fn from_bytes(bytes: &[u8]) -> Option<Self> { bytes.try_into().ok().map(u64::from_be_bytes).map(Id) }
}

/// What a tree keeps with an item, and digests: a label, as it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Label(pub Vec<u8>);

impl Payload for Label {
    fn digest_bytes(&self) -> Cow<'_, [u8]> { Cow::Borrowed(&self.0) }
}

impl Encode for Label {
    fn encode(&self) -> Vec<u8> { self.0.clone() }

    fn decode(bytes: &[u8]) -> Result<Self, StoreError> { Ok(Label(bytes.to_vec())) }
}

/// A member: an id, a number, which an item without one can't give, and a
/// label.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    pub id: Id,
    pub number: Option<i64>,
    pub label: Label,
}

/// An item that has no number, and so no key.
#[derive(Debug, thiserror::Error)]
#[error("the item has no number")]
pub struct NoNumber;

impl Member<Number> for Item {
    type Id = Id;
    type Payload = Label;
    type KeyError = NoNumber;

    fn id(&self) -> Id { self.id }

    fn value(&self, _: &IndexKeyPart<Number>) -> Result<Option<Value>, NoNumber> {
        self.number.map(|number| Some(Value::I64(number))).ok_or(NoNumber)
    }

    fn payload(&self) -> Label { self.label.clone() }
}

/// Item `id` numbered `number`, labelled by its label's text, or by nothing:
/// `item!(3, 30)`, `item!(3, 30, "relabelled")`, and `item!(3, None)` for one
/// without a number.
#[doc(hidden)]
#[macro_export]
macro_rules! __digest_trees_fixtures_item {
    ($id:expr, $number:expr) => {
        $crate::__digest_trees_fixtures_item!($id, $number, "")
    };

    ($id:expr, $number:expr, $label:expr) => {
        $crate::sync::conformance::fixtures::Item {
            id: $crate::sync::conformance::fixtures::Id($id),
            number: ::std::option::Option::<i64>::from($number),
            label: $crate::sync::conformance::fixtures::Label(::std::vec::Vec::from($label)),
        }
    };
}

#[doc(inline)]
pub use crate::__digest_trees_fixtures_item as item;

pub(crate) use crate::test_helpers::map_conformance::fixtures::{before_hang, entity, poll_once};

/// The key spec the trees are keyed by: an item's number, ascending.
pub fn by_number() -> KeySpec<Number> { KeySpec::new(vec![IndexKeyPart::asc(Number, ValueType::I64)]) }

/// A tree of items on the engine `SE`, summed in the group `G`.
pub type Tree<SE, G> = DigestTree<Number, Id, Label, G, EngineAdapter<SE, Number, Label, G>>;

/// Changes to a tree of items on the engine `SE`.
pub type Changes<SE, G> = ankurah_digest_tree::Transaction<Number, Id, Label, G, EngineAdapter<SE, Number, Label, G>>;

/// Check that `tree` holds exactly `items`: their ids in key order, their
/// digest, and every row of the tree consistent with them.
pub async fn assert_holds<SE: StorageEngine + 'static, G: DigestGroup>(tree: &Tree<SE, G>, items: &[Item]) -> anyhow::Result<()> {
    let mut expected = items.to_vec();
    expected.sort_by_key(|item| (item.number, item.id));

    let read: Vec<Id> = tree.view().range(..)?.map_ok(|leaf| leaf.id).try_collect().await?;
    assert_eq!(read, expected.iter().map(|item| item.id).collect::<Vec<_>>(), "the members, in key order");

    let digests = items.iter().map(|item| member_digest::<_, _, G>(&by_number(), item)).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(tree.view().span_digest(Span::all()).await?.digest, sum_digests(digests)?, "the tree's digest");

    assert_eq!(tree.audit().await?.members, items.len() as u64);
    Ok(())
}

/// A tree's rows as they stood at some moment, by their keys' bytes.
pub type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

/// The tree's rows as they stand, in a snapshot of their own.
pub async fn rows_now<SE: StorageEngine + 'static, G: DigestGroup>(engine: &Arc<SE>, tree: &Tree<SE, G>) -> anyhow::Result<Rows> {
    let rows = tree.store().inner().rows.clone();
    let everything =
        MapRead::Range { map: rows, start: Bound::Unbounded, end: Bound::Unbounded, direction: ScanDirection::Forward, limit: usize::MAX };
    let MapRows::Ranged(rows) = engine.map_snapshot(&[]).await?.map_read(vec![everything]).await?.remove(0) else {
        panic!("a range's entries")
    };

    Ok(rows.into_iter().collect())
}

/// Whether the row under the key these bytes encode is one the record cache
/// keeps.
pub fn cached(key: &[u8]) -> bool { RowKey::decode(key).is_ok_and(|key| admits(&key)) }

/// The storage adapter's own error that a call's failure carries, if it failed
/// with one.
pub fn cause<T, E: Carries>(result: &Result<T, E>) -> Option<&Error> { result.as_ref().err().and_then(Carries::sync_error) }

/// A failure that may carry the storage adapter's own error, somewhere among its
/// sources.
pub trait Carries {
    fn sync_error(&self) -> Option<&Error>;
}

impl Carries for MutationError {
    fn sync_error(&self) -> Option<&Error> {
        match self {
            MutationError::General(error) => among_sources(error.as_ref()),
            _ => None,
        }
    }
}

impl Carries for RetrievalError {
    fn sync_error(&self) -> Option<&Error> {
        match self {
            RetrievalError::StorageError(error) => among_sources(error.as_ref()),
            _ => None,
        }
    }
}

impl Carries for ankurah_digest_tree::Error {
    fn sync_error(&self) -> Option<&Error> { among_sources(self) }
}

fn among_sources<'e>(mut error: &'e (dyn std::error::Error + 'static)) -> Option<&'e Error> {
    loop {
        if let Some(found) = error.downcast_ref() {
            return Some(found);
        }

        error = error.source()?;
    }
}

/// A node's test stores, with room for every record the scenarios read by key.
pub fn stores<SE: StorageEngine + 'static>(engine: &Arc<SE>) -> TestStores<SE> {
    TestStores::new(Arc::clone(engine), CacheSettings { bytes: 1 << 20 })
}

/// Test stores with caching disabled, so every read reaches storage.
pub fn uncached_stores<SE: StorageEngine + 'static>(engine: &Arc<SE>) -> TestStores<SE> {
    TestStores::new(Arc::clone(engine), CacheSettings { bytes: 0 })
}

/// Create tree `id` holding `items` on `trees`, in a storage transaction of
/// its own.
pub async fn created<SE: StorageEngine + 'static, G: DigestGroup>(
    trees: &TestStores<SE>,
    engine: &SE,
    id: &str,
    items: impl IntoIterator<Item = Item>,
) -> anyhow::Result<Tree<SE, G>> {
    let mut trx = engine.transaction();
    let created = trees.create(&mut trx, TreeId::new(id), by_number(), items).await?;
    trx.commit().await?.committed()?;

    Ok(created.landed())
}

/// Apply `changes` to `tree` on `trees`, in a storage transaction of its own
/// on `engine` that commits, and land them.
pub async fn commit<SE: StorageEngine + 'static, G: DigestGroup>(
    trees: &TestStores<SE>,
    engine: &SE,
    tree: &Tree<SE, G>,
    changes: impl FnOnce(&mut Changes<SE, G>) -> Result<(), ankurah_digest_tree::Error>,
) -> anyhow::Result<()> {
    let mut transaction = tree.transaction();
    changes(&mut transaction)?;

    let mut trx = engine.transaction();
    let applied = trees.apply(&mut trx, vec![TreeChange::Changes(transaction)]).await?;
    let committed = trx.commit().await?.committed()?;
    applied.landed(&committed);

    Ok(())
}
