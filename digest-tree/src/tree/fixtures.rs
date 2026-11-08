//! What the tree's tests share beyond the members and stores: a member's
//! key, and a tree's nodes and rows as its store holds them.

use ankurah_core_types::key_spec::KeySpec;
use ankurah_core_types::ValueType;

use crate::common::{asc, item, stored_rows, ItemTree, Property, Tag};
use crate::key::member_key;
use crate::{DigestGroup, NodePrefix, ReadTrx, Row, RowKey, Store, StoreTrx, WriteItem};

/// The key of a member holding `value` for property `A`, ascending.
pub(super) fn key_of(value: impl Into<Option<i64>>) -> Vec<u8> {
    let value: Option<i64> = value.into();
    member_key(&KeySpec::new(vec![asc!(Property::A, ValueType::I64)]), &item!(0, value, b"")).unwrap()
}

impl<G: DigestGroup> ItemTree<G> {
    /// The prefix of every node of the tree's trie.
    pub(super) async fn node_prefixes(&self) -> Vec<NodePrefix> {
        let rows = stored_rows(self.store()).await;
        rows.into_iter()
            .filter_map(|(key, _)| match key {
                RowKey::Node(prefix) => Some(prefix),
                _ => None,
            })
            .collect()
    }

    /// The row the tree's store holds under `key`, for tests that damage
    /// rows.
    pub(super) async fn stored_row(&self, key: &RowKey) -> Option<Row<Property, Tag, G>> {
        self.store().begin_read().await.unwrap().get(key).await.unwrap()
    }

    /// Write `batch` to the tree's store behind the tree's back, as damage
    /// would.
    pub(super) async fn damage(&self, batch: Vec<WriteItem<RowKey, Row<Property, Tag, G>>>) {
        let mut trx = self.store().begin().await;
        trx.write(batch).await.unwrap();
        trx.commit().await.unwrap();
    }
}
