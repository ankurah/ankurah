//! The leaf rows and the entity lookup: reading leaves by address, by span
//! or by entity, and writing and removing them with the lookup kept in step.

use std::collections::BTreeSet;

use ankurah_core_types::EntityId;

use super::TreeTransaction;
use crate::address::{leaf_address, TreeSpan};
use crate::digest::DigestGroup;
use crate::error::TreeError;
use crate::store::layout;
use crate::store::{LeafRow, MapAccess, MapName, PreparedLeaf};

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// The leaf row of the entity `entity_id` filed under `key`.
    pub async fn leaf(&mut self, key: &[u8], entity_id: EntityId) -> Result<Option<LeafRow<G>>, TreeError> {
        let address = leaf_address(key, entity_id);
        self.transaction.get(MapName::Leaves, &address).await?.map(|value| layout::leaf_row(&address, &value)).transpose()
    }

    /// Every leaf row of an entity, one per key it is filed under, in address
    /// order: how a change to the entity finds the leaves it must subtract or
    /// move, since the change names only the entity. A missing leaf named by
    /// the lookup is an error: omitting it would lose the old contribution
    /// that a change must subtract.
    pub async fn entity_leaves(&mut self, entity_id: EntityId) -> Result<Vec<LeafRow<G>>, TreeError> {
        let (_, keys) = self.keys_of(entity_id).await?;
        let mut rows = Vec::new();
        for key in keys {
            let row = self.leaf(&key, entity_id).await?.ok_or(TreeError::MissingLeaf { entity_id, key })?;
            rows.push(row);
        }
        // Key order is not address order where one key is a prefix of another.
        rows.sort_by_cached_key(LeafRow::address);
        Ok(rows)
    }

    /// The first `limit` leaf rows in `range`, in address order. Reading all
    /// of them from some address on, a page at a time, walks the leaves in
    /// order.
    pub async fn leaves(&mut self, range: &TreeSpan, limit: usize) -> Result<Vec<LeafRow<G>>, TreeError> {
        let rows = self.transaction.scan(MapName::Leaves, range.bounds(), limit).await?;
        rows.iter().map(|(address, value)| layout::leaf_row(address, value)).collect()
    }

    /// Write the leaf row at its address, if the row there now is `expected`,
    /// the one the writer read at that address (`None`: no row), and list its
    /// key under its entity. [`TreeError::Conflict`] when another change got
    /// there first.
    pub(crate) async fn put_leaf(&mut self, row: LeafRow<G>, expected: Option<&LeafRow<G>>) -> Result<(), TreeError> {
        self.put_prepared_leaf(&PreparedLeaf::new(row), expected).await
    }

    pub(crate) async fn put_prepared_leaf(&mut self, prepared: &PreparedLeaf<G>, expected: Option<&LeafRow<G>>) -> Result<(), TreeError> {
        let row = prepared.row();
        debug_assert!(expected.is_none_or(|expected| expected.address() == row.address()), "the expectation is for the row's own address");
        let expected = expected.map(layout::leaf_value);
        self.transaction.put_if(MapName::Leaves, &row.address(), expected.as_deref(), prepared.value()).await?;
        let (listed, mut keys) = self.keys_of(row.entity_id).await?;
        if keys.insert(row.key.clone()) {
            if keys.len() == 2 {
                self.adjust_multiple_keys(true).await?;
            }
            self.transaction.put_if(MapName::Lookup, &row.entity_id.to_bytes(), listed.as_deref(), &layout::lookup_value(&keys)).await?;
        }
        Ok(())
    }

    /// Remove the leaf row `row`, as the writer read it, and its key from
    /// under its entity. [`TreeError::Conflict`] when the row there now is
    /// another, or none.
    pub(crate) async fn delete_leaf(&mut self, row: &LeafRow<G>) -> Result<(), TreeError> {
        self.transaction.delete_if(MapName::Leaves, &row.address(), &layout::leaf_value(row)).await?;
        let (listed, mut keys) = self.keys_of(row.entity_id).await?;
        if keys.remove(&row.key) {
            if keys.len() == 1 {
                self.adjust_multiple_keys(false).await?;
            }
            let listed = listed.as_deref().expect("the key was listed, so its entity has a lookup value");
            let id = row.entity_id.to_bytes();
            match keys.is_empty() {
                true => self.transaction.delete_if(MapName::Lookup, &id, listed).await?,
                false => self.transaction.put_if(MapName::Lookup, &id, Some(listed), &layout::lookup_value(&keys)).await?,
            }
        }
        Ok(())
    }

    pub(crate) async fn all_leaves(&mut self, range: &TreeSpan) -> Result<Vec<LeafRow<G>>, TreeError> {
        let mut rows = Vec::new();
        let mut remaining = range.clone();
        loop {
            let page = self.leaves(&remaining, 1024).await?;
            let done = page.len() < 1024;
            if let Some(last) = page.last() {
                remaining = remaining.after(&last.address());
            }
            rows.extend(page);
            if done {
                return Ok(rows);
            }
        }
    }

    /// The keys the lookup lists for an entity, with the value they were read
    /// from, which a conditional write of the lookup expects to find.
    async fn keys_of(&mut self, entity_id: EntityId) -> Result<(Option<Vec<u8>>, BTreeSet<Vec<u8>>), TreeError> {
        let listed = self.transaction.get(MapName::Lookup, &entity_id.to_bytes()).await?;
        let keys = match &listed {
            Some(value) => layout::lookup_keys(value)?,
            None => BTreeSet::new(),
        };
        Ok((listed, keys))
    }

    /// Lookup's empty key is a count of entities filed under multiple keys.
    /// Entity lookup keys are always 32 bytes, so the metadata cannot collide.
    pub(crate) async fn multiple_key_entities(&mut self) -> Result<u64, TreeError> {
        match self.transaction.get(MapName::Lookup, layout::MULTIPLE_KEYS).await? {
            Some(value) => {
                Ok(u64::from_be_bytes(value.as_slice().try_into().map_err(|_| TreeError::Audit("invalid multi-key entity count"))?))
            }
            None => Ok(0),
        }
    }

    async fn adjust_multiple_keys(&mut self, add: bool) -> Result<(), TreeError> {
        let stored = self.transaction.get(MapName::Lookup, layout::MULTIPLE_KEYS).await?;
        let count = self.multiple_key_entities().await?;
        let count =
            if add { count.checked_add(1) } else { count.checked_sub(1) }.ok_or(TreeError::Audit("invalid multi-key entity count"))?;
        if count == 0 {
            self.transaction
                .delete_if(
                    MapName::Lookup,
                    layout::MULTIPLE_KEYS,
                    stored.as_deref().ok_or(TreeError::Audit("missing multi-key entity count"))?,
                )
                .await
        } else {
            self.transaction.put_if(MapName::Lookup, layout::MULTIPLE_KEYS, stored.as_deref(), &count.to_be_bytes()).await
        }
    }
}

#[cfg(test)]
mod tests;
