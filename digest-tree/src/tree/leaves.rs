//! The leaf rows and the entity lookup: reading leaves by address, by span
//! or by entity, and writing and removing them with the lookup kept in step.

use std::collections::{BTreeMap, BTreeSet};

use ankurah_core_types::EntityId;

use super::{TreeTransaction, PAGE};
use crate::address::{leaf_address, TreeSpan};
use crate::digest::DigestGroup;
use crate::error::TreeError;
use crate::store::layout;
use crate::store::{EncodedLeaf, LeafRow, MapAccess, MapName};

impl<G: DigestGroup, T: MapAccess> TreeTransaction<G, T> {
    /// The leaf row of the entity `entity_id` filed under `key`.
    pub async fn leaf(&mut self, key: &[u8], entity_id: EntityId) -> Result<Option<LeafRow<G>>, TreeError> {
        let address = leaf_address(key, entity_id);
        self.transaction.get(MapName::Leaves, &address).await?.map(|value| layout::leaf_row(&address, &value)).transpose()
    }

    /// Every leaf row of an entity, one per key the entity lookup lists for
    /// it, in address order. A listed key whose leaf row is missing is
    /// [`TreeError::MissingLeaf`]: the rows are damaged.
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

    /// The first `limit` leaf rows in `span`, in address order. Reading all
    /// of them from some address on, a page at a time, walks the leaves in
    /// order.
    pub async fn leaves(&mut self, span: &TreeSpan, limit: usize) -> Result<Vec<LeafRow<G>>, TreeError> {
        let rows = self.transaction.scan(MapName::Leaves, span.bounds(), limit).await?;
        rows.iter().map(|(address, value)| layout::leaf_row(address, value)).collect()
    }

    /// Insert the leaf row at its address, where the transaction sees no row,
    /// and list its key under its entity. [`TreeError::Conflict`] when
    /// another change got there first, and [`TreeError::Inconsistent`] when
    /// the lookup lists the key already, for a row that is not there.
    pub(crate) async fn insert_leaf(&mut self, leaf: &EncodedLeaf<G>) -> Result<(), TreeError> {
        self.transaction.insert(MapName::Leaves, leaf.address(), leaf.value()).await?;
        let row = leaf.row();
        let (listed, mut keys) = self.keys_of(row.entity_id).await?;
        if !keys.insert(row.key.clone()) {
            return Err(TreeError::Inconsistent("the lookup lists a leaf that is absent"));
        }
        if keys.len() == 2 {
            self.adjust_multiple_keys(true).await?;
        }
        self.transaction.put_if(MapName::Lookup, &row.entity_id.to_bytes(), listed.as_deref(), &layout::lookup_value(&keys)).await
    }

    /// Replace the leaf `old`, as the transaction read it, with `new`, filed
    /// at the same address: a change to the entity's head alone, which keeps
    /// its key listed under it. [`TreeError::Conflict`] when the row there now
    /// is another.
    pub(crate) async fn replace_leaf(&mut self, old: &EncodedLeaf<G>, new: &EncodedLeaf<G>) -> Result<(), TreeError> {
        debug_assert_eq!(old.address(), new.address(), "a change of head alone keeps the leaf's address");
        self.transaction.put_if(MapName::Leaves, new.address(), Some(old.value()), new.value()).await
    }

    /// Remove the leaf `leaf`, as the transaction read it, and its key from
    /// under its entity. [`TreeError::Conflict`] when the row there now is
    /// another, or none, and [`TreeError::Inconsistent`] when the lookup does
    /// not list the row's key.
    pub(crate) async fn delete_leaf(&mut self, leaf: &EncodedLeaf<G>) -> Result<(), TreeError> {
        self.transaction.delete_if(MapName::Leaves, leaf.address(), leaf.value()).await?;
        let row = leaf.row();
        let (listed, mut keys) = self.keys_of(row.entity_id).await?;
        if !keys.remove(&row.key) {
            return Err(TreeError::Inconsistent("the lookup does not list a leaf"));
        }
        if keys.len() == 1 {
            self.adjust_multiple_keys(false).await?;
        }
        let listed = listed.as_deref().expect("the key was listed, so its entity has a lookup value");
        let id = row.entity_id.to_bytes();
        match keys.is_empty() {
            true => self.transaction.delete_if(MapName::Lookup, &id, listed).await,
            false => self.transaction.put_if(MapName::Lookup, &id, Some(listed), &layout::lookup_value(&keys)).await,
        }
    }

    /// Every leaf in `span`, in address order, each with its address and the
    /// bytes it is stored as, read a page at a time.
    pub(crate) async fn all_leaves(&mut self, span: &TreeSpan) -> Result<Vec<EncodedLeaf<G>>, TreeError> {
        self.all_leaves_reusing(span, &mut BTreeMap::new()).await
    }

    /// Every leaf in `span`, as [`Self::all_leaves`] reads them, except that
    /// a leaf `known` holds under its address, written by this transaction,
    /// is taken from `known` rather than decoded again.
    pub(crate) async fn all_leaves_reusing(
        &mut self,
        span: &TreeSpan,
        known: &mut BTreeMap<Vec<u8>, EncodedLeaf<G>>,
    ) -> Result<Vec<EncodedLeaf<G>>, TreeError> {
        let mut leaves = Vec::new();
        let mut remaining = span.clone();
        loop {
            let page = self.transaction.scan(MapName::Leaves, remaining.bounds(), PAGE).await?;
            let done = page.len() < PAGE;
            if let Some((last, _)) = page.last() {
                remaining = remaining.after(last);
            }
            for (address, value) in page {
                let leaf = match known.remove(&address) {
                    Some(leaf) => {
                        debug_assert_eq!(leaf.value(), value.as_slice(), "the known leaf is the one stored");
                        leaf
                    }
                    None => EncodedLeaf::decode(address, value)?,
                };
                leaves.push(leaf);
            }
            if done {
                return Ok(leaves);
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

    /// How many entities are filed under more than one key: the count the
    /// lookup keeps under its empty key.
    pub(crate) async fn multiple_key_entities(&mut self) -> Result<u64, TreeError> {
        let stored = self.transaction.get(MapName::Lookup, layout::MULTIPLE_KEYS).await?;
        layout::multiple_keys_count(stored.as_deref())
    }

    /// Count one more entity filed under more than one key when `add`, and
    /// one fewer otherwise, removing the count row when it reaches zero.
    async fn adjust_multiple_keys(&mut self, add: bool) -> Result<(), TreeError> {
        let stored = self.transaction.get(MapName::Lookup, layout::MULTIPLE_KEYS).await?;
        let count = layout::multiple_keys_count(stored.as_deref())?;
        let count = if add { count.checked_add(1) } else { count.checked_sub(1) }
            .ok_or(TreeError::Inconsistent("multi-key entity count out of range"))?;
        match stored {
            Some(stored) if count == 0 => self.transaction.delete_if(MapName::Lookup, layout::MULTIPLE_KEYS, &stored).await,
            stored => {
                let value = layout::multiple_keys_value(count);
                self.transaction.put_if(MapName::Lookup, layout::MULTIPLE_KEYS, stored.as_deref(), &value).await
            }
        }
    }
}

#[cfg(test)]
mod tests;
