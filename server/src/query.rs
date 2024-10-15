use anyhow::anyhow;
use axum::extract::State;
use hydra_proto as proto;
use sled::IVec;
use ulid::Ulid;

use crate::{appstate::AppState, error::AppError};

pub trait Key {
    type Bytes: AsRef<[u8]>;
    fn as_bytes(&self) -> Self::Bytes;
}

impl Key for Vec<u8> {
    type Bytes = Self;
    fn as_bytes(&self) -> Self::Bytes {
        // hack
        self.to_vec()
    }
}
impl Key for &[u8] {
    type Bytes = Self;
    fn as_bytes(&self) -> Self::Bytes {
        self
    }
}

impl Key for Ulid {
    type Bytes = &'static [u8; 16];
    fn as_bytes(&self) -> Self::Bytes {
        self.as_bytes()
    }
}

impl Key for usize {
    type Bytes = [u8; std::mem::size_of::<usize>()];
    fn as_bytes(&self) -> Self::Bytes {
        self.to_be_bytes()
    }
}

pub enum FetchCursor<K: Key> {
    None,
    Excluding(K),
    // Including(K),
}

impl<K: Key> FetchCursor<K> {
    fn into_bound(self) -> Bound<K::Bytes> {
        match self {
            FetchCursor::None => Bound::Unbounded,
            FetchCursor::Excluding(k) => Bound::Excluded(k.as_bytes()),
            // FetchCursor::Including(k) => Bound::Included(k.as_bytes()),
        }
    }
}

pub struct FetchRecordQuery<K: Key> {
    pub cursor: FetchCursor<K>,
    pub limit: usize,
    pub order: proto::Direction,
}

impl<K: Key> FetchRecordQuery<K> {
    pub fn new() -> Self {
        FetchRecordQuery {
            cursor: FetchCursor::None,
            limit: 100,
            order: proto::Direction::Ascending,
        }
    }

    pub fn cursor(mut self, value: FetchCursor<K>) -> Self {
        self.cursor = value;
        self
    }

    pub fn limit(mut self, value: usize) -> Self {
        self.limit = value;
        self
    }

    pub fn direction(mut self, order: proto::Direction) -> Self {
        self.order = order;
        self
    }
}

pub struct FetchRecordResult<T: proto::Record> {
    pub items: Vec<(IVec, T)>,
    pub order: proto::Direction,
    pub more_records: bool,
}

impl<T: proto::Record> FetchRecordResult<T> {
    pub fn ids(&self) -> Vec<T::ID> {
        self.items.iter().map(|(_, r)| r.id().clone()).collect()
    }
}

use std::ops::Bound;

pub fn fetch_records<T: proto::Record, K: Key>(
    tree: &sled::Tree,
    query: FetchRecordQuery<K>,
) -> Result<FetchRecordResult<T>, AppError> {
    let limit = query.limit;
    let fetch_limit = limit + 1; // Fetch one extra to determine if there are more records

    let mut items = Vec::with_capacity(fetch_limit);

    match query.order {
        proto::Direction::Ascending => {
            let iter = tree.range((query.cursor.into_bound(), Bound::Unbounded));
            for item in iter.take(fetch_limit) {
                let (key, value) = item?;
                items.push((key, bincode::deserialize(&value)?));
            }
        }
        proto::Direction::Descending => {
            let iter = tree
                .range((Bound::Unbounded, query.cursor.into_bound()))
                .rev();
            for item in iter.take(fetch_limit) {
                let (key, value) = item?;
                items.push((key, bincode::deserialize(&value)?));
            }
        }
    }

    let more_records = items.len() > limit;
    items.truncate(limit);

    Ok(FetchRecordResult {
        items,
        more_records,
        order: query.order,
    })
}

pub struct PaginatedFetchRequest {
    pub tree: &'static str,
    pub cursor: proto::PaginatedCursor,
    pub limit: usize,
    pub direction: proto::Direction,
}

pub struct PaginatedFetchResponse<T> {
    pub items: Vec<FetchResultItem<T>>,
    pub limit: usize,
    pub has_more_before: bool,
    pub has_more_after: bool,
}

pub struct FetchResultItem<T> {
    pub key: Vec<u8>,
    pub item: T,
}

pub fn fetch_paginated<T: proto::Record>(
    state: &AppState,
    request: PaginatedFetchRequest,
) -> Result<PaginatedFetchResponse<T>, AppError> {
    let tree = state.storage.subtree(request.tree)?;

    let mut query = FetchRecordQuery::new();

    let display_order = request.direction;

    let mut has_more_before = false;
    let mut has_more_after = false;

    //get the page before or after the given key
    let (cursor, query_order) = match request.cursor {
        proto::PaginatedCursor::Before(ref before) => {
            has_more_after = true;

            (
                FetchCursor::Excluding(before.clone()),
                display_order.inverse(),
            )
            // display order ascending 5,6 -> before 5 -> descending 4,3
            // display order descending 6,5 -> before 6 -> ascending 7,8
        }
        proto::PaginatedCursor::After(ref after) => {
            has_more_before = true;
            (FetchCursor::Excluding(after.clone()), display_order)

            // display order ascending 5,6 -> after 6 -> ascending 7,8
            // display order descending 6,5 -> after 5 -> descending 4,3
        }
        _ => (FetchCursor::None, display_order),
    };

    query = query.cursor(cursor);
    query = query.direction(query_order);
    query = query.limit(request.limit);

    let fetch_result = crate::query::fetch_records::<T, _>(&tree, query)?;

    if query_order == display_order {
        has_more_after = fetch_result.more_records;
    } else {
        has_more_before = fetch_result.more_records;
    }

    let mut items = fetch_result.items;
    if display_order != query_order {
        items.reverse();
    }

    return Ok(PaginatedFetchResponse {
        items: items
            .into_iter()
            .map(|(key, item)| FetchResultItem {
                key: key.to_vec(),
                item,
            })
            .collect(),
        limit: request.limit,
        has_more_before,
        has_more_after,
    });
}

#[cfg(test)]
mod tests {
    use proto::Direction;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::storage::StorageEngine;

    #[derive(Serialize, Deserialize)]
    pub struct TestRecord {
        pub id: usize,
        pub value: String,
    }

    impl proto::Record for TestRecord {
        type ID = usize;
        fn id(&self) -> &Self::ID {
            &self.id
        }
    }

    #[test]
    fn test_fetch() {
        let storage = StorageEngine::new_test().unwrap();

        let tree = storage.subtree("test").unwrap();

        // first we have to load up the db with test records
        for id in 0usize..12 {
            let record = TestRecord {
                id,
                value: format!("test value {}", id),
            };

            // use BigEndian to ensure lexicographic ordering
            tree.insert(&id.to_be_bytes(), bincode::serialize(&record).unwrap())
                .unwrap();
        }

        // now lets run fetch
        let query = FetchRecordQuery::<usize>::new()
            .limit(5)
            .direction(Direction::Ascending);
        let result = fetch_records::<TestRecord, _>(&tree, query).unwrap();

        //the first 5 should be the oldest 5
        assert_eq!(result.items.len(), 5);
        assert_eq!(result.ids(), &[0, 1, 2, 3, 4]);

        // "next page" button is shown
        assert!(result.more_records);

        // user clicks "next page"
        let query = FetchRecordQuery::<usize>::new()
            .cursor(FetchCursor::Excluding(4))
            .limit(5);
        let result = fetch_records::<TestRecord, _>(&tree, query).unwrap();
        assert_eq!(result.items.len(), 5);
        assert_eq!(result.ids(), &[5, 6, 7, 8, 9]);

        // "next page" button is shown
        assert!(result.more_records);

        // user clicks "next page" and a partial page is returned
        let query = FetchRecordQuery::<usize>::new()
            .cursor(FetchCursor::Excluding(9))
            .limit(5);
        let result = fetch_records::<TestRecord, _>(&tree, query).unwrap();
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.ids(), &[10, 11]);

        // "next page" button is not shown
        assert!(!result.more_records);

        // user clicks "previous page" button
        let query = FetchRecordQuery::<usize>::new()
            .cursor(FetchCursor::Excluding(10))
            .limit(5)
            .direction(Direction::Descending);
        let result = fetch_records::<TestRecord, _>(&tree, query).unwrap();
        assert_eq!(result.items.len(), 5);
        assert_eq!(result.ids(), &[9, 8, 7, 6, 5]);
        // "previous page" button is shown
        assert!(result.more_records);

        // user clicks "previous page" button
        let query = FetchRecordQuery::<usize>::new()
            .cursor(FetchCursor::Excluding(5))
            .limit(5)
            .direction(Direction::Descending);
        let result = fetch_records::<TestRecord, _>(&tree, query).unwrap();
        assert_eq!(result.items.len(), 5);
        assert_eq!(result.ids(), &[4, 3, 2, 1, 0]);

        // "previous page" button is not shown
        assert!(!result.more_records);

        // lets test the case where the cursor is the first record
        let query = FetchRecordQuery::<usize>::new()
            .cursor(FetchCursor::Excluding(0))
            .limit(5);
        let result = fetch_records::<TestRecord, _>(&tree, query).unwrap();
        assert_eq!(result.items.len(), 5);
        assert_eq!(result.ids(), &[1, 2, 3, 4, 5]);
        assert!(result.more_records);

        // now lets check what happens when the cursor is the first record and we're descending
        let query = FetchRecordQuery::<usize>::new()
            .cursor(FetchCursor::Excluding(0))
            .limit(5)
            .direction(Direction::Descending);
        let result = fetch_records::<TestRecord, _>(&tree, query).unwrap();
        assert_eq!(result.items.len(), 0);
        assert!(!result.more_records);

        // Lets do last cursor ascending
        let query = FetchRecordQuery::<usize>::new()
            .cursor(FetchCursor::Excluding(11))
            .limit(5)
            .direction(Direction::Ascending);
        let result = fetch_records::<TestRecord, _>(&tree, query).unwrap();
        assert_eq!(result.items.len(), 0);
        assert!(!result.more_records);
    }
}
