use ankurah_proto::QueryId;
use std::{collections::HashMap, sync::Arc};

use crate::util::IVec;

/// Wraps an Arc-shared list of changes with per-query offsets to avoid cloning events
pub struct CandidateChanges<C> {
    changes: Arc<Vec<C>>,
    query_offsets: HashMap<QueryId, IVec<usize, 8>>,
    entity_offsets: IVec<usize, 8>,
}

/// A query-specific view of candidates that borrows from CandidateChanges
pub struct QueryCandidate<'a, C> {
    pub query_id: &'a QueryId,
    changes: &'a Arc<Vec<C>>,
    offsets: &'a [usize],
}

impl<C> CandidateChanges<C> {
    /// Create a new candidate list from a shared change batch
    pub fn new(changes: Arc<Vec<C>>) -> Self { Self { changes, query_offsets: HashMap::new(), entity_offsets: IVec::new() } }

    /// Add an offset for an entity-level subscription (not tied to any query)
    pub fn add_entity(&mut self, offset: usize) { self.entity_offsets.push(offset); }

    /// Add an offset to the candidate list for a specific query
    pub fn add_query(&mut self, query_id: QueryId, offset: usize) { self.query_offsets.entry(query_id).or_default().add(offset); }

    /// Returns true if there are no candidates
    pub fn is_empty(&self) -> bool { self.query_offsets.is_empty() && self.entity_offsets.is_empty() }

    /// Returns the number of query candidates
    pub fn query_count(&self) -> usize { self.query_offsets.len() }

    /// Iterate over query candidates
    pub fn query_iter(&self) -> impl Iterator<Item = QueryCandidate<'_, C>> + '_ {
        self.query_offsets.iter().map(|(query_id, offsets)| QueryCandidate {
            query_id,
            changes: &self.changes,
            offsets: offsets.as_slice(),
        })
    }

    /// Iterate over entity-level candidates
    pub fn entity_iter(&self) -> impl Iterator<Item = &C> + '_ { self.entity_offsets.iter().map(move |&offset| &self.changes[offset]) }

    /// Get direct access to the shared changes array
    pub fn changes(&self) -> &Arc<Vec<C>> { &self.changes }
}

impl<'a, C> QueryCandidate<'a, C> {
    /// Iterate over the candidate changes for this query
    pub fn iter(&self) -> impl Iterator<Item = &C> + '_ { self.offsets.iter().map(move |&offset| &self.changes[offset]) }
}

impl<C> Clone for CandidateChanges<C> {
    fn clone(&self) -> Self {
        Self { changes: self.changes.clone(), query_offsets: self.query_offsets.clone(), entity_offsets: self.entity_offsets.clone() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candidate_changes_empty() {
        let changes: Arc<Vec<i32>> = Arc::from(vec![]);
        let candidates = CandidateChanges::new(changes);
        assert!(candidates.is_empty());
        assert_eq!(candidates.query_count(), 0);
    }

    #[test]
    fn test_candidate_changes_add_query() {
        let changes: Arc<Vec<i32>> = Arc::from(vec![10, 20, 30, 40, 50]);
        let mut candidates = CandidateChanges::new(changes);

        let q1 = QueryId::new();
        let q2 = QueryId::new();

        candidates.add_query(q1, 1); // 20
        candidates.add_query(q1, 3); // 40
        candidates.add_query(q2, 0); // 10

        assert_eq!(candidates.query_count(), 2);
        assert!(!candidates.is_empty());

        let mut query_map: HashMap<QueryId, Vec<i32>> = HashMap::new();
        for qc in candidates.query_iter() {
            let values: Vec<i32> = qc.iter().copied().collect();
            query_map.insert(*qc.query_id, values);
        }

        assert_eq!(query_map[&q1], vec![20, 40]);
        assert_eq!(query_map[&q2], vec![10]);
    }

    #[test]
    fn test_candidate_changes_entity_level() {
        let changes: Arc<Vec<i32>> = Arc::from(vec![10, 20, 30]);
        let mut candidates = CandidateChanges::new(changes);

        candidates.add_entity(0);
        candidates.add_entity(2);

        let entities: Vec<i32> = candidates.entity_iter().copied().collect();
        assert_eq!(entities, vec![10, 30]);
    }
}
