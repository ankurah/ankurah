//! Immutable canonical-state reads for core's test engine.

use super::{Pause, TestData};
use crate::{
    entity::TemporaryEntity,
    error::RetrievalError,
    selection::filter::{evaluate_predicate, membership_match, Error},
    selection::ordering::OrderBoundary,
    storage::{StateCursor, StateScan, StateSnapshot},
};
use ankql::ast::{Resolved, Selection};
use ankurah_proto::{Attested, EntityId, EntityState};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
};

pub(super) struct Snapshot {
    states: BTreeMap<EntityId, Attested<EntityState>>,
    changed: std::vec::IntoIter<EntityId>,
    cursor: StateCursor,
    hold_page: Arc<Mutex<Option<(usize, Pause)>>>,
    hold_finish: Arc<Mutex<Option<Pause>>>,
    reject_finish: Arc<AtomicBool>,
    point_reads: Arc<AtomicUsize>,
    selection: Option<Selection<Resolved>>,
    selection_reads: Arc<AtomicUsize>,
}

impl Snapshot {
    pub(super) fn new(
        data: &TestData,
        scan: StateScan<'_>,
        hold_page: Arc<Mutex<Option<(usize, Pause)>>>,
        hold_finish: Arc<Mutex<Option<Pause>>>,
        reject_finish: Arc<AtomicBool>,
        point_reads: Arc<AtomicUsize>,
        selection_reads: Arc<AtomicUsize>,
    ) -> Result<Self, RetrievalError> {
        let since = match scan {
            StateScan::Changes(since) | StateScan::SelectionChanges { since, .. } => since,
            StateScan::Selection(_) => None,
        };
        let since = match since {
            None => 0,
            Some(cursor) => {
                let (incarnation, revision): (EntityId, u64) =
                    bincode::deserialize(cursor.as_bytes()).map_err(|_| RetrievalError::InvalidStateCursor)?;
                if incarnation != data.incarnation || revision > data.revision {
                    return Err(RetrievalError::InvalidStateCursor);
                }
                revision
            }
        };
        let changed = match scan {
            StateScan::Changes(_) | StateScan::SelectionChanges { .. } => {
                data.state_revisions.iter().filter_map(|(id, revision)| (*revision > since).then_some(*id)).collect::<Vec<_>>()
            }
            StateScan::Selection(selection) => {
                selection_reads.fetch_add(1, AtomicOrdering::Relaxed);
                select(&data.states, selection)?.into_iter().map(|state| state.payload.entity_id).collect()
            }
        };
        let selection = match scan {
            StateScan::SelectionChanges { selection, .. } => {
                if selection.limit.is_none() {
                    return Err(RetrievalError::Other("a selection snapshot requires LIMIT".into()));
                }
                Some(selection.clone())
            }
            _ => None,
        };
        let cursor = StateCursor::new(bincode::serialize(&(data.incarnation, data.revision)).unwrap());
        Ok(Self {
            states: data.states.clone(),
            changed: changed.into_iter(),
            cursor,
            hold_page,
            hold_finish,
            reject_finish,
            point_reads,
            selection,
            selection_reads,
        })
    }
}

#[async_trait::async_trait]
impl StateSnapshot for Snapshot {
    async fn next_page(&mut self, limit: NonZeroUsize) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let pause = {
            let mut hold = self.hold_page.lock().unwrap();
            match hold.as_mut() {
                Some((0, _)) => hold.take().map(|(_, pause)| pause),
                Some((remaining, _)) => {
                    *remaining -= 1;
                    None
                }
                None => None,
            }
        };
        if let Some((entered, resume)) = pause {
            entered.send(()).unwrap();
            let _ = resume.await;
        }
        Ok(self.changed.by_ref().take(limit.get()).map(|id| self.states[&id].clone()).collect())
    }

    async fn get_states(&mut self, ids: &[EntityId]) -> Result<Vec<Option<Attested<EntityState>>>, RetrievalError> {
        self.point_reads.fetch_add(ids.len(), AtomicOrdering::Relaxed);
        Ok(ids.iter().map(|id| self.states.get(id).cloned()).collect())
    }

    async fn matching_ids(&mut self, ids: &[EntityId]) -> Result<Vec<EntityId>, RetrievalError> {
        let selection = self.selection.as_ref().ok_or_else(|| RetrievalError::Other("snapshot has no prepared selection".into()))?;
        ids.iter()
            .filter_map(|id| self.states.get(id))
            .filter_map(|state| match matches(state, selection) {
                Ok(true) => Some(Ok(state.payload.entity_id)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    async fn select_after(
        &mut self,
        after: Option<&OrderBoundary>,
        limit: NonZeroUsize,
    ) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
        let selection = self.selection.as_ref().ok_or_else(|| RetrievalError::Other("snapshot has no prepared selection".into()))?;
        if after.is_some_and(|after| !after.matches_order(selection)) {
            return Err(RetrievalError::Other("continuation boundary has another order".into()));
        }
        self.selection_reads.fetch_add(1, AtomicOrdering::Relaxed);
        let mut selection = selection.clone();
        selection.limit = Some(limit.get() as u64);
        select_after(&self.states, &selection, after)
    }

    async fn finish(self: Box<Self>) -> Result<StateCursor, RetrievalError> {
        let pause = self.hold_finish.lock().unwrap().take();
        if let Some((entered, resume)) = pause {
            entered.send(()).unwrap();
            let _ = resume.await;
        }
        if self.reject_finish.load(AtomicOrdering::SeqCst) {
            return Err(crate::error::MutationError::WriteConflict.into());
        }
        Ok(self.cursor)
    }
}

pub(super) fn select(
    states: &BTreeMap<EntityId, Attested<EntityState>>,
    selection: &Selection<Resolved>,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    select_after(states, selection, None)
}

fn select_after(
    states: &BTreeMap<EntityId, Attested<EntityState>>,
    selection: &Selection<Resolved>,
    after: Option<&OrderBoundary>,
) -> Result<Vec<Attested<EntityState>>, RetrievalError> {
    let limit = selection.limit.ok_or_else(|| RetrievalError::Other("snapshot selection requires LIMIT".into()))?;
    let mut matching = Vec::new();
    for state in states.values() {
        if matches(state, selection)? {
            let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
            if after.is_none_or(|after| after.precedes(&entity)) {
                matching.push((state, entity));
            }
        }
    }
    let selection = ankql::selection::deterministic_order(selection);
    matching.sort_by(|(_, left), (_, right)| {
        crate::selection::ordering::compare_items(left, right, selection.order_by.as_deref().unwrap_or_default())
    });
    Ok(matching.into_iter().take(usize::try_from(limit).unwrap_or(usize::MAX)).map(|(state, _)| state.clone()).collect())
}

fn matches(state: &Attested<EntityState>, selection: &Selection<Resolved>) -> Result<bool, RetrievalError> {
    if membership_match(&selection.predicate, &|model| state.payload.state.memberships.contains(model)) == Some(false) {
        return Ok(false);
    }
    let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
    match evaluate_predicate(&entity, &selection.predicate) {
        Ok(value) => Ok(value),
        Err(Error::PropertyNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}
