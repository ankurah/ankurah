//! Immutable canonical-state reads for core's test engine.

use super::{Pause, TestData};
use crate::{
    entity::TemporaryEntity,
    error::RetrievalError,
    selection::filter::{evaluate_predicate, membership_match, Error},
    storage::{StateCursor, StateScan, StateSnapshot},
};
use ankql::ast::{Resolved, Selection};
use ankurah_proto::{Attested, EntityId, EntityState};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc, Mutex,
    },
};

pub(super) struct Snapshot {
    states: BTreeMap<EntityId, Attested<EntityState>>,
    changed: std::vec::IntoIter<EntityId>,
    cursor: StateCursor,
    hold_page: Arc<Mutex<Option<(usize, Pause)>>>,
    reject_finish: Arc<AtomicBool>,
}

impl Snapshot {
    pub(super) fn new(
        data: &TestData,
        scan: StateScan<'_>,
        hold_page: Arc<Mutex<Option<(usize, Pause)>>>,
        reject_finish: Arc<AtomicBool>,
    ) -> Result<Self, RetrievalError> {
        let since = match scan {
            StateScan::Changes(since) => since,
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
            StateScan::Changes(_) => {
                data.state_revisions.iter().filter_map(|(id, revision)| (*revision > since).then_some(*id)).collect::<Vec<_>>()
            }
            StateScan::Selection(selection) => select(&data.states, selection)?.into_iter().map(|state| state.payload.entity_id).collect(),
        };
        let cursor = StateCursor::new(bincode::serialize(&(data.incarnation, data.revision)).unwrap());
        Ok(Self { states: data.states.clone(), changed: changed.into_iter(), cursor, hold_page, reject_finish })
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
        Ok(ids.iter().map(|id| self.states.get(id).cloned()).collect())
    }

    async fn finish(self: Box<Self>) -> Result<StateCursor, RetrievalError> {
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
    let limit = selection.limit.ok_or_else(|| RetrievalError::Other("snapshot selection requires LIMIT".into()))?;
    let mut matching = Vec::new();
    for state in states.values() {
        if membership_match(&selection.predicate, &|model| state.payload.state.memberships.contains(model)) == Some(false) {
            continue;
        }
        let entity = TemporaryEntity::new(state.payload.entity_id, &state.payload.state)?;
        match evaluate_predicate(&entity, &selection.predicate) {
            Ok(true) => matching.push((state, entity)),
            Ok(false) | Err(Error::PropertyNotFound(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    let selection = ankql::selection::deterministic_order(selection);
    matching.sort_by(|(_, left), (_, right)| {
        crate::selection::ordering::compare_items(left, right, selection.order_by.as_deref().unwrap_or_default())
    });
    Ok(matching.into_iter().take(usize::try_from(limit).unwrap_or(usize::MAX)).map(|(state, _)| state.clone()).collect())
}
