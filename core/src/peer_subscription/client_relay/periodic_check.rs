//! When the relay checks an established live query's copy against its peer's
//! result, and what it does with each check's outcome. A push lost after the
//! query became ready is otherwise invisible until the entity's next change or
//! a reconnect; the comparison itself is state_sync's.

use super::{until_canceled, RemoteQuerySubscriber, Status, SubscriptionRelay};
use crate::{
    error::RetrievalError,
    node::{
        state_sync::{CheckBasis, CheckOutcome},
        ContextData,
    },
    util::{cancel_flag::CancelFlag, Instant},
};
use ankurah_proto as proto;
use std::time::Duration;
use tracing::{debug, warn};

/// The wait before a query's first check, and after a disagreement or a repair.
const FIRST_CHECK: Duration = Duration::from_secs(30);
/// The longest wait between checks, which doubles while the copies agree or
/// their repair fails to apply.
const MAX_CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// One established live query's schedule of checks.
pub(super) struct PeriodicCheck {
    pub(super) basis: CheckBasis,
    interval: Duration,
    due: Instant,
    /// Whether the last check disagreed, so that the next one to disagree
    /// repairs, as it does after a repair that failed to apply.
    disagreed: bool,
    /// Whether a check is running, so that a later tick does not start another.
    running: bool,
}

impl PeriodicCheck {
    pub(super) fn new(basis: CheckBasis) -> Self {
        Self { basis, interval: FIRST_CHECK, due: Instant::now() + FIRST_CHECK, disagreed: false, running: false }
    }

    pub(super) fn is_due(&self, now: Instant) -> bool { !self.running && now >= self.due }

    /// Start a check, unless one is running: whether it confirms the last
    /// check's disagreement.
    pub(super) fn start(&mut self) -> Option<bool> {
        if self.running {
            return None;
        }
        self.running = true;
        Some(self.disagreed)
    }

    /// Schedule the next check after one found `outcome`.
    pub(super) fn finish(&mut self, outcome: CheckOutcome, now: Instant) {
        self.running = false;
        let backed_off = self.interval.saturating_mul(2).min(MAX_CHECK_INTERVAL);
        (self.interval, self.disagreed) = match outcome {
            CheckOutcome::Agreed => (backed_off, false),
            // Soon enough to confirm the disagreement, late enough for pushes in flight to land.
            CheckOutcome::Disagreed => (FIRST_CHECK, true),
            CheckOutcome::Repaired => (FIRST_CHECK, false),
            // A refusal that persists, as a policy's does, gains nothing from a repair every thirty
            // seconds; one that clears is repaired by the next check to disagree, without a second.
            CheckOutcome::RepairFailed { .. } => (backed_off, true),
        };
        self.due = now + self.interval;
    }
}

impl<CD: ContextData, Q: RemoteQuerySubscriber> SubscriptionRelay<CD, Q> {
    /// Start the periodic check of each established query whose check is due.
    pub(super) fn run_due_checks(&self) {
        let now = Instant::now();
        let due: Vec<_> = {
            let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            subscriptions
                .iter()
                .filter(|(_, state)| state.check.as_ref().is_some_and(|check| check.is_due(now)))
                .map(|(id, _)| *id)
                .collect()
        };
        for query_id in due {
            let relay = self.clone();
            crate::task::spawn(async move { relay.run_check(query_id).await });
        }
    }

    /// Run established query `query_id`'s periodic check and schedule its
    /// next one. A repair that fails to apply is reported and leaves the query
    /// established; a check that fails subscribes the query again.
    pub(super) async fn run_check(&self, query_id: proto::QueryId) {
        let Some(node) = self.inner.node.get() else { return };
        let started = {
            let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            subscriptions.get_mut(&query_id).and_then(|state| {
                let Status::Established(peer, _) = state.status else { return None };
                let check = state.check.as_mut()?;
                let confirm = check.start()?;
                Some((peer, state.content.clone(), check.basis.clone(), confirm, state.cancel.clone()))
            })
        };
        let Some((peer, content, basis, confirm, cancel)) = started else { return };
        match until_canceled(&cancel, node.check_query(peer, content.selection.clone(), content.sessions.current(), basis, confirm)).await {
            None => debug!("Stopped the superseded check of predicate {}", query_id),
            Some(Ok(outcome)) => {
                if let CheckOutcome::RepairFailed { entity_id, error } = &outcome {
                    warn!("The check of predicate {} could not repair entity {} from peer {}: {}", query_id, entity_id, peer, error);
                }
                let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
                if !cancel.canceled() {
                    if let Some(check) = subscriptions.get_mut(&query_id).and_then(|state| state.check.as_mut()) {
                        check.finish(outcome, Instant::now());
                    }
                }
            }
            Some(Err(error)) => self.resubscribe_after_failed_check(query_id, peer, error, &cancel),
        }
    }

    /// Subscribe established query `query_id` again after its check failed
    /// with `error`, keeping its data, as after a sync that stalled. A check's
    /// failure says nothing lasting about a query that was established: its
    /// causes, such as a catalog change on either side, clear with a new
    /// subscription, so it never marks the query failed.
    fn resubscribe_after_failed_check(&self, query_id: proto::QueryId, peer: proto::EntityId, error: RetrievalError, cancel: &CancelFlag) {
        let mut subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        if cancel.canceled() {
            return;
        }
        if let Some(info) = subscriptions.get_mut(&query_id) {
            if let Status::Established(..) = info.status {
                info.status = Status::PendingRemote;
                warn!("The check of predicate {} with peer {} failed: {} - will subscribe again", query_id, peer, error);
            }
        }
    }

    /// Run every established query's periodic check now, whatever its schedule.
    #[cfg(feature = "test-helpers")]
    pub(crate) async fn check_all_now(&self) {
        let established: Vec<_> = {
            let subscriptions = self.inner.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            subscriptions.iter().filter(|(_, state)| state.check.is_some()).map(|(id, _)| *id).collect()
        };
        for query_id in established {
            self.run_check(query_id).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ApplyError, ApplyErrorItem, MutationError};
    use ankurah_core_types::KeySpec;
    use std::sync::Arc;

    fn check() -> PeriodicCheck {
        let spec = ankurah_proto::TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True);
        PeriodicCheck::new(CheckBasis { spec, round_trip: Duration::ZERO })
    }

    #[test]
    fn checks_back_off_while_the_copies_agree_and_return_to_thirty_seconds_after_a_repair() {
        let mut check = check();
        let now = Instant::now();
        assert!(!check.is_due(now), "the first check waits thirty seconds");
        let mut intervals = Vec::new();
        for _ in 0..6 {
            assert_eq!(check.start(), Some(false));
            check.finish(CheckOutcome::Agreed, now);
            intervals.push(check.interval.as_secs());
        }
        assert_eq!(intervals, [60, 120, 240, 300, 300, 300], "doubling, capped at five minutes");
        assert_eq!(check.start(), Some(false));
        check.finish(CheckOutcome::Disagreed, now);
        assert_eq!((check.interval, check.start()), (FIRST_CHECK, Some(true)), "the next check confirms a disagreement");
        check.finish(CheckOutcome::Repaired, now);
        assert_eq!((check.interval, check.start()), (FIRST_CHECK, Some(false)));
        assert_eq!(check.start(), None, "one check at a time");
        assert!(!check.is_due(now + MAX_CHECK_INTERVAL), "a running check is never due again");
    }

    #[test]
    fn a_failed_repair_backs_off_and_the_next_check_repairs_at_its_first_disagreement() {
        let mut check = check();
        let now = Instant::now();
        let entity_id = ankurah_proto::EntityId::random();
        let failed = || CheckOutcome::RepairFailed {
            entity_id,
            error: Arc::new(ApplyError::Items(vec![ApplyErrorItem { entity_id, cause: MutationError::InvalidEvent }])),
        };
        assert_eq!(check.start(), Some(false));
        check.finish(CheckOutcome::Disagreed, now);
        let mut intervals = Vec::new();
        for _ in 0..5 {
            assert_eq!(check.start(), Some(true), "each check confirms the last one's disagreement");
            check.finish(failed(), now);
            intervals.push(check.interval.as_secs());
        }
        assert_eq!(intervals, [60, 120, 240, 300, 300], "doubling from the disagreement's thirty seconds, capped at five minutes");
        assert_eq!(check.start(), Some(true), "the check after a failed repair repairs at its first disagreement");
        check.finish(CheckOutcome::Repaired, now);
        assert_eq!((check.interval, check.start()), (FIRST_CHECK, Some(false)), "a repair that applies ends the back-off");
    }
}
