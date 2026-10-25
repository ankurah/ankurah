//! When an established live query checks its copy against its peer's result.
//! A push lost after the query became ready is otherwise invisible until the
//! entity's next change or a reconnect; the check itself is state_sync's.

use crate::{
    node::state_sync::{CheckOutcome, SyncedQuery},
    util::Instant,
};
use std::time::Duration;

/// The wait before a query's first check, and after a disagreement or a repair.
const FIRST_CHECK: Duration = Duration::from_secs(30);
/// The longest wait between checks, which doubles while the copies agree or
/// their repair fails to apply.
const MAX_CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// One established live query's schedule of checks.
pub(super) struct PeriodicCheck {
    pub(super) synced: SyncedQuery,
    interval: Duration,
    due: Instant,
    /// Whether the last check disagreed, so that the next one to disagree
    /// repairs, as it does after a repair that failed to apply.
    disagreed: bool,
    /// Whether a check is running, so that a later tick does not start another.
    running: bool,
}

impl PeriodicCheck {
    pub(super) fn new(synced: SyncedQuery) -> Self {
        Self { synced, interval: FIRST_CHECK, due: Instant::now() + FIRST_CHECK, disagreed: false, running: false }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ApplyError, ApplyErrorItem, MutationError};
    use ankurah_core_types::KeySpec;
    use std::sync::Arc;

    fn check() -> PeriodicCheck {
        let spec = ankurah_proto::TreeSpec::new(KeySpec::new(vec![]), ankql::ast::Predicate::True);
        PeriodicCheck::new(SyncedQuery { spec, round_trip: Duration::ZERO })
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
