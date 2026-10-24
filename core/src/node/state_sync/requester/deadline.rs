//! The requester's idle deadline: how long it waits for its copy of a query's
//! result to progress before comparing again, and how many comparisons
//! without progress it makes before the sync fails. It measures progress by
//! the copy alone, never by what a peer says it sent, so a peer that answers
//! but sends nothing cannot keep a query waiting.

use futures::future::Either;
use std::time::Duration;

/// Comparisons without progress after which a sync fails.
const MAX_IDLE_COMPARISONS: u32 = 3;
/// The shortest wait for progress, however short the round trip.
const MIN_IDLE: Duration = Duration::from_secs(1);
/// Round trips a requester waits for progress at first.
const ROUND_TRIPS_IDLE: u32 = 4;

pub(super) struct IdleDeadline {
    /// The wait after progress: max(1 s, four of the last measured round trips).
    base: Duration,
    /// The wait now, doubled after each comparison without progress.
    interval: Duration,
    comparisons: u32,
}

impl IdleDeadline {
    pub(super) fn new() -> Self { Self { base: MIN_IDLE, interval: MIN_IDLE, comparisons: 0 } }

    /// Measure a round trip to the peer, which sets the wait after progress.
    pub(super) fn measured(&mut self, round_trip: Duration) { self.base = MIN_IDLE.max(round_trip * ROUND_TRIPS_IDLE); }

    /// The copy progressed: wait the base interval again, with no comparison
    /// counted against it.
    pub(super) fn progressed(&mut self) {
        self.interval = self.base;
        self.comparisons = 0;
    }

    /// How long to wait for progress now; also each comparison request's timeout.
    pub(super) fn interval(&self) -> Duration { self.interval }

    /// The interval passed without progress. Count one more comparison and
    /// double the wait, unless the last allowed comparison has already failed
    /// to bring progress, in which case the sync gives up: false.
    pub(super) fn compare_again(&mut self) -> bool {
        if self.comparisons == MAX_IDLE_COMPARISONS {
            return false;
        }
        self.comparisons += 1;
        self.interval = self.interval.saturating_mul(2);
        true
    }
}

/// `future`'s output, or none if it takes longer than `timeout`.
pub(super) async fn within<T>(timeout: Duration, future: impl std::future::Future<Output = T>) -> Option<T> {
    futures::pin_mut!(future);
    match futures::future::select(future, futures_timer::Delay::new(timeout)).await {
        Either::Left((output, _)) => Some(output),
        Either::Right(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wait_follows_the_round_trip_and_doubles_until_three_comparisons_fail() {
        let mut deadline = IdleDeadline::new();
        deadline.measured(Duration::from_millis(100));
        deadline.progressed();
        assert_eq!(deadline.interval(), MIN_IDLE, "four short round trips still wait a second");
        deadline.measured(Duration::from_millis(500));
        deadline.progressed();
        assert_eq!(deadline.interval(), Duration::from_secs(2), "four round trips of half a second");
        for expected in [4, 8, 16] {
            assert!(deadline.compare_again());
            assert_eq!(deadline.interval(), Duration::from_secs(expected));
        }
        assert!(!deadline.compare_again(), "the third comparison without progress was the last");
        deadline.progressed();
        assert_eq!(deadline.interval(), Duration::from_secs(2), "progress restores the base wait");
        assert!(deadline.compare_again(), "and the comparisons it allows");
    }
}
