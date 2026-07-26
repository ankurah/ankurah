use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::Notify;

/// A close-and-drain gate between node reset and in-flight response effects.
///
/// The node sends requests to peers (fetches, subscriptions, catalog warms);
/// each response later applies effects to node state. Hard reset wipes that
/// state, so a response mid-application must finish before the wipe, and a
/// response arriving after the wipe belongs to the dead epoch and must never
/// apply. Without this gate, a late response half-resurrects pre-reset data
/// into the fresh state.
///
/// This is not a mutex: admitted effects run concurrently and never exclude
/// one another. It is the async shutdown idiom, close then drain:
/// [`Self::invalidate`] permanently refuses new leases, and
/// [`Self::wait_drained`] completes once every admitted lease has dropped.
///
/// The two halves in use:
///
/// ```ignore
/// // Node setup: response effects admit through the fence.
/// let fence = RequestFence::new();
/// let validity = RequestValidity::fenced(fence.clone());
///
/// // Response path: hold the lease across the whole effect.
/// match validity.try_acquire() {
///     Some(_lease) => apply_response(response), // admitted; reset waits for us
///     None => return,                           // node is resetting; drop it
/// } // lease drops here: the effect is finished and reset may proceed
///
/// // Reset path: refuse new effects, then wait out the admitted ones.
/// fence.invalidate();
/// fence.wait_drained().await;
/// wipe_state(); // nothing admitted before the wipe is still running
/// ```
#[derive(Clone, Debug)]
pub(crate) struct RequestFence(Arc<RequestFenceInner>);

#[derive(Debug)]
struct RequestFenceInner {
    current: AtomicBool,
    in_flight: AtomicUsize,
    drained: Notify,
}

// The admit/invalidate handshake is a store-buffer shape: try_acquire writes
// in_flight then reads current, while reset writes current then reads in_flight.
// Anything weaker than SeqCst on those operations lets both sides read the stale
// value at once, and a lease escapes past a completed drain.
impl RequestFence {
    pub(crate) fn new() -> Self {
        Self(Arc::new(RequestFenceInner { current: AtomicBool::new(true), in_flight: AtomicUsize::new(0), drained: Notify::new() }))
    }

    pub(crate) fn is_current(&self) -> bool { self.0.current.load(Ordering::SeqCst) }

    /// Stop admitting effects immediately. Callers that need a reset barrier
    /// must follow this with [`Self::wait_drained`].
    pub(crate) fn invalidate(&self) { self.0.current.store(false, Ordering::SeqCst); }

    pub(crate) async fn wait_drained(&self) {
        loop {
            // Notify does not retain notify_waiters permits. Arm the waiter
            // before reading the count so the final lease cannot disappear
            // between our observation and await.
            let drained = self.0.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self.0.in_flight.load(Ordering::SeqCst) == 0 {
                return;
            }
            drained.await;
        }
    }

    pub(crate) fn try_acquire(&self) -> Option<RequestLease> {
        if !self.is_current() {
            return None;
        }
        self.0.in_flight.fetch_add(1, Ordering::SeqCst);
        // Invalidation may have won between the first check and increment.
        // In that case this lease was never admitted and must not escape.
        if !self.is_current() {
            RequestLease::release(&self.0);
            return None;
        }
        Some(RequestLease(Some(self.0.clone())))
    }
}

/// Owned admission held continuously while a request effect is applied. It is
/// deliberately Send, unlike a blocking lock guard.
#[derive(Debug)]
pub(crate) struct RequestLease(Option<Arc<RequestFenceInner>>);

impl RequestLease {
    pub(crate) fn unguarded() -> Self { Self(None) }

    fn release(inner: &RequestFenceInner) {
        if inner.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            inner.drained.notify_waiters();
        }
    }
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            Self::release(&inner);
        }
    }
}

/// The "is this response still wanted" check one request attempt carries.
/// The subscription relay uses the predicate to reject superseded attempts
/// (a newer request for the same query has replaced this one); the node
/// additionally attaches a [`RequestFence`] so reset can quiesce effects
/// that already passed the predicate.
///
/// Attempt-local predicates compose onto the node's fence without losing it:
///
/// ```ignore
/// // The node's reset fence, narrowed to one request attempt: the response
/// // applies only while no reset is underway AND this attempt is still the
/// // latest one for its query (see SubscriptionRelay's request_generation).
/// let validity = RequestValidity::fenced(node_fence)
///     .and(move || current_generation() == my_generation);
/// ```
#[derive(Clone)]
pub(crate) struct RequestValidity {
    check: Arc<dyn Fn() -> bool + Send + Sync>,
    fence: Option<RequestFence>,
}

impl fmt::Debug for RequestValidity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("RequestValidity(..)") }
}

impl RequestValidity {
    // The predicate-only constructor and `and` composition (the doc example
    // above) are called by the request paths and subscription relay of later
    // slices; only the fenced form has callers this phase.
    #[allow(dead_code)]
    pub(crate) fn new(check: impl Fn() -> bool + Send + Sync + 'static) -> Self { Self { check: Arc::new(check), fence: None } }

    pub(crate) fn fenced(fence: RequestFence) -> Self {
        let current = fence.clone();
        Self { check: Arc::new(move || current.is_current()), fence: Some(fence) }
    }

    /// Compose another attempt-local predicate without losing the owner's
    /// quiescing fence.
    #[allow(dead_code)]
    pub(crate) fn and(self, check: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        let previous = self.check.clone();
        Self { check: Arc::new(move || previous() && check()), fence: self.fence }
    }

    pub(crate) fn is_current(&self) -> bool { (self.check)() }

    pub(crate) fn try_acquire(&self) -> Option<RequestLease> {
        if !self.is_current() {
            return None;
        }
        let lease = match &self.fence {
            Some(fence) => fence.try_acquire()?,
            None => RequestLease::unguarded(),
        };
        // A relay generation can change independently of the owner fence.
        // Recheck after admission before permitting schema ingestion.
        self.is_current().then_some(lease)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalidation_rejects_new_work_and_waits_for_admitted_work() {
        let fence = RequestFence::new();
        let validity = RequestValidity::fenced(fence.clone());
        let admitted = validity.try_acquire().expect("current validity must admit a lease");

        let reset = {
            let fence = fence.clone();
            tokio::spawn(async move {
                fence.invalidate();
                fence.wait_drained().await;
            })
        };
        tokio::task::yield_now().await;

        assert!(!reset.is_finished(), "reset must wait while an admitted response is still applying");
        assert!(validity.try_acquire().is_none(), "invalidation must reject later response effects");

        drop(admitted);
        tokio::time::timeout(std::time::Duration::from_secs(1), reset)
            .await
            .expect("reset must wake when the final admitted response finishes")
            .expect("reset task must not panic");
    }
}
