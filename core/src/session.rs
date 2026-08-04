//! Represents one or more sessions: the state each principal acts
//! under — one [`Session`] per ordinary Context, enumerable through the
//! node's [`SessionSet`].
//!
//! A Session wraps its current ContextData in a signal, so a long-lived
//! handle's state can change (a token refresh, a re-login) without
//! rebuilding the Context or the livequeries under it: holders keep the
//! session and read the current value at use time, and change
//! subscribers hear each effective update (standing queries re-permission
//! on it in https://github.com/ankurah/ankurah/pull/426). What a session
//! holds is whatever its PolicyAgent evaluates — a bearer token today; a
//! session id, challenge state, or anything else as ContextData
//! generalizes. The SessionSet tracks sessions, undeduplicated: two
//! sessions whose states compare equal today are still independent and
//! may diverge tomorrow, so any deduplication happens at the point of
//! consumption, over current values.

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, Weak,
};

use ankurah_signals::{
    broadcast::{Broadcast, BroadcastId},
    signal::{Listener, ListenerGuard},
    subscribe::IntoSubscribeListener,
    Get, Mut, Peek, Signal, Subscribe, SubscriptionGuard,
};

/// Represents the user session - or whatever other context the PolicyAgent
/// needs to perform its evaluation. The credential vocabulary lives here
/// beside its live holder; node.rs re-exports it for path compatibility.
///
/// `Eq` is operational identity: values compare equal only when
/// substituting one for the other changes nothing observable (for a
/// token credential, the token included), and `Hash` agrees with it.
/// [`Session::update`] delivery gates on this — an update comparing
/// equal to the current value is a no-op.
///
/// `Clone` is assumed cheap: sessions, snapshots, and sends clone
/// values freely — wrap heavy payloads in `Arc`.
pub trait ContextData: Send + Sync + Clone + std::hash::Hash + Eq + 'static {}

/// One live credential: the value a single principal acts under,
/// replaceable in place. An ordinary context owns exactly one in its
/// private set, attached to the node's [`SessionSet`]. Queries and
/// their machinery (livequeries, gap fetchers, the relay) hold their
/// context's set and read its current values once per operation.
/// Cloning shares the one session: handles alias the same cell.
pub struct Session<CD: ContextData>(Arc<SessionInner<CD>>);

impl<CD: ContextData> Clone for Session<CD> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct SessionInner<CD: ContextData> {
    /// The current data for this Session
    current: Mut<CD>,
}

impl<CD: ContextData> Session<CD> {
    /// Begin a session holding `cdata` as its current credential. In no
    /// set until something owns it (a context's source owns its session;
    /// the source is attached to the node's registry at construction).
    pub fn new(cdata: CD) -> Self { Self(Arc::new(SessionInner { current: Mut::new(cdata) })) }

    /// A snapshot of the current credential. Take one per logical
    /// operation, so a mid-operation update cannot mix credentials. (Named
    /// snapshot, not get: the signals lexicon's `get` is an
    /// observer-tracked read, and this is deliberately untracked.)
    pub fn snapshot(&self) -> CD { self.0.current.value() }

    /// Replace the credential. A value comparing equal to the current one
    /// is a complete no-op, no store and no notification: `Eq` is
    /// operational identity per [`ContextData`], so
    /// equal means nothing observable changed and there is nothing to
    /// re-permission. A token refresh carries a new token, compares
    /// unequal, and notifies holders and change subscribers.
    pub fn update(&self, cdata: CD) {
        // The compare and the store take the lock separately; a racing
        // update can only make this comparison stale in ways that
        // linearize legally (a suppressed call was a no-op against SOME
        // current value, and the racing update's own notification covers
        // the change).
        if self.0.current.value() == cdata {
            return;
        }
        self.0.current.set(cdata);
    }
}

// A Session IS a signal over its credential, so it implements the standard
// signals vocabulary by delegation (beside the inherent, doc-carrying
// `snapshot`, the same coexistence `Mut::value` has with its trait impls).
impl<CD: ContextData> Signal for Session<CD> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0.current.listen(listener) }
    fn broadcast_id(&self) -> BroadcastId { self.0.current.broadcast_id() }
}

impl<CD: ContextData> Get<CD> for Session<CD> {
    fn get(&self) -> CD { self.0.current.get() }
}

impl<CD: ContextData> Peek<CD> for Session<CD> {
    fn peek(&self) -> CD { self.0.current.value() }
}

impl<CD: ContextData> Subscribe<CD> for Session<CD> {
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<CD> {
        self.0.current.subscribe(listener)
    }
}

impl<CD: ContextData> std::fmt::Debug for Session<CD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The credential is deliberately omitted: CDs can carry tokens,
        // and Debug output reaches logs.
        f.debug_struct("Session").finish_non_exhaustive()
    }
}

/// A set of sessions — the universal credential-source shape. An
/// ordinary context's source is one (owning exactly the session it
/// acts as); the node's registry is another, made the continuous
/// superset of every session backing a context by each context
/// attaching its source at construction: attached sets join the union
/// through live edges, so their additions, removals, and value changes
/// are reflected as they happen.
pub struct SessionSet<CD: ContextData>(Arc<SessionSetInner<CD>>);

impl<CD: ContextData> Clone for SessionSet<CD> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<CD: ContextData> std::fmt::Debug for SessionSet<CD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Member values are deliberately omitted (CDs can carry tokens,
        // and Debug reaches logs); counts only, so a Debug line does not
        // walk the recursive union.
        let members = self.0.sessions.lock().unwrap_or_else(|e| e.into_inner()).len();
        let attached = self.0.attached.lock().unwrap_or_else(|e| e.into_inner()).len();
        f.debug_struct("SessionSet").field("members", &members).field("attached", &attached).finish()
    }
}

struct SessionSetInner<CD: ContextData> {
    sessions: Mutex<HashMap<u64, SessionSlot<CD>>>,
    next_slot: AtomicU64,
    /// Fires on any membership change (own, attach, an attached source
    /// dropping) and on any member's value change (each slot and each
    /// attached set forwards its signal).
    changed: Broadcast<()>,
    /// Attached sets, unioned into this one through live edges: reads
    /// recurse through the edge, so additions and removals in an
    /// attached set are reflected here as they happen. Edges are weak —
    /// a dead set is skipped on read.
    attached: Mutex<Vec<AttachedSet<CD>>>,
    /// Parents holding an edge to this set, notified when it drops so a
    /// watcher of the parent hears this set's members leave.
    parents: Mutex<Vec<Weak<SessionSetInner<CD>>>>,
}

struct AttachedSet<CD: ContextData> {
    set: Weak<SessionSetInner<CD>>,
    /// Forwards the attached set's change signal into `changed`;
    /// dropped with the edge.
    _forward: ListenerGuard,
}

impl<CD: ContextData> Drop for SessionSetInner<CD> {
    fn drop(&mut self) {
        // A dying set's members leave every parent's union; fire so
        // watchers hear the departure (the dead edge itself is skipped
        // on the next read).
        for parent in self.parents.lock().unwrap_or_else(|e| e.into_inner()).drain(..) {
            if let Some(parent) = parent.upgrade() {
                parent.changed.send(());
            }
        }
    }
}

struct SessionSlot<CD: ContextData> {
    /// The set is this member's liveness: it lives as long as the set
    /// (a context's own session, a server query's subscriber session).
    session: Session<CD>,
    /// Forwards the member's change signal into `changed`; dropped with
    /// the slot.
    _forward: ListenerGuard,
}

impl<CD: ContextData> SessionSet<CD> {
    pub fn new() -> Self {
        Self(Arc::new(SessionSetInner {
            sessions: Mutex::new(HashMap::new()),
            next_slot: AtomicU64::new(0),
            changed: Broadcast::new(),
            attached: Mutex::new(Vec::new()),
            parents: Mutex::new(Vec::new()),
        }))
    }

    /// Attach another set: its members join this set's union through a
    /// live edge — additions, removals, and value changes over there
    /// are reflected here as they happen, because reads recurse through
    /// the edge and its change signal forwards into this set's. This is
    /// how the node's registry stays the continuous superset of every
    /// session backing a context. A no-op for self-attachment (the
    /// registry backing a context is its own source); an attach
    /// that would form a cycle is refused, warned and ignored.
    pub fn attach(&self, other: &SessionSet<CD>) {
        if Arc::ptr_eq(&self.0, &other.0) {
            return;
        }
        if other.reaches(self) {
            tracing::warn!("refusing attach: it would form a cycle");
            return;
        }
        {
            let mut attached = self.0.attached.lock().unwrap_or_else(|e| e.into_inner());
            // Dead edges are skipped on read; prune them here so
            // re-attachment churn cannot grow the list without bound.
            attached.retain(|edge| edge.set.strong_count() > 0);
            let other_weak = Arc::downgrade(&other.0);
            if attached.iter().any(|edge| edge.set.ptr_eq(&other_weak)) {
                return;
            }
            let forward = {
                let changed = self.0.changed.clone();
                // listen, not subscribe: this edge only needs the change
                // notification — subscribe would recompute the attached
                // set's whole recursive union on every fire just to
                // discard it.
                other.listen(Arc::new(move |_| changed.send(())))
            };
            attached.push(AttachedSet { set: other_weak, _forward: forward });
        }
        other.0.parents.lock().unwrap_or_else(|e| e.into_inner()).push(Arc::downgrade(&self.0));
        // Fire outside the locks: subscribers read the set back.
        self.0.changed.send(());
    }

    /// Take ownership of a session: the set becomes the member's
    /// liveness, holding it strong until the set drops. This is how a
    /// context holds the principal it acts as. A no-op when this set
    /// already owns it.
    pub fn own(&self, session: &Session<CD>) {
        let mut slots = self.0.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if slots.values().any(|slot| Arc::ptr_eq(&slot.session.0, &session.0)) {
            return;
        }
        let slot = self.0.next_slot.fetch_add(1, Ordering::Relaxed);
        let forward = {
            let changed = self.0.changed.clone();
            // listen, not subscribe: this slot only needs the change
            // notification — subscribe would clone the credential value
            // on every fire just to discard it.
            session.listen(Arc::new(move |_| changed.send(())))
        };
        slots.insert(slot, SessionSlot { session: session.clone(), _forward: forward });
        drop(slots);
        // Fire outside the lock: subscribers read the set back.
        self.0.changed.send(());
    }

    /// Every currently live session — owned members in slot order, then
    /// each attached set's, depth-first. A set union, not a path
    /// expansion: each set is visited once (a diamond's shared source
    /// contributes once) and each session listed once by handle identity
    /// (distinct sessions whose values compare equal stay distinct).
    pub fn sessions(&self) -> Vec<Session<CD>> {
        let mut visited = HashSet::new();
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        self.collect_sessions(&mut visited, &mut seen, &mut out);
        out
    }

    fn collect_sessions(&self, visited: &mut HashSet<*const ()>, seen: &mut HashSet<*const ()>, out: &mut Vec<Session<CD>>) {
        if !visited.insert(Arc::as_ptr(&self.0) as *const ()) {
            return;
        }
        let own: Vec<Session<CD>> = {
            let map = self.0.sessions.lock().unwrap_or_else(|e| e.into_inner());
            let mut live: Vec<_> = map.iter().map(|(slot, entry)| (*slot, entry.session.clone())).collect();
            live.sort_by_key(|(slot, _)| *slot);
            live.into_iter().map(|(_, session)| session).collect()
        };
        for session in own {
            if seen.insert(Arc::as_ptr(&session.0) as *const ()) {
                out.push(session);
            }
        }
        // Snapshot the edges, then recurse with no lock held.
        let edges: Vec<_> = self.0.attached.lock().unwrap_or_else(|e| e.into_inner()).iter().map(|edge| edge.set.clone()).collect();
        for edge in edges {
            if let Some(inner) = edge.upgrade() {
                SessionSet(inner).collect_sessions(visited, seen, out);
            }
        }
    }

    /// Whether `target` is reachable from this set through attached
    /// edges (including being this set). Attach refuses cycle-forming
    /// edges, so the walk terminates.
    fn reaches(&self, target: &SessionSet<CD>) -> bool {
        if Arc::ptr_eq(&self.0, &target.0) {
            return true;
        }
        let edges: Vec<_> = self.0.attached.lock().unwrap_or_else(|e| e.into_inner()).iter().map(|edge| edge.set.clone()).collect();
        edges.into_iter().filter_map(|edge| edge.upgrade()).any(|inner| SessionSet(inner).reaches(target))
    }

    /// The current value of every live session, in slot order.
    pub fn current(&self) -> Vec<CD> { self.sessions().iter().map(|session| session.snapshot()).collect() }

    /// The single credential write paths act as. A write is one
    /// principal's act — the wire enforces the same rule, exactly one
    /// cdata per CommitTransaction / RegisterSchema request — so a
    /// source with zero or several current sessions refuses. Read paths
    /// use every member (the union) instead.
    pub fn write_credential(&self) -> Result<CD, crate::policy::AccessDenied> {
        let mut sessions = self.sessions().into_iter();
        match (sessions.next(), sessions.next()) {
            (Some(session), None) => Ok(session.snapshot()),
            (None, _) => Err(crate::policy::AccessDenied::ByPolicy("write operations require a session; this context's source has none")),
            _ => Err(crate::policy::AccessDenied::ByPolicy(
                "write operations act as one principal; this context's source has several sessions",
            )),
        }
    }
}

impl<CD: ContextData> From<CD> for SessionSet<CD> {
    /// Bare state becomes a private set owning one freshly minted
    /// session — the shape of an ordinary context's source.
    fn from(cdata: CD) -> Self {
        let set = SessionSet::new();
        set.own(&Session::new(cdata));
        set
    }
}

impl<CD: ContextData> From<Session<CD>> for SessionSet<CD> {
    /// An existing session becomes a private set owning it; the
    /// caller's handle keeps working (clones share the one session).
    fn from(session: Session<CD>) -> Self {
        let set = SessionSet::new();
        set.own(&session);
        set
    }
}

// The SessionSet is a signal over the union of its members' current
// credentials: it fires on membership changes and on any member's value
// change, and reads compute the union fresh (consumers recompute from the
// full current set; diffs would buy them nothing).
impl<CD: ContextData> Signal for SessionSet<CD> {
    fn listen(&self, listener: Listener) -> ListenerGuard { ListenerGuard::new(self.0.changed.reference().listen(listener)) }
    fn broadcast_id(&self) -> BroadcastId { self.0.changed.id() }
}

impl<CD: ContextData> Get<Vec<CD>> for SessionSet<CD> {
    fn get(&self) -> Vec<CD> {
        ankurah_signals::CurrentObserver::track(self);
        self.current()
    }
}

impl<CD: ContextData> Peek<Vec<CD>> for SessionSet<CD> {
    fn peek(&self) -> Vec<CD> { self.current() }
}

impl<CD: ContextData> Subscribe<Vec<CD>> for SessionSet<CD> {
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<Vec<CD>> {
        let listener = listener.into_subscribe_listener();
        // A weak inner breaks the cycle set -> broadcast -> listener -> set.
        let weak = Arc::downgrade(&self.0);
        let subscription = self.listen(Arc::new(move |_| {
            if let Some(inner) = weak.upgrade() {
                listener(SessionSet(inner).current());
            }
        }));
        SubscriptionGuard::new(subscription)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A value-carrying test credential. Equality is full-value
    /// (operational identity per the [`ContextData`] contract), so a
    /// token refresh — same subject, new token — compares unequal and an
    /// identical update compares equal.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct TestCd {
        subject: u8,
        token: u8,
    }
    impl ContextData for TestCd {}

    /// A context-shaped source — a private set owning its session,
    /// attached to a registry — joins the registry's union while it
    /// lives, and its members leave when it drops.
    #[test]
    fn attached_set_liveness() {
        let registry: SessionSet<TestCd> = SessionSet::new();
        let source: SessionSet<TestCd> = TestCd { subject: 1, token: 0 }.into();
        registry.attach(&source);
        assert_eq!(registry.sessions().len(), 1, "attached members join the union");

        let second: SessionSet<TestCd> = TestCd { subject: 2, token: 0 }.into();
        registry.attach(&second);
        assert_eq!(registry.sessions().len(), 2);

        drop(second);
        assert_eq!(registry.sessions().len(), 1, "a dropped source's members leave the union");
        drop(source);
        assert!(registry.sessions().is_empty());
    }

    /// Updates are visible to every holder and fire change subscribers
    /// with the new value.
    #[test]
    fn update_is_shared_and_reactive() {
        let session = Session::new(TestCd { subject: 1, token: 1 });
        let holder = session.clone();

        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let _guard = session.subscribe(move |value: TestCd| {
            sink.lock().unwrap().push(value.token);
        });

        session.update(TestCd { subject: 2, token: 2 });
        assert_eq!(holder.snapshot().token, 2, "holders read the new value");
        assert_eq!(seen.lock().unwrap().as_slice(), &[2], "subscriber fired with the new value");
    }

    /// A token refresh — same subject, new token — is a real change: it
    /// compares unequal and fires the subscriber. An identical update is
    /// a complete no-op: no notification.
    #[test]
    fn refresh_notifies_and_identical_update_is_a_noop() {
        let session = Session::new(TestCd { subject: 1, token: 1 });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let _guard = session.subscribe(move |value: TestCd| {
            sink.lock().unwrap().push(value.token);
        });

        let refreshed = TestCd { subject: 1, token: 2 };
        assert_ne!(session.snapshot(), refreshed, "a refresh carries a new token, so it compares unequal");
        session.update(refreshed);
        assert_eq!(seen.lock().unwrap().as_slice(), &[2], "the refresh fires the subscriber");

        session.update(TestCd { subject: 1, token: 2 });
        assert_eq!(seen.lock().unwrap().as_slice(), &[2], "an identical update does not notify");
        assert_eq!(session.snapshot().token, 2, "the stored value is unchanged");
    }

    /// A new session belongs to no set until something owns it.
    #[test]
    fn new_sessions_belong_to_no_set() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let _infra = Session::new(TestCd { subject: 1, token: 0 });
        assert!(set.sessions().is_empty());
    }

    /// The set is a signal over the union of current values: it fires on
    /// own, on any member's update, on attach, and when an attached
    /// source drops.
    #[test]
    fn set_fires_on_membership_and_member_updates() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let fired = Arc::new(Mutex::new(Vec::new()));
        let sink = fired.clone();
        let _guard = set.subscribe(move |current: Vec<TestCd>| {
            sink.lock().unwrap().push(current.iter().map(|cd| cd.token).collect::<Vec<_>>());
        });

        let a = Session::new(TestCd { subject: 1, token: 10 });
        set.own(&a);
        assert_eq!(fired.lock().unwrap().last(), Some(&vec![10]), "owning fires with the new union");

        a.update(TestCd { subject: 1, token: 11 });
        assert_eq!(fired.lock().unwrap().last(), Some(&vec![11]), "a member's update fires with its new value");

        let child: SessionSet<TestCd> = TestCd { subject: 2, token: 20 }.into();
        set.attach(&child);
        assert_eq!(fired.lock().unwrap().last(), Some(&vec![11, 20]), "attaching fires with the joined union");

        let count = fired.lock().unwrap().len();
        drop(child);
        assert_eq!(fired.lock().unwrap().last(), Some(&vec![11]), "a dropped source fires the shrunken union");
        assert_eq!(fired.lock().unwrap().len(), count + 1, "exactly the drop notification fired");
    }

    /// Peek reads the union without tracking; the porcelain and the
    /// inherent accessors agree.
    #[test]
    fn porcelain_and_inherent_accessors_agree() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let session = Session::new(TestCd { subject: 1, token: 1 });
        set.own(&session);
        assert_eq!(session.peek(), session.snapshot());
        assert_eq!(set.peek(), set.current());
        assert_eq!(set.peek(), vec![TestCd { subject: 1, token: 1 }]);
    }
}
