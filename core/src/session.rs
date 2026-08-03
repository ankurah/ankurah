//! Live credential holders: one [`Session`] per ordinary Context,
//! enumerable through the node's [`SessionSet`].
//!
//! A Session wraps the current ContextData in a signal, so a long-lived
//! handle's credential can change (a token refresh, a re-login) without
//! rebuilding the Context or the livequeries under it: holders keep the
//! session and read the current value at use time, and subscribers (the
//! relay) react to a change by re-permissioning remote subscriptions.
//! The SessionSet tracks the node's live sessions, undeduplicated: two
//! sessions whose credentials compare equal today are still independent
//! and may diverge tomorrow, so any deduplication happens at the point of
//! consumption, over current values. Membership is liveness itself: the
//! `Arc<Session>` strong count is the token, and the last drop culls the
//! slot.

use std::collections::HashMap;
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
pub trait ContextData: Send + Sync + Clone + std::hash::Hash + Eq + 'static {}

/// One live credential: the value a single principal acts under,
/// replaceable in place. An ordinary context registers exactly one in
/// the node's [`SessionSet`]; the system context reads through the
/// whole set instead. Queries and their machinery (livequeries, gap
/// fetchers, the relay) hold their context's [`Sessions`] source and
/// read its current value once per operation.
pub struct Session<CD: ContextData> {
    current: Mut<CD>,
    /// Needed to close the TOCTOU between deriving state from a credential
    /// snapshot and subscribing to its changes (the livequery constructor
    /// re-checks it once its listener is live). Value comparison cannot
    /// serve as that re-check: a change-and-revert inside the window (a
    /// re-login to another user and back) leaves the value equal to the
    /// snapshot while an in-window send may have shipped the interloper.
    /// Bumped before the value stores, once per effective update.
    generation: std::sync::atomic::AtomicU64,
    /// The owning [`SessionSet`] slot, absent for detached sessions.
    registry: Option<(Weak<SessionSetInner<CD>>, u64)>,
}

impl<CD: ContextData> Session<CD> {
    /// A session outside any SessionSet: infrastructure credentials (and
    /// server-side per-query holders) that must not participate in the
    /// node's active-session enumeration.
    pub fn detached(cdata: CD) -> Arc<Self> {
        Arc::new(Self { current: Mut::new(cdata), generation: std::sync::atomic::AtomicU64::new(0), registry: None })
    }

    /// A snapshot of the current credential. Take one per logical
    /// operation, so a mid-operation update cannot mix credentials. (Named
    /// snapshot, not get: the signals lexicon's `get` is an
    /// observer-tracked read, and this is deliberately untracked.)
    pub fn snapshot(&self) -> CD { self.current.value() }

    /// Replace the credential. A value comparing equal to the current one
    /// is a complete no-op — no store, no generation bump, no
    /// notification: `Eq` is operational identity per [`ContextData`], so
    /// equal means nothing observable changed and there is nothing to
    /// re-permission. A token refresh carries a new token, compares
    /// unequal, and notifies holders and change subscribers.
    pub fn update(&self, cdata: CD) {
        // The compare and the store take the lock separately; a racing
        // update can only make this comparison stale in ways that
        // linearize legally (a suppressed call was a no-op against SOME
        // current value, and the racing update's own notification covers
        // the change).
        if self.current.value() == cdata {
            return;
        }
        self.generation.fetch_add(1, Ordering::SeqCst);
        if let Some((registry, _)) = &self.registry {
            if let Some(registry) = registry.upgrade() {
                registry.generation.fetch_add(1, Ordering::SeqCst);
            }
        }
        self.current.set(cdata);
    }

    /// The effective-update count; see the field doc for the TOCTOU it closes.
    pub fn generation(&self) -> u64 { self.generation.load(Ordering::SeqCst) }

    /// Subscribe to credential changes; the listener receives each new
    /// value. Dropping the guard unsubscribes.
    pub fn subscribe_changes<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<CD> {
        self.current.subscribe(listener)
    }
}

// A Session IS a signal over its credential, so it implements the standard
// signals vocabulary by delegation (beside the inherent, doc-carrying
// `snapshot`, the same coexistence `Mut::value` has with its trait impls).
impl<CD: ContextData> Signal for Session<CD> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.current.listen(listener) }
    fn broadcast_id(&self) -> BroadcastId { self.current.broadcast_id() }
}

impl<CD: ContextData> Get<CD> for Session<CD> {
    fn get(&self) -> CD { self.current.get() }
}

impl<CD: ContextData> Peek<CD> for Session<CD> {
    fn peek(&self) -> CD { self.current.value() }
}

impl<CD: ContextData> Subscribe<CD> for Session<CD> {
    fn subscribe<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<CD> {
        self.current.subscribe(listener)
    }
}

impl<CD: ContextData> Session<CD> {
    /// Remove this session from its set NOW, without waiting for the last
    /// Arc to drop. Culling by Drop alone defers removal to Arc liveness:
    /// a reader's upgraded Arc (a snapshot in flight) keeps the slot
    /// listed, so a replace that registered successors could be observed
    /// TOGETHER with the members it replaced. Explicit revocation removes
    /// the slot immediately; the eventual Drop then finds it gone.
    pub(crate) fn revoke(&self) {
        if let Some((registry, slot)) = &self.registry {
            if let Some(registry) = registry.upgrade() {
                registry.generation.fetch_add(1, Ordering::SeqCst);
                if registry.sessions.lock().unwrap_or_else(|e| e.into_inner()).remove(slot).is_some() {
                    // Fire outside the lock: subscribers read the set back.
                    registry.changed.send(());
                }
            }
        }
    }
}

impl<CD: ContextData> Drop for Session<CD> {
    fn drop(&mut self) { self.revoke(); }
}

impl<CD: ContextData> std::fmt::Debug for Session<CD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The credential is deliberately omitted: CDs can carry tokens,
        // and Debug output reaches logs.
        f.debug_struct("Session").field("detached", &self.registry.is_none()).finish()
    }
}

/// The node's live sessions. Registration happens at Context construction;
/// culling is RAII (the last `Arc<Session>` drop removes the slot).
pub struct SessionSet<CD: ContextData>(Arc<SessionSetInner<CD>>);

impl<CD: ContextData> Clone for SessionSet<CD> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

struct SessionSetInner<CD: ContextData> {
    sessions: Mutex<HashMap<u64, SessionSlot<CD>>>,
    next_slot: AtomicU64,
    /// Fires on any membership change (register, cull) and on any live
    /// member's value change (each slot forwards its session's signal).
    changed: Broadcast<()>,
    /// Composite update count, bumped BEFORE the change it counts becomes
    /// observable (registrations, culls, and member updates alike), so the
    /// snapshot-then-subscribe re-check ([`Session::generation`]) works
    /// over the whole set.
    generation: AtomicU64,
}

struct SessionSlot<CD: ContextData> {
    session: Weak<Session<CD>>,
    /// Forwards the member's change signal into `changed`; dropped with
    /// the slot.
    _forward: SubscriptionGuard,
}

impl<CD: ContextData> SessionSet<CD> {
    pub fn new() -> Self {
        Self(Arc::new(SessionSetInner {
            sessions: Mutex::new(HashMap::new()),
            next_slot: AtomicU64::new(0),
            changed: Broadcast::new(),
            generation: AtomicU64::new(0),
        }))
    }

    /// Register a new live session holding `cdata`.
    pub fn register(&self, cdata: CD) -> Arc<Session<CD>> {
        self.0.generation.fetch_add(1, Ordering::SeqCst);
        let slot = self.0.next_slot.fetch_add(1, Ordering::Relaxed);
        let session =
            Arc::new(Session { current: Mut::new(cdata), generation: AtomicU64::new(0), registry: Some((Arc::downgrade(&self.0), slot)) });
        let forward = {
            let changed = self.0.changed.clone();
            session.subscribe_changes(move |_new: CD| changed.send(()))
        };
        self.0
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(slot, SessionSlot { session: Arc::downgrade(&session), _forward: forward });
        // Fire outside the lock: subscribers read the set back.
        self.0.changed.send(());
        session
    }

    /// Every currently live session, in slot order.
    pub fn sessions(&self) -> Vec<Arc<Session<CD>>> {
        let map = self.0.sessions.lock().unwrap_or_else(|e| e.into_inner());
        let mut live: Vec<_> = map.iter().filter_map(|(slot, entry)| Some((*slot, entry.session.upgrade()?))).collect();
        live.sort_by_key(|(slot, _)| *slot);
        live.into_iter().map(|(_, session)| session).collect()
    }

    /// The current credential of every live session, in slot order.
    pub fn current(&self) -> Vec<CD> { self.sessions().iter().map(|session| session.snapshot()).collect() }

    /// The composite update count; see [`Session::generation`] for the
    /// TOCTOU it closes. Moves on registrations, culls, and member
    /// updates alike.
    pub fn generation(&self) -> u64 { self.0.generation.load(Ordering::SeqCst) }
}

/// The credential source behind a query: exactly one session (an ordinary
/// context's query), the node's whole live set (a system query), or a
/// held set the query's server side replaces wholesale on re-validated
/// subscribes. Sends snapshot the CURRENT credentials; subscribers fire
/// on any change, and the composite generation closes the same
/// snapshot-then-subscribe TOCTOU as [`Session::generation`]. Only `One`
/// can write: the other arms act as no single principal.
pub enum Sessions<CD: ContextData> {
    One(Arc<Session<CD>>),
    Set(SessionSet<CD>),
    Held(SessionHolder<CD>),
}

impl<CD: ContextData> Clone for Sessions<CD> {
    fn clone(&self) -> Self {
        match self {
            Self::One(session) => Self::One(session.clone()),
            Self::Set(set) => Self::Set(set.clone()),
            Self::Held(holder) => Self::Held(holder.clone()),
        }
    }
}

impl<CD: ContextData> std::fmt::Debug for Sessions<CD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::One(session) => f.debug_tuple("Sessions::One").field(session).finish(),
            Self::Set(set) => f.debug_tuple("Sessions::Set").field(&set.sessions().len()).finish(),
            Self::Held(holder) => f.debug_tuple("Sessions::Held").field(&holder.0.set.sessions().len()).finish(),
        }
    }
}

impl<CD: ContextData> Sessions<CD> {
    /// The current credentials, one per live session.
    pub fn snapshot(&self) -> Vec<CD> {
        match self {
            Self::One(session) => vec![session.snapshot()],
            Self::Set(set) => set.current(),
            Self::Held(holder) => holder.0.set.current(),
        }
    }

    /// Subscribe to credential changes; the listener receives the new
    /// current credentials. For a set, membership changes fire too.
    pub fn subscribe_changes<F>(&self, listener: F) -> SubscriptionGuard
    where F: IntoSubscribeListener<Vec<CD>> {
        match self {
            Self::One(session) => {
                let listener = listener.into_subscribe_listener();
                session.subscribe_changes(move |cdata: CD| listener(vec![cdata]))
            }
            Self::Set(set) => set.subscribe(listener),
            Self::Held(holder) => holder.0.set.subscribe(listener),
        }
    }

    /// The update count; see [`Session::generation`] for the TOCTOU it
    /// closes.
    pub fn generation(&self) -> u64 {
        match self {
            Self::One(session) => session.generation(),
            Self::Set(set) => set.generation(),
            Self::Held(holder) => holder.0.set.generation(),
        }
    }
}

impl<CD: ContextData> From<Arc<Session<CD>>> for Sessions<CD> {
    fn from(session: Arc<Session<CD>>) -> Self { Self::One(session) }
}

impl<CD: ContextData> From<SessionHolder<CD>> for Sessions<CD> {
    fn from(holder: SessionHolder<CD>) -> Self { Self::Held(holder) }
}

/// A set plus the memberships that constitute it -- the server's
/// per-query credential holder ("held" as in: it keeps the member
/// sessions registered, and replaces them wholesale when a re-validated
/// subscribe delivers fresh credentials). Replacement revokes FIRST so a
/// concurrent reader's snapshot sees the old members, the new members,
/// partial-old, or an empty set, but never old-union-new: during a
/// narrowing re-subscribe the transient fails closed. The set is private
/// to its holder and has no change subscribers, so registrations and
/// culls dispatch to nobody; mutating it under a lock is safe. Cloning
/// shares the ONE holder (handles alias the same set and held members).
pub struct SessionHolder<CD: ContextData>(Arc<SessionHolderInner<CD>>);

struct SessionHolderInner<CD: ContextData> {
    set: SessionSet<CD>,
    held: Mutex<Vec<Arc<Session<CD>>>>,
}

impl<CD: ContextData> Clone for SessionHolder<CD> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

impl<CD: ContextData> SessionHolder<CD> {
    pub(crate) fn new(cdatas: Vec<CD>) -> Self {
        let set = SessionSet::new();
        let held = cdatas.into_iter().map(|cdata| set.register(cdata)).collect();
        Self(Arc::new(SessionHolderInner { set, held: Mutex::new(held) }))
    }

    /// Replace every member with the given credentials.
    pub(crate) fn replace(&self, cdatas: Vec<CD>) {
        let mut held = self.0.held.lock().unwrap_or_else(|e| e.into_inner());
        // Revoke explicitly BEFORE registering successors: Drop-based
        // culling is deferred by any reader's upgraded Arc, under which
        // an overlapping snapshot could list old and new members
        // together — the union would read under the credential being
        // replaced. Revocation removes the slots immediately, so the
        // transient states are old, partial-old, empty, or new: never a
        // union, failing closed in the narrowing direction.
        for session in held.drain(..) {
            session.revoke();
        }
        *held = cdatas.into_iter().map(|cdata| self.0.set.register(cdata)).collect();
    }
}

impl<CD: ContextData> From<SessionSet<CD>> for Sessions<CD> {
    fn from(set: SessionSet<CD>) -> Self { Self::Set(set) }
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

impl<CD: ContextData> Default for SessionSet<CD> {
    fn default() -> Self { Self::new() }
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

    /// Revocation removes a member IMMEDIATELY, even while an Arc still
    /// holds the Session alive — the property SessionHolder::replace
    /// relies on so an overlapping snapshot can never list a replaced
    /// member alongside its successor (a union would read under the
    /// credential being replaced).
    #[test]
    fn revocation_is_immediate_despite_live_holders() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let old_member = set.register(TestCd { subject: 1, token: 0 });
        old_member.revoke();
        let _new_member = set.register(TestCd { subject: 2, token: 0 });
        let visible: Vec<u8> = set.current().into_iter().map(|cd| cd.subject).collect();
        assert_eq!(visible, vec![2], "the revoked member must not appear while its Arc lives, got {:?}", visible);
        drop(old_member); // the eventual Drop finds the slot already gone
        assert_eq!(set.current().len(), 1);
    }

    /// Sessions appear in the set while any holder lives and vanish when
    /// the last holder drops.
    #[test]
    fn membership_is_liveness() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let a = set.register(TestCd { subject: 1, token: 0 });
        let b = set.register(TestCd { subject: 2, token: 0 });
        assert_eq!(set.sessions().len(), 2);

        let extension = a.clone();
        drop(a);
        assert_eq!(set.sessions().len(), 2, "an extension holder keeps the session live");
        drop(extension);
        assert_eq!(set.sessions().len(), 1, "the last drop culls the slot");
        drop(b);
        assert!(set.sessions().is_empty());
    }

    /// Updates are visible to every holder and fire change subscribers
    /// with the new value.
    #[test]
    fn update_is_shared_and_reactive() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let session = set.register(TestCd { subject: 1, token: 1 });
        let holder = session.clone();

        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let _guard = session.subscribe_changes(move |value: TestCd| {
            sink.lock().unwrap().push(value.token);
        });

        session.update(TestCd { subject: 2, token: 2 });
        assert_eq!(holder.snapshot().token, 2, "holders read the new value");
        assert_eq!(seen.lock().unwrap().as_slice(), &[2], "subscriber fired with the new value");
    }

    /// A token refresh — same subject, new token — is a real change: it
    /// compares unequal and fires the subscriber. An identical update is
    /// a complete no-op: no notification, no generation bump.
    #[test]
    fn refresh_notifies_and_identical_update_is_a_noop() {
        let session = Session::detached(TestCd { subject: 1, token: 1 });
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let _guard = session.subscribe_changes(move |value: TestCd| {
            sink.lock().unwrap().push(value.token);
        });

        let refreshed = TestCd { subject: 1, token: 2 };
        assert_ne!(session.snapshot(), refreshed, "a refresh carries a new token, so it compares unequal");
        session.update(refreshed);
        assert_eq!(seen.lock().unwrap().as_slice(), &[2], "the refresh fires the subscriber");

        let before = session.generation();
        session.update(TestCd { subject: 1, token: 2 });
        assert_eq!(seen.lock().unwrap().as_slice(), &[2], "an identical update does not notify");
        assert_eq!(session.generation(), before, "an identical update does not bump the generation");
        assert_eq!(session.snapshot().token, 2, "the stored value is unchanged");
    }

    /// Detached sessions never enter the set.
    #[test]
    fn detached_sessions_are_invisible() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let _infra = Session::detached(TestCd { subject: 1, token: 0 });
        assert!(set.sessions().is_empty());
    }

    /// The generation bumps once per effective update and not for gated
    /// no-ops: the snapshot-then-subscribe re-check must see exactly the
    /// updates that changed something.
    #[test]
    fn generation_counts_every_effective_update() {
        let session = Session::detached(TestCd { subject: 1, token: 1 });
        let before = session.generation();
        session.update(TestCd { subject: 1, token: 2 });
        assert_eq!(session.generation(), before + 1, "a refresh bumps");
        session.update(TestCd { subject: 2, token: 3 });
        assert_eq!(session.generation(), before + 2);
        session.update(TestCd { subject: 2, token: 3 });
        assert_eq!(session.generation(), before + 2, "an identical update does not bump");
    }

    /// The set is a signal over the union of current credentials: it
    /// fires on registration, on any member's update, and on a cull, and
    /// a culled member stops firing it.
    #[test]
    fn set_fires_on_membership_and_member_updates() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let fired = Arc::new(Mutex::new(Vec::new()));
        let sink = fired.clone();
        let _guard = set.subscribe(move |current: Vec<TestCd>| {
            sink.lock().unwrap().push(current.iter().map(|cd| cd.token).collect::<Vec<_>>());
        });

        let a = set.register(TestCd { subject: 1, token: 10 });
        assert_eq!(fired.lock().unwrap().last(), Some(&vec![10]), "registration fires with the new union");

        a.update(TestCd { subject: 1, token: 11 });
        assert_eq!(fired.lock().unwrap().last(), Some(&vec![11]), "a member's update fires with its new value");

        let b = set.register(TestCd { subject: 2, token: 20 });
        assert_eq!(fired.lock().unwrap().last(), Some(&vec![11, 20]));

        drop(a);
        assert_eq!(fired.lock().unwrap().last(), Some(&vec![20]), "a cull fires with the shrunken union");

        let count = fired.lock().unwrap().len();
        // The culled member's session is gone; nothing it held fires again.
        drop(b);
        assert_eq!(fired.lock().unwrap().last(), Some(&Vec::new()));
        assert_eq!(fired.lock().unwrap().len(), count + 1, "exactly the final cull fired");
    }

    /// The set's composite generation moves on registrations, culls, and
    /// member updates alike, closing the construction window for
    /// set-backed consumers the way [`Session::generation`] does for
    /// single-session ones.
    #[test]
    fn set_generation_counts_every_change() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let after_new = set.generation();
        let a = set.register(TestCd { subject: 1, token: 0 });
        assert!(set.generation() > after_new, "a registration bumps");
        let after_register = set.generation();
        a.update(TestCd { subject: 1, token: 1 });
        assert!(set.generation() > after_register, "a member update bumps the set too");
        let after_update = set.generation();
        drop(a);
        assert!(set.generation() > after_update, "a cull bumps");
    }

    /// A `Sessions` source unifies the two credential shapes: One
    /// snapshots a vec of one and forwards its session's updates; Set
    /// snapshots the union.
    #[test]
    fn sessions_source_unifies_one_and_set() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let session = set.register(TestCd { subject: 1, token: 1 });

        let one: Sessions<TestCd> = session.clone().into();
        assert_eq!(one.snapshot(), vec![TestCd { subject: 1, token: 1 }]);

        let many: Sessions<TestCd> = set.clone().into();
        let _b = set.register(TestCd { subject: 2, token: 2 });
        assert_eq!(many.snapshot().len(), 2, "a Set source snapshots every live credential");

        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let _guard = one.subscribe_changes(move |current: Vec<TestCd>| {
            sink.lock().unwrap().push(current.len());
        });
        session.update(TestCd { subject: 1, token: 9 });
        assert_eq!(seen.lock().unwrap().as_slice(), &[1], "a One source forwards its session's update as a vec of one");
    }

    /// Peek reads the union without tracking; the porcelain and the
    /// inherent accessors agree.
    #[test]
    fn porcelain_and_inherent_accessors_agree() {
        let set: SessionSet<TestCd> = SessionSet::new();
        let session = set.register(TestCd { subject: 1, token: 1 });
        assert_eq!(session.peek(), session.snapshot());
        assert_eq!(set.peek(), set.current());
        assert_eq!(set.peek(), vec![TestCd { subject: 1, token: 1 }]);
    }
}
