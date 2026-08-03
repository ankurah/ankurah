use std::{
    marker::PhantomData,
    sync::{Arc, Weak},
};

use ankurah_proto::{self as proto, CollectionId};

use ankurah_signals::{
    broadcast::BroadcastId,
    porcelain::subscribe::{IntoSubscribeListener, SubscriptionGuard},
    signal::{Listener, ListenerGuard},
    Get, Mut, Peek, Read, Signal, Subscribe,
};
use tracing::{debug, warn};

use crate::{
    changes::ChangeSet,
    entity::Entity,
    error::RetrievalError,
    model::View,
    node::{MatchArgs, NodeInner, TNodeErased},
    policy::PolicyAgent,
    reactor::{
        fetch_gap::{GapFetcher, QueryGapFetcher},
        ReactorSubscription, ReactorUpdate,
    },
    resultset::{EntityResultSet, ResultSet},
    storage::StorageEngine,
    Node,
};

/// A local subscription that handles both reactor subscription and remote cleanup
/// This is a type-erased version that can be used in the TContext trait
///
/// Whether the query keeps its node alive is a construction-time choice:
/// [`EntityLiveQuery::new`] holds the node strongly, [`EntityLiveQuery::new_weak_node`] does not.
#[derive(Clone)]
pub struct EntityLiveQuery(Arc<Inner>);

/// Type-erased reference to a node. Strong variants keep the node alive; weak variants do not.
trait NodeRef: Send + Sync {
    fn upgrade(&self) -> Option<Box<dyn TNodeErased>>;
}

/// Strong node reference — keeps the node alive as long as Inner exists.
struct StrongNodeRef<SE, PA: PolicyAgent>(Arc<NodeInner<SE, PA>>);

impl<SE, PA> NodeRef for StrongNodeRef<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn upgrade(&self) -> Option<Box<dyn TNodeErased>> { Some(Box::new(Node(self.0.clone()))) }
}

/// Weak node reference — does NOT keep the node alive.
struct WeakNodeRefImpl<SE, PA: PolicyAgent>(Weak<NodeInner<SE, PA>>);

impl<SE, PA> NodeRef for WeakNodeRefImpl<SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn upgrade(&self) -> Option<Box<dyn TNodeErased>> { self.0.upgrade().map(|inner| Box::new(Node(inner)) as Box<dyn TNodeErased>) }
}

/// Re-derive the policy-filtered, type-resolved selection from a user
/// intent under the session's CURRENT credential. Type-erased so the
/// credential-generic policy machinery is reachable from the erased Inner.
type RefilterFn = Box<dyn Fn(ankql::ast::Selection) -> Result<ankql::ast::Selection, RetrievalError> + Send + Sync>;

/// The two-leg condition of a livequery, one reactive value exposed by
/// [`EntityLiveQuery::status`]. Each leg reports where the current
/// selection stands. Nothing here latches: a query fails, un-fails, and
/// re-fails as credentials change, peers come and go, and the server
/// answers, and the status follows.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryStatus {
    pub local: LocalStatus,
    pub remote: RemoteStatus,
}

/// The local leg: what this node's reactor serves for the query.
#[derive(Debug, Clone, PartialEq)]
pub enum LocalStatus {
    /// No registration has landed yet: construction through first
    /// activation, and the window between a heal from [`Self::Denied`]
    /// and its activation completing. An activation FAILURE leaves the
    /// last truthful state standing (Pending, or the prior Active
    /// version, whichever held) with the error latched on `error()`;
    /// Active is only ever reported by a registration that actually
    /// landed.
    Pending,
    /// No current credential grants access. The reactor holds no
    /// registration, the resultset is empty (clawed back from the live
    /// view; locally persisted rows are untouched), and the user's intent
    /// is retained; the next credential change re-derives. Single-
    /// credential queries fail CONSTRUCTION loud instead of starting
    /// here, but any standing query enters this state when a credential
    /// change revokes its access.
    Denied { reason: String },
    /// The effective selection at `version` IS the reactor registration:
    /// reported by the registration itself as it completes (the
    /// resultset's loaded flag tracks content arrival).
    Active { version: u32 },
}

/// The authoritative effective selection, guarded by [`Inner::update_lock`]:
/// policy-filtered under the credential current at derivation and
/// type-resolved — EXCEPT under denial, where it deliberately holds the
/// RAW intent (there is no effective selection and no reactor
/// registration; the server applies its own filtering to whatever
/// arrives). The `selection` Mut is porcelain fed from this via the
/// effects queue.
struct CurrentSelection {
    selection: ankql::ast::Selection,
    version: u32,
}

/// A deferred signal dispatch. Mutations enqueue effects under their
/// locks; [`Inner::drain_effects`] fires them AFTER release, in enqueue
/// order, through a single dispatcher — so a subscriber that re-enters
/// the query (update_selection, a credential refresh) finds every lock
/// free, and its own enqueues are dispatched by the active drainer.
enum Effect {
    Status(QueryStatus),
    Selection(ankql::ast::Selection, u32),
    /// The error latch, versioned by WHERE it is enqueued: only an
    /// activation whose version was still current at completion (checked
    /// under the update lock) enqueues, so a superseded failure or
    /// success can never overwrite the latch that belongs to the newer
    /// version, and dispatch order through the queue settles the rest.
    Error(Option<RetrievalError>),
}

struct EffectState {
    /// The authoritative status fold (the `status` Mut lags during
    /// dispatch; readers of record go through here).
    current: QueryStatus,
    /// Floor for remote-leg reports: the relay stamps each report under
    /// its own lock, and delivery order is thread scheduling, so a
    /// delayed older report must not overwrite a newer one.
    remote_seq: u64,
    queue: std::collections::VecDeque<Effect>,
    draining: bool,
}

/// The remote leg: where the upstream subscription stands. The relay
/// reports every transition; durable-node queries have no remote leg.
#[derive(Debug, Clone, PartialEq)]
pub enum RemoteStatus {
    /// No remote leg exists (this node answers the query itself).
    None,
    /// Waiting for a durable peer, or queued for retry.
    Pending,
    Requested {
        version: u32,
    },
    Established {
        version: u32,
    },
    /// This node's policy refused to sign the request with the current
    /// credentials; a credential change re-attempts.
    Denied {
        reason: String,
    },
    /// A non-retryable failure. The server's refusals arrive here as its
    /// error text (the wire has no structured denial yet); retryable
    /// transport failures report as [`RemoteStatus::Pending`] instead.
    Error {
        message: String,
    },
}

struct Inner {
    pub(crate) query_id: proto::QueryId,
    // subscription must be declared before node so it drops first —
    // dropping node (StrongNodeRef) deallocates the reactor, and
    // subscription's Drop needs the reactor to unsubscribe.
    pub(crate) subscription: ReactorSubscription,
    pub(crate) node: Box<dyn NodeRef>,
    pub(crate) resultset: EntityResultSet,
    pub(crate) error: Mut<Option<RetrievalError>>,
    pub(crate) initialized: tokio::sync::Notify,
    pub(crate) initialized_version: std::sync::atomic::AtomicU32,
    // Version tracking for predicate updates
    pub(crate) current_version: std::sync::atomic::AtomicU32,
    // Porcelain mirror of the authoritative selection in `update_lock`
    // (reactive, observable in WASM); fed through the effects queue, so
    // it may briefly lag the authority mid-dispatch.
    pub(crate) selection: Mut<(ankql::ast::Selection, u32)>,
    // The user's PRE-FILTER intent, retained so a credential change can
    // re-derive the effective selection instead of re-sending a filter
    // baked under the old credential.
    pub(crate) intent_selection: std::sync::Mutex<ankql::ast::Selection>,
    // See RefilterFn.
    pub(crate) refilter: RefilterFn,
    // THE serialization point, guarding the authoritative selection:
    // reissue's read-refilter-mint-store, activation's snapshot and its
    // post-registration re-validation, and the initialization report all
    // bracket under it, so a selection update, a credential
    // re-permission, and an in-flight activation can never interleave
    // into a stale registration or a resurrected resultset. This code
    // never FIRES a signal while holding it (see Effect) — though a
    // concurrent drainer may dispatch effects enqueued here while the
    // lock is still held; that drainer holds no locks, so a re-entrant
    // subscriber blocks only for this critical section, never deadlocks.
    pub(crate) update_lock: std::sync::Mutex<CurrentSelection>,
    // Store collection_id for selection updates
    pub(crate) collection_id: CollectionId,
    // Gap fetcher for reactor.add_query (type-erased)
    pub(crate) gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>>,
    // Fires re-permission on credential change; installed once by
    // new_with_node_ref after construction.
    pub(crate) session_guard: std::sync::OnceLock<SubscriptionGuard>,
    // The two-leg status, porcelain mirror of `effects.current`; fed
    // through the effects queue so subscribers never run under a lock.
    pub(crate) status: Mut<QueryStatus>,
    // The status fold of record plus the deferred-dispatch queue.
    pub(crate) effects: std::sync::Mutex<EffectState>,
    // Governs CONSTRUCTION under denial: set-backed queries construct
    // into the Denied state; single-credential queries fail loud. (Post-
    // construction re-permission denial is a state for every kind.)
    pub(crate) set_backed: bool,
}

/// Weak reference to EntityLiveQuery for breaking circular dependencies
pub struct WeakEntityLiveQuery(Weak<Inner>);

impl WeakEntityLiveQuery {
    pub fn upgrade(&self) -> Option<EntityLiveQuery> { self.0.upgrade().map(EntityLiveQuery) }

    /// Re-permission after a credential change: re-derive the effective
    /// selection under the new credential and push it through the same
    /// versioned flow. Runs for EVERY livequery (durable-local queries
    /// re-filter their reactor registration; relayed queries additionally
    /// re-send upstream, where the durable side re-validates and swaps).
    fn credential_updated(&self) {
        if let Some(lq) = self.upgrade() {
            if let Err(e) = lq.reissue(None) {
                // Log-only is honest TODAY: every policy failure arrives
                // as AccessDenied and becomes the Denied state inside
                // reissue, and type resolution is infallible, so the only
                // error reaching here is node teardown. If the refilter
                // ever grows a non-denial failure mode, this must latch
                // observably (enqueue_error), not just log.
                tracing::warn!("credential re-permission failed for query {}: {}", lq.0.query_id, e);
                lq.0.enqueue_error(Some(e));
                lq.0.drain_effects();
            }
        }
    }
}

impl Clone for WeakEntityLiveQuery {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}

#[derive(Clone)]
pub struct LiveQuery<R: View>(EntityLiveQuery, PhantomData<R>);

impl<R: View> std::ops::Deref for LiveQuery<R> {
    type Target = EntityLiveQuery;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Inner {
    fn node(&self) -> Option<Box<dyn TNodeErased>> { self.node.upgrade() }

    /// The local leg of record (the `status` Mut may lag mid-dispatch).
    fn local_status(&self) -> LocalStatus { self.effects.lock().unwrap_or_else(|e| e.into_inner()).current.local.clone() }

    /// Fold a local-leg transition and queue its dispatch. The caller
    /// drains after releasing its locks.
    fn enqueue_local_status(&self, local: LocalStatus) {
        let mut st = self.effects.lock().unwrap_or_else(|e| e.into_inner());
        st.current.local = local;
        let report = st.current.clone();
        st.queue.push_back(Effect::Status(report));
    }

    /// Queue a porcelain-selection dispatch (the authority is already
    /// stored in `update_lock` by the caller).
    fn enqueue_selection(&self, selection: ankql::ast::Selection, version: u32) {
        let mut st = self.effects.lock().unwrap_or_else(|e| e.into_inner());
        st.queue.push_back(Effect::Selection(selection, version));
    }

    /// Queue an error-latch update; see [`Effect::Error`] for the version
    /// ownership rule.
    fn enqueue_error(&self, error: Option<RetrievalError>) {
        let mut st = self.effects.lock().unwrap_or_else(|e| e.into_inner());
        st.queue.push_back(Effect::Error(error));
    }

    pub(crate) fn set_remote_status(&self, seq: u64, remote: RemoteStatus) {
        {
            let mut st = self.effects.lock().unwrap_or_else(|e| e.into_inner());
            if seq <= st.remote_seq {
                return;
            }
            st.remote_seq = seq;
            st.current.remote = remote;
            let report = st.current.clone();
            st.queue.push_back(Effect::Status(report));
        }
        self.drain_effects();
    }

    /// Fire queued signal dispatches, none under any lock. One drainer at
    /// a time: an enqueue that finds a drain in progress returns
    /// immediately and the active drainer picks its effect up — including
    /// enqueues made by re-entrant subscribers mid-dispatch, which is what
    /// makes re-entry (a status subscriber calling update_selection) safe
    /// instead of a deadlock.
    fn drain_effects(&self) {
        // Unwind-safe handoff: a panicking subscriber must not leave the
        // draining flag set forever, or every later drainer would see a
        // dispatch in progress and return, wedging the queue.
        struct DrainingReset<'a>(&'a std::sync::Mutex<EffectState>);
        impl Drop for DrainingReset<'_> {
            fn drop(&mut self) { self.0.lock().unwrap_or_else(|e| e.into_inner()).draining = false; }
        }
        loop {
            let effect = {
                let mut st = self.effects.lock().unwrap_or_else(|e| e.into_inner());
                if st.draining {
                    return;
                }
                match st.queue.pop_front() {
                    Some(effect) => {
                        st.draining = true;
                        effect
                    }
                    None => return,
                }
            };
            let reset = DrainingReset(&self.effects);
            match effect {
                Effect::Status(status) => self.status.set(status),
                Effect::Selection(selection, version) => self.selection.set((selection, version)),
                Effect::Error(error) => self.error.set(error),
            }
            drop(reset);
        }
    }

    /// Returns when the CURRENT selection has initialized, or when the
    /// query is Denied (initialization cannot proceed until a credential
    /// change re-grants; waiting through that would hang the caller
    /// indefinitely). Callers that care which happened read `status()`.
    async fn wait_initialized(&self) {
        loop {
            let notified = self.initialized.notified();
            tokio::pin!(notified);
            // Register interest BEFORE checking the condition: a wake
            // landing between the check and the await stores no permit,
            // so an unregistered waiter would sleep through its only
            // notification and hang despite a satisfied condition.
            notified.as_mut().enable();
            if self.initialized_version.load(std::sync::atomic::Ordering::Relaxed)
                >= self.current_version.load(std::sync::atomic::Ordering::Relaxed)
                || matches!(self.local_status(), LocalStatus::Denied { .. })
            {
                // The wake's report was enqueued before notify (see
                // mark_initialized and the denial arms); drain so a
                // caller that peeks right after waiting observes it.
                self.drain_effects();
                return;
            }
            notified.await;
        }
    }

    /// Activate the LiveQuery by fetching entities and calling reactor.add_query or reactor.update_query
    /// Called after deltas have been applied for both initial subscription and selection updates
    /// Gets all parameters from self (collection_id, query_id, selection)
    /// Rejects activation if the version is older than the current selection to prevent regression
    async fn activate(&self, version: u32) -> Result<(), RetrievalError> {
        // Snapshot under the update lock: the authoritative selection and
        // the denial state change together under it, so this cannot
        // observe a half-applied re-permission. A denied query holds no
        // reactor registration and its stored selection is the raw
        // intent; nothing to activate until a credential change
        // re-derives.
        let (selection, first_activation) = {
            let current = self.update_lock.lock().unwrap_or_else(|e| e.into_inner());
            if let LocalStatus::Denied { .. } = self.local_status() {
                debug!("LiveQuery - Skipping activation for denied query {}", self.query_id);
                return Ok(());
            }
            if version < current.version {
                warn!("LiveQuery - Dropped stale activation request for version {} (current version is {})", version, current.version);
                return Ok(());
            }
            // The add-vs-update decision is part of the snapshot: read
            // outside the lock it can go stale against a concurrent
            // denial-and-heal and pick the wrong path.
            (current.selection.clone(), self.initialized_version.load(std::sync::atomic::Ordering::Relaxed) == 0)
        };

        debug!("LiveQuery.activate() for predicate {} (version {})", self.query_id, version);

        let node = self.node().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))?;
        let reactor = node.reactor();

        let hook = InnerPreNotifyHook(self);
        let result = if first_activation {
            // First activation, or a heal from Denied. A registration may
            // already exist (an activation that raced the denial installed
            // one the denial's removal ran too early to see, or a twin of
            // this activation got there first). It is NEVER removed here:
            // a stale activation must not be able to uninstall a newer
            // live registration. On conflict this falls back to the
            // update path, whose version floor arbitrates: a zombie or a
            // same-version twin is superseded or idempotently re-applied,
            // and a stale attempt no-ops.
            let add_result = reactor
                .add_query_and_notify(
                    self.subscription.id(),
                    self.query_id,
                    self.collection_id.clone(),
                    selection.clone(),
                    &*node,
                    self.resultset.clone(),
                    self.gap_fetcher.clone(),
                    version,
                    &hook,
                )
                .await;
            match add_result {
                Err(e) if e.downcast_ref::<crate::reactor::QueryAlreadyRegistered>().is_some() => {
                    reactor
                        .update_query_and_notify(
                            self.subscription.id(),
                            self.query_id,
                            self.collection_id.clone(),
                            selection,
                            &*node,
                            version,
                            &hook,
                        )
                        .await
                }
                other => other,
            }
        } else {
            // Subsequent activation (including cached re-initialization or selection update): use update_query_and_notify
            // This handles both: (1) cached queries re-activating after remote deltas, and (2) selection updates
            reactor
                .update_query_and_notify(
                    self.subscription.id(),
                    self.query_id,
                    self.collection_id.clone(),
                    selection,
                    &*node,
                    version,
                    &hook,
                )
                .await
        };

        // Re-validate under the lock: a denial that landed during the
        // await found no registration to remove (ours had not installed
        // yet), so re-assert its end state — deregistered, empty,
        // uninitialized. The resultset empties under the lock (a heal
        // cannot interleave with it) but the broadcast fires after
        // release, via the write guard's deferred notification.
        let (clawed_back, outcome) = {
            let current = self.update_lock.lock().unwrap_or_else(|e| e.into_inner());
            // The ACTIVATION error latch is versioned by enqueueing UNDER
            // this lock, where currency is decided: a still-current
            // completion clears it (the healthy terminal, so `error()`
            // and `status()` agree after a failure-then-recovery), a
            // still-current failure latches, and a SUPERSEDED outcome of
            // either kind owns nothing — the latch belongs to the newer
            // version's attempt, whose own enqueue is ordered after this
            // one by the same lock. Dispatch rides the effects queue like
            // every other signal. (Relay-leg failures and re-permission
            // failures ride the same queue through their own call sites.)
            let still_current = version >= current.version;
            let outcome = match result {
                Ok(()) => {
                    if still_current {
                        self.enqueue_error(None);
                    }
                    Ok(())
                }
                Err(e) => {
                    let error: RetrievalError = e.into();
                    if still_current {
                        // The caller's copy is a lossy summary; the TYPED
                        // error goes to the latch, and callers only log.
                        let summary = RetrievalError::Other(error.to_string());
                        self.enqueue_error(Some(error));
                        Err(summary)
                    } else {
                        Err(error)
                    }
                }
            };
            if let LocalStatus::Denied { .. } = self.local_status() {
                // The deregistration runs before the write guard exists:
                // remove_query reads the resultset keys for its watcher
                // cleanup, and holding the guard across it would
                // self-deadlock. The discard is safe: the error space
                // is absence-only, and absence is legitimate (denial can
                // precede the first activation's registration).
                let _ = self.subscription.remove_predicate(self.query_id);
                self.initialized_version.store(0, std::sync::atomic::Ordering::Relaxed);
                self.initialized.notify_waiters();
                let mut write = self.resultset.write();
                write.replace_all(Vec::new());
                (Some(write), outcome)
            } else {
                (None, outcome)
            }
        };
        drop(clawed_back);
        self.drain_effects();
        outcome
    }

    /// The registration's own completion report (the reactor's pre-notify
    /// hook): marks initialization and reports `Active` — the ONLY writer
    /// of `Active`, so the status never claims a registration that did
    /// not land.
    fn mark_initialized(&self, version: u32) {
        // TASK: Serialize or coalesce concurrent activations to prevent version regression https://github.com/ankurah/ankurah/issues/146
        // Under the update lock: a denial owns initialization state (skip
        // entirely), and a report for a superseded version is dropped —
        // its registration content has been or is being replaced, so
        // reporting it would regress the floor (a wait_initialized
        // against the current version would hang) and the status.
        let current = self.update_lock.lock().unwrap_or_else(|e| e.into_inner());
        if matches!(self.local_status(), LocalStatus::Denied { .. }) {
            return;
        }
        if version < current.version {
            return;
        }
        self.initialized_version.fetch_max(version, std::sync::atomic::Ordering::Relaxed);
        // Enqueue before waking: a woken waiter drains, so it observes
        // this report immediately after wait_initialized returns.
        self.enqueue_local_status(LocalStatus::Active { version });
        self.initialized.notify_waiters();
        drop(current);
        // Drain now, before the reactor sends its ReactorUpdate content
        // notification (this hook runs just ahead of it, with no reactor
        // locks held): a CHANGESET subscriber that receives the
        // initialized content and then peeks the status finds Active.
        // Scope honestly: the RESULTSET broadcast (the write guard inside
        // update_query) fires before this hook, so a resultset subscriber
        // can observe loaded content while the status still says Pending
        // for one dispatch beat.
        self.drain_effects();
    }
}

/// Adapts a borrowed Inner to the reactor's PreNotifyHook (previously implemented on &EntityLiveQuery,
/// but activation now lives on Inner so both LiveQuery variants share it)
struct InnerPreNotifyHook<'a>(&'a Inner);
impl crate::reactor::PreNotifyHook for &InnerPreNotifyHook<'_> {
    fn pre_notify(&self, version: u32) {
        // Mark as initialized before notification is sent
        self.0.mark_initialized(version);
    }
}

/// Helper: create the Inner and set up initialization (shared by strong- and weak-node constructors)
fn create_inner<SE, PA>(
    node: &Node<SE, PA>,
    node_ref: Box<dyn NodeRef>,
    collection_id: CollectionId,
    mut args: MatchArgs,
    sessions: crate::session::Sessions<PA::ContextData>,
) -> Result<(Arc<Inner>, proto::QueryId), RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    // One derivation authority for the effective selection: policy checks
    // and filtering run under the session's credential AT DERIVATION TIME
    // (creation now, every re-permission later), then type resolution.
    let refilter: RefilterFn = {
        let weak = Arc::downgrade(&node.0);
        let sessions = sessions.clone();
        let collection_id = collection_id.clone();
        Box::new(move |intent: ankql::ast::Selection| {
            let node = Node(weak.upgrade().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))?);
            let cdatas = sessions.snapshot();
            node.policy_agent.can_access_collection(&cdatas, &collection_id)?;
            let mut effective = intent;
            effective.predicate = node.policy_agent.filter_predicate(&cdatas, &collection_id, effective.predicate)?;
            // Resolve types in the AST (converts literals for JSON path
            // comparisons), AFTER filtering so injected policy clauses get
            // typed literals too.
            Ok(node.type_resolver.resolve_selection_types(effective))
        })
    };
    // Held is deliberately NOT set-backed: it is the server-side holder
    // arm and never constructs livequeries; if one ever did, failing
    // construction loud (the One rule) is the conservative default.
    let set_backed = matches!(sessions, crate::session::Sessions::Set(_));
    // A set-backed query survives credential denial as a reported status,
    // its intent retained and shipped upstream for the server's own
    // verdict; a single-credential query keeps failing construction.
    let (effective, local_status) = match (refilter)(args.selection.clone()) {
        Ok(effective) => (effective, LocalStatus::Pending),
        Err(RetrievalError::AccessDenied(denied)) if set_backed => {
            (args.selection.clone(), LocalStatus::Denied { reason: denied.to_string() })
        }
        Err(error) => return Err(error),
    };

    let subscription = node.reactor.subscribe();

    let resultset = EntityResultSet::empty();
    let query_id = proto::QueryId::new();
    let gap_fetcher: std::sync::Arc<dyn GapFetcher<Entity>> = std::sync::Arc::new(QueryGapFetcher::new(&node, sessions));

    // Check if this is a durable node (no relay) or ephemeral node (has relay)
    let has_relay = node.subscription_relay.is_some();

    let initial_status = QueryStatus { local: local_status, remote: if has_relay { RemoteStatus::Pending } else { RemoteStatus::None } };
    let inner = Arc::new(Inner {
        query_id,
        node: node_ref,
        subscription,
        resultset: resultset.clone(),
        error: Mut::new(None),
        initialized: tokio::sync::Notify::new(),
        initialized_version: std::sync::atomic::AtomicU32::new(0), // 0 means uninitialized
        current_version: std::sync::atomic::AtomicU32::new(1),     // Start at version 1
        selection: Mut::new((effective.clone(), 1)),               // porcelain; the authority is update_lock below
        intent_selection: std::sync::Mutex::new(args.selection.clone()),
        refilter,
        update_lock: std::sync::Mutex::new(CurrentSelection { selection: effective, version: 1 }),
        collection_id: collection_id.clone(),
        gap_fetcher,
        session_guard: std::sync::OnceLock::new(),
        status: Mut::new(initial_status.clone()),
        effects: std::sync::Mutex::new(EffectState {
            current: initial_status,
            remote_seq: 0,
            queue: std::collections::VecDeque::new(),
            draining: false,
        }),
        set_backed,
    });

    if args.cached || !has_relay {
        // Durable node: spawn initialization task directly (no remote subscription needed)
        let inner2 = inner.clone();

        debug!("LiveQuery::new() spawning initialization task for durable node predicate {}", query_id);
        crate::task::spawn(async move {
            debug!("LiveQuery initialization task starting for predicate {}", query_id);
            // The error latch is owned by activate (versioned there);
            // callers only log.
            if let Err(e) = inner2.activate(1).await {
                debug!("LiveQuery initialization failed for predicate {}: {}", query_id, e);
            } else {
                debug!("LiveQuery initialization completed for predicate {}", query_id);
            }
        });
    }

    Ok((inner, query_id))
}

impl EntityLiveQuery {
    pub fn new<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: CollectionId,
        args: MatchArgs,
        sessions: impl Into<crate::session::Sessions<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(StrongNodeRef(Arc::clone(&node.0)));
        Self::new_with_node_ref(node, node_ref, collection_id, args, sessions.into())
    }

    /// Create a LiveQuery that does NOT keep the node alive.
    ///
    /// Used by PolicyAgent and other internal subscribers that should not create
    /// reference cycles (node → agent → livequery → node). Operations that need
    /// the node (activation, selection updates) fail with "Node has been dropped"
    /// once the node is gone.
    pub fn new_weak_node<SE, PA>(
        node: &Node<SE, PA>,
        collection_id: CollectionId,
        args: MatchArgs,
        sessions: impl Into<crate::session::Sessions<PA::ContextData>>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let node_ref: Box<dyn NodeRef> = Box::new(WeakNodeRefImpl(Arc::downgrade(&node.0)));
        Self::new_with_node_ref(node, node_ref, collection_id, args, sessions.into())
    }

    fn new_with_node_ref<SE, PA>(
        node: &Node<SE, PA>,
        node_ref: Box<dyn NodeRef>,
        collection_id: CollectionId,
        args: MatchArgs,
        sessions: crate::session::Sessions<PA::ContextData>,
    ) -> Result<Self, RetrievalError>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let has_relay = node.subscription_relay.is_some();
        let credential_generation = sessions.generation();
        let (inner, query_id) = create_inner(node, node_ref, collection_id.clone(), args, sessions.clone())?;

        let me = Self(inner.clone());

        // Ephemeral node: register with relay for remote subscription
        // Remote will call activate() after applying deltas via subscription_established
        if has_relay {
            node.subscribe_remote_query(query_id, collection_id, inner.selection.value().0, sessions.clone(), 1, me.weak());
        }

        // Every query re-permissions on credential change (see
        // credential_updated). The guard installs after the relay
        // registration (a listener firing before the query is registered
        // would find no relay entry), which leaves a window between the
        // creation-time derivation and this line. The session's generation
        // closes it: an update the derivation missed either bumped before
        // the re-check below, or stored after the listener was live.
        let weak = me.weak();
        let guard = sessions.subscribe_changes({
            let weak = weak.clone();
            move |_new: Vec<PA::ContextData>| weak.credential_updated()
        });
        // Single install site (OnceLock): if a second install path ever
        // appears, the discarded NEW guard would silently unsubscribe
        // re-permission for this query. Keep it impossible.
        let _ = inner.session_guard.set(guard);
        if sessions.generation() != credential_generation {
            weak.credential_updated();
        }

        Ok(me)
    }
    pub fn map<R: View>(self) -> LiveQuery<R> { LiveQuery(self, PhantomData) }

    /// Wait for the LiveQuery to be fully initialized with initial states
    pub async fn wait_initialized(&self) { self.0.wait_initialized().await; }

    pub fn update_selection(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        let new_selection = new_selection.try_into().map_err(|e| e.into())?;
        self.reissue(Some(new_selection))
    }

    /// Re-derive the effective selection and push it through the versioned
    /// update flow. `Some(intent)` replaces the stored user intent (a
    /// selection update); `None` re-derives from the existing intent under
    /// the session's CURRENT credential (re-permission after a credential
    /// change). The lock makes read-refilter-mint-store one atomic step, so
    /// a selection update, a re-permission, and an in-flight activation
    /// can never interleave into a stale selection stored at a newer
    /// version; out-of-order DISPATCH is harmless because the server
    /// upsert, the relay's content store, and activation all enforce a
    /// version floor. No signal fires under the lock: the resultset
    /// broadcast rides the write guard past release, and selection/status
    /// dispatch through the effects queue — so observers may re-enter
    /// selection updates or re-permission freely.
    fn reissue(&self, new_intent: Option<ankql::ast::Selection>) -> Result<(), RetrievalError> {
        let node = self.0.node().ok_or_else(|| RetrievalError::Other("Node has been dropped".into()))?;

        let (effective, new_version, resultset_write) = {
            let mut current = self.0.update_lock.lock().unwrap_or_else(|e| e.into_inner());
            let mut intent = self.0.intent_selection.lock().unwrap_or_else(|e| e.into_inner());
            let previous_intent = new_intent.as_ref().map(|_| intent.clone());
            if let Some(new_intent) = new_intent {
                *intent = new_intent;
            }
            // A rejected selection UPDATE restores the previous intent and
            // errors loud: the caller is present to hear it, and a later
            // re-permission must not silently apply what they were told
            // failed. A denied RE-PERMISSION (new_intent = None) is a
            // state, not a failure, for every query kind: the credential
            // changed out from under a standing query, so the local leg
            // empties (the resultset is clawed back; locally persisted
            // rows are untouched), the intent ships upstream for the
            // server's own verdict, and the next credential change
            // re-derives.
            let (effective, denial) = match (self.0.refilter)(intent.clone()) {
                Ok(effective) => (effective, None),
                Err(RetrievalError::AccessDenied(denied)) if previous_intent.is_none() => (intent.clone(), Some(denied.to_string())),
                Err(error) => {
                    if let Some(previous) = previous_intent {
                        *intent = previous;
                    }
                    return Err(error);
                }
            };
            drop(intent);

            let new_version = self.0.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            current.selection = effective.clone();
            current.version = new_version;
            self.0.enqueue_selection(effective.clone(), new_version);

            // The reactor deregistration runs BEFORE the resultset write
            // guard exists: remove_query reads the resultset (its keys
            // drive the watcher cleanup), so holding the guard across it
            // self-deadlocks on the same thread.
            if denial.is_some() {
                // Absence-only error space; absence is legitimate (a
                // denied query may never have registered).
                let _ = self.0.subscription.remove_predicate(self.0.query_id);
            }
            // The resultset mutations happen here under the lock, but
            // their broadcast rides the returned write guard, which drops
            // after release.
            let mut write = self.0.resultset.write();
            // Not loaded: the selection is changing under the content.
            write.set_loaded(false);
            match denial {
                None => {
                    if matches!(self.0.local_status(), LocalStatus::Denied { .. }) {
                        // Returning from denial re-enters the reactor via
                        // the first-activation path (the registration was
                        // removed at denial); Active is reported by the
                        // registration when it lands.
                        self.0.initialized_version.store(0, std::sync::atomic::Ordering::Relaxed);
                        self.0.enqueue_local_status(LocalStatus::Pending);
                    }
                    // An already-active query keeps reporting its old
                    // version until the new registration lands
                    // (mark_initialized owns Active); a pending one stays
                    // pending.
                }
                Some(reason) => {
                    // Empty the local results (the registration is already
                    // gone, above). Waiters are woken to observe the
                    // denial rather than sleeping until an eventual heal.
                    write.replace_all(Vec::new());
                    self.0.initialized_version.store(0, std::sync::atomic::Ordering::Relaxed);
                    self.0.enqueue_local_status(LocalStatus::Denied { reason });
                    self.0.initialized.notify_waiters();
                }
            }
            (effective, new_version, write)
        };
        // Broadcasts, none under the lock: the resultset first (a Denied
        // observer must find it already empty), then the queued selection
        // and status reports in order.
        drop(resultset_write);
        self.0.drain_effects();

        // Check if this node has a relay (ephemeral) or not (durable)
        let has_relay = node.has_subscription_relay();

        if has_relay {
            // Ephemeral node: delegate to relay, which will call update_selection_init after applying deltas
            node.update_remote_query(self.0.query_id, effective, new_version)?;
        } else {
            // Durable node: spawn task to call update_selection_init directly
            let inner = self.0.clone();
            let query_id = self.0.query_id;

            crate::task::spawn(async move {
                // The error latch is owned by activate (versioned there).
                if let Err(e) = inner.activate(new_version).await {
                    tracing::error!("LiveQuery update failed for predicate {}: {}", query_id, e);
                }
            });
        }

        Ok(())
    }

    pub async fn update_selection_wait(
        &self,
        new_selection: impl TryInto<ankql::ast::Selection, Error = impl Into<RetrievalError>>,
    ) -> Result<(), RetrievalError> {
        self.update_selection(new_selection)?;
        self.0.wait_initialized().await;
        Ok(())
    }

    pub fn error(&self) -> Read<Option<RetrievalError>> { self.0.error.read() }
    /// The two-leg status: what the local reactor serves, and where the
    /// upstream subscription stands. Reactive; see [`QueryStatus`].
    pub fn status(&self) -> Read<QueryStatus> { self.0.status.read() }
    pub fn query_id(&self) -> proto::QueryId { self.0.query_id }
    pub fn selection(&self) -> Read<(ankql::ast::Selection, u32)> { self.0.selection.read() }
    pub fn resultset(&self) -> EntityResultSet { self.0.resultset.clone() }

    /// Create a weak reference to this LiveQuery
    pub fn weak(&self) -> WeakEntityLiveQuery { WeakEntityLiveQuery(Arc::downgrade(&self.0)) }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(node) = self.node.upgrade() {
            node.unsubscribe_remote_predicate(self.query_id);
        }
    }
}

// Implement RemoteQuerySubscriber for WeakEntityLiveQuery to break circular dependencies
#[async_trait::async_trait]
impl crate::peer_subscription::RemoteQuerySubscriber for WeakEntityLiveQuery {
    async fn subscription_established(&self, version: u32) {
        // Try to upgrade the weak reference
        if let Some(inner) = self.0.upgrade() {
            // Activate the query (fetch entities, call reactor, and mark
            // initialized); the error latch is owned by activate.
            tracing::debug!("Subscription established for query {}: {}", inner.query_id, version);
            if let Err(e) = inner.activate(version).await {
                tracing::error!("Failed to activate subscription for query {}: {}", inner.query_id, e);
            }
        }
        // If upgrade fails, the LiveQuery was already dropped - nothing to do
    }

    fn set_last_error(&self, seq: u64, error: RetrievalError) {
        // Try to upgrade the weak reference
        if let Some(inner) = self.0.upgrade() {
            tracing::info!("Setting last error for LiveQuery {}: {}", inner.query_id, error);
            // Floored on the report sequence, so a superseded failure
            // that reaches here late cannot overwrite the error state a
            // newer attempt already settled. Equality cannot occur here:
            // per-entry seqs strictly increase and this latch runs before
            // its own paired Failed status is recorded, so remote_seq is
            // always below this seq unless a NEWER report already landed.
            // A newer report landing between this latch and the paired
            // status dispatch keeps the error while dropping the status;
            // a bounded skew the next transition settles. Rides the
            // ordered dispatch like every other latch write.
            {
                let mut st = inner.effects.lock().unwrap_or_else(|e| e.into_inner());
                if seq < st.remote_seq {
                    return;
                }
                st.queue.push_back(Effect::Error(Some(error)));
            }
            inner.drain_effects();
        }
        // If upgrade fails, the LiveQuery was already dropped - nothing to do
    }

    fn remote_status(&self, seq: u64, status: RemoteStatus) {
        if let Some(inner) = self.0.upgrade() {
            inner.set_remote_status(seq, status);
        }
    }
}

impl<R: View> LiveQuery<R> {
    /// Wait for the LiveQuery to be fully initialized with initial states
    pub async fn wait_initialized(&self) { self.0.wait_initialized().await; }

    pub fn resultset(&self) -> ResultSet<R> { self.0 .0.resultset.wrap::<R>() }

    pub fn loaded(&self) -> bool { self.0 .0.resultset.is_loaded() }

    pub fn ids(&self) -> Vec<proto::EntityId> { self.0 .0.resultset.keys().collect() }

    pub fn ids_sorted(&self) -> Vec<proto::EntityId> {
        use itertools::Itertools;
        self.0 .0.resultset.keys().sorted().collect()
    }
}

// Implement Signal trait - delegate to the subscription (not resultset)
// This ensures that LiveQuery tracking fires on ALL entity changes, not just membership changes
impl<R: View> Signal for LiveQuery<R> {
    fn listen(&self, listener: Listener) -> ListenerGuard { self.0 .0.subscription.listen(listener) }

    fn broadcast_id(&self) -> BroadcastId { self.0 .0.subscription.broadcast_id() }
}

// Implement Get trait - delegate to ResultSet<R>
impl<R: View + Clone + 'static> Get<Vec<R>> for LiveQuery<R> {
    fn get(&self) -> Vec<R> {
        use ankurah_signals::CurrentObserver;
        CurrentObserver::track(&self);
        self.0 .0.resultset.wrap::<R>().peek()
    }
}

// Implement Peek trait - delegate to ResultSet<R>
impl<R: View + Clone + 'static> Peek<Vec<R>> for LiveQuery<R> {
    fn peek(&self) -> Vec<R> { self.0 .0.resultset.wrap().peek() }
}

// Implement Subscribe trait - convert ReactorUpdate to ChangeSet<R>
impl<R: View> Subscribe<ChangeSet<R>> for LiveQuery<R>
where R: Clone + Send + Sync + 'static
{
    fn subscribe<L>(&self, listener: L) -> SubscriptionGuard
    where L: IntoSubscribeListener<ChangeSet<R>> {
        let listener = listener.into_subscribe_listener();

        let me = self.clone();
        // Subscribe to the underlying ReactorUpdate stream and convert to ChangeSet<R>
        self.0 .0.subscription.subscribe(move |reactor_update: ReactorUpdate| {
            // A batch decided before a revocation can be dispatched after
            // it: the claw-back already emptied the resultset and reported
            // Denied, so the dead registration's membership changes would
            // contradict the changeset's own (now empty) resultset.
            // Denial is checked at delivery, so the dispatch that CARRIES
            // the revocation still reaches the subscriber performing it.
            if matches!(me.0 .0.local_status(), LocalStatus::Denied { .. }) {
                return;
            }
            let changeset: ChangeSet<R> = livequery_change_set_from(me.0 .0.resultset.wrap::<R>(), reactor_update);
            listener(changeset);
        })
    }
}

/// Notably, this function does not filter by query_id, because it should only be used by LiveQuery, which entails a single-predicate subscription
fn livequery_change_set_from<R: View>(resultset: ResultSet<R>, reactor_update: ReactorUpdate) -> ChangeSet<R>
where R: View {
    use crate::changes::{ChangeSet, ItemChange};

    let mut changes = Vec::new();

    for item in reactor_update.items {
        let view = R::from_entity(item.entity);

        // Determine the change type based on predicate relevance
        // ignore the query_id, because it should only be used by LiveQuery, which entails a single-predicate subscription
        if let Some((_, membership_change)) = item.predicate_relevance.first() {
            match membership_change {
                crate::reactor::MembershipChange::Initial => {
                    changes.push(ItemChange::Initial { item: view });
                }
                crate::reactor::MembershipChange::Add => {
                    changes.push(ItemChange::Add { item: view, events: item.events });
                }
                crate::reactor::MembershipChange::Remove => {
                    changes.push(ItemChange::Remove { item: view, events: item.events });
                }
            }
        } else {
            // No membership change, just an update
            changes.push(ItemChange::Update { item: view, events: item.events });
        }
    }

    ChangeSet { changes, resultset }
}
