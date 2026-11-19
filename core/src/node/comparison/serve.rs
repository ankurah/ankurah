//! Authorized comparison captures, bounded independently of network activity.

use std::{
    collections::HashMap,
    ops::Bound,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock, Weak,
    },
};

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core_types::{key_spec::KeySpec, PropertyId};
use ankurah_digest_tree::{Page, Span};
use ankurah_proto::{ComparisonFailure, ComparisonId, ComparisonPage, ComparisonTarget, KnownState};
use serde::Serialize;
use web_time::Instant;

use super::super::{
    trees::{ComparisonTree, ComparisonView},
    Node, PeerState,
};
use crate::{
    connector::ConnectionId,
    error::{MutationError, RetrievalError},
    policy::{ComparisonLimits, ContextPolicy, PolicyAgent},
    session::ContextData,
    storage::StorageEngine,
};

type Key = (ConnectionId, ComparisonId);
type Registry<SE, CD> = Mutex<HashMap<Key, Arc<Slot<SE, CD>>>>;

pub(in crate::node) struct Captures<SE, CD: ContextData> {
    inner: Arc<Registry<SE, CD>>,
}

impl<SE, CD: ContextData> Default for Captures<SE, CD> {
    fn default() -> Self { Self { inner: Arc::new(Mutex::new(HashMap::new())) } }
}

struct Retained<SE> {
    // Only this handle participates in ongoing TreeManager maintenance.
    _owner: ComparisonTree<SE>,
    view: ComparisonView<SE>,
    discovery: Predicate<Resolved>,
    selection: Selection<Resolved>,
}

struct Slot<SE, CD: ContextData> {
    connection: Weak<PeerState<CD>>,
    selection: Selection<Resolved>,
    credentials: Vec<CD>,
    created: Instant,
    expires: Instant,
    limits: ComparisonLimits,
    steps: AtomicUsize,
    active: AtomicBool,
    cancel_expiry: Arc<tokio::sync::Notify>,
    retained: OnceLock<Retained<SE>>,
}

impl<SE, CD: ContextData> Drop for Slot<SE, CD> {
    fn drop(&mut self) {
        self.cancel_expiry.notify_waiters();
        self.cancel_expiry.notify_one();
    }
}

/// Admission is installed before capture work. Dropping an unfinished begin,
/// including by cancellation, returns its slot immediately.
struct Reservation<SE, CD: ContextData> {
    registry: Weak<Registry<SE, CD>>,
    key: Key,
    slot: Arc<Slot<SE, CD>>,
    installed: bool,
}

impl<SE, CD: ContextData> Drop for Reservation<SE, CD> {
    fn drop(&mut self) {
        if !self.installed {
            remove(&self.registry, self.key, &self.slot);
        }
    }
}

fn remove<SE, CD: ContextData>(registry: &Weak<Registry<SE, CD>>, key: Key, slot: &Arc<Slot<SE, CD>>) {
    slot.active.store(false, Ordering::Release);
    slot.cancel_expiry.notify_waiters();
    slot.cancel_expiry.notify_one();
    if let Some(registry) = registry.upgrade() {
        let mut entries = registry.lock().unwrap();
        if entries.get(&key).is_some_and(|entry| Arc::ptr_eq(entry, slot)) {
            entries.remove(&key);
        }
    }
}

impl<SE: StorageEngine + 'static, CD: ContextData> Captures<SE, CD> {
    fn reserve(
        &self,
        key: Key,
        connection: &Arc<PeerState<CD>>,
        selection: Selection<Resolved>,
        credentials: Vec<CD>,
        limits: ComparisonLimits,
    ) -> Result<Reservation<SE, CD>, RetrievalError> {
        let created = Instant::now();
        let expires = created.checked_add(limits.lifetime).ok_or_else(|| failure(ComparisonFailure::LimitExceeded))?;
        let slot = Arc::new(Slot {
            connection: Arc::downgrade(connection),
            selection,
            credentials,
            created,
            expires,
            limits,
            steps: AtomicUsize::new(0),
            active: AtomicBool::new(true),
            cancel_expiry: Arc::new(tokio::sync::Notify::new()),
            retained: OnceLock::new(),
        });
        {
            let mut entries = self.inner.lock().unwrap();
            entries.retain(|_, entry| {
                let live = entry.active.load(Ordering::Acquire) && entry.expires > created && entry.connection.strong_count() > 0;
                if !live {
                    entry.active.store(false, Ordering::Release);
                    entry.cancel_expiry.notify_waiters();
                    entry.cancel_expiry.notify_one();
                }
                live
            });
            if entries.contains_key(&key) {
                return Err(failure(ComparisonFailure::InvalidRequest("comparison identity is already in use".into())));
            }
            if limits.lifetime.is_zero()
                || limits.max_steps == 0
                || limits.max_bytes == 0
                || entries.keys().filter(|(connection_id, _)| *connection_id == key.0).count() >= limits.max_captures
            {
                return Err(failure(ComparisonFailure::LimitExceeded));
            }
            entries.insert(key, slot.clone());
        }
        let registry = Arc::downgrade(&self.inner);
        let weak_slot = Arc::downgrade(&slot);
        let timeout_registry = registry.clone();
        let cancel = slot.cancel_expiry.clone();
        crate::task::spawn(async move {
            tokio::select! {
                _ = cancel.notified() => {},
                _ = futures_timer::Delay::new(expires.saturating_duration_since(Instant::now())) => {
                    if let Some(slot) = weak_slot.upgrade() { remove(&timeout_registry, key, &slot); }
                }
            }
        });
        Ok(Reservation { registry, key, slot, installed: false })
    }

    pub(in crate::node) fn release(&self, connection_id: ConnectionId, id: ComparisonId) {
        if let Some(slot) = self.inner.lock().unwrap().remove(&(connection_id, id)) {
            slot.active.store(false, Ordering::Release);
            slot.cancel_expiry.notify_waiters();
            slot.cancel_expiry.notify_one();
        }
    }

    pub(in crate::node) fn release_peer(&self, connection_id: ConnectionId) {
        self.inner.lock().unwrap().retain(|(owner, _), slot| {
            if *owner != connection_id {
                return true;
            }
            slot.active.store(false, Ordering::Release);
            slot.cancel_expiry.notify_waiters();
            slot.cancel_expiry.notify_one();
            false
        });
    }

    fn get(
        &self,
        connection_id: ConnectionId,
        id: ComparisonId,
        limits: ComparisonLimits,
    ) -> Result<CheckedCapture<SE, CD>, RetrievalError> {
        let slot = self.inner.lock().unwrap().get(&(connection_id, id)).cloned().ok_or_else(|| failure(ComparisonFailure::Expired))?;
        let checked = CheckedCapture {
            registry: Arc::downgrade(&self.inner),
            key: (connection_id, id),
            limits: Mutex::new(restrict(slot.limits, limits)),
            slot,
        };
        checked.check_active()?;
        if checked.slot.retained.get().is_none() {
            return Err(failure(ComparisonFailure::InvalidRequest("comparison capture is not ready".into())));
        }
        Ok(checked)
    }
}

/// A request's borrowed capture. Revalidate after asynchronous work and before
/// disclosure; a retained Arc alone does not grant continued authority.
pub(in crate::node) struct CheckedCapture<SE, CD: ContextData> {
    registry: Weak<Registry<SE, CD>>,
    key: Key,
    slot: Arc<Slot<SE, CD>>,
    limits: Mutex<ComparisonLimits>,
}

impl<SE, CD: ContextData> CheckedCapture<SE, CD> {
    pub(in crate::node) fn view(&self) -> &ComparisonView<SE> { &self.slot.retained.get().expect("a checked capture is ready").view }
    pub(in crate::node) fn limits(&self) -> ComparisonLimits { *self.limits.lock().unwrap() }

    pub(in crate::node) fn connection(&self) -> Result<Arc<PeerState<CD>>, RetrievalError> {
        self.check_active()?;
        self.slot.connection.upgrade().ok_or_else(|| failure(ComparisonFailure::Expired))
    }

    pub(in crate::node) async fn cancelled(&self) {
        let notified = self.slot.cancel_expiry.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.slot.active.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }

    pub(in crate::node) fn remaining(&self) -> std::time::Duration {
        self.slot.expires.saturating_duration_since(Instant::now()).min(self.limits().lifetime.saturating_sub(self.slot.created.elapsed()))
    }

    pub(in crate::node) fn check_active(&self) -> Result<(), RetrievalError> {
        if !self.slot.active.load(Ordering::Acquire)
            || Instant::now() >= self.slot.expires
            || self.slot.created.elapsed() >= self.limits().lifetime
        {
            remove(&self.registry, self.key, &self.slot);
            return Err(failure(ComparisonFailure::Expired));
        }
        Ok(())
    }

    /// One unit per span/member and per started KiB crossing the boundary.
    /// Charging before work makes concurrent requests share the same budget.
    pub(in crate::node) fn charge(&self, work: usize, bytes: usize) -> Result<(), RetrievalError> {
        self.check_active()?;
        let limits = self.limits();
        let units = work.checked_add(bytes.div_ceil(1024));
        if bytes > limits.max_bytes
            || units.is_none()
            || self
                .slot
                .steps
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    used.checked_add(units.unwrap()).filter(|total| *total <= limits.max_steps)
                })
                .is_err()
        {
            remove(&self.registry, self.key, &self.slot);
            return Err(failure(ComparisonFailure::LimitExceeded));
        }
        Ok(())
    }
}

impl<SE: StorageEngine + 'static, PA: PolicyAgent> Node<SE, PA> {
    pub(in crate::node) async fn begin_comparison(
        &self,
        connection_id: ConnectionId,
        id: ComparisonId,
        selection: Selection<Resolved>,
        key_spec: KeySpec<PropertyId>,
        known_state: KnownState,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<ComparisonTarget, RetrievalError> {
        let mut scoped = selection.clone();
        scoped.predicate = ContextPolicy::from_credentials(&self.policy_agent, credentials).filter_predicate(scoped.predicate)?;
        self.begin_scoped_comparison(connection_id, id, selection, scoped, key_spec, known_state, credentials).await
    }

    pub(crate) fn release_comparison(&self, connection_id: ConnectionId, id: ComparisonId) { self.captures.release(connection_id, id); }

    /// Bind the request spelling while capturing the same scoped query that
    /// the subscription installed in Reactor.
    pub(crate) async fn begin_scoped_comparison(
        &self,
        connection_id: ConnectionId,
        id: ComparisonId,
        selection: Selection<Resolved>,
        scoped: Selection<Resolved>,
        key_spec: KeySpec<PropertyId>,
        known_state: KnownState,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<ComparisonTarget, RetrievalError> {
        self.system.check_not_halted()?;
        let limits = self.policy_agent.comparison_limits(credentials);
        let connection = self.peer_connections.get(&connection_id).ok_or_else(|| failure(ComparisonFailure::Expired))?;
        let mut reservation = self.captures.reserve((connection_id, id), &connection, selection.clone(), credentials.clone(), limits)?;
        let checked = CheckedCapture {
            registry: reservation.registry.clone(),
            key: (connection_id, id),
            slot: reservation.slot.clone(),
            limits: Mutex::new(limits),
        };
        checked.charge(1, size(&(&selection, &key_spec, &known_state))?)?;
        let work = async {
            let (owner, tree, discovery) = self.capture_comparison(scoped.clone(), credentials).await.map_err(restart)?;
            let actual_spec = super::wire::key_spec_to_wire(tree.key_spec());
            if key_spec != actual_spec {
                return Err(failure(ComparisonFailure::KeySpecMismatch));
            }
            reservation
                .slot
                .retained
                .set(Retained { _owner: owner, view: tree.view(), discovery, selection: scoped })
                .unwrap_or_else(|_| unreachable!("a reservation installs its capture once"));
            let digest = checked.view().span_digest(..).await.map_err(tree_failure)?.digest;
            let page = if digest == known_state.digest {
                None
            } else {
                if limits.max_spans == 0 {
                    return Err(failure(ComparisonFailure::LimitExceeded));
                }
                let mut pages = checked.view().pages(vec![Span::all()]).await.map_err(tree_failure)?;
                Some(super::wire::page_to_wire(pages.pop().ok_or(crate::sync::Error::AnswerMisfit)?))
            };
            let target = ComparisonTarget { id, key_spec: actual_spec, known_state: KnownState { digest }, page };
            checked.charge(target.page.as_ref().map_or(0, page_work), 0)?;
            self.revalidate_comparison(&checked, credentials).await?;
            checked.charge(0, size(&target)?)?;
            Ok(target)
        };
        let target = tokio::select! {
            biased;
            result = work => result?,
            _ = checked.cancelled() => return Err(failure(ComparisonFailure::Expired)),
            _ = futures_timer::Delay::new(checked.remaining()) => return Err(failure(ComparisonFailure::Expired)),
        };
        reservation.installed = true;
        Ok(target)
    }

    pub(in crate::node) async fn checked_comparison(
        &self,
        connection_id: ConnectionId,
        id: ComparisonId,
        selection: &Selection<Resolved>,
        credentials: &Vec<PA::ContextData>,
        work: usize,
    ) -> Result<CheckedCapture<SE, PA::ContextData>, RetrievalError> {
        let checked = self.captures.get(connection_id, id, self.policy_agent.comparison_limits(credentials))?;
        if checked.slot.selection != *selection || checked.slot.credentials != *credentials {
            remove(&checked.registry, checked.key, &checked.slot);
            return Err(failure(ComparisonFailure::AuthorizationChanged));
        }
        checked.charge(work, 0)?;
        let result = tokio::select! {
            biased;
            result = self.revalidate_comparison(&checked, credentials) => result,
            _ = checked.cancelled() => Err(failure(ComparisonFailure::Expired)),
            _ = futures_timer::Delay::new(checked.remaining()) => Err(failure(ComparisonFailure::Expired)),
        };
        if result.is_err() {
            remove(&checked.registry, checked.key, &checked.slot);
        }
        result?;
        Ok(checked)
    }

    /// Recheck synchronous authority immediately before sending prepared data.
    /// Callers remain responsible for current per-state/event checks.
    pub(in crate::node) fn check_comparison_disclosure(
        &self,
        checked: &CheckedCapture<SE, PA::ContextData>,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<(), RetrievalError> {
        let result = (|| {
            self.system.check_not_halted()?;
            checked.check_active()?;
            self.peer_connections.get(&checked.key.0).ok_or_else(|| failure(ComparisonFailure::Expired))?;
            if checked.slot.credentials != *credentials {
                return Err(failure(ComparisonFailure::AuthorizationChanged));
            }
            let limits = restrict(checked.slot.limits, self.policy_agent.comparison_limits(credentials));
            {
                let mut current = checked.limits.lock().unwrap();
                *current = restrict(*current, limits);
            }
            checked.check_active()?;
            let captures = self.captures.inner.lock().unwrap().keys().filter(|(connection_id, _)| *connection_id == checked.key.0).count();
            if checked.slot.steps.load(Ordering::Acquire) > limits.max_steps || captures > limits.max_captures {
                return Err(failure(ComparisonFailure::LimitExceeded));
            }
            let retained = checked.slot.retained.get().expect("checked capture");
            let allowed = self
                .comparison_discovery(&retained.selection, credentials)
                .map_err(|_| failure(ComparisonFailure::AuthorizationChanged))?;
            if allowed != retained.discovery {
                return Err(failure(ComparisonFailure::AuthorizationChanged));
            }
            Ok(())
        })();
        if result.is_err() {
            remove(&checked.registry, checked.key, &checked.slot);
        }
        result
    }

    pub(in crate::node) async fn revalidate_comparison(
        &self,
        checked: &CheckedCapture<SE, PA::ContextData>,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<(), RetrievalError> {
        let result = async {
            self.check_comparison_disclosure(checked, credentials)?;
            let retained = checked.slot.retained.get().expect("checked capture");
            self.validate_comparison_authorization(&retained.selection, checked.view(), credentials, &retained.discovery, &|work| {
                checked.charge(work, 0)
            })
            .await
            .map_err(restart)?;
            self.check_comparison_disclosure(checked, credentials)
        }
        .await;
        if result.is_err() {
            remove(&checked.registry, checked.key, &checked.slot);
        }
        result
    }

    pub(in crate::node) async fn comparison_pages(
        &self,
        connection_id: ConnectionId,
        id: ComparisonId,
        selection: &Selection<Resolved>,
        spans: Vec<Span>,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<Vec<ComparisonPage>, RetrievalError> {
        validate_spans(&spans, self.policy_agent.comparison_limits(credentials))?;
        let checked = self.checked_comparison(connection_id, id, selection, credentials, spans.len()).await?;
        let requested = spans.len();
        let work = async {
            validate_spans(&spans, checked.limits())?;
            checked.charge(0, size(&spans)?)?;
            let pages: Vec<_> =
                checked.view().pages(spans).await.map_err(tree_failure)?.into_iter().map(super::wire::page_to_wire).collect();
            checked.charge(pages.iter().map(page_work).sum(), 0)?;
            self.revalidate_comparison(&checked, credentials).await?;
            if requested > checked.limits().max_spans {
                return Err(failure(ComparisonFailure::LimitExceeded));
            }
            checked.charge(0, size(&pages)?)?;
            Ok(pages)
        };
        let result = tokio::select! {
            biased;
            result = work => result,
            _ = checked.cancelled() => Err(failure(ComparisonFailure::Expired)),
            _ = futures_timer::Delay::new(checked.remaining()) => Err(failure(ComparisonFailure::Expired)),
        };
        if result.is_err() {
            remove(&checked.registry, checked.key, &checked.slot);
        }
        result
    }
}

fn validate_spans(spans: &[Span], limits: ComparisonLimits) -> Result<(), RetrievalError> {
    if spans.is_empty() || spans.len() > limits.max_spans || size(spans)? > limits.max_bytes {
        return Err(failure(ComparisonFailure::LimitExceeded));
    }
    for span in spans {
        let empty = match (&span.start, &span.end) {
            (Bound::Included(a), Bound::Included(b)) => a > b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a >= b,
            _ => false,
        };
        if empty {
            return Err(failure(ComparisonFailure::InvalidRequest("page span is empty or reversed".into())));
        }
    }
    Ok(())
}

fn page_work(page: &ComparisonPage) -> usize {
    match page {
        Page::Digests(parts) => parts.len(),
        Page::Members(members) => members.len(),
    }
}

fn size(value: &(impl Serialize + ?Sized)) -> Result<usize, RetrievalError> {
    bincode::serialized_size(value)?.try_into().map_err(|_| failure(ComparisonFailure::LimitExceeded))
}

fn failure(error: ComparisonFailure) -> RetrievalError { RetrievalError::storage(error) }

pub(in crate::node) fn tree_failure(error: ankurah_digest_tree::Error) -> RetrievalError {
    match error {
        ankurah_digest_tree::Error::Store(error) => match error.downcast::<RetrievalError>() {
            Ok(error) => restart(error),
            Err(error) => RetrievalError::storage(ankurah_digest_tree::Error::Store(error)),
        },
        error => RetrievalError::storage(error),
    }
}

pub(super) fn restart(error: RetrievalError) -> RetrievalError {
    match &error {
        RetrievalError::MutationError(cause) if matches!(**cause, MutationError::WriteConflict) => failure(ComparisonFailure::Restart),
        RetrievalError::StorageError(cause)
            if matches!(cause.downcast_ref::<crate::sync::Error>(), Some(crate::sync::Error::Retired(_)))
                || matches!(cause.downcast_ref::<ankurah_digest_tree::Error>(), Some(ankurah_digest_tree::Error::Store(error)) if error.is_retryable()) =>
        {
            failure(ComparisonFailure::Restart)
        }
        _ => error,
    }
}

fn restrict(a: ComparisonLimits, b: ComparisonLimits) -> ComparisonLimits {
    ComparisonLimits {
        max_spans: a.max_spans.min(b.max_spans),
        max_members: a.max_members.min(b.max_members),
        max_bytes: a.max_bytes.min(b.max_bytes),
        max_steps: a.max_steps.min(b.max_steps),
        max_captures: a.max_captures.min(b.max_captures),
        lifetime: a.lifetime.min(b.lifetime),
        max_capture_members: a.max_capture_members.min(b.max_capture_members),
        max_capture_bytes: a.max_capture_bytes.min(b.max_capture_bytes),
        max_events: a.max_events.min(b.max_events),
    }
}

#[cfg(test)]
mod tests;
