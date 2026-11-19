//! Reconcile live digest trees after canonical commits, including cancelled callers.

use super::Node;
use crate::{
    entity::TemporaryEntity,
    error::RetrievalError,
    policy::{AccessDenied, ComparisonLimits, ContextPolicy, PolicyAgent},
    selection::filter::{evaluate_predicate, Error as FilterError},
    storage::{StateScan, StorageEngine},
    sync::{EngineAdapter, EntityHead, EntityMemberId, TreeProperty},
};
use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_digest_tree::{ComparisonError, DigestTree, DigestTreeView, Page, PageGetter, Ristretto255, Span};
use ankurah_proto::{ComparisonFailure, EntityId, State};
use ankurah_signals::Wait;
use futures::TryStreamExt;
use std::{num::NonZeroUsize, time::Duration};

use super::comparison::serve::tree_failure;

pub(super) type ComparisonTree<SE> =
    DigestTree<TreeProperty, EntityMemberId, EntityHead, Ristretto255, EngineAdapter<SE, TreeProperty, EntityHead, Ristretto255>>;
pub(super) type ComparisonView<SE> =
    DigestTreeView<TreeProperty, EntityMemberId, EntityHead, Ristretto255, EngineAdapter<SE, TreeProperty, EntityHead, Ristretto255>>;

impl<SE: StorageEngine + 'static, PA: PolicyAgent> Node<SE, PA> {
    /// Configure when maintained digest-tree changes are persisted.
    pub fn set_tree_write_behind(&self, policy: crate::sync::WriteBehind) {
        self.trees.set_write_behind(policy);
        self.tree_maintenance.notify_one();
    }

    /// Persist this node's pending digest-tree maintenance.
    pub async fn flush_trees(&self) -> Result<(), RetrievalError> { self.lifecycle.while_running(self.trees.flush()).await }

    pub(crate) async fn digest_tree(&self, selection: Selection<Resolved>) -> Result<Box<dyn Send + Sync>, RetrievalError> {
        self.lifecycle
            .while_running(async {
                let view = self.trees.view(selection).await?;
                self.tree_maintenance.notify_one();
                Ok(Box::new(view) as Box<dyn Send + Sync>)
            })
            .await
    }

    /// Capture an already scoped Selection without refilling its LIMIT when
    /// current credentials exclude members of the installed selection.
    pub(crate) async fn capture_comparison(
        &self,
        selection: Selection<Resolved>,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<(ComparisonTree<SE>, ComparisonTree<SE>, Predicate<Resolved>), RetrievalError> {
        self.lifecycle
            .while_running(async {
                let allowed = self.comparison_discovery(&selection, credentials)?;
                let policy = ContextPolicy::from_credentials(&self.policy_agent, credentials);
                let mut budget = AuthorizationBudget::new(self.policy_agent.comparison_limits(credentials));
                let mut authorize = |id, state: &State| {
                    budget.inspect(state)?;
                    if !discovery_allows(id, state, &allowed)? {
                        return Ok(false);
                    }
                    match policy.check_read_state(&id, state) {
                        Ok(()) => Ok(true),
                        Err(AccessDenied::ByPolicy(_) | AccessDenied::ModelDenied(_)) => Ok(false),
                        Err(error) => Err(error.into()),
                    }
                };
                let proven = self.policy_agent.state_reads_follow_predicate(credentials) && implies(&selection.predicate, &allowed);
                let (owner, capture) = self.trees.capture_for_comparison(selection.clone(), (!proven).then_some(&mut authorize)).await?;
                // Policy can change independently of entity commits while a scan awaits storage.
                if self.comparison_discovery(&selection, credentials)? != allowed
                    || (proven && !self.policy_agent.state_reads_follow_predicate(credentials))
                {
                    return Err(RetrievalError::storage(ComparisonFailure::AuthorizationChanged));
                }
                self.tree_maintenance.notify_one();
                Ok((owner, capture, allowed))
            })
            .await
    }

    /// Prove authority at a fresh canonical read boundary before disclosing a
    /// retained capture, then recheck mutable policy without another await.
    /// A concurrent commit after that boundary may change membership, as with
    /// any snapshot read; credentials alone never establish current authority.
    pub(crate) async fn validate_comparison_authorization(
        &self,
        selection: &Selection<Resolved>,
        captured: &ComparisonView<SE>,
        credentials: &Vec<PA::ContextData>,
        original_discovery: &Predicate<Resolved>,
        charge: &(dyn Fn(usize) -> Result<(), RetrievalError> + Send + Sync),
    ) -> Result<(), RetrievalError> {
        self.lifecycle
            .while_running(async {
                let changed = || RetrievalError::storage(ComparisonFailure::AuthorizationChanged);
                let allowed = self.comparison_discovery(selection, credentials).map_err(|_| changed())?;
                if allowed != *original_discovery {
                    return Err(changed());
                }
                let catalog = crate::schema::is_catalog_read(&selection.predicate);
                let proven = self.policy_agent.state_reads_follow_predicate(credentials) && implies(&selection.predicate, &allowed);
                if proven && allowed == Predicate::True && !catalog {
                    return Ok(());
                }

                if proven {
                    // An unchanged grant can stop covering a member when its state changes.
                    // Compare against a fresh scoped tree: equal subtrees need no state reads,
                    // and changed heads at the same address still prove current membership.
                    // An old-only address also covers moves and LIMIT exits; restarting is
                    // conservative and avoids retaining an independent member index.
                    charge(1)?;
                    let (_owner, current) = self.trees.capture_for_comparison(selection.clone(), None).await?;
                    let batch = NonZeroUsize::new(self.policy_agent.comparison_limits(credentials).max_spans.min(256))
                        .ok_or_else(|| RetrievalError::storage(ComparisonFailure::LimitExceeded))?;
                    let getter = AuthorizationPages { view: current.view(), charge };
                    let mut comparison = captured.compare(getter, batch);
                    while let Some(differences) = comparison.next_batch().await.map_err(|error| match error {
                        ComparisonError::Getter(error) => error,
                        ComparisonError::View(error) => tree_failure(error),
                        _ => RetrievalError::storage(ComparisonFailure::Restart),
                    })? {
                        charge(differences.len())?;
                        if differences.iter().any(|difference| difference.ours.is_some() && difference.theirs.is_none()) {
                            return Err(RetrievalError::storage(ComparisonFailure::Restart));
                        }
                    }
                    if self.comparison_discovery(selection, credentials).map_err(|_| changed())? != allowed
                        || !self.policy_agent.state_reads_follow_predicate(credentials)
                    {
                        return Err(changed());
                    }
                    return Ok(());
                }

                let mut budget = AuthorizationBudget::new(self.policy_agent.comparison_limits(credentials));
                let mut snapshot = self.storage.state_snapshot(StateScan::Changes(None)).await?;
                let mut members = captured.range(..).map_err(RetrievalError::storage)?;
                let mut canonical = Vec::new();
                loop {
                    let mut ids = Vec::new();
                    while ids.len() < 256 {
                        let Some(member) = members.try_next().await.map_err(RetrievalError::storage)? else { break };
                        ids.push(member.id.0);
                    }
                    if ids.is_empty() {
                        break;
                    }
                    charge(ids.len())?;
                    let states = snapshot.get_states(&ids).await?;
                    if states.len() != ids.len() {
                        return Err(crate::sync::Error::AnswerMisfit.into());
                    }
                    for (id, state) in ids.into_iter().zip(states) {
                        let state = state.ok_or_else(changed)?;
                        if state.payload.entity_id != id {
                            return Err(crate::sync::Error::AnswerMisfit.into());
                        }
                        let before = budget.bytes;
                        budget.inspect(&state.payload.state)?;
                        charge((budget.bytes - before).div_ceil(1024))?;
                        canonical.push((id, state.payload.state));
                    }
                }
                snapshot.finish().await?;
                if self.comparison_discovery(selection, credentials).map_err(|_| changed())? != allowed {
                    return Err(changed());
                }
                let limits = self.policy_agent.comparison_limits(credentials);
                if budget.members > limits.max_capture_members || budget.bytes > limits.max_capture_bytes {
                    return Err(RetrievalError::storage(ComparisonFailure::LimitExceeded));
                }
                // No await follows these checks: revocation while reading a later batch or
                // finishing the snapshot must also apply to members read earlier.
                let policy = ContextPolicy::from_credentials(&self.policy_agent, credentials);
                for (id, state) in canonical {
                    if !discovery_allows(id, &state, &allowed)?
                        || (catalog && !discovery_allows(id, &state, &selection.predicate)?)
                        || policy.check_read_state(&id, &state).is_err()
                    {
                        return Err(changed());
                    }
                }
                Ok(())
            })
            .await
    }

    pub(super) fn comparison_discovery(
        &self,
        selection: &Selection<Resolved>,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<Predicate<Resolved>, RetrievalError> {
        if crate::schema::is_catalog_read(&selection.predicate) {
            Ok(Predicate::True)
        } else {
            Ok(self.policy_agent.query_predicate(credentials)?)
        }
    }

    /// Call while holding publication, before polling the canonical storage commit.
    /// The wake survives cancellation; reconciliation waits for publication to end.
    pub(crate) fn invalidate_trees(&self) {
        self.trees.invalidate();
        self.tree_maintenance.notify_one();
    }

    pub(super) fn start_tree_maintenance(&self) {
        let trees = std::sync::Arc::downgrade(&self.trees);
        let changed = self.tree_maintenance.clone();
        let lifecycle = self.lifecycle.clone();
        let run = self.run.clone();
        crate::task::spawn(async move {
            loop {
                // Another Node may write shared storage without sending this Node a
                // notification. Queries reconcile before serving; retained views also
                // catch up periodically while any tree remains managed.
                let delay = trees.upgrade().and_then(|trees| {
                    let flush = trees.flush_after();
                    if trees.trees().is_empty() {
                        flush
                    } else {
                        Some(flush.map_or(Duration::from_secs(1), |delay| delay.min(Duration::from_secs(1))))
                    }
                });
                let flush_due = async {
                    match delay {
                        Some(delay) => futures_timer::Delay::new(delay).await,
                        None => futures::future::pending().await,
                    }
                };
                tokio::select! {
                    biased;
                    _ = run.wait_value(false) => return,
                    _ = changed.notified() => {},
                    _ = flush_due => {},
                }
                let mut retry_delay = Duration::from_millis(100);
                loop {
                    let maintain = async {
                        let Some(trees) = trees.upgrade() else { return Ok(()) };
                        let _operation = match lifecycle.admit() {
                            Ok(operation) => operation,
                            Err(_) => return Ok(()),
                        };
                        trees.maintain().await
                    };
                    let result = tokio::select! {
                        biased;
                        _ = run.wait_value(false) => return,
                        result = maintain => result,
                    };
                    match result {
                        Ok(()) => break,
                        Err(error) => tracing::warn!(%error, "Digest-tree maintenance will retry"),
                    }
                    // Dropping the Node cancels an in-flight scan or this retry wait.
                    tokio::select! {
                        biased;
                        _ = run.wait_value(false) => return,
                        _ = changed.notified() => {},
                        _ = futures_timer::Delay::new(retry_delay) => {},
                    }
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(10));
                }
            }
        });
    }
}

/// Charge each descent, including equal subtrees that produce no differences.
struct AuthorizationPages<'a, SE> {
    view: ComparisonView<SE>,
    charge: &'a (dyn Fn(usize) -> Result<(), RetrievalError> + Send + Sync),
}

impl<SE: StorageEngine + 'static> PageGetter<EntityMemberId, EntityHead, Ristretto255> for AuthorizationPages<'_, SE> {
    type Error = RetrievalError;

    async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<EntityMemberId, EntityHead, Ristretto255>>, Self::Error> {
        (self.charge)(spans.len())?;
        let pages = self.view.pages(spans).await.map_err(tree_failure)?;
        for page in &pages {
            (self.charge)(match page {
                Page::Digests(digests) => digests.len(),
                Page::Members(members) => members.len(),
            })?;
        }
        Ok(pages)
    }
}

fn discovery_allows(id: EntityId, state: &State, allowed: &Predicate<Resolved>) -> Result<bool, RetrievalError> {
    if *allowed == Predicate::True {
        return Ok(true);
    }
    match evaluate_predicate(&TemporaryEntity::new(id, state)?, allowed) {
        Ok(allowed) => Ok(allowed),
        Err(FilterError::PropertyNotFound(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// Only prove syntactically visible restrictions. Failure to prove implication
/// falls back to checking canonical states; it never broadens authorization.
fn implies(selected: &Predicate<Resolved>, allowed: &Predicate<Resolved>) -> bool {
    if *allowed == Predicate::True || selected == allowed {
        return true;
    }
    if let Predicate::And(left, right) = allowed {
        return implies(selected, left) && implies(selected, right);
    }
    matches!(selected, Predicate::And(left, right) if implies(left, allowed) || implies(right, allowed))
}

struct AuthorizationBudget {
    limits: ComparisonLimits,
    members: usize,
    bytes: usize,
}

impl AuthorizationBudget {
    fn new(limits: ComparisonLimits) -> Self { Self { limits, members: 0, bytes: 0 } }

    fn inspect(&mut self, state: &State) -> Result<(), RetrievalError> {
        let exhausted = || RetrievalError::storage(ComparisonFailure::LimitExceeded);
        self.members = self.members.checked_add(1).filter(|count| *count <= self.limits.max_capture_members).ok_or_else(exhausted)?;
        let bytes: usize = bincode::serialized_size(state)?.try_into().map_err(|_| exhausted())?;
        self.bytes = self
            .bytes
            .checked_add(32)
            .and_then(|total| total.checked_add(bytes))
            .filter(|total| *total <= self.limits.max_capture_bytes)
            .ok_or_else(exhausted)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
