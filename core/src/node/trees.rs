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
use ankurah_digest_tree::{DigestTree, DigestTreeView, Ristretto255};
use ankurah_proto::{ComparisonFailure, EntityId, KnownEntity, KnownState, State};
use ankurah_signals::Wait;
use futures::TryStreamExt;
use std::time::Duration;

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
    pub async fn flush_trees(&self) -> Result<(), RetrievalError> {
        self.system.check_not_halted()?;
        self.trees.flush().await
    }

    pub(crate) async fn digest_tree(&self, selection: Selection<Resolved>) -> Result<Box<dyn Send + Sync>, RetrievalError> {
        let view = self.trees.view(selection).await?;
        self.tree_maintenance.notify_one();
        Ok(Box::new(view))
    }

    /// Capture an already scoped Selection without refilling its LIMIT when
    /// current credentials exclude members of the installed selection.
    pub(crate) async fn capture_comparison(
        &self,
        selection: Selection<Resolved>,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<(ComparisonTree<SE>, ComparisonTree<SE>, Predicate<Resolved>), RetrievalError> {
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
    }

    /// Reevaluate authority before disclosing any part of a retained capture.
    /// Credentials and an unchanged entity cursor do not establish that a
    /// state-dependent policy still permits its members.
    pub(crate) async fn validate_comparison_authorization(
        &self,
        selection: &Selection<Resolved>,
        captured: &ComparisonView<SE>,
        credentials: &Vec<PA::ContextData>,
        original_discovery: &Predicate<Resolved>,
    ) -> Result<(), RetrievalError> {
        let changed = || RetrievalError::storage(ComparisonFailure::AuthorizationChanged);
        let allowed = self.comparison_discovery(selection, credentials).map_err(|_| changed())?;
        if allowed != *original_discovery {
            return Err(changed());
        }
        if self.policy_agent.state_reads_follow_predicate(credentials) {
            return Ok(());
        }

        let policy = ContextPolicy::from_credentials(&self.policy_agent, credentials);
        let mut budget = AuthorizationBudget::new(self.policy_agent.comparison_limits(credentials));
        let mut snapshot = self.storage.state_snapshot(StateScan::Changes(None)).await?;
        let mut members = captured.range(..).map_err(RetrievalError::storage)?;
        loop {
            let mut ids = Vec::new();
            while ids.len() < 256 {
                let Some(member) = members.try_next().await.map_err(RetrievalError::storage)? else { break };
                ids.push(member.id.0);
            }
            if ids.is_empty() {
                break;
            }
            let states = snapshot.get_states(&ids).await?;
            if states.len() != ids.len() {
                return Err(crate::sync::Error::AnswerMisfit.into());
            }
            for (id, state) in ids.into_iter().zip(states) {
                let state = state.ok_or_else(changed)?;
                budget.inspect(&state.payload.state)?;
                if !discovery_allows(id, &state.payload.state, &allowed)? || policy.check_read_state(&id, &state.payload.state).is_err() {
                    return Err(changed());
                }
            }
        }
        snapshot.finish().await?;
        if self.comparison_discovery(selection, credentials).map_err(|_| changed())? != allowed {
            return Err(changed());
        }
        Ok(())
    }

    fn comparison_discovery(
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

    /// Prepare both wire accounts for an already policy-scoped selection from
    /// one stable tree version, checking current discovery and state restrictions.
    pub(crate) async fn prepare_known(
        &self,
        selection: Selection<Resolved>,
        credentials: &Vec<PA::ContextData>,
    ) -> Result<(Box<dyn Send + Sync>, KnownState, Vec<KnownEntity>), RetrievalError> {
        let policy = ContextPolicy::from_credentials(&self.policy_agent, credentials);
        // Reconnects reuse the installed selection with current credentials. Gate
        // disclosure without changing that tree's identity or refilling its LIMIT.
        let allowed = if crate::schema::is_catalog_read(&selection.predicate) {
            Predicate::True
        } else {
            policy.filter_predicate(Predicate::True)?
        };
        let (view, state, members) = self
            .trees
            .prepare_known(selection, |id, state| {
                if allowed != Predicate::True {
                    match evaluate_predicate(&TemporaryEntity::new(id, state)?, &allowed) {
                        Ok(true) => {}
                        Ok(false) | Err(FilterError::PropertyNotFound(_)) => return Ok(false),
                        Err(error) => return Err(error.into()),
                    }
                }
                match policy.check_read_state(&id, state) {
                    Ok(()) => Ok(true),
                    Err(AccessDenied::ByPolicy(_) | AccessDenied::ModelDenied(_)) => Ok(false),
                    Err(error) => Err(error.into()),
                }
            })
            .await?;
        self.tree_maintenance.notify_one();
        Ok((Box::new(view), state, members))
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
mod tests {
    use super::*;
    use ankurah_proto::ModelId;

    #[test]
    fn predicate_proof_requires_every_restriction_and_does_not_treat_alternatives_as_grants() {
        let first = Predicate::MemberOf(ModelId::EntityId(EntityId::from_bytes([1; 32])));
        let second = Predicate::MemberOf(ModelId::EntityId(EntityId::from_bytes([2; 32])));
        let both = Predicate::And(Box::new(first.clone()), Box::new(second.clone()));
        let either = Predicate::Or(Box::new(first.clone()), Box::new(second.clone()));
        assert!(implies(&both, &first));
        assert!(implies(&both, &Predicate::And(Box::new(second.clone()), Box::new(first.clone()))));
        assert!(!implies(&first, &both));
        assert!(!implies(&either, &first));
        assert!(!implies(&Predicate::True, &first));
    }

    #[test]
    fn authorization_limits_include_member_identity_and_canonical_state_bytes() -> anyhow::Result<()> {
        let state = State::default();
        let one_member_bytes = 32 + bincode::serialized_size(&state)? as usize;
        for limits in [
            ComparisonLimits { max_capture_members: 1, ..Default::default() },
            ComparisonLimits { max_capture_bytes: one_member_bytes, ..Default::default() },
        ] {
            let mut budget = AuthorizationBudget::new(limits);
            budget.inspect(&state)?;
            assert!(matches!(budget.inspect(&state), Err(RetrievalError::StorageError(error))
                if error.downcast_ref::<ComparisonFailure>() == Some(&ComparisonFailure::LimitExceeded)));
        }
        let mut budget = AuthorizationBudget::new(ComparisonLimits { max_capture_bytes: one_member_bytes - 1, ..Default::default() });
        assert!(budget.inspect(&state).is_err());
        Ok(())
    }
}
