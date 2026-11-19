//! Reconcile live digest trees after canonical commits, including cancelled callers.

use super::Node;
use crate::{
    entity::TemporaryEntity,
    error::RetrievalError,
    policy::{AccessDenied, ContextPolicy, PolicyAgent},
    selection::filter::{evaluate_predicate, Error as FilterError},
    storage::StorageEngine,
};
use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_proto::{KnownEntity, KnownState};
use ankurah_signals::Wait;
use std::time::Duration;

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
