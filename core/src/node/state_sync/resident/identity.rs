//! What a resident admits and which requests may share it.

use super::bootstrap::ReadPolicy;
use crate::{
    node::{
        state_sync::{
            tree::{request_for, serves, tree_keys_rank_like_storage},
            Snapshot,
        },
        Node,
    },
    policy::{ContextPolicy, PolicyAgent},
    reactor::maintenance::SourceError,
    session::ContextData,
    storage::StorageEngine,
};
use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_digest_tree::{TreeMatch, TreeSpec};
use ankurah_proto as proto;
use std::sync::Arc;

/// What a resident admits and for whom: its coverage, the selection it was
/// requested with and that selection filtered by policy, the retrieval
/// predicate and the TreeSpec it was admitted under, whose read checks decide
/// which candidates it admits, and the credentials that admitted it.
#[derive(Clone, PartialEq)]
pub(super) struct Identity<CD> {
    pub(super) coverage: Coverage,
    pub(super) selection: Selection<Resolved>,
    pub(super) filtered: Selection<Resolved>,
    pub(super) retrieval: Arc<Predicate<Resolved>>,
    pub(super) spec: proto::TreeSpec,
    pub(super) access: Access,
    /// The credentials that admitted the resident. A restricted resident's
    /// candidates are admitted by their read checks; any resident's batches
    /// are checked against their policy.
    pub(super) credentials: Vec<CD>,
}

/// Whose read checks decide which of a resident's candidates it admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Access {
    /// The admitting credentials held a fresh proof of unrestricted state
    /// reads: the resident admits every candidate, so it may serve any
    /// principal whose own proof holds, checked when it takes a lease and on
    /// every use.
    Unrestricted,
    /// The admitting credentials' per-entity read checks decide: only the same
    /// credentials can share the resident, since another principal's checks
    /// could differ.
    Restricted,
}

impl<CD: ContextData> Identity<CD> {
    /// What `credentials` may read of `selection`, under the node's current
    /// policy, in a tree under `spec`.
    pub(super) fn requested<SE, PA>(
        node: &Node<SE, PA>,
        selection: Selection<Resolved>,
        spec: proto::TreeSpec,
        credentials: Vec<CD>,
    ) -> anyhow::Result<Self>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &credentials);
        let mut filtered = selection.clone();
        filtered.predicate = policy.filter_predicate(filtered.predicate)?;
        Ok(Self {
            coverage: Coverage::for_selection(&selection),
            selection,
            filtered,
            retrieval: policy.retrieval_predicate(),
            spec,
            access: if policy.allows_unrestricted_state_reads() { Access::Unrestricted } else { Access::Restricted },
            credentials,
        })
    }

    /// The identity of the resident built for this request. A complete resident
    /// admits the request's whole predicate, without its order or limit; a
    /// limited one keeps the request's cut, which no other selection can share.
    /// Either is keyed as the request asked, over the request's own predicate,
    /// so a peer's broader spec does not widen what the resident loads.
    pub(super) fn admission(&self) -> Self {
        let (selection, filtered) = match &self.coverage {
            Coverage::Complete => (Selection::from(self.selection.predicate.clone()), Selection::from(self.filtered.predicate.clone())),
            Coverage::Limited(_) => (self.selection.clone(), self.filtered.clone()),
        };
        let spec = TreeSpec::new(self.spec.key_spec().clone(), self.selection.predicate.clone());
        Self { selection, filtered, spec, ..self.clone() }
    }

    /// The view this request reads from whichever resident serves it.
    pub(super) fn view(&self) -> QueryView<CD> {
        QueryView {
            selection: self.selection.clone(),
            filtered: self.filtered.clone(),
            spec: self.spec.clone(),
            credentials: self.credentials.clone(),
        }
    }

    /// Whether a resident admitted under this identity may serve `requested`'s
    /// principal: a restricted resident only the same credentials, and an
    /// unrestricted one any principal for which both `requested`'s and this
    /// identity's credentials hold a fresh proof of unrestricted state reads.
    pub(super) fn shares_access(&self, requested: &Self, proof: impl Fn(&[CD]) -> bool) -> bool {
        match self.access {
            Access::Restricted => self.credentials == requested.credentials,
            Access::Unrestricted => requested.access == Access::Unrestricted && proof(&self.credentials),
        }
    }

    /// Fail with `PolicyChanged` unless `holder`, the policy of a principal this
    /// resident serves, still reads what the resident admitted: the same
    /// retrieval predicate and, for an unrestricted resident, a fresh proof of
    /// unrestricted state reads.
    pub(super) fn ensure_holder_admitted<PA: PolicyAgent<ContextData = CD>>(&self, holder: &ReadPolicy<'_, PA>) -> anyhow::Result<()> {
        anyhow::ensure!(holder.retrieval_predicate() == self.retrieval, SourceError::PolicyChanged);
        anyhow::ensure!(self.access == Access::Restricted || holder.allows_unrestricted_state_reads(), SourceError::PolicyChanged);
        Ok(())
    }

    /// Fail with `PolicyChanged` unless policy still gives a restricted
    /// resident's admitting credentials the filtered selection and retrieval
    /// predicate it was admitted with. An unrestricted resident's admission
    /// depends on no one principal; each principal it serves is checked on its
    /// own.
    pub(super) fn ensure_policy_unchanged<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        if self.access == Access::Unrestricted {
            return Ok(());
        }
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        ensure_filter_unchanged(&policy, &self.selection, &self.filtered)?;
        anyhow::ensure!(policy.retrieval_predicate() == self.retrieval, SourceError::PolicyChanged);
        Ok(())
    }
}

/// Fail with `PolicyChanged` unless `policy` still filters `selection` to the
/// predicate `filtered` recorded.
pub(super) fn ensure_filter_unchanged<PA: PolicyAgent>(
    policy: &ReadPolicy<'_, PA>,
    selection: &Selection<Resolved>,
    filtered: &Selection<Resolved>,
) -> anyhow::Result<()> {
    anyhow::ensure!(policy.filter_predicate(selection.predicate.clone())? == filtered.predicate, SourceError::PolicyChanged);
    Ok(())
}

/// Which of its predicate's matches a resident holds: all of them, or only a
/// limited selection's cut, which cannot answer any other cut.
#[derive(Clone, PartialEq)]
pub(super) enum Coverage {
    Complete,
    Limited(Selection<Resolved>),
}
impl Coverage {
    pub(super) fn for_selection(selection: &Selection<Resolved>) -> Self {
        if selection.limit.is_some() {
            Self::Limited(selection.clone())
        } else {
            Self::Complete
        }
    }
    pub(super) fn permits(&self, selection: &Selection<Resolved>) -> bool {
        match self {
            Self::Complete => true,
            Self::Limited(original) => original == selection,
        }
    }
    pub(super) async fn select(
        &self,
        snapshot: Snapshot,
        selection: &Selection<Resolved>,
        catalog: &crate::schema::catalog::CatalogManager,
    ) -> anyhow::Result<Snapshot> {
        match self {
            Self::Complete => snapshot.select(selection, catalog).await,
            Self::Limited(original) => {
                anyhow::ensure!(original == selection, "limited resident does not cover this selection");
                Ok(snapshot)
            }
        }
    }
}

/// What one lease reads from its resident: the selection it asked for, that
/// selection filtered by policy, the TreeSpec its comparisons describe, and the
/// credentials of the principal it reads for.
#[derive(Clone)]
pub(super) struct QueryView<CD> {
    pub(super) selection: Selection<Resolved>,
    pub(super) filtered: Selection<Resolved>,
    pub(super) spec: proto::TreeSpec,
    pub(super) credentials: Vec<CD>,
}

pub(super) fn compatible_selection<CD: ContextData>(
    existing: &Identity<CD>,
    requested: &Identity<CD>,
    catalog: &crate::schema::catalog::CatalogManager,
) -> bool {
    let spec = &existing.spec;
    if !serves(spec, &requested.selection, catalog) {
        return false;
    }
    if matches!(existing.coverage, Coverage::Limited(_)) || requested.selection.limit.is_none() {
        return true;
    }
    // A shared complete index must answer the rank cut from its own keys.
    // Opaque ordering and descending nullable fields need a separate bounded
    // resident, whose storage refill cannot stall this tree's other owners.
    if !tree_keys_rank_like_storage(&requested.selection) {
        return false;
    }
    request_for(&requested.selection, catalog).ok().is_some_and(|request| spec.serves(&request).is_some())
}

/// A policy may depend on the requested predicate. A shared resident is safe
/// only when selecting from its admitted rows gives the same filtered query.
pub(super) fn compatible_access<CD: ContextData>(
    existing: &Identity<CD>,
    requested: &Identity<CD>,
    catalog: &crate::schema::catalog::CatalogManager,
) -> bool {
    if existing.selection.predicate == requested.selection.predicate {
        return existing.filtered.predicate == requested.filtered.predicate;
    }
    let intersection = TreeSpec::new(
        existing.spec.key_spec().clone(),
        Predicate::And(Box::new(existing.filtered.predicate.clone()), Box::new(requested.selection.predicate.clone())),
    );
    let mut filtered = requested.filtered.clone();
    filtered.limit = None;
    filtered.order_by = None;
    request_for(&filtered, catalog).ok().and_then(|request| intersection.serves(&request)) == Some(TreeMatch::Whole)
}
