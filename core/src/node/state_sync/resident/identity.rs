//! What a resident admits and which requests may share it.

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
/// predicate and the TreeSpec it was admitted under, and the credentials that
/// admitted it.
#[derive(Clone, PartialEq)]
pub(super) struct Identity<CD> {
    pub(super) coverage: Coverage,
    pub(super) selection: Selection<Resolved>,
    pub(super) filtered: Selection<Resolved>,
    pub(super) retrieval: Arc<Predicate<Resolved>>,
    pub(super) spec: proto::TreeSpec,
    pub(super) credentials: Vec<CD>,
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
    pub(super) fn view(&self) -> QueryView {
        QueryView { selection: self.selection.clone(), filtered: self.filtered.clone(), spec: self.spec.clone() }
    }

    pub(super) fn check<SE, PA>(&self, node: &Node<SE, PA>) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent<ContextData = CD> + Send + Sync + 'static,
    {
        let policy = ContextPolicy::from_credentials(&node.policy_agent, &self.credentials);
        anyhow::ensure!(
            policy.filter_predicate(self.selection.predicate.clone())? == self.filtered.predicate
                && policy.retrieval_predicate() == self.retrieval,
            SourceError::Changed
        );
        Ok(())
    }
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
/// selection filtered by policy, and the TreeSpec its comparisons describe.
#[derive(Clone)]
pub(super) struct QueryView {
    pub(super) selection: Selection<Resolved>,
    pub(super) filtered: Selection<Resolved>,
    pub(super) spec: proto::TreeSpec,
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
