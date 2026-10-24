//! What a resident admits and which requests may share it.

use crate::{
    node::{
        state_sync::{QueryTree, Snapshot},
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

/// A limited resident cannot establish completeness for another rank cut.
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

/// The query view is separate from the resident tree's admission and lifetime.
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
    if !QueryTree::serves(spec, &requested.selection, catalog) {
        return false;
    }
    if matches!(existing.coverage, Coverage::Limited(_)) || requested.selection.limit.is_none() {
        return true;
    }
    // A shared complete index must answer the rank cut from its own keys.
    // Opaque ordering and descending nullable fields need a separate bounded
    // resident, whose storage refill cannot stall this tree's other owners.
    if requested.selection.order_by.iter().flatten().any(|item| {
        item.direction == ankql::ast::OrderDirection::Desc
            && item.path != ankql::ast::PropertyPath::id()
            && (!item.path.subpath.is_empty() || requested.selection.predicate.assume_null(&[item.path.property_id()]) != Predicate::False)
    }) {
        return false;
    }
    QueryTree::request(&requested.selection, catalog).ok().is_some_and(|request| spec.serves(&request).is_some())
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
    QueryTree::request(&filtered, catalog).ok().and_then(|request| intersection.serves(&request)) == Some(TreeMatch::Whole)
}
