//! The TreeSpec a selection's resident uses and the selections a TreeSpec serves.

use crate::schema::catalog::CatalogManager;
use ankql::ast::{Resolved, Selection};
use ankurah_core_types::{IndexKeyPart, KeySpec, PropertyPath, ValueType};
use ankurah_digest_tree::{CanonicalCodec, PropertyTypes, TreeRequest};
use ankurah_proto::{self as proto, TreeSpec};

struct CatalogTypes<'a>(&'a CatalogManager);
impl PropertyTypes for CatalogTypes<'_> {
    fn value_type(&self, path: &PropertyPath) -> Option<ValueType> {
        if !path.subpath.is_empty() {
            return None;
        }
        match path.property_id() {
            proto::PropertyId::Id => Some(ValueType::EntityId),
            proto::PropertyId::System(property) => Some(crate::schema::resolver::system_property_value_type(property)),
            proto::PropertyId::EntityId(id) => {
                self.0.property_by_id(&id).ok().flatten().and_then(|property| ValueType::from_property_str(&property.value_type))
            }
        }
    }
}

/// The TreeSpec a resident for `selection` uses: keyed by the selection's
/// ordering where the catalog types every key part, otherwise by entity id
/// alone, which also serves exact selections over expressions or JSON paths
/// that the range planner cannot address.
pub(crate) fn spec_for(selection: &Selection<Resolved>, catalog: &CatalogManager) -> TreeSpec {
    TreeSpec::for_selection(selection, &CatalogTypes(catalog)).unwrap_or_else(|error| {
        tracing::debug!(%error, "query uses entity-id digest keys");
        TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone())
    })
}

/// The spec a peer built its known state under, accepted when a tree under it
/// can serve `selection`.
pub(crate) fn accept_peer_spec(
    selection: &Selection<Resolved>,
    catalog: &CatalogManager,
    known: &proto::KnownState,
) -> anyhow::Result<TreeSpec> {
    anyhow::ensure!(serves(&known.spec, selection, catalog), "unsupported comparison tree spec");
    Ok(known.spec.clone())
}

/// The digest-tree request that selects `selection`'s members, typed by the
/// catalog.
pub(crate) fn request_for(selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<TreeRequest> {
    let mut membership = selection.clone();
    if membership.limit.is_none() {
        // Unbounded query ordering does not affect digest membership and
        // must not require types for otherwise unused ordering paths.
        membership.order_by = None;
    }
    Ok(TreeRequest::for_selection(&membership, &CatalogTypes(catalog))?)
}

/// Whether a tree under `spec` can serve `selection`: every key part carries
/// the catalog's current type and can encode an absent value, and the tree
/// either admits exactly the selection's predicate or proves, by the digest
/// tree's reuse rules, that it holds every member at addresses the request can
/// select.
pub(crate) fn serves(spec: &TreeSpec, selection: &Selection<Resolved>, catalog: &CatalogManager) -> bool {
    let types = CatalogTypes(catalog);
    if key_paths(spec).iter().zip(&spec.key_spec().keyparts).any(|(path, part)| types.value_type(path) != Some(part.value_type))
        || CanonicalCodec::encode_nullable(spec.key_spec(), &vec![None; spec.key_spec().keyparts.len()]).is_err()
    {
        return false;
    }
    // A tree admitting exactly the selection's predicate holds every member.
    // Under a limit, entity-id keys cannot rank the members, so a limited
    // resident takes its cut from storage and holds only that cut.
    if spec.predicate() == &selection.predicate && (selection.limit.is_none() || spec.key_spec().keyparts.is_empty()) {
        return true;
    }
    request_for(selection, catalog).ok().is_some_and(|request| spec.serves(&request).is_some())
}

/// The property path of each of `spec`'s key parts, in key order.
pub(crate) fn key_paths(spec: &TreeSpec) -> Vec<PropertyPath> { spec.key_spec().keyparts.iter().map(IndexKeyPart::path).collect() }
