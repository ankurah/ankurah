//! The TreeSpec a selection's resident uses and the selections a TreeSpec serves.

use super::QueryTree;
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

impl QueryTree {
    pub fn spec(selection: &Selection<Resolved>, catalog: &CatalogManager) -> TreeSpec {
        // Entity-ID keys also support exact selections with expressions or JSON
        // paths that the scalar range planner cannot address.
        TreeSpec::for_selection(selection, &CatalogTypes(catalog)).unwrap_or_else(|error| {
            tracing::debug!(%error, "query uses entity-id digest keys");
            TreeSpec::new(KeySpec::new(vec![]), selection.predicate.clone())
        })
    }

    pub fn offered_spec(selection: &Selection<Resolved>, catalog: &CatalogManager, known: &proto::KnownState) -> anyhow::Result<TreeSpec> {
        anyhow::ensure!(Self::serves(&known.spec, selection, catalog), "unsupported comparison tree spec");
        Ok(known.spec.clone())
    }

    pub fn request(selection: &Selection<Resolved>, catalog: &CatalogManager) -> anyhow::Result<TreeRequest> {
        let mut membership = selection.clone();
        if membership.limit.is_none() {
            // Unbounded query ordering does not affect digest membership and
            // must not require types for otherwise unused ordering paths.
            membership.order_by = None;
        }
        Ok(TreeRequest::for_selection(&membership, &CatalogTypes(catalog))?)
    }

    pub fn serves(spec: &TreeSpec, selection: &Selection<Resolved>, catalog: &CatalogManager) -> bool {
        let types = CatalogTypes(catalog);
        if Self::key_paths(spec).iter().zip(&spec.key_spec().keyparts).any(|(path, part)| types.value_type(path) != Some(part.value_type))
            || CanonicalCodec::encode_nullable(spec.key_spec(), &vec![None; spec.key_spec().keyparts.len()]).is_err()
        {
            return false;
        }
        // Unknown scalar types and opaque predicates use exact admission with
        // entity-ID keys. Limited views requiring property ordering are
        // selected through storage before their metadata is scoped.
        if spec.predicate() == &selection.predicate && (selection.limit.is_none() || spec.key_spec().keyparts.is_empty()) {
            return true;
        }
        Self::request(selection, catalog).ok().is_some_and(|request| spec.serves(&request).is_some())
    }

    pub fn key_paths(spec: &TreeSpec) -> Vec<PropertyPath> { spec.key_spec().keyparts.iter().map(IndexKeyPart::path).collect() }
}
