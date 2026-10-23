//! The tree spec: what defines a tree.

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core_types::{IndexDirection, KeySpec, PropertyId, PropertyPath};
use serde::{Deserialize, Serialize};

use super::request::{part, path, PropertyTypes, TreeRequest};
use crate::error::TreeError;

/// What defines a tree: the leaves it admits and the order it keeps them in.
///
/// A tree spec is the resolved, canonical description of a tree, so that two
/// trees can be checked to use the same admission and addressing rules before
/// their digests are compared. Spec equality does not establish equal content,
/// and a compatible scope can reuse a tree with a different admission predicate.
/// One spec stands behind every use of a tree: an index's tree, a partial
/// replica's scope, a live query's result (where the tree is the matched set,
/// each entity with its head), a durable subscription, a fetch. What a use
/// compares is its scope, which the use keeps and the spec does not carry: the
/// spans of the tree it reads, as a list of span digests with tree node prefixes with key
/// bounds where a span cuts a tree node, the order it walks them in, and the
/// count it takes under a limit.
///
/// Its two fields divide into ordering and admission.
///
/// The key spec is the ordering. It fixes the address of every leaf, the
/// canonical key bytes followed by the entity id, and with it the order the
/// leaves are kept in, the prefixes the tree nodes are named by, and the spans
/// a scope can name. Every part of it ascends and none places nulls:
/// [`TreeSpec::new`] drops the directions and nulls placements of the key spec
/// it is handed, since the tree encodes every part ascending, with nulls at
/// one fixed end, and leaves the direction to whoever walks it. So every order
/// over the same parts names one tree: the index on (a, b) in any directions,
/// `ORDER BY a ASC, b DESC` and its reverse all read the one tree keyed
/// (a, b). Which way a use walks each part, and where nulls fall for it, is
/// the use's order, a [`KeySpec`] with directions over the same parts that the
/// use keeps with its scope. A digest is a sum, so the order changes no
/// digest, only which leaves fall under a limit's cut, and that cut is a few
/// spans of the one tree: the tiers before the cut whole, then within the
/// cut's tier the leaves past the cut value in the walk's direction, then
/// within the cut's own value the entity ids up to it. The default key for a
/// new Selection tree uses equality-pinned parts in canonical order, followed
/// by nonconstant ORDER BY parts under a limit (entity-id order when omitted).
/// Without a limit, it adds one constrained range part if available, using a
/// matching ORDER BY part to choose among several, then a canonical tie-break.
/// This is a creation policy; reuse considers the existing tree's layout.
/// Thus `project = 'acme' AND foo > 7` uses (project, foo), with or without
/// `ORDER BY foo`. The range predicate supplies foo; ordering is not needed
/// to make narrower foo ranges addressable. The entity-id tree has the
/// empty key spec, so its address is the entity id alone.
///
/// The predicate is the admission. It chooses which entities file a leaf,
/// never where they file: the Selection's whole predicate, resolved to
/// property ids, membership in a component included as a term of it.
/// Missing key parts require an explicit encoding or admission policy: a key
/// choice must not silently exclude entities the predicate admits.
/// A filtered tree can also serve a narrower
/// Selection when it admits every entity the Selection requires and the
/// remaining restriction is exactly expressed by bounds in that tree's key
/// order. Stored bucket boundaries need not coincide with those bounds: a
/// partial tree node must be descended or its qualifying leaves read, never
/// included with its whole digest. For example,
/// the tree admitting `project = 'acme' AND foo > 7`, keyed by (project, foo),
/// serves `project = 'acme' AND foo > 10` as a span. The requesting predicate
/// need not equal the tree's admission predicate. Both uses read the same
/// leaves at the same addresses; reuse creates no second tree.
///
/// Whoever feeds the tree decides admission against entity state and derives
/// keys with the canonical codec. The reuse planner analyzes predicates to
/// prove coverage and exact bounds, but does not load or evaluate entity state.
/// Whether a tree is an index tree is a fact about its owner, not its spec.
#[derive(Clone, Serialize)]
pub struct TreeSpec {
    key_spec: KeySpec<PropertyId>,
    predicate: Predicate<Resolved>,
    /// The bytes equality and hashing compare, made once by [`TreeSpec::new`].
    #[serde(skip)]
    identity: Vec<u8>,
}

impl std::fmt::Debug for TreeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeSpec").field("key_spec", &self.key_spec).field("predicate", &self.predicate).finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for TreeSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            key_spec: KeySpec<PropertyId>,
            predicate: Predicate<Resolved>,
        }
        let fields = Fields::deserialize(deserializer)?;
        Ok(Self::new(fields.key_spec, fields.predicate))
    }
}

impl TreeSpec {
    /// The spec of the tree that serves `key_spec` and `predicate`: the key
    /// spec with its directions and nulls placements dropped, every part
    /// ascending, and the whole predicate. `for_selection` uses this same
    /// constructor, so a spec never carries a direction.
    pub fn new(mut key_spec: KeySpec<PropertyId>, predicate: Predicate<Resolved>) -> Self {
        for part in &mut key_spec.keyparts {
            part.direction = IndexDirection::Asc;
            part.nulls = None;
        }
        let identity = Self::identity_bytes(&key_spec, &predicate);
        Self { key_spec, predicate, identity }
    }

    /// Choose a new layout: equality-pinned paths first; under a limit, the
    /// nonconstant ordering paths; otherwise one constrained range path, with
    /// ORDER BY breaking ties. Entity identity is always the final address part.
    pub fn for_selection(selection: &Selection<Resolved>, types: &impl PropertyTypes) -> Result<Self, TreeError> {
        let request = TreeRequest::for_selection(selection, types)?;
        let mut paths: Vec<_> =
            request.types.keys().filter(|p| p.property_id() != PropertyId::Id && request.pinned(p).is_some()).cloned().collect();
        if request.limit.is_some() {
            for key in &request.order.keyparts {
                let path = path(key);
                if path == PropertyPath::id() {
                    break;
                }
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
        } else {
            let ranges: BTreeSet<_> = request
                .clauses
                .iter()
                .flat_map(|c| c.fields.keys())
                .filter(|p| p.property_id() != PropertyId::Id && !paths.contains(p))
                .cloned()
                .collect();
            let ordered = request.order.keyparts.iter().map(path).find(|p| ranges.contains(p));
            if let Some(range) = ordered.or_else(|| ranges.into_iter().next()) {
                paths.push(range);
            }
        }
        let parts = paths.iter().map(|p| part(p, request.types[p])).collect();
        Ok(Self::new(KeySpec::new(parts), selection.predicate.clone()))
    }

    /// The ordering: the parts that address the leaves, every one ascending.
    /// Empty for the entity-id tree. A predicate's range part can be present
    /// without an ORDER BY, allowing narrower selections to use key bounds.
    pub fn key_spec(&self) -> &KeySpec<PropertyId> { &self.key_spec }

    /// The admission: the Selection's whole predicate, resolved.
    /// Key availability must not silently add another membership restriction.
    pub fn predicate(&self) -> &Predicate<Resolved> { &self.predicate }

    /// Identity ignores display labels but preserves predicate structure and
    /// literal bits. The stored predicate retains its labels for diagnostics.
    fn identity_bytes(key_spec: &KeySpec<PropertyId>, predicate: &Predicate<Resolved>) -> Vec<u8> {
        let predicate = ankql::selection::map_references::<Resolved, Resolved>(
            &Selection::from(predicate.clone()),
            &|path| {
                let mut reference = PropertyPath::from(path.property_id());
                reference.subpath = path.subpath.clone();
                reference
            },
            &|model| *model,
        )
        .predicate;
        bincode::serialize(&(key_spec, predicate)).expect("a tree spec serializes")
    }
}

/// Two specs are equal when their identity bytes agree. Display labels do not
/// participate. A float literal compares by
/// its bits, so 0.0 and -0.0, or two NaNs with different payloads, make
/// different specs, and a NaN equals itself: canonicalizing literals, so that
/// equal Selections share a tree, is the caller's job, done before it builds
/// the spec. So is the order of a conjunction's terms, which the bytes keep.
impl PartialEq for TreeSpec {
    fn eq(&self, other: &Self) -> bool { self.identity == other.identity }
}

/// Equality compares bytes, which compare totally, float literals included.
impl Eq for TreeSpec {}

/// Hashes the bytes equality compares, so equal specs hash alike.
impl Hash for TreeSpec {
    fn hash<H: Hasher>(&self, state: &mut H) { self.identity.hash(state) }
}

#[cfg(test)]
mod tests;
