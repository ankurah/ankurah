//! The tree spec: what defines a tree.

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use ankql::ast::{Predicate, Resolved, Selection};
use ankurah_core_types::{IndexDirection, IndexKeyPart, KeySpec, PropertyId, PropertyPath};
use serde::{Deserialize, Serialize};

use super::request::{PropertyTypes, TreeRequest};
use crate::error::TreeError;

/// What defines a tree: the leaves it admits and the order it keeps them in.
///
/// A tree spec is the resolved, canonical description of a tree, so that two
/// trees can be checked to use the same admission and addressing rules before
/// their digests are compared. Spec equality does not establish equal content,
/// and a request can read a tree whose admission predicate is not its own:
/// see [`TreeSpec::serves`]. What a use of a tree compares is its scope, which
/// the use keeps and the spec does not carry: the spans of the tree it reads,
/// the order it walks them in, and the count it takes under a limit.
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
/// within the cut's own value the entity ids up to it. The entity-id tree has
/// the empty key spec, so its address is the entity id alone.
///
/// The predicate is the admission. It chooses which entities file a leaf,
/// never where they file: the Selection's whole predicate, resolved to
/// property ids, membership in a component included as a term of it. Whoever
/// feeds the tree decides admission against entity state and derives keys
/// with [`CanonicalCodec`](crate::CanonicalCodec), which files an entity
/// missing a key part under that part's null encoding, so a key never leaves
/// out an entity the predicate admits.
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

    /// The spec of a new tree for `selection`, its key chosen by this policy:
    /// the parts the Selection pins by equality first, in canonical order;
    /// then, under a limit, its nonconstant ORDER BY parts, up to the entity
    /// id if the order names it; and otherwise one part the Selection bounds
    /// by a range, the first ORDER BY part among them when there are several.
    /// The entity id always ends the address. So `project = 'acme' AND foo >
    /// 7` gets the key (project, foo) with or without `ORDER BY foo`: the
    /// range supplies foo, and a narrower range of foo is then a span of the
    /// tree. This policy only makes new trees: whether an existing tree serves
    /// a Selection is [`TreeSpec::serves`]'s question, whatever its key.
    pub fn for_selection(selection: &Selection<Resolved>, types: &impl PropertyTypes) -> Result<Self, TreeError> {
        let request = TreeRequest::for_selection(selection, types)?;
        let mut paths: Vec<_> =
            request.types.keys().filter(|p| p.property_id() != PropertyId::Id && request.pinned(p).is_some()).cloned().collect();
        if request.limit.is_some() {
            for key in &request.order.keyparts {
                let path = key.path();
                if path == PropertyPath::id() {
                    break;
                }
                if !paths.contains(&path) {
                    paths.push(path);
                }
            }
        } else {
            let ranged: BTreeSet<_> = request
                .clauses
                .iter()
                .flat_map(|c| c.fields.keys())
                .filter(|p| p.property_id() != PropertyId::Id && !paths.contains(p))
                .cloned()
                .collect();
            let ordered = request.order.keyparts.iter().map(IndexKeyPart::path).find(|p| ranged.contains(p));
            if let Some(path) = ordered.or_else(|| ranged.into_iter().next()) {
                paths.push(path);
            }
        }
        let parts = paths.iter().map(|p| IndexKeyPart::asc_on(p, request.types[p])).collect();
        Ok(Self::new(KeySpec::new(parts), selection.predicate.clone()))
    }

    /// The ordering: the parts that address the leaves, every one ascending;
    /// empty for the entity-id tree.
    pub fn key_spec(&self) -> &KeySpec<PropertyId> { &self.key_spec }

    /// The admission: the Selection's whole predicate, resolved.
    pub fn predicate(&self) -> &Predicate<Resolved> { &self.predicate }

    /// Identity ignores display labels but preserves predicate structure and
    /// literal bits. The stored predicate retains its labels for diagnostics.
    fn identity_bytes(key_spec: &KeySpec<PropertyId>, predicate: &Predicate<Resolved>) -> Vec<u8> {
        bincode::serialize(&(key_spec, unlabeled(predicate))).expect("a tree spec serializes")
    }
}

/// `predicate` with each property path's display label replaced by its
/// property's identity, so that predicates differing only in labels compare
/// equal byte for byte.
pub(super) fn unlabeled(predicate: &Predicate<Resolved>) -> Predicate<Resolved> {
    ankql::selection::map_references::<Resolved, Resolved>(
        &Selection::from(predicate.clone()),
        &|path| {
            let mut reference = PropertyPath::from(path.property_id());
            reference.subpath = path.subpath.clone();
            reference
        },
        &|model| *model,
    )
    .predicate
}

/// Two specs are equal when their identity bytes agree, display labels aside.
/// A float literal compares by its bits, so 0.0 and -0.0, or two NaNs with
/// different payloads, make different specs, and a NaN equals itself:
/// canonicalizing literals, so that equal Selections share a tree, is the
/// caller's job, done before it builds the spec. So is the order of a
/// conjunction's terms, which the bytes keep.
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
