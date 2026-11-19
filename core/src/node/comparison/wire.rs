//! Convert core's trait wrappers at the protocol boundary.

use ankurah_core_types::{
    key_spec::{IndexKeyPart, KeySpec},
    PropertyId,
};
use ankurah_digest_tree::{Leaf, Page, Ristretto255};
use ankurah_proto::ComparisonPage;

use crate::sync::{EntityHead, EntityMemberId, TreeProperty};

pub(super) fn key_spec_to_wire(spec: &KeySpec<TreeProperty>) -> KeySpec<PropertyId> {
    KeySpec::new(
        spec.keyparts
            .iter()
            .map(|part| IndexKeyPart {
                key: part.key.0,
                sub_path: part.sub_path.clone(),
                direction: part.direction,
                value_type: part.value_type,
                nulls: part.nulls,
                collation: part.collation.clone(),
            })
            .collect(),
    )
}

pub(super) fn page_to_wire(page: Page<EntityMemberId, EntityHead, Ristretto255>) -> ComparisonPage {
    match page {
        Page::Digests(digests) => Page::Digests(digests),
        Page::Members(members) => {
            Page::Members(members.into_iter().map(|leaf| Leaf { key: leaf.key, id: leaf.id.0, payload: leaf.payload.0 }).collect())
        }
    }
}

pub(super) fn page_from_wire(page: ComparisonPage) -> Page<EntityMemberId, EntityHead, Ristretto255> {
    match page {
        Page::Digests(digests) => Page::Digests(digests),
        Page::Members(members) => Page::Members(
            members
                .into_iter()
                .map(|leaf| Leaf { key: leaf.key, id: EntityMemberId(leaf.id), payload: EntityHead(leaf.payload) })
                .collect(),
        ),
    }
}
