//! A peer's side of reconciling: span digests sent across the wire, the
//! spans whose members differ, and one tree repaired from another.

use std::collections::BTreeMap;

use ankurah_digest_tree::{leaf_address, DigestGroup, Leaf, ScanRange, Span, SpanDigest};
use futures::TryStreamExt;

use super::albums::{Album, Title, Tree, View};
use super::members::Id;

/// A span digest as the other peer receives it.
pub fn across_the_wire<G: DigestGroup>(span_digest: &SpanDigest<G>) -> SpanDigest<G> {
    bincode::deserialize(&bincode::serialize(span_digest).unwrap()).unwrap()
}

/// The narrowest spans whose members differ between two views of the same
/// spans: start from the root, keep spans whose digests differ, and refine
/// each with whichever view has structure beneath it.
pub async fn differing_spans<G: DigestGroup>(ours: &View<G>, theirs: &View<G>) -> Vec<Span> {
    let mut pending = vec![across_the_wire(&ours.span_digest(&Span::all()).await.unwrap())];
    let mut differing = Vec::new();

    while let Some(span_digest) = pending.pop() {
        let their_digest = theirs.span_digest(&span_digest.bounds).await.unwrap();
        if ours.span_digest(&span_digest.bounds).await.unwrap().digest == their_digest.digest {
            continue;
        }

        let mut refined = ours.refine(&span_digest).await.unwrap();
        if refined.is_empty() {
            refined = theirs.refine(&across_the_wire(&span_digest)).await.unwrap();
        }

        match refined.is_empty() {
            true => differing.push(span_digest.bounds),
            false => pending.extend(refined.iter().map(across_the_wire)),
        }
    }

    differing
}

/// Every member of a view within `span`, by address.
async fn members<G: DigestGroup>(view: &View<G>, span: &Span) -> BTreeMap<Vec<u8>, Leaf<Id, Title>> {
    view.range(ScanRange::forward(span.clone())).map_ok(|leaf| (leaf_address(&leaf.key, &leaf.id), leaf)).try_collect().await.unwrap()
}

/// One member's two sides as a reconciliation finds them: our leaf, if we
/// hold one, and their album, if they hold one.
type Sides = (Option<Leaf<Id, Title>>, Option<Album>);

/// Make our tree hold what theirs holds within `spans`, in one
/// transaction: for each member whose leaves differ in the differing spans,
/// our leaf, if we hold one, names the version a change replaces, and their
/// album, as their owner gives it, the version that replaces it. Returns how
/// many members that changed.
pub async fn reconcile<G: DigestGroup>(ours: &Tree<G>, theirs: &Tree<G>, their_albums: &BTreeMap<u64, Album>, spans: &[Span]) -> usize {
    let (our_view, their_view) = (ours.select(spans.to_vec()), theirs.select(spans.to_vec()));

    // By id: a member moved to another key differs in two spans, and is one
    // update from our leaf at its old key to their album at its new one.
    let mut changes: BTreeMap<Id, Sides> = BTreeMap::new();
    for span in differing_spans(&our_view, &their_view).await {
        let (mine, yours) = (members(&our_view, &span).await, members(&their_view, &span).await);
        for (address, leaf) in &mine {
            if yours.get(address) != Some(leaf) {
                changes.entry(leaf.id).or_default().0 = Some(leaf.clone());
            }
        }

        for (address, leaf) in &yours {
            if mine.get(address) != Some(leaf) {
                changes.entry(leaf.id).or_default().1 = Some(their_albums[&leaf.id.0].clone());
            }
        }
    }

    let count = changes.len();
    let mut trx = ours.transaction();

    for change in changes.into_values() {
        match change {
            (Some(ours), Some(theirs)) => trx.update(ours, theirs),
            (Some(ours), None) => trx.remove(ours),
            (None, theirs) => trx.insert(theirs.expect("a member that differs has a leaf on one side")),
        }
        .unwrap();
    }

    trx.commit().await.unwrap();
    assert!(differing_spans(&ours.select(spans.to_vec()), &their_view).await.is_empty(), "reconciled");

    count
}
