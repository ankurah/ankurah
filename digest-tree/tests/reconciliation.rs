//! Two peers reconcile their trees through this crate's public API alone:
//! they compare span digests, sent across the wire, refine where they differ
//! using whichever tree has structure there, and exchange members only where
//! the narrowest spans still differ. Comparison and repair are tested here,
//! not how a peer decides which member wins.

mod common;

use std::collections::BTreeMap;

use ankurah_digest_tree::{DigestGroup, LtHash16, Ristretto255, Span};
use common::{album, reconcile, tree, Album, Id};

/// Peers that differ in an album's title, in an album each lacks, and in an
/// album's year, over a tree large enough to branch.
async fn asymmetric_peers_reconcile<G: DigestGroup>() {
    let catalog = |n: u64| album!(n, 1980 + (n % 40) as i64, &format!("Album {n}"));
    let ours = tree::<G>((0..300).map(catalog)).await;

    let mut albums: BTreeMap<u64, Album> = (0..300).map(|n| (n, catalog(n))).collect();
    albums.insert(17, album!(17, 1997, "Album 17 (Live)"));
    albums.remove(&81);
    albums.insert(82, album!(82, 2024, "Album 82"));
    albums.extend([(500, album!(500, 1975, "Album 500")), (501, album!(501, 2025, "Album 501"))]);
    let theirs = tree::<G>(albums.values().cloned()).await;

    // An album retitled, one removed, one moved to another year, and two
    // added.
    assert_eq!(reconcile(&ours, &theirs, &albums, &[Span::all()]).await, 5);

    let (our_root, their_root) =
        (ours.view().span_digest(&Span::all()).await.unwrap(), theirs.view().span_digest(&Span::all()).await.unwrap());

    assert_eq!(our_root.digest, their_root.digest);
    let moved_key = ours.key_spec().collate([2024]);
    let moved = (ours.view().member(&moved_key, &Id(82)).await.unwrap(), theirs.view().member(&moved_key, &Id(82)).await.unwrap());
    assert!(moved.0.is_some() && moved.0 == moved.1, "the moved album");

    let empty = tree::<G>([]).await;
    reconcile(&empty, &theirs, &albums, &[Span::all()]).await;

    assert_eq!(empty.view().len().await.unwrap(), albums.len() as u64);
}

#[tokio::test]
async fn asymmetric_ristretto255_peers_reconcile() { asymmetric_peers_reconcile::<Ristretto255>().await }

#[tokio::test]
async fn asymmetric_lthash16_peers_reconcile() { asymmetric_peers_reconcile::<LtHash16>().await }

/// Peers reconcile one range of years, and albums outside it stay as they
/// were.
#[tokio::test]
async fn a_range_reconciles_without_touching_members_outside_it() {
    let ours = tree::<Ristretto255>((0..120).map(|n| album!(n, 1900 + n as i64, &format!("Album {n}")))).await;
    let their_albums: BTreeMap<u64, Album> = (0..120).map(|n| (n, album!(n, 1900 + n as i64, &format!("Album {n} (Live)")))).collect();
    let theirs = tree::<Ristretto255>(their_albums.values().cloned()).await;

    let years = ours.key_spec();
    let range = Span::for_key_range(years.collate([1910])..years.collate([1930]));
    assert_eq!(reconcile(&ours, &theirs, &their_albums, &[range]).await, 20);

    for (id, live) in [(9, false), (10, true), (29, true), (30, false)] {
        let title = ours.view().member(&years.collate([1900 + id as i64]), &Id(id)).await.unwrap().unwrap().payload;
        assert_eq!(title.0.ends_with("(Live)"), live, "album {id}");
    }
}
