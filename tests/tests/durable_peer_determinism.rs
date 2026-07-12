//! Determinism of the two node-level durable-peer emission paths (issue #279).
//!
//! - `get_durable_peers` must return a stable, id-sorted order so peer fan-out is reproducible.
//! - `get_durable_peer_random` must draw from the node's seeded RNG, so two nodes seeded
//!   identically produce an identical selection sequence for identical peer sets.
//!
//! These paths are only reachable in multi-durable-peer topologies, which the current smoke
//! scenarios do not exercise; the C1 simulation audit will arm them for C5.

mod common;
use ankurah::{proto, Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;

/// Insert a batch of durable peers into a node via the test-only seam.
fn seed_peers(node: &Node<SledStorageEngine, PermissiveAgent>, peers: &[proto::NodeId]) {
    for p in peers {
        node.insert_durable_peer_for_test(*p);
    }
}

fn peer_ids(count: u8) -> Vec<proto::NodeId> { (1..=count).map(|tag| proto::NodeId::from_bytes([tag; 32])).collect() }

/// Path 2: get_durable_peers returns a stable, id-sorted order across repeated calls.
///
/// The peers are inserted in a deliberately unsorted sequence; the underlying set is unordered,
/// so a naive to_vec could surface any order. The returned order must equal the ascending-id sort
/// and be identical on every call.
#[tokio::test]
async fn test_get_durable_peers_is_stable_and_sorted() -> Result<()> {
    let node = common::durable_sled_setup().await?;

    // Generate several peer ids, then insert them in an order that is not their sorted order.
    let mut peers = peer_ids(8);
    peers.sort();
    let mut insertion = peers.clone();
    insertion.reverse();
    seed_peers(&node, &insertion);

    let run1 = node.get_durable_peers();
    let run2 = node.get_durable_peers();

    assert_eq!(run1, run2, "get_durable_peers must be identical across calls");
    assert_eq!(run1, peers, "get_durable_peers must be sorted ascending by id");

    Ok(())
}

/// Path 3: get_durable_peer_random draws from the node's seeded RNG.
///
/// Two nodes constructed with the same seed and the same peer set must produce an identical
/// sequence of selections. A run with a different seed must (with overwhelming probability over a
/// long sequence and a wide peer set) diverge, proving randomness is preserved, not removed.
#[tokio::test]
async fn test_get_durable_peer_random_same_seed_same_sequence() -> Result<()> {
    // Shared peer set; both seeded nodes see the identical set.
    let peers = peer_ids(16);

    let draw_sequence = |seed: u64| -> Vec<proto::NodeId> {
        let node = Node::new_durable_with_seed(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new(), seed);
        seed_peers(&node, &peers);
        (0..64).map(|_| node.get_durable_peer_random().expect("peer set is non-empty")).collect()
    };

    let seq_a1 = draw_sequence(0xA11CE);
    let seq_a2 = draw_sequence(0xA11CE);
    let seq_b = draw_sequence(0xB0B);

    assert_eq!(seq_a1, seq_a2, "same seed and same peer set must yield an identical selection sequence");
    assert_ne!(seq_a1, seq_b, "a different seed must yield a different sequence (randomness preserved)");

    // Sanity: selection actually varies across the sequence (not pinned to one peer).
    let distinct: std::collections::HashSet<_> = seq_a1.iter().collect();
    assert!(distinct.len() > 1, "seeded selection should visit more than one peer over 64 draws");

    Ok(())
}

/// A node's default (entropy-seeded) construction still selects a valid peer, and selection is
/// confined to the registered peer set. Guards against the seam accidentally breaking production.
#[tokio::test]
async fn test_get_durable_peer_random_default_stays_in_set() -> Result<()> {
    let node = common::durable_sled_setup().await?;
    let peers = peer_ids(4);
    seed_peers(&node, &peers);

    let peer_set: std::collections::HashSet<_> = peers.iter().copied().collect();
    for _ in 0..32 {
        let picked = node.get_durable_peer_random().expect("peer set is non-empty");
        assert!(peer_set.contains(&picked), "selected peer must be a registered durable peer");
    }

    // With no peers, selection yields None rather than panicking.
    let empty = common::durable_sled_setup().await?;
    assert!(empty.get_durable_peer_random().is_none(), "no durable peers means no selection");

    Ok(())
}
