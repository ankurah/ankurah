//! Determinism of the two node-level durable-peer emission paths (issue #279).
//!
//! - `get_durable_peers` returns connections in registration order so nodes send requests to durable peers in a reproducible order.
//! - `get_durable_peer_random` must draw from the node's seeded RNG, so two nodes seeded
//!   identically produce an identical selection sequence for identical peer sets.
//!
//! These paths are only reachable in multi-durable-peer topologies, which the current smoke
//! scenarios do not exercise; the C1 simulation audit will arm them for C5.

mod common;
use ankurah::core::connector::ConnectionId;
use ankurah::{Node, PermissiveAgent};
use ankurah_storage_sled::SledStorageEngine;
use anyhow::Result;
use std::sync::Arc;

/// Connection numbers for testing durable peer selection without opening connections.
fn seed_peers(node: &Node<SledStorageEngine, PermissiveAgent>, count: usize) -> Vec<ConnectionId> {
    (0..count).map(|_| node.insert_durable_peer_for_test()).collect()
}

/// Path 2: get_durable_peers returns connections in registration order on every call.
///
/// The underlying set does not preserve insertion order, so the returned connections must have the same
/// registration order on every call.
#[tokio::test]
async fn test_get_durable_peers_is_stable_and_sorted() -> Result<()> {
    let node = common::durable_sled_setup().await?;
    let peers = seed_peers(&node, 8);

    let run1 = node.get_durable_peers();
    let run2 = node.get_durable_peers();

    assert_eq!(run1, run2, "get_durable_peers must be identical across calls");
    assert_eq!(run1, peers, "get_durable_peers must be in registration order");

    Ok(())
}

/// Path 3: get_durable_peer_random draws from the node's seeded RNG.
///
/// Two nodes constructed with the same seed and the same peer set must produce an identical
/// sequence of selections. A run with a different seed must (with overwhelming probability over a
/// long sequence and a wide peer set) diverge, proving randomness is preserved, not removed.
#[tokio::test]
async fn test_get_durable_peer_random_same_seed_same_sequence() -> Result<()> {
    // Each test node assigns the same sixteen connection numbers, so identical seeds select from identical inputs.
    let draw_sequence = |seed: u64| -> Vec<ConnectionId> {
        let node = Node::new_durable_with_seed(Arc::new(SledStorageEngine::new_test().unwrap()), PermissiveAgent::new(), seed);
        seed_peers(&node, 16);
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
    let peers = seed_peers(&node, 4);

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
