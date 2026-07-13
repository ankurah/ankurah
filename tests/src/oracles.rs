//! Shared test oracles for the integration suites.
//!
//! Hoisted here (D2 M5 fixture pre-task; flagged by the M4 test-adequacy
//! panel) so every test binary can consume the same brute-force ground
//! truth instead of re-implementing it per file.

use ankurah::proto;
use std::collections::HashMap;

/// Recompute every event's topological level by walking parent edges over the
/// dumped event set: depth(genesis) = 1, depth(e) = 1 + max(depth(parents)).
/// This is the brute-force oracle R-D2-2a compares stamped generations
/// against (plan REV 4 section 3 M2; 266-A defines the level), and the M5
/// generation-vs-depth arm reuses it on honest corpora. The recursion is a
/// transient assertion-time walk over event payloads, not a store (the
/// registry ban permits transient locals only; this map lives for one
/// assertion).
///
/// Panics if a parent event is missing from the set: the oracle needs full
/// lineage, and a hole means the harness dumped a partial history.
pub fn brute_force_depths(events: &[proto::Attested<proto::Event>]) -> HashMap<proto::EventId, u32> {
    let by_id: HashMap<proto::EventId, &proto::Event> = events.iter().map(|e| (e.payload.id(), &e.payload)).collect();

    fn depth(id: &proto::EventId, by_id: &HashMap<proto::EventId, &proto::Event>, memo: &mut HashMap<proto::EventId, u32>) -> u32 {
        if let Some(d) = memo.get(id) {
            return *d;
        }
        let event = by_id.get(id).unwrap_or_else(|| panic!("parent event {id} missing from the dumped set; the oracle needs full lineage"));
        let d = if event.parent.is_empty() {
            1
        } else {
            1 + event.parent.iter().map(|p| depth(p, by_id, memo)).max().expect("nonempty parent clock")
        };
        memo.insert(id.clone(), d);
        d
    }

    let mut memo = HashMap::new();
    for e in events {
        depth(&e.payload.id(), &by_id, &mut memo);
    }
    memo
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah::proto::{Attested, Clock, EntityId, Event, EventId, OperationSet};
    use std::collections::BTreeMap;

    fn event(seed: u8, parents: &[&Event]) -> Event {
        let mut entity_id_bytes = [0u8; 16];
        entity_id_bytes[0] = seed;
        Event {
            entity_id: EntityId::from_bytes(entity_id_bytes),
            model: EntityId::from_bytes([0xEE; 16]),
            operations: OperationSet(BTreeMap::new()),
            parent: Clock::from(parents.iter().map(|p| p.id()).collect::<Vec<EventId>>()),
            generation: Event::generation_from_parents(parents.iter().map(|p| p.generation)),
        }
    }

    /// The oracle computes 266-A levels: genesis 1, child 1 + max(parents),
    /// with the max (not min or sum) over a diamond's two branches.
    #[test]
    fn depths_follow_the_level_recurrence() {
        let g = event(1, &[]);
        let a = event(2, &[&g]); // 2
        let b = event(3, &[&a]); // 3 (the long branch)
        let c = event(4, &[&g]); // 2 (the short branch)
        let m = event(5, &[&b, &c]); // 1 + max(3, 2) = 4

        let all: Vec<Attested<Event>> = [&g, &a, &b, &c, &m].iter().map(|e| Attested::opt((*e).clone(), None)).collect();
        let depths = brute_force_depths(&all);
        assert_eq!(depths[&g.id()], 1);
        assert_eq!(depths[&a.id()], 2);
        assert_eq!(depths[&b.id()], 3);
        assert_eq!(depths[&c.id()], 2);
        assert_eq!(depths[&m.id()], 4);
    }
}
