#![cfg(test)]

use crate::error::RetrievalError;
use crate::event_dag::EventLayer;
use crate::property::backend::lww::LWWBackend;
use crate::property::backend::PropertyBackend;
use crate::retrieval::GetEvents;
use crate::value::Value;

fn prop(name: &str) -> ankurah_proto::PropertyId {
    let mut bytes = [0u8; 32];
    let n = name.as_bytes();
    let len = n.len().min(32);
    bytes[..len].copy_from_slice(&n[..len]);
    ankurah_proto::PropertyId::EntityId(EntityId::from_bytes(bytes))
}

use super::comparison::compare;
use super::relation::AbstractCausalRelation;
use ankurah_proto::{AuthorId, Clock, EntityId, Event, EventId, EventStructureError, OperationSet};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ============================================================================
// TEST INFRASTRUCTURE
// ============================================================================

/// Mock event getter implementing `GetEvents` for comparison tests.
/// Stores events by EventId and returns them on demand.
#[derive(Clone)]
struct MockRetriever {
    events: HashMap<EventId, Event>,
}

impl MockRetriever {
    fn new() -> Self { Self { events: HashMap::new() } }

    fn clock(&self, ids: &[EventId]) -> Clock {
        Clock::from_events(ids.iter().map(|id| self.events.get(id).expect("fixture event must be present")))
    }

    fn add_event(&mut self, event: Event) { self.events.insert(event.id(), event); }
}

#[async_trait]
impl GetEvents for MockRetriever {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        self.events.get(event_id).cloned().ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }

    /// MockRetriever simulates ephemeral storage: events are retrievable via
    /// get_event but not considered durably stored.
    async fn event_stored(&self, _event_id: &EventId) -> Result<bool, RetrievalError> { Ok(false) }
}

/// The body for a synthetic fixture event: a genesis when it has no parents,
/// an update otherwise. The nonce comes from the fixture's seed rather than
/// from entropy, so every fixture id is reproducible.
///
/// These fixtures keep their seed-derived entity ids, so a genesis built here
/// does not satisfy `Event::validate_structure`. Nothing on the paths under
/// test consults it: lineage comparison sees only ids and parent clocks.
fn fixture_body(nonce_seed: &[u8], parent: &Clock, operations: OperationSet) -> ankurah_proto::EventBody {
    let mut nonce = [0u8; 32];
    nonce[..nonce_seed.len()].copy_from_slice(nonce_seed);
    let author = ankurah_proto::AuthorId::Unknown;
    if parent.is_empty() {
        ankurah_proto::EventBody::Genesis { system: None, nonce, timestamp: 0, author, operations }
    } else {
        ankurah_proto::EventBody::Update { nonce, timestamp: 0, author, operations }
    }
}

/// Create a test event with deterministic content-hashed IDs.
/// The seed differentiates events; parent_ids determine the parent clock,
/// and `retriever` must already hold the parent events.
/// Returns the event (call `.id()` on it to get the computed EventId).
fn make_test_event(retriever: &MockRetriever, seed: u8, parent_ids: &[EventId]) -> Event {
    let mut entity_id_bytes = [0u8; 32];
    entity_id_bytes[0] = seed;
    let entity_id = EntityId::from_bytes(entity_id_bytes);

    let parent = retriever.clock(parent_ids);
    Event { entity_id, body: fixture_body(&[seed], &parent, OperationSet::default()), parent }
}

/// Like make_test_event, but over parent events the test holds itself: for a
/// test whose retriever deliberately lacks a parent.
fn make_test_event_over(seed: u8, parents: &[&Event]) -> Event {
    let mut entity_id_bytes = [0u8; 32];
    entity_id_bytes[0] = seed;
    let parent = Clock::from_events(parents.iter().copied());
    Event { entity_id: EntityId::from_bytes(entity_id_bytes), body: fixture_body(&[seed], &parent, OperationSet::default()), parent }
}

/// Like make_test_event but with a two-byte seed, for tests that need a wide
/// or exhaustive seed space (id-ordering searches, randomized DAG generation).
fn make_test_event_u16(retriever: &MockRetriever, seed: u16, parent_ids: &[EventId]) -> Event {
    let mut entity_id_bytes = [0u8; 32];
    entity_id_bytes[0..2].copy_from_slice(&seed.to_be_bytes());
    let entity_id = EntityId::from_bytes(entity_id_bytes);
    let parent = retriever.clock(parent_ids);
    Event { entity_id, body: fixture_body(&seed.to_be_bytes(), &parent, OperationSet::default()), parent }
}

/// Create a test event with LWW operations.
/// Uses a seed to create deterministic but different entity IDs for each event.
fn make_lww_event(seed: u8, properties: Vec<(&str, &str)>) -> Event {
    // Use seed for entity_id to get different event hashes
    let mut entity_id_bytes = [0u8; 32];
    entity_id_bytes[0] = seed;
    let entity_id = EntityId::from_bytes(entity_id_bytes);

    let backend = LWWBackend::new();
    for (name, value) in properties {
        backend.set(prop(name), Some(Value::String(value.into())));
    }
    let ops = backend.to_operations().unwrap().unwrap();
    let parent = Clock::default();
    Event {
        entity_id,
        body: fixture_body(&[seed], &parent, OperationSet::from_backends(BTreeMap::from([("lww".to_string(), ops)]))),
        parent,
    }
}

/// Like make_lww_event but with an explicit parent clock, for multi-layer DAG
/// scenarios; `retriever` must already hold the parent events.
fn make_lww_event_with_parent(retriever: &MockRetriever, seed: u8, properties: Vec<(&str, &str)>, parent_ids: &[EventId]) -> Event {
    let event = make_lww_event(seed, properties);
    let parent = retriever.clock(parent_ids);
    Event { body: fixture_body(&[seed], &parent, event.operations().clone()), parent, ..event }
}

fn layer_from_refs_with_context(already_applied: &[&Event], to_apply: &[&Event], context_events: &[&Event]) -> EventLayer {
    let mut dag = BTreeMap::new();
    for event in already_applied.iter().chain(to_apply.iter()).chain(context_events.iter()) {
        dag.insert(event.id(), event.parent.clone());
    }
    EventLayer::new(already_applied.iter().map(|e| (*e).clone()).collect(), to_apply.iter().map(|e| (*e).clone()).collect(), Arc::new(dag))
}

fn layer_from_refs(already_applied: &[&Event], to_apply: &[&Event]) -> EventLayer {
    layer_from_refs_with_context(already_applied, to_apply, &[])
}

// ============================================================================
// BASIC COMPARISON TESTS
// ============================================================================

#[tokio::test]
async fn test_linear_history() {
    let mut retriever = MockRetriever::new();

    // Create a linear chain: ev1 <- ev2 <- ev3
    let ev1 = make_test_event(&retriever, 1, &[]);
    let id1 = ev1.id();
    retriever.add_event(ev1);

    let ev2 = make_test_event(&retriever, 2, &[id1.clone()]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(&retriever, 3, &[id2]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

    let ancestor = retriever.clock(&[id1.clone()]);
    let descendant = retriever.clock(&[id3.clone()]);

    // descendant descends from ancestor
    let result = compare(retriever.clone(), &descendant, &ancestor, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // ancestor is strictly before descendant (subject is older)
    let result = compare(retriever.clone(), &ancestor, &descendant, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
}

#[tokio::test]
async fn test_concurrent_history() {
    let mut retriever = MockRetriever::new();

    // Example history (arrows represent descendency not reference, which is the reverse direction):
    //      1
    //   ↙  ↓  ↘
    //  2   3   4  - at this point, we have a non-ancestral concurrency (1 is not concurrent)
    //   ↘ ↙ ↘ ↙
    //    5   6    - ancestral concurrency introduced
    //     ↘ ↙
    //      7
    let ev1 = make_test_event(&retriever, 1, &[]);
    let id1 = ev1.id();
    retriever.add_event(ev1);

    let ev2 = make_test_event(&retriever, 2, &[id1.clone()]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(&retriever, 3, &[id1.clone()]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

    let ev4 = make_test_event(&retriever, 4, &[id1.clone()]);
    let id4 = ev4.id();
    retriever.add_event(ev4);

    let ev5 = make_test_event(&retriever, 5, &[id2.clone(), id3.clone()]);
    let id5 = ev5.id();
    retriever.add_event(ev5);

    let ev6 = make_test_event(&retriever, 6, &[id3.clone(), id4]);
    let id6 = ev6.id();
    retriever.add_event(ev6);

    let ev7 = make_test_event(&retriever, 7, &[id5.clone(), id6.clone()]);
    let _id7 = ev7.id();
    retriever.add_event(ev7);

    {
        // concurrency in lineage *between* clocks, but the descendant clock fully descends from the ancestor clock
        let ancestor = retriever.clock(&[id1.clone()]);
        let descendant = retriever.clock(&[id5.clone()]);
        let result = compare(retriever.clone(), &descendant, &ancestor, 100).await.unwrap();
        assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));
        // ancestor is strictly before descendant
        let result = compare(retriever.clone(), &ancestor, &descendant, 100).await.unwrap();
        assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
    }
    {
        // this ancestor clock has internal concurrency, but is fully descended by the descendant clock
        let ancestor = retriever.clock(&[id2.clone(), id3.clone()]);
        let descendant = retriever.clock(&[id5.clone()]);

        let result = compare(retriever.clone(), &descendant, &ancestor, 100).await.unwrap();
        assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));
        // ancestor is strictly before descendant
        let result = compare(retriever.clone(), &ancestor, &descendant, 100).await.unwrap();
        assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
    }

    {
        // a and b are fully concurrent, but still comparable
        let a = retriever.clock(&[id2.clone()]);
        let b = retriever.clock(&[id3.clone()]);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id1.clone()]
        ));
        let result = compare(retriever.clone(), &b, &a, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id1.clone()]
        ));
    }

    {
        // a partially descends from b, but b has a component that is not in a
        let a = retriever.clock(&[id6.clone()]);
        let b = retriever.clock(&[id2.clone(), id3.clone()]);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id3.clone()]
        ));
    }
}

#[tokio::test]
async fn test_incomparable() {
    let mut retriever = MockRetriever::new();

    //   1        6
    //   ↓  ↘     ↓
    //   2   4    7
    //   ↓   ↓    ↓
    //   3   5    8
    let ev1 = make_test_event(&retriever, 1, &[]);
    let id1 = ev1.id();
    retriever.add_event(ev1);

    let ev2 = make_test_event(&retriever, 2, &[id1.clone()]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(&retriever, 3, &[id2.clone()]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

    let ev4 = make_test_event(&retriever, 4, &[id1.clone()]);
    let _id4 = ev4.id();
    retriever.add_event(ev4);

    let ev5 = make_test_event(&retriever, 5, &[_id4]);
    let id5 = ev5.id();
    retriever.add_event(ev5);

    // 6 is an unrelated root event
    let ev6 = make_test_event(&retriever, 6, &[]);
    let id6 = ev6.id();
    retriever.add_event(ev6);

    let ev7 = make_test_event(&retriever, 7, &[id6.clone()]);
    let id7 = ev7.id();
    retriever.add_event(ev7);

    let ev8 = make_test_event(&retriever, 8, &[id7]);
    let id8 = ev8.id();
    retriever.add_event(ev8);

    {
        // fully incomparable (different roots) - properly returns Disjoint
        let a = retriever.clock(&[id3.clone()]);
        let b = retriever.clock(&[id8.clone()]);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::Disjoint { ref subject_root, ref other_root, .. }
                if *subject_root == id1 && *other_root == id6
        ));
    }
    {
        // fully incomparable (just a different tier) - properly returns Disjoint
        let a = retriever.clock(&[id2.clone()]);
        let b = retriever.clock(&[id8.clone()]);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::Disjoint { ref subject_root, ref other_root, .. }
                if *subject_root == id1 && *other_root == id6
        ));
    }
    {
        // partially incomparable: [3] shares root 1 with [5], but [8] has different root 6
        let a = retriever.clock(&[id3.clone()]);
        let b = retriever.clock(&[id5.clone(), id8.clone()]);
        let result = compare(retriever.clone(), &a, &b, 100).await.unwrap();
        assert!(matches!(
            result.relation,
            AbstractCausalRelation::DivergedSince { ref meet, .. } if meet.is_empty()
        ));
    }
}

#[tokio::test]
async fn equal_heads_reject_conflicting_generations_without_reads() {
    let (a, b) = (EventId::from_bytes([1; 32]), EventId::from_bytes([2; 32]));
    let comparison = Clock::new(vec![(2, a.clone()), (7, b.clone())]).unwrap();
    // No events are available. Check even a non-first tip before returning Equal.
    for claimed in [6, 8] {
        let subject = Clock::new(vec![(2, a.clone()), (claimed, b.clone())]).unwrap();
        assert!(matches!(
            compare(MockRetriever::new(), &subject, &comparison, 100).await,
            Err(RetrievalError::EventStructure(EventStructureError::GenerationMismatch { event, claimed: c, known: 7 }))
                if event == b && c == claimed
        ));
    }
    assert_eq!(compare(MockRetriever::new(), &comparison, &comparison, 100).await.unwrap().relation, AbstractCausalRelation::Equal);
}

#[tokio::test]
async fn direct_descendant_checks_subject_tip_generations() {
    let mut retriever = MockRetriever::new();
    let root = make_test_event(&retriever, 1, &[]);
    retriever.add_event(root.clone());
    let left = make_test_event(&retriever, 2, &[root.id()]);
    let right = make_test_event(&retriever, 3, &[root.id()]);
    retriever.add_event(left.clone());
    retriever.add_event(right.clone());
    let subject = Clock::new(vec![(left.generation(), left.id()), (99, right.id())]).unwrap();
    assert!(matches!(
        compare(retriever, &subject, &Clock::singleton(&root), 100).await,
        Err(RetrievalError::EventStructure(EventStructureError::GenerationMismatch { event, claimed: 99, known: 2 }))
            if event == right.id()
    ));
}

#[tokio::test]
async fn ancestor_checks_subject_tip_generations() {
    let mut retriever = MockRetriever::new();
    let root = make_test_event(&retriever, 1, &[]);
    retriever.add_event(root.clone());
    let child = make_test_event(&retriever, 2, &[root.id()]);
    retriever.add_event(child.clone());
    let comparison = Clock::singleton(&child);
    assert_eq!(
        compare(retriever.clone(), &Clock::singleton(&root), &comparison, 100).await.unwrap().relation,
        AbstractCausalRelation::StrictAscends
    );
    let subject = Clock::new(vec![(99, root.id())]).unwrap();
    assert!(matches!(
        compare(retriever, &subject, &comparison, 100).await,
        Err(RetrievalError::EventStructure(EventStructureError::GenerationMismatch { event, claimed: 99, known: 1 }))
            if event == root.id()
    ));
}

#[tokio::test]
async fn direct_descendant_checks_each_parent_without_fetching_it() {
    let (older, newer) = (EventId::from_bytes([1; 32]), EventId::from_bytes([2; 32]));
    let comparison = Clock::new(vec![(2, older.clone()), (3, newer.clone())]).unwrap();
    // Both false claims leave the child's generation at 4. Neither parent is retrievable.
    for claimed in [1, 3] {
        let parent = Clock::new(vec![(claimed, older.clone()), (3, newer.clone())]).unwrap();
        let event = Event::update(EntityId::from_bytes([3; 32]), parent, AuthorId::Unknown, OperationSet::default());
        assert_eq!(event.generation(), 4);
        let subject = Clock::singleton(&event);
        let mut retriever = MockRetriever::new();
        retriever.add_event(event);
        assert!(matches!(
            compare(retriever, &subject, &comparison, 100).await,
            Err(RetrievalError::EventStructure(EventStructureError::GenerationMismatch { event, claimed: c, known: 2 }))
                if event == older && c == claimed
        ));
    }
}

#[tokio::test]
async fn comparison_rejects_false_generation_claims_in_read_ancestors() {
    let mut retriever = MockRetriever::new();
    let root = make_test_event(&retriever, 1, &[]);
    retriever.add_event(root.clone());
    let forged = Event::update(root.entity_id, Clock::new(vec![(9, root.id())]).unwrap(), AuthorId::Unknown, OperationSet::default());
    retriever.add_event(forged.clone());
    let tip = make_test_event(&retriever, 3, &[forged.id()]);
    retriever.add_event(tip.clone());
    // The tip correctly reports its parent's generation. The false claim is one event farther back.
    assert!(matches!(
        compare(retriever, &Clock::singleton(&tip), &Clock::singleton(&root), 100).await,
        Err(RetrievalError::EventStructure(EventStructureError::GenerationMismatch { event, claimed: 9, known: 1 }))
            if event == root.id()
    ));
}

#[tokio::test]
async fn diverged_comparison_checks_available_parent_generations() {
    let mut retriever = MockRetriever::new();
    let root = make_test_event(&retriever, 1, &[]);
    retriever.add_event(root.clone());
    let child = make_test_event(&retriever, 2, &[root.id()]);
    retriever.add_event(child.clone());
    let forged = Event::update(root.entity_id, Clock::new(vec![(9, root.id())]).unwrap(), AuthorId::Unknown, OperationSet::default());
    retriever.add_event(forged.clone());
    let subject = Clock::singleton(&forged);
    let comparison = Clock::singleton(&child);
    assert!(matches!(
        compare(retriever.clone(), &subject, &comparison, 100).await,
        Err(RetrievalError::EventStructure(EventStructureError::GenerationMismatch { event, claimed: 9, known: 1 }))
            if event == root.id()
    ));

    // With no parent event and matching claims on both sides, its generation remains unverified.
    retriever.events.remove(&root.id());
    let other = Event::update(root.entity_id, forged.parent.clone(), AuthorId::Unknown, OperationSet::default());
    let comparison = Clock::singleton(&other);
    retriever.add_event(other);
    assert!(matches!(compare(retriever, &subject, &comparison, 100).await.unwrap().relation, AbstractCausalRelation::DivergedSince { .. }));
}

#[tokio::test]
async fn annotated_multitip_head_keeps_the_direct_descendant_fast_path() {
    let (a, b) = (EventId::from_bytes([1; 32]), EventId::from_bytes([2; 32]));
    // Generations decrease in ID order.
    let parents = Clock::new(vec![(7, a), (2, b)]).unwrap();
    let event = Event::update(EntityId::from_bytes([3; 32]), parents.clone(), ankurah_proto::AuthorId::Unknown, OperationSet::default());
    let subject = Clock::singleton(&event);
    let expected = AbstractCausalRelation::StrictDescends { chain: vec![event.id()] };
    let mut retriever = MockRetriever::new();
    retriever.add_event(event);
    // Only the child is available; the fast path must not fetch either parent.
    assert_eq!(compare(retriever.clone(), &subject, &parents, 100).await.unwrap().relation, expected);
}

#[tokio::test]
async fn test_empty_clocks() {
    let mut retriever = MockRetriever::new();

    let ev1 = make_test_event(&retriever, 1, &[]);
    let id1 = ev1.id();
    retriever.add_event(ev1);

    let empty = Clock::default();
    let non_empty = retriever.clock(&[id1.clone()]);

    // Two empty clocks are the same clock: Equal, not diverged.
    let result = compare(retriever.clone(), &empty, &empty, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::Equal);

    // One-sided empty clocks share no history: diverged with an empty meet.
    let expected =
        AbstractCausalRelation::DivergedSince { meet: vec![], subject: vec![], other: vec![], subject_chain: vec![], other_chain: vec![] };
    let result = compare(retriever.clone(), &non_empty, &empty, 100).await.unwrap();
    assert_eq!(result.relation, expected);
    let result = compare(retriever.clone(), &empty, &non_empty, 100).await.unwrap();
    assert_eq!(result.relation, expected);
}

#[tokio::test]
async fn test_budget_exceeded() {
    let mut retriever = MockRetriever::new();

    // Create a long chain: ev1 <- ev2 <- ... <- ev20
    // With budget=1, max_budget=4, a chain of 20 events will still exceed
    let mut ids: Vec<EventId> = Vec::new();
    for i in 0..20u8 {
        let parents = if i == 0 { vec![] } else { vec![ids[i as usize - 1].clone()] };
        let ev = make_test_event(&retriever, i + 1, &parents);
        ids.push(ev.id());
        retriever.add_event(ev);
    }

    let ancestor = retriever.clock(&[ids[0].clone()]);
    let descendant = retriever.clock(&[ids[19].clone()]);

    // With budget=1 (escalates to max 4), a 20-long chain will exceed
    let result = compare(retriever.clone(), &descendant, &ancestor, 1).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::BudgetExceeded { .. }));

    // With high budget, should resolve
    let result = compare(retriever.clone(), &descendant, &ancestor, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // ancestor is strictly before descendant - determined quickly because
    // expanding descendant immediately walks backward toward ancestor
    let result = compare(retriever.clone(), &ancestor, &descendant, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
}

#[tokio::test]
async fn test_self_comparison() {
    let mut retriever = MockRetriever::new();

    // Create a simple event to compare with itself
    let ev1 = make_test_event(&retriever, 1, &[]);
    let id1 = ev1.id();
    retriever.add_event(ev1);

    let clock = retriever.clock(&[id1.clone()]);

    // A clock does NOT descend itself
    let result = compare(retriever.clone(), &clock, &clock, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::Equal);
}

#[tokio::test]
async fn multiple_roots() {
    let mut retriever = MockRetriever::new();

    // Six independent roots
    let mut root_ids = Vec::new();
    for i in 1..=6u8 {
        let ev = make_test_event(&retriever, i, &[]);
        root_ids.push(ev.id());
        retriever.add_event(ev);
    }

    // merge-point 7 references all six heads
    let ev7 = make_test_event(&retriever, 7, &root_ids);
    let id7 = ev7.id();
    retriever.add_event(ev7);

    // subject head 8 descends only from 7
    let ev8 = make_test_event(&retriever, 8, &[id7]);
    let id8 = ev8.id();
    retriever.add_event(ev8);

    let subject = retriever.clock(&[id8.clone()]);
    let big_other = retriever.clock(&root_ids);

    // 8 descends from all heads in big_other via 7
    let result = compare(retriever.clone(), &subject, &big_other, 1_000).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // In the opposite direction, big_other is strictly before subject
    let result = compare(retriever.clone(), &big_other, &subject, 1_000).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
}

#[tokio::test]
async fn test_compare_event_unstored() {
    let mut retriever = MockRetriever::new();

    // Create a chain: ev1 <- ev2 <- ev3 (stored)
    let ev1 = make_test_event(&retriever, 1, &[]);
    let id1 = ev1.id();
    retriever.add_event(ev1);

    let ev2 = make_test_event(&retriever, 2, &[id1.clone()]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(&retriever, 3, &[id2.clone()]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

    // Create an unstored event that would descend from ev3
    let unstored_event = make_test_event(&retriever, 4, &[id3.clone()]);

    let clock_1 = retriever.clock(&[id1.clone()]);
    let clock_2 = retriever.clock(&[id2.clone()]);
    let clock_3 = retriever.clock(&[id3.clone()]);

    // Stage the unstored event so compare can find it
    retriever.add_event(unstored_event.clone());

    // The unstored event should descend from all ancestors
    let result = compare(retriever.clone(), &Clock::singleton(&unstored_event), &clock_1, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    let result = compare(retriever.clone(), &Clock::singleton(&unstored_event), &clock_2, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    let result = compare(retriever.clone(), &Clock::singleton(&unstored_event), &clock_3, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // Test with an unstored event that has multiple parents
    let unstored_merge_event = make_test_event(&retriever, 5, &[id2.clone(), id3.clone()]);
    retriever.add_event(unstored_merge_event.clone());

    let result = compare(retriever.clone(), &Clock::singleton(&unstored_merge_event), &clock_1, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // Test with an incomparable case - different roots should return Disjoint
    let ev10 = make_test_event(&retriever, 10, &[]); // Independent root
    let id10 = ev10.id();
    retriever.add_event(ev10);
    let incomparable_clock = retriever.clock(&[id10.clone()]);

    let result = compare(retriever.clone(), &Clock::singleton(&unstored_event), &incomparable_clock, 100).await.unwrap();
    assert!(matches!(
        result.relation,
        AbstractCausalRelation::Disjoint { ref subject_root, ref other_root, .. }
            if *subject_root == id1 && *other_root == id10
    ));

    // Test root event case
    let root_event = make_test_event(&retriever, 11, &[]);
    retriever.add_event(root_event.clone());

    let empty_clock = Clock::default();
    let result = compare(retriever.clone(), &Clock::singleton(&root_event), &empty_clock, 100).await.unwrap();
    assert!(matches!(
        result.relation,
        AbstractCausalRelation::DivergedSince { ref meet, .. } if meet.is_empty()
    ));

    let result = compare(retriever.clone(), &Clock::singleton(&root_event), &clock_1, 100).await.unwrap();
    // Two independent root events with different lineages should be Disjoint
    assert!(matches!(result.relation, AbstractCausalRelation::Disjoint { .. }));

    // Test that a non-empty unstored event does not descend from an empty clock
    let empty_clock = Clock::default();
    let result = compare(retriever.clone(), &Clock::singleton(&unstored_event), &empty_clock, 100).await.unwrap();
    assert!(matches!(
        result.relation,
        AbstractCausalRelation::DivergedSince { ref meet, .. } if meet.is_empty()
    ));
}

#[tokio::test]
async fn test_compare_event_redundant_delivery() {
    let mut retriever = MockRetriever::new();

    // Create a chain: ev1 <- ev2 <- ev3 (stored)
    let ev1 = make_test_event(&retriever, 1, &[]);
    let id1 = ev1.id();
    retriever.add_event(ev1);

    let ev2 = make_test_event(&retriever, 2, &[id1]);
    let id2 = ev2.id();
    retriever.add_event(ev2);

    let ev3 = make_test_event(&retriever, 3, &[id2]);
    let id3 = ev3.id();
    retriever.add_event(ev3);

    // Create an unstored event that would descend from ev3
    let unstored_event = make_test_event(&retriever, 4, &[id3.clone()]);
    let id4 = unstored_event.id();

    // Stage the unstored event so compare can find it
    retriever.add_event(unstored_event.clone());

    // Test the normal case first
    let clock_3 = retriever.clock(&[id3.clone()]);
    let result = compare(retriever.clone(), &Clock::singleton(&unstored_event), &clock_3, 100).await.unwrap();
    assert!(matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }));

    // Now store event 4 to simulate it being applied
    let ev4_stored = make_test_event(&retriever, 4, &[id3.clone()]);
    retriever.add_event(ev4_stored);

    // Test redundant delivery: the event is already in the clock (exact match)
    let clock_with_event = retriever.clock(&[id4.clone()]);
    // The equality check should catch this case and return Equal
    let result = compare(retriever.clone(), &Clock::singleton(&unstored_event), &clock_with_event, 100).await.unwrap();
    assert_eq!(result.relation, AbstractCausalRelation::Equal);

    // Test case where the event is in the clock but with other events too
    let clock_with_multiple = retriever.clock(&[id3.clone(), id4.clone()]);
    // Event 4 is already in the head [3, 4], so this is redundant delivery.
    // Since id4 descends from id3, comparing [id4] vs [id3, id4] may return
    // StrictDescends (id4 covers all of [id3, id4]) rather than Equal.
    // Either way, it's NOT DivergedSince, so the caller knows it's a no-op.
    let result = compare(retriever.clone(), &Clock::singleton(&unstored_event), &clock_with_multiple, 100).await.unwrap();
    assert!(
        matches!(result.relation, AbstractCausalRelation::Equal | AbstractCausalRelation::StrictDescends { .. }),
        "Redundant delivery with multiple heads should return Equal or StrictDescends, got {:?}",
        result.relation
    );
}

// ============================================================================
// MISSING EVENT BUSYLOOP TESTS
// ============================================================================

/// Verifies that compare() returns an error when a frontier event can't be fetched.
///
/// Setup: A (creation, in retriever) -> B (child of A, NOT in retriever) -> C (child of B, in retriever).
/// compare(subject=[C], comparison=[A]) starts BFS. It fetches C (ok, parents=[B]),
/// adds B to subject frontier. Tries to fetch B -> EventNotFound -> error.
///
/// Previously this was a busyloop because EventNotFound did `continue`, skipping
/// process_event (the only place that removes IDs from frontiers). Now it errors.
#[tokio::test]
async fn test_missing_event_busyloop() {
    let mut retriever = MockRetriever::new();

    // A: creation event, in retriever
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    // B: child of A, deliberately NOT added to retriever (missing event)
    let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
    let id_b = ev_b.id();
    // Do NOT add ev_b to retriever

    // C: child of B, in retriever
    let ev_c = make_test_event_over(3, &[&ev_b]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    // compare(subject=[C], comparison=[A]):
    // BFS fetches C (ok), adds parent B to subject frontier.
    // BFS fetches A (ok, it's comparison frontier).
    // BFS tries to fetch B -> EventNotFound -> returns error
    let result = compare(retriever.clone(), &retriever.clock(&[id_c.clone()]), &retriever.clock(&[id_a.clone()]), 100).await;

    let err = result.err().expect("Expected EventNotFound error for missing event B, but got Ok");
    assert!(
        matches!(err, RetrievalError::EventNotFound(ref id) if *id == id_b),
        "Error should be EventNotFound for event B, got {:?}",
        err
    );
}

/// Siblings B and C both point to unavailable A. Once both frontiers reach A,
/// comparison can identify it as their meet without needing its payload.
#[tokio::test]
async fn test_both_frontiers_unfetchable_meet_point() {
    let mut retriever = MockRetriever::new();

    // A: creation event, deliberately NOT added to retriever
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    // Do NOT add ev_a to retriever

    // B: child of A, in retriever
    let ev_b = make_test_event_over(2, &[&ev_a]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    // C: child of A, in retriever
    let ev_c = make_test_event_over(3, &[&ev_a]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    // compare(subject=[B], comparison=[C])
    // BFS will walk B and C back to A, which is on both frontiers but unfetchable.
    // The both-frontiers optimization should process A as common ancestor with empty parents.
    let result = compare(retriever.clone(), &retriever.clock(&[id_b.clone()]), &retriever.clock(&[id_c.clone()]), 100).await;

    let result = result.expect("Should succeed because A is on both frontiers and processed as common ancestor");
    assert!(
        matches!(result.relation, AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id_a.clone()]),
        "Should find meet at A (the unfetchable common ancestor), got {:?}",
        result.relation
    );
}

// ============================================================================
// MULTI-HEAD BUG FIX TESTS (Phase 2A)
// ============================================================================

#[tokio::test]
async fn test_multihead_event_extends_one_tip() {
    let mut retriever = MockRetriever::new();

    // Create:
    //     A
    //    / \
    //   B   C   <- entity head is [B, C]
    //       |
    //       D   <- incoming event with parent [C]
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(&retriever, 3, &[id_a]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    // Event D extends C
    let event_d = make_test_event(&retriever, 4, &[id_c.clone()]);

    // Entity head is [B, C]
    let entity_head = retriever.clock(&[id_b.clone(), id_c.clone()]);

    // Stage event D so compare can find it
    retriever.add_event(event_d.clone());

    // D should NOT return StrictAscends - it should be DivergedSince
    // because D extends C which is a tip, and is concurrent with B
    let result = compare(retriever.clone(), &Clock::singleton(&event_d), &entity_head, 100).await.unwrap();

    // The meet should be [C]; B is concurrent (not a child of meet C) so
    // it appears in other_chain rather than the immediate-children `other` field.
    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    // Verify the meet field
    if let AbstractCausalRelation::DivergedSince { meet, .. } = &result.relation {
        assert_eq!(meet, &vec![id_c], "Meet should be [C]");
    }
}

#[tokio::test]
async fn test_multihead_event_extends_multiple_tips() {
    let mut retriever = MockRetriever::new();

    // Create:
    //     A
    //    / \
    //   B   C   <- entity head is [B, C]
    //    \ /
    //     D   <- incoming event with parent [B, C]
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(&retriever, 3, &[id_a]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    // Event D merges both tips
    let event_d = make_test_event(&retriever, 4, &[id_b.clone(), id_c.clone()]);

    // Entity head is [B, C]
    let entity_head = retriever.clock(&[id_b.clone(), id_c.clone()]);

    // Stage event D so compare can find it
    retriever.add_event(event_d.clone());

    // D's parent equals entity head, so this should be StrictDescends
    let result = compare(retriever.clone(), &Clock::singleton(&event_d), &entity_head, 100).await.unwrap();

    assert!(
        matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }),
        "Expected StrictDescends when event merges all tips, got {:?}",
        result.relation
    );
}

#[tokio::test]
async fn test_multihead_three_way_concurrency() {
    let mut retriever = MockRetriever::new();

    // Create:
    //       A
    //      /|\
    //     B C D   <- entity head is [B, C, D]
    //     |
    //     E       <- incoming event with parent [B]
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(&retriever, 3, &[id_a.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(&retriever, 4, &[id_a]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    // Event E extends only B
    let event_e = make_test_event(&retriever, 5, &[id_b.clone()]);

    // Entity head is [B, C, D]
    let entity_head = retriever.clock(&[id_b.clone(), id_c.clone(), id_d.clone()]);

    // Stage event E so compare can find it
    retriever.add_event(event_e.clone());

    let result = compare(retriever.clone(), &Clock::singleton(&event_e), &entity_head, 100).await.unwrap();

    // Should be DivergedSince because E extends B but is concurrent with C and D
    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    if let AbstractCausalRelation::DivergedSince { meet, .. } = &result.relation {
        assert_eq!(meet, &vec![id_b], "Meet should be [B]");
        // C and D are siblings of B (children of A), not children of meet B,
        // so they appear in other_chain rather than the immediate `other` field.
    }
}

// ============================================================================
// DEEP CONCURRENCY TESTS (Phase 2G)
// ============================================================================

#[tokio::test]
async fn test_deep_diamond_asymmetric_branches() {
    let mut retriever = MockRetriever::new();

    // Create asymmetric diamond:
    //       A
    //      / \
    //     B   C
    //     |   |
    //     D   E
    //     |   |
    //     F   G
    //     |   |
    //     H   I
    //
    // Compare H vs I - meet should be A
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(&retriever, 3, &[id_a.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(&retriever, 4, &[id_b]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    let ev_e = make_test_event(&retriever, 5, &[id_c]);
    let id_e = ev_e.id();
    retriever.add_event(ev_e);

    let ev_f = make_test_event(&retriever, 6, &[id_d]);
    let id_f = ev_f.id();
    retriever.add_event(ev_f);

    let ev_g = make_test_event(&retriever, 7, &[id_e]);
    let id_g = ev_g.id();
    retriever.add_event(ev_g);

    let ev_h = make_test_event(&retriever, 8, &[id_f]);
    let id_h = ev_h.id();
    retriever.add_event(ev_h);

    let ev_i = make_test_event(&retriever, 9, &[id_g]);
    let id_i = ev_i.id();
    retriever.add_event(ev_i);

    let clock_h = retriever.clock(&[id_h.clone()]);
    let clock_i = retriever.clock(&[id_i.clone()]);

    let result = compare(retriever.clone(), &clock_h, &clock_i, 100).await.unwrap();

    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    if let AbstractCausalRelation::DivergedSince { meet, subject_chain, other_chain, .. } = &result.relation {
        assert_eq!(meet, &vec![id_a], "Meet should be A");
        // subject_chain should go from A to H: B, D, F, H
        assert_eq!(subject_chain.len(), 4, "Subject chain should have 4 events");
        // other_chain should go from A to I: C, E, G, I
        assert_eq!(other_chain.len(), 4, "Other chain should have 4 events");
    }
}

#[tokio::test]
async fn test_short_branch_from_deep_point() {
    let mut retriever = MockRetriever::new();

    // Create:
    // A -> B -> C -> D -> E -> F -> G -> H (long chain, head)
    //             |
    //             X -> Y (short branch from D, arrives late)
    //
    // Compare Y vs H - meet should be D, NOT genesis A!
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(&retriever, 2, &[id_a]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(&retriever, 3, &[id_b]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(&retriever, 4, &[id_c]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    let ev_e = make_test_event(&retriever, 5, &[id_d.clone()]);
    let id_e = ev_e.id();
    retriever.add_event(ev_e);

    let ev_f = make_test_event(&retriever, 6, &[id_e]);
    let id_f = ev_f.id();
    retriever.add_event(ev_f);

    let ev_g = make_test_event(&retriever, 7, &[id_f]);
    let id_g = ev_g.id();
    retriever.add_event(ev_g);

    let ev_h = make_test_event(&retriever, 8, &[id_g]);
    let id_h = ev_h.id();
    retriever.add_event(ev_h);

    // Short branch from D
    let ev_x = make_test_event(&retriever, 9, &[id_d]); // X (parent is D)
    let id_x = ev_x.id();
    retriever.add_event(ev_x);

    let ev_y = make_test_event(&retriever, 10, &[id_x]); // Y
    let _id_y = ev_y.id();

    let clock_h = retriever.clock(&[id_h.clone()]);

    // Stage event Y so compare can find it
    retriever.add_event(ev_y.clone());

    // Event Y arrives late, with parent X
    let result = compare(retriever.clone(), &Clock::singleton(&ev_y), &clock_h, 100).await.unwrap();

    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    if let AbstractCausalRelation::DivergedSince { meet, subject_chain, other_chain, .. } = &result.relation {
        // Meet should not be empty
        assert!(!meet.is_empty(), "Meet should not be empty");
        // Subject chain: just Y (since we're comparing unstored event)
        assert!(!subject_chain.is_empty(), "Subject chain should not be empty");
        // Other chain: events from meet to head H
        assert!(!other_chain.is_empty(), "Other chain should not be empty");
    }
}

#[tokio::test]
async fn test_late_arrival_long_branch_from_genesis() {
    let mut retriever = MockRetriever::new();

    // Create:
    //     A (genesis)
    //    / \
    //   B   X (long branch arrives late)
    //   |   |
    //   C   Y
    //   |   |
    //   D   Z
    //
    // Entity at [D], then X->Y->Z branch arrives
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(&retriever, 3, &[id_b]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(&retriever, 4, &[id_c]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    // Long branch from genesis
    let ev_x = make_test_event(&retriever, 5, &[id_a.clone()]);
    let id_x = ev_x.id();
    retriever.add_event(ev_x);

    let ev_y = make_test_event(&retriever, 6, &[id_x]);
    let id_y = ev_y.id();
    retriever.add_event(ev_y);

    let ev_z = make_test_event(&retriever, 7, &[id_y]);
    let id_z = ev_z.id();
    retriever.add_event(ev_z);

    let clock_d = retriever.clock(&[id_d.clone()]);
    let clock_z = retriever.clock(&[id_z.clone()]);

    let result = compare(retriever.clone(), &clock_d, &clock_z, 100).await.unwrap();

    assert!(matches!(result.relation, AbstractCausalRelation::DivergedSince { .. }), "Expected DivergedSince, got {:?}", result.relation);

    if let AbstractCausalRelation::DivergedSince { meet, subject_chain, other_chain, .. } = &result.relation {
        assert_eq!(meet, &vec![id_a], "Meet should be A (genesis)");
        // Both chains should have 3 events
        assert_eq!(subject_chain.len(), 3, "Subject chain B, C, D should have 3 events");
        assert_eq!(other_chain.len(), 3, "Other chain X, Y, Z should have 3 events");
    }
}

// ============================================================================
// CHAIN ORDERING TESTS (Phase 2G)
// ============================================================================

#[tokio::test]
async fn test_forward_chain_ordering() {
    let mut retriever = MockRetriever::new();

    // Create: A -> B -> C -> D -> E
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(&retriever, 3, &[id_b.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(&retriever, 4, &[id_c.clone()]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    let ev_e = make_test_event(&retriever, 5, &[id_d.clone()]);
    let id_e = ev_e.id();
    retriever.add_event(ev_e);

    let clock_a = retriever.clock(&[id_a.clone()]);
    let clock_e = retriever.clock(&[id_e.clone()]);

    let result = compare(retriever.clone(), &clock_e, &clock_a, 100).await.unwrap();

    // E strictly descends from A
    if let AbstractCausalRelation::StrictDescends { chain } = &result.relation {
        // Chain should be in forward (causal) order: B, C, D, E (from A toward E)
        assert_eq!(chain.len(), 4, "Chain should have 4 events");
        assert_eq!(chain, &vec![id_b, id_c, id_d, id_e], "Chain should be [B, C, D, E] in causal order");
    } else {
        panic!("Expected StrictDescends, got {:?}", result.relation);
    }
}

#[tokio::test]
async fn test_diverged_chains_ordering() {
    let mut retriever = MockRetriever::new();

    // Create:
    //     A
    //    / \
    //   B   C
    //   |   |
    //   D   E
    //
    // Compare D vs E - both chains should be in forward order from meet A
    let ev_a = make_test_event(&retriever, 1, &[]);
    let id_a = ev_a.id();
    retriever.add_event(ev_a);

    let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
    let id_b = ev_b.id();
    retriever.add_event(ev_b);

    let ev_c = make_test_event(&retriever, 3, &[id_a.clone()]);
    let id_c = ev_c.id();
    retriever.add_event(ev_c);

    let ev_d = make_test_event(&retriever, 4, &[id_b.clone()]);
    let id_d = ev_d.id();
    retriever.add_event(ev_d);

    let ev_e = make_test_event(&retriever, 5, &[id_c.clone()]);
    let id_e = ev_e.id();
    retriever.add_event(ev_e);

    let clock_d = retriever.clock(&[id_d.clone()]);
    let clock_e = retriever.clock(&[id_e.clone()]);

    let result = compare(retriever.clone(), &clock_d, &clock_e, 100).await.unwrap();

    if let AbstractCausalRelation::DivergedSince { meet, subject_chain, other_chain, .. } = &result.relation {
        assert_eq!(meet, &vec![id_a], "Meet should be A");
        // Subject chain: B, D (from A toward D)
        assert_eq!(subject_chain, &vec![id_b, id_d], "Subject chain should be [B, D]");
        // Other chain: C, E (from A toward E)
        assert_eq!(other_chain, &vec![id_c, id_e], "Other chain should be [C, E]");
    } else {
        panic!("Expected DivergedSince, got {:?}", result.relation);
    }
}

// ============================================================================
// IDEMPOTENCY TESTS (Phase 2G)
// ============================================================================

// ============================================================================
// LWW LAYER APPLICATION TESTS (Phase 3)
// ============================================================================

#[cfg(test)]
mod lww_layer_tests {
    use super::*;

    #[test]
    fn test_apply_layer_higher_event_id_wins() {
        let backend = LWWBackend::new();

        // Create two concurrent events with same property but different seeds
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);

        // Determine which has higher ID (computed from content hash)
        let (winner_value, _loser_value) = if event_a.id() > event_b.id() { ("value_a", "value_b") } else { ("value_b", "value_a") };

        // Apply layer where both events are in to_apply
        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&event_a, &event_b];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Higher event_id should win
        assert_eq!(backend.get(&prop("x")), Some(Value::String(winner_value.into())));
    }

    #[test]
    fn test_apply_layer_already_applied_wins() {
        let backend = LWWBackend::new();

        // Create two concurrent events
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);

        // Determine which has higher ID
        let (winner_event, loser_event, _winner_value) =
            if event_a.id() > event_b.id() { (&event_a, &event_b, "value_a") } else { (&event_b, &event_a, "value_b") };

        // Put winner in already_applied, loser in to_apply
        let already_applied: Vec<&Event> = vec![winner_event];
        let to_apply: Vec<&Event> = vec![loser_event];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Nothing should be set because winner is in already_applied
        // (already_applied values are already in state, we don't mutate for them)
        assert_eq!(backend.get(&prop("x")), None);
    }

    #[test]
    fn test_apply_layer_to_apply_overwrites_already_applied() {
        let backend = LWWBackend::new();

        // Create two concurrent events
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);

        // Determine which has higher ID
        let (winner_event, loser_event, winner_value) =
            if event_a.id() > event_b.id() { (&event_a, &event_b, "value_a") } else { (&event_b, &event_a, "value_b") };

        // Put loser in already_applied, winner in to_apply
        let already_applied: Vec<&Event> = vec![loser_event];
        let to_apply: Vec<&Event> = vec![winner_event];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // to_apply event should win and be applied
        assert_eq!(backend.get(&prop("x")), Some(Value::String(winner_value.into())));
    }

    #[test]
    fn test_apply_layer_multiple_properties() {
        let backend = LWWBackend::new();

        // Event A sets x only
        let event_a = make_lww_event(1, vec![("x", "x_from_a")]);
        // Event B sets x and y
        let event_b = make_lww_event(2, vec![("x", "x_from_b"), ("y", "y_from_b")]);

        // Apply layer with both in to_apply
        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&event_a, &event_b];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // x: winner is determined by higher event ID
        let expected_x = if event_a.id() > event_b.id() { "x_from_a" } else { "x_from_b" };
        assert_eq!(backend.get(&prop("x")), Some(Value::String(expected_x.into())));

        // y: only event_b sets it, so it wins
        assert_eq!(backend.get(&prop("y")), Some(Value::String("y_from_b".into())));
    }

    #[test]
    fn test_apply_layer_three_way_concurrency() {
        let backend = LWWBackend::new();

        // Three concurrent events all setting same property
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);
        let event_c = make_lww_event(3, vec![("x", "value_c")]);

        // Find which event has highest ID
        let events = vec![(&event_a, "value_a"), (&event_b, "value_b"), (&event_c, "value_c")];
        let (winner_event, winner_value) = events.iter().max_by_key(|(e, _)| e.id()).unwrap();

        // Put a different event in already_applied
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![&event_b, &event_c];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Winner should be set (if winner is in to_apply) or nothing (if winner is in already_applied)
        if winner_event.id() == event_a.id() {
            // Winner was in already_applied - nothing should be set
            // Actually no, the other events should compete and the highest among to_apply wins
            // Let's verify: winner among all is in already_applied, so among to_apply, the higher one wins
            let _to_apply_winner_value = if event_b.id() > event_c.id() { "value_b" } else { "value_c" };
            assert_eq!(backend.get(&prop("x")), None);
        } else {
            // Winner is in to_apply - that value should win
            assert_eq!(backend.get(&prop("x")), Some(Value::String((*winner_value).into())));
        }
    }

    #[test]
    fn test_cross_layer_already_applied_winner_persistence() {
        // M -> A and M -> X -> B. A has the larger ID, but B's greater
        // generation must win even when A was already stored before the merge.
        let mut retriever = MockRetriever::new();
        let meet = make_test_event(&retriever, 50, &[]);
        retriever.add_event(meet.clone());
        let remote_mid = make_test_event(&retriever, 51, &[meet.id()]);
        retriever.add_event(remote_mid.clone());
        let event_b = make_lww_event_with_parent(&retriever, 2, vec![("x", "value_from_B")], &[remote_mid.id()]);
        let event_a = (0u8..=255)
            .map(|seed| make_lww_event_with_parent(&retriever, seed, vec![("x", "value_from_A")], &[meet.id()]))
            .find(|a| a.id() > event_b.id())
            .unwrap();

        let backend_local = LWWBackend::new();
        backend_local.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();
        backend_local.apply_layer(&layer_from_refs_with_context(&[&event_a], &[&remote_mid], &[&meet, &event_b])).unwrap();
        assert_eq!(backend_local.get(&prop("x")), Some(Value::String("value_from_A".into())));
        assert_eq!(backend_local.get_event_id(&prop("x")), Some(event_a.id()));

        // A replica receiving A for the first time reaches the same state.
        let backend_remote = LWWBackend::new();
        backend_remote.apply_layer(&layer_from_refs_with_context(&[], &[&event_a, &remote_mid], &[&meet, &event_b])).unwrap();
        assert_eq!(backend_remote.to_state_buffer().unwrap(), backend_local.to_state_buffer().unwrap());

        let layer2 = layer_from_refs_with_context(&[], &[&event_b], &[&meet, &event_a, &remote_mid]);
        backend_local.apply_layer(&layer2).unwrap();
        backend_remote.apply_layer(&layer2).unwrap();
        assert_eq!(backend_local.get(&prop("x")), Some(Value::String("value_from_B".into())));
        assert_eq!(backend_local.get_event_id(&prop("x")), Some(event_b.id()));
        assert_eq!(backend_remote.to_state_buffer().unwrap(), backend_local.to_state_buffer().unwrap());
    }
}

// ============================================================================
// YRS LAYER APPLICATION TESTS (Phase 3e - Category 2)
// ============================================================================

#[cfg(test)]
mod yrs_layer_tests {
    use super::*;
    use crate::property::backend::yrs::YrsBackend;

    /// Create a test event with Yrs text operations.
    /// Each text operation inserts the given string at position 0.
    fn make_yrs_event(seed: u8, text_field: &str, insert_text: &str) -> Event {
        let text_field = prop(text_field);
        let mut entity_id_bytes = [0u8; 32];
        entity_id_bytes[0] = seed;
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        let backend = YrsBackend::new();
        backend.insert(&text_field, 0, insert_text).unwrap();
        let ops = backend.to_operations().unwrap().unwrap();

        let parent = Clock::default();
        Event {
            entity_id,
            body: fixture_body(&[seed], &parent, OperationSet::from_backends(BTreeMap::from([("yrs".to_string(), ops)]))),
            parent,
        }
    }

    #[test]
    fn test_yrs_apply_layer_concurrent_inserts() {
        let backend = YrsBackend::new();

        // Two concurrent events inserting at position 0
        let event_a = make_yrs_event(1, "text", "hello");
        let event_b = make_yrs_event(2, "text", "world");

        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&event_a, &event_b];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Both inserts should be applied (Yrs CRDT merges them)
        let result = backend.get_string(&prop("text")).unwrap();
        // Order depends on Yrs internal logic, but both should be present
        assert!(result.contains("hello") || result.contains("world"), "Expected at least one insert to be present, got: {}", result);
    }

    #[test]
    fn test_yrs_apply_layer_ignores_already_applied() {
        let backend = YrsBackend::new();

        // Apply event_a first
        let event_a = make_yrs_event(1, "text", "hello");
        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&event_a];
        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        let initial_text = backend.get_string(&prop("text")).unwrap();
        assert_eq!(initial_text, "hello");

        // Now apply with event_a in already_applied and event_b in to_apply
        let event_b = make_yrs_event(2, "text", "world");
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![&event_b];
        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Only event_b should be applied again (if it wasn't already)
        // The text should contain "world" from event_b
        let final_text = backend.get_string(&prop("text")).unwrap();
        assert!(final_text.contains("world"), "Expected 'world' to be in text, got: {}", final_text);
    }

    #[test]
    fn test_yrs_apply_layer_order_independent() {
        // Apply events in different orders, verify same result
        let event_a = make_yrs_event(1, "text", "A");
        let event_b = make_yrs_event(2, "text", "B");
        let event_c = make_yrs_event(3, "text", "C");

        // Order 1: A, B, C
        let backend1 = YrsBackend::new();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_b])).unwrap();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_c])).unwrap();
        let result1 = backend1.get_string(&prop("text")).unwrap();

        // Order 2: C, A, B
        let backend2 = YrsBackend::new();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_c])).unwrap();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_b])).unwrap();
        let result2 = backend2.get_string(&prop("text")).unwrap();

        // Order 3: B, C, A
        let backend3 = YrsBackend::new();
        backend3.apply_layer(&layer_from_refs(&[], &[&event_b])).unwrap();
        backend3.apply_layer(&layer_from_refs(&[], &[&event_c])).unwrap();
        backend3.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();
        let result3 = backend3.get_string(&prop("text")).unwrap();

        // All results should be identical (CRDT convergence)
        assert_eq!(result1, result2, "Order 1 vs Order 2 should produce same result");
        assert_eq!(result2, result3, "Order 2 vs Order 3 should produce same result");
    }

    #[test]
    fn test_yrs_apply_layer_empty_to_apply() {
        let backend = YrsBackend::new();
        backend.insert(&prop("text"), 0, "initial").unwrap();

        let event_a = make_yrs_event(1, "text", "hello");

        // Apply with event_a in already_applied but nothing in to_apply
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Text should be unchanged (only "initial")
        let result = backend.get_string(&prop("text")).unwrap();
        assert_eq!(result, "initial");
    }
}

// ============================================================================
// DETERMINISM VERIFICATION TESTS (Phase 3e - Category 5)
// ============================================================================

#[cfg(test)]
mod determinism_tests {
    use super::*;

    #[test]
    fn test_two_event_determinism() {
        // Create two concurrent events setting same property
        let event_a = make_lww_event(1, vec![("x", "value_a")]);
        let event_b = make_lww_event(2, vec![("x", "value_b")]);

        // Order 1: Apply A then B
        let backend1 = LWWBackend::new();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_a, &event_b])).unwrap();
        let result1 = backend1.get(&prop("x"));

        // Order 2: Apply B then A (as a single layer - order within layer shouldn't matter)
        let backend2 = LWWBackend::new();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_b, &event_a])).unwrap();
        let result2 = backend2.get(&prop("x"));

        // Both should produce same result (lexicographic winner)
        assert_eq!(result1, result2, "Different orderings should produce same result");
    }

    #[test]
    fn test_three_event_determinism() {
        let event_a = make_lww_event(1, vec![("x", "A")]);
        let event_b = make_lww_event(2, vec![("x", "B")]);
        let event_c = make_lww_event(3, vec![("x", "C")]);

        // All permutations of applying the three events
        let permutations: Vec<Vec<&Event>> = vec![
            vec![&event_a, &event_b, &event_c],
            vec![&event_a, &event_c, &event_b],
            vec![&event_b, &event_a, &event_c],
            vec![&event_b, &event_c, &event_a],
            vec![&event_c, &event_a, &event_b],
            vec![&event_c, &event_b, &event_a],
        ];

        let mut results = Vec::new();
        for perm in &permutations {
            let backend = LWWBackend::new();
            backend.apply_layer(&layer_from_refs(&[], perm)).unwrap();
            results.push(backend.get(&prop("x")));
        }

        // All results should be identical
        for (i, result) in results.iter().enumerate() {
            assert_eq!(result, &results[0], "Permutation {} produced different result than permutation 0", i);
        }
    }

    #[test]
    fn test_multi_property_determinism() {
        // Events setting different properties
        let event_a = make_lww_event(1, vec![("x", "x_from_a"), ("y", "y_from_a")]);
        let event_b = make_lww_event(2, vec![("x", "x_from_b"), ("z", "z_from_b")]);
        let event_c = make_lww_event(3, vec![("y", "y_from_c"), ("z", "z_from_c")]);

        // Order 1
        let backend1 = LWWBackend::new();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_a, &event_b, &event_c])).unwrap();

        // Order 2 (reversed)
        let backend2 = LWWBackend::new();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_c, &event_b, &event_a])).unwrap();

        // Same final state for all properties
        assert_eq!(backend1.get(&prop("x")), backend2.get(&prop("x")));
        assert_eq!(backend1.get(&prop("y")), backend2.get(&prop("y")));
        assert_eq!(backend1.get(&prop("z")), backend2.get(&prop("z")));
    }

    #[test]
    fn test_sequential_layer_determinism() {
        // Simulate two layers being applied
        let event_a = make_lww_event(1, vec![("x", "layer1_a")]);
        let event_b = make_lww_event(2, vec![("x", "layer1_b")]);
        let event_c = make_lww_event(3, vec![("x", "layer2_c")]);
        let event_d = make_lww_event(4, vec![("x", "layer2_d")]);

        // Apply layer 1 then layer 2
        let backend1 = LWWBackend::new();
        backend1.apply_layer(&layer_from_refs(&[], &[&event_a, &event_b])).unwrap();
        backend1.apply_layer(&layer_from_refs(&[&event_a, &event_b], &[&event_c, &event_d])).unwrap();

        // Apply layer 1 (reversed) then layer 2 (reversed)
        let backend2 = LWWBackend::new();
        backend2.apply_layer(&layer_from_refs(&[], &[&event_b, &event_a])).unwrap();
        backend2.apply_layer(&layer_from_refs(&[&event_b, &event_a], &[&event_d, &event_c])).unwrap();

        // Final state should be the same
        assert_eq!(backend1.get(&prop("x")), backend2.get(&prop("x")));
    }
}

// ============================================================================
// EDGE CASE TESTS (Phase 3e - Category 7)
// ============================================================================

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    fn empty_layer() -> EventLayer { EventLayer::new(Vec::new(), Vec::new(), Arc::new(BTreeMap::new())) }

    #[test]
    fn test_empty_layer_application() {
        let backend = LWWBackend::new();
        let init_event = make_lww_event(1, vec![("x", "initial")]);
        backend.apply_layer(&layer_from_refs(&[], &[&init_event])).unwrap();

        // Apply empty layer
        backend.apply_layer(&empty_layer()).unwrap();

        // State should be unchanged
        assert_eq!(backend.get(&prop("x")), Some(Value::String("initial".into())));
    }

    #[test]
    fn test_event_with_no_lww_operations() {
        let backend = LWWBackend::new();
        let init_event = make_lww_event(1, vec![("x", "initial")]);
        backend.apply_layer(&layer_from_refs(&[], &[&init_event])).unwrap();

        // Create an event with empty operations
        let entity_id = EntityId::from_bytes([99u8; 32]);
        let parent = Clock::default();
        let empty_event = Event {
            entity_id,
            body: fixture_body(&[99], &parent, OperationSet::default()), // No operations
            parent,
        };

        let already_applied: Vec<&Event> = vec![];
        let to_apply: Vec<&Event> = vec![&empty_event];

        backend.apply_layer(&layer_from_refs_with_context(&already_applied, &to_apply, &[&init_event])).unwrap();

        // State should be unchanged
        assert_eq!(backend.get(&prop("x")), Some(Value::String("initial".into())));
    }

    #[test]
    fn test_same_event_in_both_lists() {
        // Edge case: what if same event appears in both already_applied and to_apply?
        // This shouldn't happen in practice, but the backend should handle it gracefully
        let backend = LWWBackend::new();

        let event_a = make_lww_event(1, vec![("x", "value_a")]);

        // Put same event in both lists
        let already_applied: Vec<&Event> = vec![&event_a];
        let to_apply: Vec<&Event> = vec![&event_a];

        backend.apply_layer(&layer_from_refs(&already_applied, &to_apply)).unwrap();

        // Event should be applied once (its value should be set)
        assert_eq!(backend.get(&prop("x")), Some(Value::String("value_a".into())));
    }

    #[test]
    fn test_many_concurrent_events() {
        let backend = LWWBackend::new();

        // Create 10 concurrent events
        let events: Vec<Event> = (0..10).map(|i| make_lww_event(i as u8, vec![("x", &format!("value_{}", i))])).collect();

        let event_refs: Vec<&Event> = events.iter().collect();

        backend.apply_layer(&layer_from_refs(&[], &event_refs)).unwrap();

        // Winner should be the one with highest EventId
        let winner_idx = events.iter().enumerate().max_by_key(|(_, e)| e.id()).map(|(i, _)| i).unwrap();

        let expected = format!("value_{}", winner_idx);
        assert_eq!(backend.get(&prop("x")), Some(Value::String(expected)));
    }

    #[test]
    fn test_property_deletion() {
        let backend = LWWBackend::new();
        let init_event = make_lww_event(1, vec![("x", "initial")]);
        backend.apply_layer(&layer_from_refs(&[], &[&init_event])).unwrap();

        // Create an event that deletes the property (sets to None)
        let mut entity_id_bytes = [0u8; 32];
        entity_id_bytes[0] = 1;
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        let delete_backend = LWWBackend::new();
        delete_backend.set(prop("x"), None); // Delete
        let ops = delete_backend.to_operations().unwrap().unwrap();
        let parent = Clock::default();
        let delete_event = Event {
            entity_id,
            body: fixture_body(&[2], &parent, OperationSet::from_backends(BTreeMap::from([("lww".to_string(), ops)]))),
            parent,
        };

        backend.apply_layer(&layer_from_refs_with_context(&[], &[&delete_event], &[&init_event])).unwrap();

        let expected = if delete_event.id() > init_event.id() { None } else { Some(Value::String("initial".into())) };
        assert_eq!(backend.get(&prop("x")), expected);
    }

    #[test]
    fn test_stored_last_write_competes() {
        let backend = LWWBackend::new();
        let init_event = make_lww_event(1, vec![("x", "initial")]);
        backend.apply_layer(&layer_from_refs(&[], &[&init_event])).unwrap();

        let challenger_event = make_lww_event(2, vec![("x", "challenger")]);
        backend.apply_layer(&layer_from_refs_with_context(&[], &[&challenger_event], &[&init_event])).unwrap();

        let expected = if challenger_event.id() > init_event.id() {
            Some(Value::String("challenger".into()))
        } else {
            Some(Value::String("initial".into()))
        };
        assert_eq!(backend.get(&prop("x")), expected);
    }

    #[test]
    fn test_event_id_tracking_after_layer() {
        let backend = LWWBackend::new();

        let event_a = make_lww_event(1, vec![("x", "value_a")]);

        backend.apply_layer(&layer_from_refs(&[], &[&event_a])).unwrap();

        // The event_id should be tracked
        let tracked_id = backend.get_event_id(&prop("x"));
        assert!(tracked_id.is_some());
        assert_eq!(tracked_id.unwrap(), event_a.id());
    }
}

// ============================================================================
// PHASE 4 TESTS
// ============================================================================

/// Phase 4 Test 1: Stored event_id below meet loses to layer candidate.
///
/// When the LWW backend has a stored value whose event_id is NOT in the accumulated
/// DAG (i.e., it is below the meet), any layer candidate should win regardless of
/// event_id ordering. This tests the `older_than_meet` rule.
#[cfg(test)]
mod phase4_stored_below_meet {
    use super::*;

    #[test]
    fn test_stored_event_id_below_meet_loses_to_layer_candidate() {
        let backend = LWWBackend::new();

        // Step 1: Apply an initial event so the backend has a stored value with an event_id.
        let old_event = make_lww_event(1, vec![("x", "old_value")]);
        let old_event_id = old_event.id();

        // Apply the old event as a normal layer first (so the backend tracks the event_id)
        {
            let dag = BTreeMap::from([(old_event_id.clone(), old_event.parent.clone())]);
            let layer = EventLayer::new(vec![], vec![old_event.clone()], Arc::new(dag));
            backend.apply_layer(&layer).unwrap();
        }

        // Verify old value is stored
        assert_eq!(backend.get(&prop("x")), Some(Value::String("old_value".into())));
        assert_eq!(backend.get_event_id(&prop("x")), Some(old_event_id.clone()));

        // Step 2: Create a new layer candidate event. Build the DAG WITHOUT the
        // old_event_id. This simulates the case where the stored value's event_id
        // is below the meet (not in the accumulated DAG).
        let new_event = make_lww_event(2, vec![("x", "new_value")]);
        let new_event_id = new_event.id();

        // DAG only contains the new event, NOT the old event.
        // This means dag_contains(old_event_id) returns false => older_than_meet.
        let dag = BTreeMap::from([(new_event_id.clone(), new_event.parent.clone())]);
        let layer = EventLayer::new(vec![], vec![new_event.clone()], Arc::new(dag));

        backend.apply_layer(&layer).unwrap();

        // The new event should always win because the stored value is older_than_meet,
        // regardless of event_id ordering.
        assert_eq!(
            backend.get(&prop("x")),
            Some(Value::String("new_value".into())),
            "Layer candidate must beat stored value whose event_id is below the meet"
        );
        assert_eq!(backend.get_event_id(&prop("x")), Some(new_event_id), "Winning event_id must be the new event");
    }
}

/// Phase 4 Test 2: Re-delivery of historical event is rejected (idempotency).
///
/// When an event is already stored and its id appears in the comparison head (or
/// BFS discovers it as an ancestor), compare should return Equal or StrictAscends,
/// signaling a no-op to the caller.
#[cfg(test)]
mod phase4_idempotency {
    use super::*;

    #[tokio::test]
    async fn test_redelivery_of_historical_event_is_noop() {
        let mut retriever = MockRetriever::new();

        // Build a chain: A -> B -> C (current head is [C])
        let ev_a = make_test_event(&retriever, 1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(ev_a);

        let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_c = make_test_event(&retriever, 3, &[id_b.clone()]);
        let id_c = ev_c.id();
        retriever.add_event(ev_c);

        let entity_head = retriever.clock(&[id_c.clone()]);

        // Re-deliver event B (which is already in the history, but not at head).
        // With the staging+compare pattern, B is already in the retriever (same
        // content = same EventId as ev_b). compare([B], [C]) can walk backward
        // from C and discover B as an ancestor, returning StrictAscends.
        let event_b_again = make_test_event(&retriever, 2, &[id_a]);
        let result = compare(retriever.clone(), &Clock::singleton(&event_b_again), &entity_head, 100).await.unwrap();

        // Re-delivery of event C (which IS at the head) should return Equal.
        let event_c_again = make_test_event(&retriever, 3, &[id_b]);
        let result_c = compare(retriever.clone(), &Clock::singleton(&event_c_again), &entity_head, 100).await.unwrap();
        assert_eq!(result_c.relation, AbstractCausalRelation::Equal, "Re-delivery of head event must return Equal");

        // For event B (ancestor, not at head): with staging, the comparison can
        // now correctly identify B as an ancestor of C, returning StrictAscends.
        assert!(
            matches!(result.relation, AbstractCausalRelation::StrictAscends),
            "Historical non-head event re-delivery should return StrictAscends, got {:?}",
            result.relation
        );
    }
}

/// Phase 4 Test 3: Second creation event for same entity is rejected.
///
/// An entity may have at most one creation event (empty parent clock). If a second
/// creation event arrives after the entity already has a non-empty head, it must
/// be rejected with DuplicateCreation.
#[cfg(test)]
mod phase4_duplicate_creation {
    use super::*;
    use crate::error::MutationError;

    fn make_creation_event(seed: u8) -> Event {
        let mut entity_id_bytes = [0u8; 32];
        entity_id_bytes[0] = 42; // Same entity for both events
        let entity_id = EntityId::from_bytes(entity_id_bytes);

        // Use LWW operations with distinct values to produce different event IDs.
        // Without differing content, identical events would produce the same EventId.
        let backend = LWWBackend::new();
        backend.set(prop("x"), Some(Value::String(format!("value_{}", seed))));
        let ops = backend.to_operations().unwrap().unwrap();

        let parent = Clock::default(); // no parent = genesis
        Event {
            entity_id,
            body: fixture_body(&[seed], &parent, OperationSet::from_backends(BTreeMap::from([("lww".to_string(), ops)]))),
            parent,
        }
    }

    #[tokio::test]
    async fn test_second_creation_event_rejected() {
        let entity = crate::entity::state::EntityState::empty();

        let mut retriever = MockRetriever::new();

        // First creation event should succeed
        let creation_event_1 = make_creation_event(1);
        retriever.add_event(creation_event_1.clone());

        let result = entity.apply_events(&retriever, &[&creation_event_1]).await;
        assert!(result.is_ok(), "First creation event should succeed");
        assert_eq!(result.unwrap(), vec![true], "First creation event should apply");

        // Entity head should now be non-empty
        assert!(!entity.head().is_empty(), "Entity head should be non-empty after creation");

        // Second creation event (different seed = different event) should be rejected.
        // BFS detects two different roots -> Disjoint -> LineageError::Disjoint
        let creation_event_2 = make_creation_event(2);
        retriever.add_event(creation_event_2.clone());
        let result = entity.apply_events(&retriever, &[&creation_event_2]).await;

        assert!(result.is_err(), "Second creation event should fail");
        let err = result.unwrap_err();
        assert!(matches!(err, MutationError::LineageError(crate::error::LineageError::Disjoint)), "Error should be Disjoint, got: {err:?}");
    }

    #[tokio::test]
    async fn test_redelivery_of_same_creation_event_is_noop() {
        let entity = crate::entity::state::EntityState::empty();

        let mut retriever = MockRetriever::new();

        // First creation event
        let creation_event = make_creation_event(1);
        retriever.add_event(creation_event.clone());

        let result = entity.apply_events(&retriever, &[&creation_event.clone()]).await;
        assert_eq!(result.unwrap(), vec![true], "First apply should succeed");

        // Re-deliver the SAME creation event (same content, same id)
        // Since event_stored returns false but event is at head, comparison returns Equal -> no-op
        let result = entity.apply_events(&retriever, &[&creation_event]).await;
        assert!(result.is_ok(), "Re-delivery of same creation event should not error");
        assert_eq!(result.unwrap(), vec![false], "Re-delivery should be a no-op");
    }
}

/// Phase 4 Test 5: Budget escalation succeeds where initial budget fails.
///
/// Create a DAG deep enough that budget=2 fails, but the internal 4x escalation
/// (to budget=8) succeeds.
#[cfg(test)]
mod phase4_budget_escalation {
    use super::*;

    #[tokio::test]
    async fn test_budget_escalation_succeeds() {
        let mut retriever = MockRetriever::new();

        // Create a chain of 6 events: ev0 -> ev1 -> ev2 -> ev3 -> ev4 -> ev5
        // With budget=2, the initial traversal exhausts at 2 steps.
        // With 4x escalation (budget=8), it succeeds for a 6-long chain.
        let mut ids: Vec<EventId> = Vec::new();
        for i in 0..6u8 {
            let parents = if i == 0 { vec![] } else { vec![ids[i as usize - 1].clone()] };
            let ev = make_test_event(&retriever, i + 1, &parents);
            ids.push(ev.id());
            retriever.add_event(ev);
        }

        let ancestor = retriever.clock(&[ids[0].clone()]);
        let descendant = retriever.clock(&[ids[5].clone()]);

        // With budget=2, initial attempt fails. But internal escalation retries
        // with budget=8 (2 * 4), which should succeed for a 6-deep chain.
        let result = compare(retriever.clone(), &descendant, &ancestor, 2).await.unwrap();
        assert!(
            matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }),
            "Budget escalation (2 -> 8) should succeed for a 6-deep chain, got {:?}",
            result.relation
        );

        // Verify that without escalation room, it would actually fail.
        // Budget=1 escalates to max 4, which is not enough for a chain of 6.
        let result = compare(retriever.clone(), &descendant, &ancestor, 1).await.unwrap();
        assert!(
            matches!(result.relation, AbstractCausalRelation::BudgetExceeded { .. }),
            "Budget=1 (max 4) should fail for a 6-deep chain, got {:?}",
            result.relation
        );
    }
}

/// Phase 4 Test 6: TOCTOU retry exhaustion produces clean error.
///
/// This tests the retry loop in `apply_events`. A proper test would require a mock
/// that changes the entity head between comparison and CAS, which requires interior
/// mutability and a custom Retrieve implementation that modifies entity state during
/// comparison. This is too complex for a unit test.
#[cfg(test)]
mod phase4_toctou_retry {
    #[test]
    #[ignore = "Testing TOCTOU retry exhaustion requires a mock that modifies the entity \
        head between comparison and the CAS attempt inside try_mutate. This requires: \
        (1) a custom Retrieve that triggers a side-effect during get_event to modify \
        entity state, (2) precise timing control over the entity's RwLock. The expected \
        behavior is: after MAX_RETRIES (5) attempts where the head moves each time, \
        apply_events returns Err(MutationError::TOCTOUAttemptsExhausted). This error \
        variant exists and is returned at the end of the retry loop."]
    fn test_toctou_retry_exhaustion_produces_clean_error() {
        // See ignore message above.
    }
}

/// Phase 4 Test 7: Merge event with parent from non-meet branch is correctly layered.
///
/// A merge event may have parents that span the meet boundary. This tests the
/// generalized topological-sort seed in EventLayers (not just children of meet).
#[cfg(test)]
mod phase4_mixed_parent_merge {
    use super::*;
    use crate::event_dag::accumulator::EventAccumulator;

    #[tokio::test]
    async fn test_merge_event_with_parent_from_non_meet_branch() {
        let mut retriever = MockRetriever::new();

        // DAG:
        //       A (meet)
        //      / \
        //     B   G (independent lineage brought in by merge)
        //     |   |
        //     C   |
        //      \ /
        //       E  (merge: parents = [C, G])
        //
        // current head = [B] (only the B side is local)
        // incoming = [E] (brings in G from a non-meet branch)
        //
        // After layer computation:
        // - Layer 1: B (already_applied), C, G (to_apply -- all are children of meet
        //   or have all parents processed)
        //   Actually G's parent is A (the meet), and C's parent is B.
        //   So initial frontier = {B, G} (both have all in-DAG parents processed).
        //   Wait -- B's parent is A, which is in the processed set (meet).
        //   G's parent is A, also in processed set. So frontier = {B, G}.
        //
        // - Layer 1: B (already_applied), G (to_apply)
        // - After processing B: C becomes ready (parent B is processed)
        // - Layer 2: C (to_apply)
        // - After processing C: E becomes ready (parents C processed, G processed)
        // - Layer 3: E (to_apply)

        let ev_a = make_test_event(&retriever, 1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(ev_a);

        let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_g = make_test_event(&retriever, 3, &[id_a.clone()]);
        let id_g = ev_g.id();
        retriever.add_event(ev_g);

        let ev_c = make_test_event(&retriever, 4, &[id_b.clone()]);
        let id_c = ev_c.id();
        retriever.add_event(ev_c);

        let ev_e = make_test_event(&retriever, 5, &[id_c.clone(), id_g.clone()]);
        let id_e = ev_e.id();
        retriever.add_event(ev_e.clone());

        // Build accumulator manually to simulate what comparison would produce.
        // The DAG contains all events above the meet (A is the meet).
        let mut accumulator = EventAccumulator::new(retriever.clone());
        for id in [&id_b, &id_g, &id_c, &id_e] {
            let event = retriever.get_event(id).await.unwrap();
            accumulator.accumulate(&event);
        }

        // Meet = [A], current head = [B]
        let layers =
            crate::event_dag::layers::EventLayers::load(accumulator, vec![id_a.clone()], vec![id_b.clone()], &[&ev_e]).await.unwrap();
        let all_layers = layers.layers();

        // Verify we got multiple layers and all events are covered
        assert!(all_layers.len() >= 2, "Expected at least 2 layers for mixed-parent merge, got {}", all_layers.len());

        // Collect all event ids from all layers
        let mut all_event_ids: Vec<EventId> = Vec::new();
        for layer in all_layers {
            for e in &layer.already_applied {
                all_event_ids.push(e.id());
            }
            for e in &layer.to_apply {
                all_event_ids.push(e.id());
            }
        }

        // All events above meet should be present
        assert!(all_event_ids.contains(&id_b), "B should be in layers");
        assert!(all_event_ids.contains(&id_g), "G should be in layers");
        assert!(all_event_ids.contains(&id_c), "C should be in layers");
        assert!(all_event_ids.contains(&id_e), "E (merge event) should be in layers");

        // E must appear in a later layer than both C and G
        let e_layer_idx = all_layers
            .iter()
            .position(|l| l.to_apply.iter().any(|ev| ev.id() == id_e) || l.already_applied.iter().any(|ev| ev.id() == id_e))
            .expect("E must appear in some layer");

        let g_layer_idx = all_layers
            .iter()
            .position(|l| l.to_apply.iter().any(|ev| ev.id() == id_g) || l.already_applied.iter().any(|ev| ev.id() == id_g))
            .expect("G must appear in some layer");

        let c_layer_idx = all_layers
            .iter()
            .position(|l| l.to_apply.iter().any(|ev| ev.id() == id_c) || l.already_applied.iter().any(|ev| ev.id() == id_c))
            .expect("C must appear in some layer");

        assert!(e_layer_idx > g_layer_idx, "Merge event E (layer {}) must be after G (layer {})", e_layer_idx, g_layer_idx);
        assert!(e_layer_idx > c_layer_idx, "Merge event E (layer {}) must be after C (layer {})", e_layer_idx, c_layer_idx);

        // B should be in already_applied (it's in current head ancestry)
        let b_in_already = all_layers.iter().any(|l| l.already_applied.iter().any(|ev| ev.id() == id_b));
        assert!(b_in_already, "B should be in already_applied (current head ancestry)");

        // G should be in to_apply (it's NOT in current head ancestry)
        let g_in_to_apply = all_layers.iter().any(|l| l.to_apply.iter().any(|ev| ev.id() == id_g));
        assert!(g_in_to_apply, "G should be in to_apply (not in current head ancestry)");
    }
}

/// Phase 4 Test 8: Eagerly stored peer events are discoverable by BFS during comparison.
///
/// This verifies that when events are stored in the retriever (simulating eager
/// storage on receipt), the BFS in comparison can discover them and produce
/// correct causal relationships.
#[cfg(test)]
mod phase4_eager_storage_bfs {
    use super::*;

    #[tokio::test]
    async fn test_eagerly_stored_events_discoverable_by_bfs() {
        let mut retriever = MockRetriever::new();

        // Simulate a scenario where events arrive from a peer and are eagerly stored
        // before comparison. Build a DAG:
        //
        //     A (genesis)
        //    / \
        //   B   C  (concurrent branches)
        //   |   |
        //   D   E  (tips)
        //
        // All events are stored in the retriever (simulating eager storage).
        // Then compare D vs E. The BFS should discover all events via the retriever.

        let ev_a = make_test_event(&retriever, 1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(ev_a);

        let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_c = make_test_event(&retriever, 3, &[id_a.clone()]);
        let id_c = ev_c.id();
        retriever.add_event(ev_c);

        let ev_d = make_test_event(&retriever, 4, &[id_b.clone()]);
        let id_d = ev_d.id();
        retriever.add_event(ev_d.clone());

        let ev_e = make_test_event(&retriever, 5, &[id_c.clone()]);
        let id_e = ev_e.id();
        retriever.add_event(ev_e);

        let clock_d = retriever.clock(&[id_d.clone()]);
        let clock_e = retriever.clock(&[id_e.clone()]);

        // BFS should walk backward from D and E, discovering B, C, and A.
        let result = compare(retriever.clone(), &clock_d, &clock_e, 100).await.unwrap();

        // Verify BFS produced the correct result
        assert!(
            matches!(result.relation, AbstractCausalRelation::DivergedSince { ref meet, .. } if meet == &vec![id_a.clone()]),
            "BFS should discover meet at A via eagerly stored events, got {:?}",
            result.relation
        );

        // Verify the accumulator contains all events discovered by BFS.
        // The accumulator's DAG should have entries for B, C, D, E (events above meet),
        // and possibly A (the meet itself, depending on whether it's accumulated).
        let accumulator = result.accumulator();
        let dag = accumulator.dag();

        assert!(dag.contains_key(&id_b), "BFS should have accumulated event B");
        assert!(dag.contains_key(&id_c), "BFS should have accumulated event C");
        assert!(dag.contains_key(&id_d), "BFS should have accumulated event D");
        assert!(dag.contains_key(&id_e), "BFS should have accumulated event E");

        // Preparation must also load layers from the eagerly stored history.
        let layers = crate::event_dag::EventLayers::prepare(&retriever, &[&ev_d], &clock_e).await.unwrap();
        assert!(!layers.layers().is_empty(), "Should produce at least one layer from eagerly stored events");
        for layer in layers.layers() {
            assert!(!layer.to_apply.is_empty() || !layer.already_applied.is_empty(), "Layer should not be empty");
        }
    }
}

// ============================================================================
// BFS REVISIT BUG VERIFICATION (no per-side visited set in comparison.rs)
// ============================================================================

#[cfg(test)]
mod bfs_revisit_bugs {
    use super::*;

    /// CLAIM A: A second, longer path re-adds an already-processed node to the
    /// subject frontier. The re-visit re-runs the `original_comparison.contains`
    /// check and decrements `unseen_comparison_heads` a second time for the SAME
    /// head, so it can hit 0 while another comparison head (D) was never reached.
    ///
    /// DAG (parents point right):
    ///   S -> {A, B};  A -> N;  B -> C;  C -> N;  N -> R;  D -> R
    /// Head (comparison) = {N, D}. Subject = {S}.
    /// S descends from N via two paths of different length but does NOT descend
    /// from D, so the correct relation is DivergedSince { meet: [N], .. }.
    /// The bug double-decrements for N and falsely reports StrictDescends,
    /// which entity.rs turns into head = {S}, silently discarding tip D.
    #[tokio::test]
    async fn double_decrement_falsely_reports_strict_descends() {
        let mut retriever = MockRetriever::new();

        let ev_r = make_test_event(&retriever, 1, &[]);
        let id_r = ev_r.id();
        retriever.add_event(ev_r);

        let ev_n = make_test_event(&retriever, 2, &[id_r.clone()]);
        let id_n = ev_n.id();
        retriever.add_event(ev_n);

        let ev_d = make_test_event(&retriever, 3, &[id_r.clone()]);
        let id_d = ev_d.id();
        retriever.add_event(ev_d);

        let ev_a = make_test_event(&retriever, 4, &[id_n.clone()]);
        let id_a = ev_a.id();
        retriever.add_event(ev_a);

        // Pick a seed for C such that id(N) < id(C). The frontier is a BTreeSet
        // iterated in id order, so this makes the subject BFS process N (short
        // path, via A) BEFORE C re-adds N to the frontier (long path, via B),
        // forcing the re-visit. Content hashing makes this deterministic once a
        // seed is found.
        let ev_c = (10u8..=255)
            .map(|seed| make_test_event(&retriever, seed, &[id_n.clone()]))
            .find(|ev| ev.id() > id_n)
            .expect("some seed must yield id(C) > id(N)");
        let id_c = ev_c.id();
        retriever.add_event(ev_c);

        let ev_b = make_test_event(&retriever, 5, &[id_c.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_s = make_test_event(&retriever, 6, &[id_a.clone(), id_b.clone()]);
        let id_s = ev_s.id();
        retriever.add_event(ev_s);

        let subject = retriever.clock(&[id_s.clone()]);
        let comparison = retriever.clock(&[id_n.clone(), id_d.clone()]); // two concurrent tips

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_n.clone()], "meet should be exactly [N]");
            }
            other => panic!("S does NOT descend from concurrent tip D; expected DivergedSince {{ meet: [N] }} but got {:?}", other),
        }
    }

    /// Mirror of the double-decrement case: the same DAG and forced re-visit,
    /// but with subject and comparison roles swapped, so the double-count lands
    /// on the subject-head accounting and the false verdict is StrictAscends,
    /// which entity.rs turns into silently dropping the incoming event.
    ///
    /// Subject = {N, D}, comparison = {S}. Correct: DivergedSince { meet: [N] }.
    #[tokio::test]
    async fn double_decrement_falsely_reports_strict_ascends() {
        let mut retriever = MockRetriever::new();

        let ev_r = make_test_event(&retriever, 1, &[]);
        let id_r = ev_r.id();
        retriever.add_event(ev_r);

        let ev_n = make_test_event(&retriever, 2, &[id_r.clone()]);
        let id_n = ev_n.id();
        retriever.add_event(ev_n);

        let ev_d = make_test_event(&retriever, 3, &[id_r.clone()]);
        let id_d = ev_d.id();
        retriever.add_event(ev_d);

        let ev_a = make_test_event(&retriever, 4, &[id_n.clone()]);
        let id_a = ev_a.id();
        retriever.add_event(ev_a);

        // Same seed condition as the StrictDescends case: id(N) < id(C) makes
        // the comparison-side BFS process N (short path, via A) before C
        // re-encounters it (long path, via B).
        let ev_c = (10u8..=255)
            .map(|seed| make_test_event(&retriever, seed, &[id_n.clone()]))
            .find(|ev| ev.id() > id_n)
            .expect("some seed must yield id(C) > id(N)");
        let id_c = ev_c.id();
        retriever.add_event(ev_c);

        let ev_b = make_test_event(&retriever, 5, &[id_c.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_s = make_test_event(&retriever, 6, &[id_a.clone(), id_b.clone()]);
        let id_s = ev_s.id();
        retriever.add_event(ev_s);

        let subject = retriever.clock(&[id_n.clone(), id_d.clone()]); // two concurrent tips
        let comparison = retriever.clock(&[id_s.clone()]);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_n.clone()], "meet should be exactly [N]");
            }
            other => panic!(
                "comparison {{S}} does NOT reach concurrent tip D, so it cannot strictly ascend; expected DivergedSince {{ meet: [N] }} but got {:?}",
                other
            ),
        }
    }

    /// Budget must be spent per event, not per path. A chain of uneven diamonds
    /// has O(levels) events but 2^levels distinct paths; per-path bookkeeping
    /// re-processes the join node of every level and cascades exponentially,
    /// exhausting even the 4x-escalated budget. Per-event bookkeeping completes
    /// within a small multiple of the event count and yields a duplicate-free
    /// chain.
    ///
    /// Each level: J -> {A, B}; A -> prev (short); B -> C -> prev (long).
    /// C's seed is searched so id(C) > id(prev), forcing prev to be re-added to
    /// the frontier after it was already processed (the V1 re-visit shape).
    #[tokio::test]
    async fn diamond_chain_completes_within_linear_budget() {
        use std::collections::BTreeSet;

        let mut retriever = MockRetriever::new();

        const LEVELS: usize = 12;
        let mut seed = 1u16;
        let mut next_seed = move || {
            let s = seed;
            seed = seed.checked_add(1).expect("seed space exhausted");
            s
        };

        let ev_r = make_test_event_u16(&retriever, next_seed(), &[]);
        let id_r = ev_r.id();
        retriever.add_event(ev_r);

        let mut prev = id_r.clone();
        for _ in 0..LEVELS {
            let ev_a = make_test_event_u16(&retriever, next_seed(), &[prev.clone()]);
            let id_a = ev_a.id();
            retriever.add_event(ev_a);

            let ev_c = loop {
                let candidate = make_test_event_u16(&retriever, next_seed(), &[prev.clone()]);
                if candidate.id() > prev {
                    break candidate;
                }
            };
            let id_c = ev_c.id();
            retriever.add_event(ev_c);

            let ev_b = make_test_event_u16(&retriever, next_seed(), &[id_c.clone()]);
            let id_b = ev_b.id();
            retriever.add_event(ev_b);

            let ev_j = make_test_event_u16(&retriever, next_seed(), &[id_a.clone(), id_b.clone()]);
            prev = ev_j.id();
            retriever.add_event(ev_j);
        }

        let total_events = 1 + 4 * LEVELS;
        let subject = retriever.clock(&[prev.clone()]); // top join
        let comparison = retriever.clock(&[id_r.clone()]); // genesis

        let result = compare(retriever, &subject, &comparison, 2 * total_events).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::StrictDescends { chain } => {
                let unique: BTreeSet<_> = chain.iter().collect();
                assert_eq!(unique.len(), chain.len(), "chain must not contain duplicate events");
                assert_eq!(chain.len(), total_events - 1, "chain covers every event except the comparison head R");
            }
            other => panic!("expected StrictDescends within a 2x-event-count budget (per-event, not per-path); got {:?}", other),
        }
    }

    /// CLAIM B: heads are removed from `outstanding_heads` only at the moment a
    /// node FIRST becomes common, using the origins known at that moment. Here
    /// C2's origin reaches M one step later (via X), so C2 stays outstanding
    /// forever and check_result forces DivergedSince { meet: [] } even though
    /// the meet is plainly {M}.
    ///
    /// DAG: C1 -> M;  C2 -> X -> M;  S -> M;  M is root.
    /// Subject = {S}, comparison = {C1, C2}. Correct: DivergedSince meet [M].
    #[tokio::test]
    async fn late_origin_propagation_yields_empty_meet() {
        let mut retriever = MockRetriever::new();

        let ev_m = make_test_event(&retriever, 1, &[]);
        let id_m = ev_m.id();
        retriever.add_event(ev_m);

        let ev_c1 = make_test_event(&retriever, 2, &[id_m.clone()]);
        let id_c1 = ev_c1.id();
        retriever.add_event(ev_c1);

        let ev_x = make_test_event(&retriever, 3, &[id_m.clone()]);
        let id_x = ev_x.id();
        retriever.add_event(ev_x);

        let ev_c2 = make_test_event(&retriever, 4, &[id_x.clone()]);
        let id_c2 = ev_c2.id();
        retriever.add_event(ev_c2);

        let ev_s = make_test_event(&retriever, 5, &[id_m.clone()]);
        let id_s = ev_s.id();
        retriever.add_event(ev_s);

        let subject = retriever.clock(&[id_s.clone()]);
        let comparison = retriever.clock(&[id_c1.clone(), id_c2.clone()]);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(
                    meet,
                    &vec![id_m.clone()],
                    "meet should be [M]; an empty meet makes entity.rs re-layer from genesis and skips head tip removal"
                );
            }
            other => panic!("expected DivergedSince {{ meet: [M] }}, got {:?}", other),
        }
    }

    /// Found during remediation A2: origins can stall at a node that is already
    /// EXPANDED but not common. Incremental retirement (including the
    /// arrival-at-common case of A2) never sees such a head reach the meet, and
    /// the per-side dedup from A1 means the traversal will not carry it further.
    /// The exhaustion-time reachability reconciliation must retire it.
    ///
    /// DAG: C1 -> K -> M;  H -> L -> K;  S -> M;  M is root.
    /// Subject = {S}, comparison = {C1, H}. The seed search forces
    /// id(K) < id(L), so K is expanded (propagating only C1's origin onward)
    /// before L delivers H's origin to K, where it stalls.
    /// Correct: DivergedSince { meet: [M] }.
    #[tokio::test]
    async fn origin_stalled_at_processed_node_still_retires_head() {
        let mut retriever = MockRetriever::new();

        let ev_m = make_test_event(&retriever, 1, &[]);
        let id_m = ev_m.id();
        retriever.add_event(ev_m);

        let ev_k = make_test_event(&retriever, 2, &[id_m.clone()]);
        let id_k = ev_k.id();
        retriever.add_event(ev_k);

        let ev_c1 = make_test_event(&retriever, 3, &[id_k.clone()]);
        let id_c1 = ev_c1.id();
        retriever.add_event(ev_c1);

        // L: child of K on the longer path; must be expanded after K.
        let ev_l = (10u16..=u16::MAX)
            .map(|seed| make_test_event_u16(&retriever, seed, &[id_k.clone()]))
            .find(|ev| ev.id() > id_k)
            .expect("some seed must yield id(L) > id(K)");
        let id_l = ev_l.id();
        retriever.add_event(ev_l);

        let ev_h = make_test_event(&retriever, 4, &[id_l.clone()]);
        let id_h = ev_h.id();
        retriever.add_event(ev_h);

        let ev_s = make_test_event(&retriever, 5, &[id_m.clone()]);
        let id_s = ev_s.id();
        retriever.add_event(ev_s);

        let subject = retriever.clock(&[id_s.clone()]);
        let comparison = retriever.clock(&[id_c1.clone(), id_h.clone()]);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_m.clone()], "meet should be [M] even though H's origin stalled at K");
            }
            other => panic!("expected DivergedSince {{ meet: [M] }}, got {:?}", other),
        }
    }
}

// ============================================================================
// QUICK-CHECK SUBSET ACCEPTS DISJOINT EXTRA ROOT (adversarial verification)
// ============================================================================

#[cfg(test)]
mod quick_check_disjoint_verify {
    use super::*;

    /// CLAIM 1: the quick-check in comparison.rs (~L100) returns StrictDescends
    /// whenever `comparison_set.is_subset(&all_parents)`, where `all_parents` is
    /// the union of the parents of every subject head event. A subject head that
    /// carries an EXTRA, DISJOINT genesis root X contributes nothing to
    /// all_parents (X has no parents), so the subset test still passes on the
    /// strength of the *other* subject event and StrictDescends is returned even
    /// though X's ancestry is entirely disjoint from the comparison clock.
    ///
    /// DAG (parents point right):  A (root);  B -> A;  X (independent root)
    /// Subject (new head) = {B, X};  comparison (current head) = {A}.
    /// B alone descends from A, but X shares no ancestry with A, so the correct
    /// relation is NOT StrictDescends — it must be Diverged/Disjoint so that
    /// entity.rs does not wholesale-replace head with {B, X} and silently adopt
    /// X's disjoint lineage.
    #[tokio::test]
    async fn test_quick_check_disjoint_extra_root() {
        let mut retriever = MockRetriever::new();

        // A: genesis root of the comparison lineage
        let ev_a = make_test_event(&retriever, 1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(ev_a);

        // B: child of A (this is the part that genuinely descends)
        let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        // X: an INDEPENDENT genesis root, no parents, unrelated to A
        let ev_x = make_test_event(&retriever, 3, &[]);
        let id_x = ev_x.id();
        retriever.add_event(ev_x);

        let subject = retriever.clock(&[id_b.clone(), id_x.clone()]); // new head carries a disjoint extra root
        let comparison = retriever.clock(&[id_a.clone()]); // current head

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        // The bug: quick-check returns StrictDescends. Assert it does NOT.
        assert!(
            !matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }),
            "subject {{B, X}} contains disjoint root X; must not be StrictDescends over {{A}}, got {:?}",
            result.relation
        );
    }

    /// When the guard trips (a subject event's parents are not within the
    /// comparison set), the comparison must fall through to the full BFS and
    /// return its verdict, not error or misclassify.
    ///
    /// DAG: A (root);  B -> A;  D -> B;  C -> A.
    /// Subject = {D}, comparison = {C}: D's parent B is outside {C}, so the
    /// shortcut does not apply; BFS finds the true relation
    /// DivergedSince { meet: [A] }.
    #[tokio::test]
    async fn test_quick_check_guard_falls_through_to_bfs() {
        let mut retriever = MockRetriever::new();

        let ev_a = make_test_event(&retriever, 1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(ev_a);

        let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_d = make_test_event(&retriever, 3, &[id_b.clone()]);
        let id_d = ev_d.id();
        retriever.add_event(ev_d);

        let ev_c = make_test_event(&retriever, 4, &[id_a.clone()]);
        let id_c = ev_c.id();
        retriever.add_event(ev_c);

        let subject = retriever.clock(&[id_d.clone()]);
        let comparison = retriever.clock(&[id_c.clone()]);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_a.clone()], "meet should be [A]");
            }
            other => panic!("expected DivergedSince {{ meet: [A] }} via BFS fallthrough, got {:?}", other),
        }
    }

    /// The single-event form of the V3 shape, found in external review: one
    /// event D whose parent clock joins a legitimate ancestor line with an
    /// independent genesis root. D's head is grounded THROUGH B, so a
    /// per-head lineage-intersection test passes, yet adopting D grafts X's
    /// foreign line into the entity's ancestry. The exploration-boundary rule
    /// blocks the fast-forward: X is a discovered root outside the
    /// comparison's ancestry, so the traversal runs to exhaustion and the
    /// merge machinery handles it as diverged.
    ///
    /// DAG: A (root); B -> A; X (independent root); D -> [B, X].
    /// Subject = {D}, comparison = {A}. Correct: DivergedSince { meet: [A] }.
    #[tokio::test]
    async fn test_single_event_with_disjoint_extra_parent_is_not_strict_descends() {
        let mut retriever = MockRetriever::new();

        let ev_a = make_test_event(&retriever, 1, &[]);
        let id_a = ev_a.id();
        retriever.add_event(ev_a);

        let ev_b = make_test_event(&retriever, 2, &[id_a.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b);

        let ev_x = make_test_event(&retriever, 3, &[]);
        let id_x = ev_x.id();
        retriever.add_event(ev_x);

        let ev_d = make_test_event(&retriever, 4, &[id_b.clone(), id_x.clone()]);
        let id_d = ev_d.id();
        retriever.add_event(ev_d);

        let subject = retriever.clock(&[id_d.clone()]);
        let comparison = retriever.clock(&[id_a.clone()]);

        let result = compare(retriever, &subject, &comparison, 100).await.unwrap();

        assert!(
            !matches!(result.relation, AbstractCausalRelation::StrictDescends { .. }),
            "a graft event must not fast-forward over {{A}}, got {:?}",
            result.relation
        );
        match &result.relation {
            AbstractCausalRelation::DivergedSince { meet, .. } => {
                assert_eq!(meet, &vec![id_a.clone()], "meet should be [A]");
            }
            other => panic!("expected DivergedSince {{ meet: [A] }}, got {:?}", other),
        }
    }
}

/// V4: the StrictDescends "gap-jump" in entity.rs applies ONLY the incoming
/// event's operations and jumps the head, so a bridge batch applied out of
/// causal order silently loses the skipped ancestors' operations. The
/// remediation defense is ordering: batches are topologically sorted
/// (parents first) before application on both the producer and receiver
/// sides. The entity-level defense (replaying staged-but-unapplied ancestors
/// inside the StrictDescends arm) is deliberately NOT part of this
/// remediation; it is the B3 follow-up issue.
#[cfg(test)]
mod strict_descends_gap_jump {
    use super::*;
    use crate::entity::state::EntityState;
    use crate::event_dag::ordering::topo_sort_events;
    use crate::property::backend::lww::LWWBackend;
    use crate::property::backend::PropertyBackend;
    use ankurah_proto::Attested;

    /// Read a committed LWW property value out of the entity's serialized state.
    fn read_lww(entity: &EntityState, name: &str) -> Option<Value> {
        let state = entity.to_state().unwrap();
        let buf = state.state_buffers.0.get("lww")?;
        let backend = LWWBackend::from_state_buffer(buf).unwrap();
        backend.get(&super::prop(name))
    }

    /// A bridge delivering [B, X] child-first must not lose X's operations.
    /// This drives the receiver-side sequence at the unit level: stage the
    /// whole batch (add_event simulates staging), topologically sort, apply
    /// in sorted order. Without the sort, applying B first compares
    /// StrictDescends over head {A} (its ancestry resolves via staging), the
    /// head gap-jumps to {B}, and X's ops are then dropped as StrictAscends:
    /// p1 goes missing while head {B} claims X is history. On a durable node
    /// that wrong state is persisted via set_state and served as canonical.
    #[tokio::test]
    async fn test_strict_descends_gap_jump_skips_ancestor_ops() {
        let entity = crate::entity::state::EntityState::empty();

        let mut retriever = MockRetriever::new();

        // A: genesis create event (empty parent). Establishes head {A}.
        let ev_a = make_lww_event_with_parent(&retriever, 1, vec![("p0", "genesis")], &[]);
        let id_a = ev_a.id();
        retriever.add_event(ev_a.clone());
        assert!(ev_a.is_entity_create(), "A must be a creation event");

        // X: child of A, writes p1. This is the intermediate ancestor.
        let ev_x = make_lww_event_with_parent(&retriever, 2, vec![("p1", "written_by_X")], &[id_a.clone()]);
        let id_x = ev_x.id();
        retriever.add_event(ev_x.clone());

        // B: child of X, writes p2. B descends A through X.
        let ev_b = make_lww_event_with_parent(&retriever, 3, vec![("p2", "written_by_B")], &[id_x.clone()]);
        let id_b = ev_b.id();
        retriever.add_event(ev_b.clone());

        // Establish local head = {A}.
        assert_eq!(entity.apply_events(&retriever, &[&ev_a]).await.unwrap(), vec![true], "A should apply");
        assert_eq!(entity.head(), retriever.clock(&[id_a.clone()]));
        assert_eq!(read_lww(&entity, "p0"), Some(Value::String("genesis".into())));

        // The wire delivers child B before its parent X.
        let batch = vec![Attested::opt(ev_b.clone(), None), Attested::opt(ev_x.clone(), None)];
        let sorted = topo_sort_events(batch).unwrap();
        let sorted_ids: Vec<EventId> = sorted.iter().map(|e| e.payload.id()).collect();
        assert_eq!(sorted_ids, vec![id_x.clone(), id_b.clone()], "sort must place parent X before child B");

        for event in &sorted {
            assert_eq!(entity.apply_events(&retriever, &[&event.payload]).await.unwrap(), vec![true], "each event applies in causal order");
        }

        assert_eq!(entity.head(), retriever.clock(&[id_b.clone()]), "head advanced to B");
        assert_eq!(read_lww(&entity, "p2"), Some(Value::String("written_by_B".into())), "B's op (p2) applied");

        // The V4 loss: without ordered application, p1 is missing while head
        // {B} claims X is part of history.
        let p1 = read_lww(&entity, "p1");
        assert_eq!(
            p1,
            Some(Value::String("written_by_X".into())),
            "CONSISTENCY VIOLATION: head descends X but X's write (p1) is missing. p1={p1:?}"
        );
    }
}

// ============================================================================
// PROPERTY TEST: state machine verdict vs brute-force reachability oracle
// ============================================================================

#[cfg(test)]
mod comparison_property {
    use super::*;
    use std::collections::BTreeSet;

    /// Deterministic xorshift32: no external dependency, and every failure is
    /// reproducible from the printed dag_seed.
    struct Rng(u32);

    impl Rng {
        fn next(&mut self) -> u32 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            self.0 = x;
            x
        }

        fn below(&mut self, n: usize) -> usize { (self.next() as usize) % n }
    }

    /// Inclusive ancestry over the true parent map (the oracle's reachability).
    fn ancestry(parents: &BTreeMap<EventId, Vec<EventId>>, heads: &[EventId]) -> BTreeSet<EventId> {
        let mut out = BTreeSet::new();
        let mut stack: Vec<EventId> = heads.to_vec();
        while let Some(id) = stack.pop() {
            if !out.insert(id.clone()) {
                continue;
            }
            if let Some(ps) = parents.get(&id) {
                stack.extend(ps.iter().cloned());
            }
        }
        out
    }

    /// Reduce a set of events to its maximal antichain: drop members that are
    /// proper ancestors of other members.
    fn to_antichain(parents: &BTreeMap<EventId, Vec<EventId>>, ids: &BTreeSet<EventId>) -> Vec<EventId> {
        ids.iter()
            .filter(|id| !ids.iter().any(|other| other != *id && ancestry(parents, std::slice::from_ref(other)).contains(*id)))
            .cloned()
            .collect()
    }

    #[derive(Debug)]
    enum Expected {
        Equal,
        StrictDescends,
        StrictAscends,
        DivergedMeet(Vec<EventId>),
        Disjoint,
    }

    /// Brute-force specification of the comparison verdict. Precedence mirrors
    /// the machine: Equal, then the strict completions (which fire during
    /// traversal), then the exhaustion verdicts.
    fn oracle(parents: &BTreeMap<EventId, Vec<EventId>>, subject: &[EventId], comparison: &[EventId]) -> Expected {
        let s_set: BTreeSet<EventId> = subject.iter().cloned().collect();
        let c_set: BTreeSet<EventId> = comparison.iter().cloned().collect();
        if s_set == c_set {
            return Expected::Equal;
        }

        let s_cover = ancestry(parents, subject);
        let c_cover = ancestry(parents, comparison);

        // StrictDescends: cover containment plus root containment. Every
        // genesis root in the subject's cover must lie within the comparison's
        // cover; a foreign root (juxtaposed as an extra head, or grafted in
        // through one event's multi-parent clock) blocks adoption. Root
        // containment implies the older per-head grounding condition: every
        // subject head's ancestry bottoms out in shared lineage.
        if c_cover.is_subset(&s_cover)
            && s_cover.iter().filter(|id| parents.get(*id).is_some_and(|p| p.is_empty())).all(|root| c_cover.contains(root))
        {
            return Expected::StrictDescends;
        }

        // StrictAscends is deliberately cover-containment only (it drives
        // skip-incoming decisions, where covering suffices).
        if s_cover.is_subset(&c_cover) {
            return Expected::StrictAscends;
        }

        let common: BTreeSet<EventId> = s_cover.intersection(&c_cover).cloned().collect();
        if common.is_empty() {
            return Expected::Disjoint;
        }

        // A comparison head sharing nothing with the subject's cover cannot be
        // accounted to any common node: the exhaustion gate reports the
        // conservative empty meet. (Disjoint is reserved for no shared
        // history at all.)
        if comparison.iter().any(|h| ancestry(parents, std::slice::from_ref(h)).intersection(&s_cover).next().is_none()) {
            return Expected::DivergedMeet(vec![]);
        }

        Expected::DivergedMeet(to_antichain(parents, &common))
    }

    fn verdict_matches(expected: &Expected, actual: &AbstractCausalRelation<EventId>) -> bool {
        match (expected, actual) {
            (Expected::Equal, AbstractCausalRelation::Equal) => true,
            (Expected::StrictDescends, AbstractCausalRelation::StrictDescends { .. }) => true,
            (Expected::StrictAscends, AbstractCausalRelation::StrictAscends) => true,
            (Expected::DivergedMeet(want), AbstractCausalRelation::DivergedSince { meet, .. }) => {
                let mut got = meet.clone();
                got.sort();
                let mut want = want.clone();
                want.sort();
                got == want
            }
            (Expected::Disjoint, AbstractCausalRelation::Disjoint { .. }) => true,
            _ => false,
        }
    }

    /// Base of the `dag_seed` range, overridable so a nightly or manual run can
    /// shift the window without overlapping a prior run's coverage. Default
    /// matches the historical hardcoded start (`1`) so ordinary test runs are
    /// unchanged.
    fn oracle_seed_base() -> u32 { std::env::var("ORACLE_SEED_BASE").ok().and_then(|s| s.parse().ok()).unwrap_or(1) }

    /// Count of `dag_seed` values to run, overridable for the nightly high-seed
    /// scale-out (100k+). Default matches the historical hardcoded count
    /// (`300`) so ordinary test runs are unchanged.
    fn oracle_seed_count() -> u32 { std::env::var("ORACLE_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(300) }

    /// The self-contained, one-line seeded-failure artifact. Everything needed
    /// to reproduce the exact failing case from the log line alone: the
    /// dag_seed (which deterministically regenerates the DAG and all four
    /// subject/comparison pairs via the xorshift `Rng`), the mismatching
    /// verdict, and the precise command to re-run just that seed. Format
    /// mirrors C1's `SIMFAIL` line (space-separated `key=value` tokens, one
    /// line, no wrapping).
    fn artifact_line(
        dag_seed: u32,
        subject_ids: &[EventId],
        comparison_ids: &[EventId],
        expected: &Expected,
        actual: &AbstractCausalRelation<EventId>,
    ) -> String {
        format!(
            "ORACLEFAIL dag_seed={dag_seed} subject={subject_ids:?} comparison={comparison_ids:?} expected={expected:?} actual={actual:?} reproduce=\"ORACLE_SEED_BASE={dag_seed} ORACLE_SEEDS=1 cargo test -p ankurah-core --lib event_dag::tests::comparison_property::randomized_dags_match_reachability_oracle -- --exact --nocapture\""
        )
    }

    /// Randomized small DAGs (including extra genesis roots), random antichain
    /// clocks, machine verdict checked against the oracle.
    ///
    /// The `dag_seed` range is `ORACLE_SEED_BASE..(ORACLE_SEED_BASE +
    /// ORACLE_SEEDS)`, defaulting to `1..=300` (unchanged from before these
    /// env vars existed). The nightly scale-out (C2) sets `ORACLE_SEEDS` to
    /// 100k+; a hit prints an `ORACLEFAIL` line (see `artifact_line`) before
    /// panicking, so any divergence is reproducible from the log alone.
    #[tokio::test]
    async fn randomized_dags_match_reachability_oracle() {
        let seed_base = oracle_seed_base();
        let seed_count = oracle_seed_count();
        for dag_seed in seed_base..seed_base.saturating_add(seed_count) {
            let mut rng = Rng(dag_seed.wrapping_mul(0x9E37_79B9) | 1);
            let n_events = 5 + rng.below(6); // 5..=10

            let mut retriever = MockRetriever::new();
            let mut ids: Vec<EventId> = Vec::new();
            let mut parents_map: BTreeMap<EventId, Vec<EventId>> = BTreeMap::new();

            for i in 0..n_events {
                // The first event is always a root; later events occasionally
                // start a fresh root to exercise disjoint and foreign-lineage
                // shapes.
                let parent_ids: Vec<EventId> = if i == 0 || rng.below(6) == 0 {
                    vec![]
                } else {
                    let mut chosen = BTreeSet::new();
                    for _ in 0..(1 + rng.below(2)) {
                        chosen.insert(ids[rng.below(ids.len())].clone());
                    }
                    // Parent clocks are antichains in honest input.
                    to_antichain(&parents_map, &chosen)
                };

                let ev = make_test_event_u16(&retriever, i as u16 + 1, &parent_ids);
                let id = ev.id();
                parents_map.insert(id.clone(), parent_ids);
                ids.push(id);
                retriever.add_event(ev);
            }

            for _pair in 0..4 {
                let mut pick_clock = |rng: &mut Rng| -> Vec<EventId> {
                    let mut set = BTreeSet::new();
                    for _ in 0..(1 + rng.below(3)) {
                        set.insert(ids[rng.below(ids.len())].clone());
                    }
                    to_antichain(&parents_map, &set)
                };
                let subject_ids = pick_clock(&mut rng);
                let comparison_ids = pick_clock(&mut rng);

                let subject = retriever.clock(&subject_ids);
                let comparison = retriever.clock(&comparison_ids);

                let result = compare(retriever.clone(), &subject, &comparison, 4 * n_events).await.unwrap();
                let expected = oracle(&parents_map, &subject_ids, &comparison_ids);

                if !verdict_matches(&expected, &result.relation) {
                    panic!("{}", artifact_line(dag_seed, &subject_ids, &comparison_ids, &expected, &result.relation));
                }
            }
        }
    }

    #[test]
    fn artifact_line_format_is_self_contained() {
        let subject_ids = vec![];
        let comparison_ids = vec![];
        let expected = Expected::Disjoint;
        let actual = AbstractCausalRelation::Equal;
        let line = artifact_line(42, &subject_ids, &comparison_ids, &expected, &actual);

        assert!(line.starts_with("ORACLEFAIL "), "artifact line must start with the ORACLEFAIL marker: {line}");
        assert!(line.contains("dag_seed=42"), "artifact line must carry the exact dag_seed: {line}");
        assert!(line.contains("expected=Disjoint"), "artifact line must carry the expected verdict: {line}");
        assert!(line.contains("actual=Equal"), "artifact line must carry the actual verdict: {line}");
        assert!(
            line.contains(
                "reproduce=\"ORACLE_SEED_BASE=42 ORACLE_SEEDS=1 cargo test -p ankurah-core --lib event_dag::tests::comparison_property::randomized_dags_match_reachability_oracle -- --exact --nocapture\""
            ),
            "artifact line must carry a copy-pasteable single-seed repro command: {line}"
        );
        assert!(!line.contains('\n'), "artifact line must be a single line: {line}");
    }
}

mod lww_delivery_order {
    use super::*;
    use crate::entity::state::EntityState;

    fn write(seed: u8, value: &str, parents: &[&Event]) -> Event {
        let backend = LWWBackend::new();
        backend.set(prop("p"), Some(Value::String(value.into())));
        let operations = OperationSet::from_backends(BTreeMap::from([("lww".into(), backend.to_operations().unwrap().unwrap())]));
        let parent = Clock::from_events(parents.iter().copied());
        Event { entity_id: EntityId::from_bytes([seed; 32]), body: fixture_body(&[seed], &parent, operations), parent }
    }

    #[tokio::test]
    async fn triangle_converges_in_every_causal_delivery_order() {
        // G -> X1 -> X2 and G -> Y, with X1.id > Y.id > X2.id.
        // Pairwise causal-order/ID comparison loses a different candidate in
        // each delivery order; generation ranking must retain X2 in all three.
        let (g, x1, x2, y) = (0..=255)
            .map(|seed| {
                let g = write(seed, "G", &[]);
                let x1 = write(seed, "X1", &[&g]);
                let x2 = write(seed, "X2", &[&x1]);
                let y = write(seed, "Y", &[&g]);
                (g, x1, x2, y)
            })
            .find(|(_, x1, x2, y)| x1.id() > y.id() && y.id() > x2.id())
            .unwrap();
        let mut retriever = MockRetriever::new();
        for event in [&g, &x1, &x2, &y] {
            retriever.add_event(event.clone());
        }

        let mut states = Vec::new();
        for order in [[&g, &x1, &x2, &y], [&g, &x1, &y, &x2], [&g, &y, &x1, &x2]] {
            let entity = EntityState::empty();
            for event in order {
                assert_eq!(entity.apply_events(&retriever, &[event]).await.unwrap(), vec![true]);
            }
            assert_eq!(entity.head(), Clock::from_events([&x2, &y]));
            states.push(entity.to_state().unwrap().state_buffers);
        }
        assert_eq!(states[0], states[1]);
        assert_eq!(states[0], states[2]);
        let backend = LWWBackend::from_state_buffer(&states[0]["lww"]).unwrap();
        assert_eq!(backend.get(&prop("p")), Some(Value::String("X2".into())));
        assert_eq!(backend.get_event_id(&prop("p")), Some(x2.id()));
    }

    #[tokio::test]
    async fn unequal_branches_keep_deepest_write_in_every_interleaving() {
        // Four positions for Y among the three ordered X writes, with sixteen
        // different ID orderings. This exercises repeated direct and layer merges.
        for seed in 0..16 {
            let g = write(seed, "G", &[]);
            let x1 = write(seed, "X1", &[&g]);
            let x2 = write(seed, "X2", &[&x1]);
            let x3 = write(seed, "X3", &[&x2]);
            let y = write(seed, "Y", &[&g]);
            let mut retriever = MockRetriever::new();
            for event in [&g, &x1, &x2, &x3, &y] {
                retriever.add_event(event.clone());
            }
            for position in 1..=4 {
                let mut order = vec![&g, &x1, &x2, &x3];
                order.insert(position, &y);
                let entity = EntityState::empty();
                for event in &order {
                    assert_eq!(entity.apply_events(&retriever, &[event]).await.unwrap(), vec![true]);
                }
                // Every batch boundary must yield the same head and backend state as single-event delivery.
                for split in 0..=order.len() {
                    let batched = EntityState::empty();
                    batched.apply_events(&retriever, &order[..split]).await.unwrap();
                    batched.apply_events(&retriever, &order[split..]).await.unwrap();
                    assert_eq!(
                        batched.to_state().unwrap(),
                        entity.to_state().unwrap(),
                        "seed {seed}, Y position {position}, split {split}"
                    );
                }
                assert_eq!(entity.head(), Clock::from_events([&x3, &y]));
                assert_eq!(entity.value(&prop("p")), Some(Value::String("X3".into())), "seed {seed}, Y position {position}");
            }
        }
    }
}

#[cfg(test)]
mod generation_comparison {
    use super::*;
    use crate::error::{LineageError, MutationError};
    use crate::event_dag::{accumulator::EventAccumulator, CausalRelation, EventLayers};

    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct CountingRetriever {
        inner: MockRetriever,
        reads: Arc<AtomicUsize>,
    }

    impl CountingRetriever {
        fn new(inner: MockRetriever) -> Self { Self { inner, reads: Arc::new(AtomicUsize::new(0)) } }
        fn reads(&self) -> usize { self.reads.load(Ordering::Relaxed) }
    }

    #[async_trait]
    impl GetEvents for CountingRetriever {
        async fn get_event(&self, id: &EventId) -> Result<Event, RetrievalError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get_event(id).await
        }
        async fn event_stored(&self, id: &EventId) -> Result<bool, RetrievalError> { self.inner.event_stored(id).await }
    }

    fn linear_history(length: u16) -> (MockRetriever, Vec<Event>) {
        let mut retriever = MockRetriever::new();
        let mut events: Vec<Event> = Vec::new();
        for seed in 1..=length {
            let parents = events.last().map(|event| vec![event.id()]).unwrap_or_default();
            let event = make_test_event_u16(&retriever, seed, &parents);
            retriever.add_event(event.clone());
            events.push(event);
        }
        (retriever, events)
    }

    #[tokio::test]
    async fn divergent_branches_do_not_read_the_long_shared_tail() {
        for depth in [1, 5, 20] {
            let (mut retriever, events) = linear_history(300);
            let meet = events.last().unwrap();
            let mut left = meet.clone();
            let mut right = meet.clone();
            let mut left_chain = Vec::new();
            let mut right_chain = Vec::new();
            for step in 0..depth {
                left = make_test_event_u16(&retriever, 400 + 2 * step, &[left.id()]);
                retriever.add_event(left.clone());
                left_chain.push(left.id());
                right = make_test_event_u16(&retriever, 401 + 2 * step, &[right.id()]);
                retriever.add_event(right.clone());
                right_chain.push(right.id());
            }
            let retriever = CountingRetriever::new(retriever);
            let result = compare(retriever.clone(), &Clock::singleton(&left), &Clock::singleton(&right), 1000).await.unwrap();
            assert_eq!(
                result.relation,
                AbstractCausalRelation::DivergedSince {
                    meet: vec![meet.id()],
                    subject: vec![left_chain[0].clone()],
                    other: vec![right_chain[0].clone()],
                    subject_chain: left_chain,
                    other_chain: right_chain,
                }
            );
            assert_eq!(retriever.reads(), usize::from(2 * depth + 1), "depth {depth}");
        }
    }

    #[tokio::test]
    async fn uneven_ancestor_comparisons_do_not_read_below_the_older_head() {
        let (events, history) = linear_history(320);
        let ancestor = Clock::singleton(&history[299]);
        let descendant = Clock::singleton(&history[319]);
        let retriever = CountingRetriever::new(events.clone());
        let result = compare(retriever.clone(), &descendant, &ancestor, 1000).await.unwrap();
        assert_eq!(result.relation, AbstractCausalRelation::StrictDescends { chain: history[300..].iter().map(Event::id).collect() });
        assert!(retriever.reads() <= 21, "read {} events", retriever.reads());

        let retriever = CountingRetriever::new(events);
        let result = compare(retriever.clone(), &ancestor, &descendant, 1000).await.unwrap();
        assert_eq!(result.relation, AbstractCausalRelation::StrictAscends);
        assert!(retriever.reads() <= 21, "read {} events", retriever.reads());
    }

    #[tokio::test]
    async fn unresolved_meet_nodes_cannot_be_ordered_by_unverified_parent_claims() {
        let (mut retriever, chain) = linear_history(4);
        let c2 = &chain[2];
        let parent = &chain[3];
        let mut c1 = make_test_event(&retriever, 10, &[parent.id()]);
        // A second parent keeps c1's own generation honest-looking while its
        // annotation hides that the first parent descends the other meet node.
        let mut sibling = chain[0].clone();
        for seed in 11..=13 {
            sibling = make_test_event(&retriever, seed, &[sibling.id()]);
            retriever.add_event(sibling.clone());
        }
        c1.parent = Clock::new(vec![(1, parent.id()), (sibling.generation(), sibling.id())]).unwrap();
        retriever.add_event(c1.clone());
        let left = make_test_event(&retriever, 14, &[c1.id(), c2.id()]);
        retriever.add_event(left.clone());
        let right = make_test_event(&retriever, 15, &[c1.id(), c2.id()]);
        retriever.add_event(right.clone());

        let result = compare(retriever, &Clock::singleton(&left), &Clock::singleton(&right), 100).await;
        assert!(
            matches!(result, Err(RetrievalError::EventStructure(EventStructureError::GenerationMismatch { event, claimed: 1, known: 4 })) if event == parent.id())
        );
    }

    #[tokio::test]
    async fn saturated_generations_keep_the_exhaustive_walk() {
        // The opaque ancestor represents a snapshot whose earlier history is unavailable.
        let opaque = EventId::from_bytes([42; 32]);
        let mut meet = make_test_event_over(1, &[]);
        meet.parent = Clock::new(vec![(u32::MAX - 2, opaque)]).unwrap();
        meet.body = fixture_body(&[1], &meet.parent, OperationSet::default());
        let left = make_test_event_over(2, &[&meet]);
        let right = make_test_event_over(3, &[&meet]);
        let mut events = MockRetriever::new();
        for event in [&meet, &left, &right] {
            events.add_event(event.clone());
        }
        let retriever = CountingRetriever::new(events);
        let result = compare(retriever.clone(), &Clock::singleton(&left), &Clock::singleton(&right), 100).await.unwrap();
        let AbstractCausalRelation::DivergedSince { meet: found, .. } = result.relation else { panic!("expected divergence") };
        assert_eq!(found, vec![meet.id()]);
        assert_eq!(retriever.reads(), 4, "the walk includes the unavailable common ancestor below the meet");
    }

    #[tokio::test]
    async fn merge_preserves_writer_below_one_of_multiple_meet_points() {
        // R -> P -> V -> M1 -> C; R -> M2 -> C; M2 -> S; (M1, S) -> J.
        // V and S write x concurrently at the same generation. V has the greater
        // ID and must survive, although it is below only M1 in the shared meet.
        let mut retriever = MockRetriever::new();
        let root = make_test_event(&retriever, 1, &[]);
        retriever.add_event(root.clone());
        let prior = make_test_event(&retriever, 2, &[root.id()]);
        retriever.add_event(prior.clone());
        let stored = make_lww_event_with_parent(&retriever, 3, vec![("x", "stored")], &[prior.id()]);
        retriever.add_event(stored.clone());
        let m1 = make_test_event(&retriever, 4, &[stored.id()]);
        retriever.add_event(m1.clone());
        let m2 = make_test_event(&retriever, 5, &[root.id()]);
        retriever.add_event(m2.clone());
        let incoming = (6..=255)
            .map(|seed| make_lww_event_with_parent(&retriever, seed, vec![("x", "incoming")], &[m2.id()]))
            .find(|event| event.id() < stored.id())
            .unwrap();
        retriever.add_event(incoming.clone());
        assert_eq!(incoming.generation(), stored.generation());
        let current = make_test_event(&retriever, 7, &[m1.id(), m2.id()]);
        retriever.add_event(current.clone());
        let joined = make_test_event(&retriever, 8, &[m1.id(), incoming.id()]);
        retriever.add_event(joined.clone());

        let result = compare(&retriever, &Clock::singleton(&joined), &Clock::singleton(&current), 100).await.unwrap();
        let AbstractCausalRelation::DivergedSince { meet, .. } = &result.relation else { panic!("expected divergence") };
        assert_eq!(meet.iter().cloned().collect::<std::collections::BTreeSet<_>>(), [m1.id(), m2.id()].into_iter().collect());
        let backend = LWWBackend::new();
        backend.apply_layer(&layer_from_refs(&[], &[&stored])).unwrap();
        let layers = EventLayers::prepare(&retriever, &[&joined], &Clock::singleton(&current)).await.unwrap();
        for layer in layers.layers() {
            backend.apply_layer(layer).unwrap();
        }
        assert_eq!(backend.get(&prop("x")), Some(Value::String("stored".into())));
    }
    #[tokio::test]
    async fn skips_applied_frontiers_without_losing_later_events_or_policy_boundaries() {
        // R -> H (resident), R -> first -> last. The incoming clock keeps H too.
        // Exhaustive history includes R below the compared head; traverse it
        // without an application step before reaching the new branch.
        let root = make_test_event_over(1, &[]);
        let head = make_test_event_over(2, &[&root]);
        let first = make_test_event_over(3, &[&root]);
        let last = make_test_event_over(4, &[&first]);
        let mut accumulator = EventAccumulator::new(MockRetriever::new());
        for event in [&root, &head, &first, &last] {
            accumulator.accumulate(event);
        }
        let layers = EventLayers::load(accumulator, vec![head.id()], vec![head.id()], &[&head, &first, &last]).await.unwrap();
        assert_eq!(layers.per_event().map(|layers| layers.len()).collect::<Vec<_>>(), vec![0, 1, 1]);
        let [first_layer, last_layer] = layers.layers() else { panic!("expected the two new writes") };
        assert_eq!(first_layer.to_apply, vec![first.clone()]);
        assert_eq!(last_layer.to_apply, vec![last.clone()]);
        assert!(first_layer.already_applied.is_empty());
        assert!(last_layer.already_applied.is_empty());
        assert_eq!(last_layer.compare(&last.id(), &root.id()), CausalRelation::Descends);
    }

    #[tokio::test]
    async fn already_applied_comparisons_have_no_layers() {
        let (retriever, history) = linear_history(2);
        let current = Clock::singleton(&history[1]);
        let layers = EventLayers::prepare(&retriever, &[&history[1], &history[1]], &current).await.unwrap();
        assert!(layers.layers().is_empty());
        assert_eq!(layers.per_event().map(|layers| layers.len()).collect::<Vec<_>>(), vec![0, 0]);

        let layers = EventLayers::prepare(&retriever, &[&history[0]], &current).await.unwrap();
        assert!(layers.layers().is_empty());
        assert_eq!(layers.per_event().map(|layers| layers.len()).collect::<Vec<_>>(), vec![0]);
    }

    #[tokio::test]
    async fn disjoint_comparison_cannot_produce_layers() {
        let left = make_test_event_over(1, &[]);
        let right = make_test_event_over(2, &[]);
        let mut retriever = MockRetriever::new();
        retriever.add_event(left.clone());
        retriever.add_event(right.clone());
        assert!(matches!(
            EventLayers::prepare(&retriever, &[&left], &Clock::singleton(&right)).await,
            Err(MutationError::LineageError(LineageError::Disjoint))
        ));
    }

    #[tokio::test]
    async fn exhausted_preparation_returns_its_budget_and_frontiers() {
        // Equally deep branches pass the generation bound but cannot meet within the history budget.
        let (mut retriever, history) = linear_history(5000);
        let mut other = history[0].clone();
        for seed in 5001..10000 {
            other = make_test_event_u16(&retriever, seed, &[other.id()]);
            retriever.add_event(other.clone());
        }
        assert!(matches!(
            EventLayers::prepare(&retriever, &[history.last().unwrap()], &Clock::singleton(&other)).await,
            Err(MutationError::LineageError(LineageError::BudgetExceeded {
                original_budget, subject_frontier, other_frontier,
            })) if original_budget == crate::event_dag::DEFAULT_BUDGET + 1
                && !subject_frontier.is_empty() && !other_frontier.is_empty()
        ));
    }

    #[tokio::test]
    async fn direct_extension_builds_one_layer_without_reading_either_head_tip() {
        let root = make_test_event_over(1, &[]);
        let left = make_test_event_over(2, &[&root]);
        let right = make_test_event_over(3, &[&root]);
        let incoming = make_test_event_over(4, &[&left, &right]);
        let mut events = MockRetriever::new();
        events.add_event(incoming.clone());
        let retriever = CountingRetriever::new(events);
        let layers = EventLayers::prepare(&retriever, &[&incoming], &Clock::from_events([&left, &right])).await.unwrap();
        let [layer] = layers.layers() else { panic!("expected one layer") };
        assert!(layer.already_applied.is_empty());
        assert_eq!(layer.to_apply, vec![incoming.clone()]);
        assert_eq!(layer.compare(&incoming.id(), &left.id()), CausalRelation::Descends);
        assert_eq!(layer.compare(&incoming.id(), &right.id()), CausalRelation::Descends);
        assert_eq!(retriever.reads(), 1, "only the staged incoming event was read");
    }

    #[tokio::test]
    async fn empty_head_builds_a_genesis_layer_only_when_application_is_requested() {
        let genesis = make_test_event_over(1, &[]);
        let mut events = MockRetriever::new();
        events.add_event(genesis.clone());
        let retriever = CountingRetriever::new(events);
        compare(retriever.clone(), &Clock::singleton(&genesis), &Clock::default(), 100).await.unwrap();
        assert_eq!(retriever.reads(), 0, "empty-head comparison needs no history");
        let layers = EventLayers::prepare(&retriever, &[&genesis], &Clock::default()).await.unwrap();
        let [layer] = layers.layers() else { panic!("expected one layer") };
        assert!(layer.already_applied.is_empty());
        assert_eq!(layer.to_apply, vec![genesis]);
        assert_eq!(retriever.reads(), 1);
    }

    #[tokio::test]
    async fn empty_head_rejects_an_update_without_its_creation_batch() {
        let genesis = make_test_event_over(1, &[]);
        let update = make_test_event_over(2, &[&genesis]);
        let mut events = MockRetriever::new();
        events.add_event(update.clone());
        let retriever = CountingRetriever::new(events);
        assert!(matches!(EventLayers::prepare(&retriever, &[&update], &Clock::default()).await, Err(MutationError::InvalidEvent)));
        assert_eq!(retriever.reads(), 0);
    }
}
