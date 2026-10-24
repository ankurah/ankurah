use super::*;
use ankurah_proto::{AuthorId, EventId, EventStructureError};
use std::sync::Mutex;

#[test]
fn borrowed_read_agrees_with_storage_evaluation_and_clones_only_requested_metadata() -> anyhow::Result<()> {
    use crate::{
        entity::TemporaryEntity,
        property::backend::{lww::LWWBackend, yrs::YrsBackend},
        selection::filter::{Filterable, ValueLookup},
    };
    use ankurah_proto::{EntityId, SystemProperty};
    let state = EntityState::empty();
    let lww = state.get_backend::<LWWBackend>()?;
    let yrs = state.get_backend::<YrsBackend>()?;
    let first = PropertyId::System(SystemProperty::Label);
    let fallback = PropertyId::System(SystemProperty::Name);
    let json_id = EntityId::from_bytes([8; 32]);
    lww.set(first, Some(Value::String("first backend".into())));
    lww.set(fallback, None);
    lww.set(PropertyId::EntityId(json_id), Some(Value::Json(serde_json::json!({"nested": {"value": 7}}))));
    let event = crate::test_helpers::commit_backend(&*lww, 1)?;
    yrs.insert(&first, 0, "later backend")?;
    yrs.insert(&fallback, 0, "fallback value")?;
    let model = ModelId::EntityId(EntityId::from_bytes([3; 32]));
    state.add_membership(model);
    state.set_head(Clock::singleton(&event));
    let copied_head = state.with_read(event.entity_id, |read| -> anyhow::Result<Clock> {
        let oracle = TemporaryEntity::new(event.entity_id, &read.to_state()?)?;
        for (property, expected) in [
            (first, Some(Value::String("first backend".into()))),
            (fallback, Some(Value::String("fallback value".into()))),
            (PropertyId::Id, Some(Value::EntityId(event.entity_id))),
        ] {
            assert_eq!(read.value(&property), expected);
            assert_eq!(read.value(&property), oracle.value(&property));
        }
        let path = ankql::ast::PropertyPath::registered(json_id, "json", vec!["nested".into(), "value".into()]);
        assert_eq!(read.value_at(&path), Some(Value::I64(7)));
        assert!(Filterable::is_member_of(read, &model)?);
        assert_eq!(read.head(), &Clock::singleton(&event));
        Ok(read.head().clone())
    })?;
    lww.set(first, Some(Value::String("new version".into())));
    let changed = crate::test_helpers::commit_backend(&*lww, 2)?;
    state.set_head(Clock::singleton(&changed));
    state.with_read(event.entity_id, |read| {
        assert_eq!(read.value(&first), Some(Value::String("new version".into())));
        assert_eq!(read.head(), &Clock::singleton(&changed));
    });
    assert_eq!(copied_head, Clock::singleton(&event));
    Ok(())
}

/// Serves the staged event and records attempts to read any other history.
struct IncomingOnlyGetter {
    incoming: Event,
    asked: Mutex<Vec<EventId>>,
}

impl IncomingOnlyGetter {
    fn new(incoming: &Event) -> Self { Self { incoming: incoming.clone(), asked: Mutex::default() } }
}

#[async_trait::async_trait]
impl GetEvents for IncomingOnlyGetter {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        self.asked.lock().unwrap().push(event_id.clone());
        if *event_id == self.incoming.id() {
            return Ok(self.incoming.clone());
        }
        Err(RetrievalError::EventNotFound(event_id.clone()))
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        self.asked.lock().unwrap().push(event_id.clone());
        Ok(false)
    }
}

/// A direct extension checks every parent's generation without reading their payloads.
#[tokio::test]
async fn an_update_checks_each_parent_without_reading_its_event() -> anyhow::Result<()> {
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
    let older = Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, OperationSet::default());
    let sibling = Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, OperationSet::default());
    let newer = Event::update(genesis.entity_id, Clock::singleton(&sibling), AuthorId::Unknown, OperationSet::default());
    // An accepted snapshot supplies the two tip generations without retaining their events.
    let head = Clock::from_events([&older, &newer]);
    let state = EntityState::empty();
    state.set_head(head.clone());

    // Both false claims for the older parent leave the update's generation at 4.
    for claimed in [1, 3] {
        let update = Event::update(
            genesis.entity_id,
            Clock::new(vec![(claimed, older.id()), (newer.generation(), newer.id())])?,
            AuthorId::Unknown,
            OperationSet::default(),
        );
        assert_eq!(update.generation(), 4);
        let getter = IncomingOnlyGetter::new(&update);
        let error = state.apply_events(&getter, &[&update]).await.unwrap_err();
        assert!(
            matches!(
                &error,
                MutationError::EventStructure(EventStructureError::GenerationMismatch { event: parent, claimed: c, known: 2 })
                    if parent == &older.id() && *c == claimed
            ),
            "{error}"
        );
        assert!(getter.asked.lock().unwrap().iter().all(|id| *id == update.id()), "the direct path reads no event but the update");
        assert_eq!(state.head(), head);
    }

    let honest = Event::update(genesis.entity_id, head, AuthorId::Unknown, OperationSet::default());
    assert_eq!(state.apply_events(&IncomingOnlyGetter::new(&honest), &[&honest]).await?, vec![true]);
    Ok(())
}

#[tokio::test]
async fn equal_snapshot_rejects_conflicting_generations_without_changing_state() -> anyhow::Result<()> {
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
    let state = EntityState::empty();
    state.set_head(Clock::singleton(&genesis));
    let before = state.to_state()?;
    let mut snapshot = before.clone();
    snapshot.head = Clock::new(vec![(2, genesis.id())])?;
    let getter = IncomingOnlyGetter::new(&genesis);
    assert!(matches!(
        state.apply_state(&getter, &snapshot).await,
        Err(MutationError::EventStructure(EventStructureError::GenerationMismatch { event, claimed: 2, known: 1 }))
            if event == genesis.id()
    ));
    assert_eq!(state.to_state()?, before);
    assert!(getter.asked.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn publication_checks_the_complete_head_and_keeps_backend_copies_independent() -> anyhow::Result<()> {
    use crate::property::backend::LWWBackend;
    // Synthetic heads isolate the publication guard and copying behavior from event application.
    let base = Clock::new(vec![(2, EventId::from_bytes([1; 32]))]).unwrap();
    let resident = EntityState::empty();
    resident.set_head(base.clone());
    let property = PropertyId::System(ankurah_proto::SystemProperty::Name);
    resident.get_backend::<LWWBackend>()?.set(property, Some(Value::String("before".into())));
    let prepared = resident.fork();
    prepared.get_backend::<LWWBackend>()?.set(property, Some(Value::String("prepared".into())));
    prepared.set_head(Clock::new(vec![(3, EventId::from_bytes([2; 32]))]).unwrap());

    resident.set_head(Clock::new(vec![(7, EventId::from_bytes([1; 32]))]).unwrap());
    assert!(!resident.replace_if_head_matches(&base, &prepared), "a changed annotation is a changed resident, even with the same tip ID");
    assert_eq!(resident.value(&property), Some(Value::String("before".into())));

    resident.set_head(base.clone());
    assert!(resident.replace_if_head_matches(&base, &prepared));
    assert_eq!(resident.head(), prepared.head());
    assert_eq!(resident.value(&property), Some(Value::String("prepared".into())));
    prepared.get_backend::<LWWBackend>()?.set(property, Some(Value::String("later transaction mutation".into())));
    assert_eq!(resident.value(&property), Some(Value::String("prepared".into())));
    Ok(())
}

/// Serves a fixed set of events and records every id asked for.
struct ServedGetter {
    events: Vec<Event>,
    asked: Mutex<Vec<EventId>>,
}

impl ServedGetter {
    fn of(events: &[&Event]) -> Self { Self { events: events.iter().map(|event| (*event).clone()).collect(), asked: Mutex::default() } }
}

#[async_trait::async_trait]
impl GetEvents for ServedGetter {
    async fn get_event(&self, event_id: &EventId) -> Result<Event, RetrievalError> {
        self.asked.lock().unwrap().push(event_id.clone());
        self.events.iter().find(|event| event.id() == *event_id).cloned().ok_or_else(|| RetrievalError::EventNotFound(event_id.clone()))
    }

    async fn event_stored(&self, event_id: &EventId) -> Result<bool, RetrievalError> {
        Ok(self.events.iter().any(|event| event.id() == *event_id))
    }
}

/// An update may claim at most one above the head's greatest tip generation, and is refused before any read when it
/// claims more. The forged update names the tip honestly and the tip's own ancestor at a false generation, which the
/// comparison alone would never read: it settles a direct extension from the tip's parent links. An honest
/// concurrent update within the bound still applies.
#[tokio::test]
async fn an_update_claiming_more_than_one_above_the_head_is_refused_before_any_read() -> anyhow::Result<()> {
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
    let tip = Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, OperationSet::default());
    let forged = Event::update(
        genesis.entity_id,
        Clock::new(vec![(tip.generation(), tip.id()), (1000, genesis.id())])?,
        AuthorId::Unknown,
        OperationSet::default(),
    );
    let concurrent = Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, OperationSet::default());
    assert_eq!((tip.generation(), forged.generation(), concurrent.generation()), (2, 1001, 2));

    let served = ServedGetter::of(&[&genesis, &tip, &forged, &concurrent]);
    let state = EntityState::empty();
    assert_eq!(state.apply_events(&served, &[&genesis]).await?, vec![true]);
    assert_eq!(state.apply_events(&served, &[&tip]).await?, vec![true]);
    assert_eq!(state.head(), Clock::singleton(&tip));

    served.asked.lock().unwrap().clear();
    let error = state.apply_events(&served, &[&forged]).await.unwrap_err();
    assert!(
        matches!(
            &error,
            MutationError::LineageError(LineageError::BeyondHead { event, claimed: 1001, head: 2 })
                if *event == forged.id()
        ),
        "{error}"
    );
    assert!(served.asked.lock().unwrap().is_empty(), "the bound reads nothing");
    assert_eq!(state.head(), Clock::singleton(&tip));

    assert_eq!(state.apply_events(&served, &[&concurrent]).await?, vec![true]);
    assert_eq!(state.head(), Clock::from_events([&tip, &concurrent]));
    Ok(())
}

/// An update above a head that lags its chain has a missing link, even when the link is held: it is refused until
/// the chain is applied link by link, which then brings the head to it.
#[tokio::test]
async fn an_update_above_a_lagging_head_is_refused_until_its_chain_is_applied() -> anyhow::Result<()> {
    let genesis = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
    let first = Event::update(genesis.entity_id, Clock::singleton(&genesis), AuthorId::Unknown, OperationSet::default());
    let second = Event::update(genesis.entity_id, Clock::singleton(&first), AuthorId::Unknown, OperationSet::default());
    let third = Event::update(genesis.entity_id, Clock::singleton(&second), AuthorId::Unknown, OperationSet::default());
    assert_eq!(third.generation(), 4);

    let served = ServedGetter::of(&[&genesis, &first, &second, &third]);
    let state = EntityState::empty();
    assert_eq!(state.apply_events(&served, &[&genesis]).await?, vec![true]);
    let error = state.apply_events(&served, &[&third]).await.unwrap_err();
    assert!(matches!(&error, MutationError::LineageError(LineageError::BeyondHead { claimed: 4, head: 1, .. })), "{error}");
    assert_eq!(state.head(), Clock::singleton(&genesis));

    for event in [&first, &second, &third] {
        assert_eq!(state.apply_events(&served, &[event]).await?, vec![true]);
    }
    assert_eq!(state.head(), Clock::singleton(&third));
    Ok(())
}

#[tokio::test]
async fn multi_layer_application_advances_head_only_after_success() -> anyhow::Result<()> {
    // G -> A -> B -> C; G -> X -> Y. Start at [A, Y], then apply B and C together.
    let g = Event::genesis(None, AuthorId::Unknown, OperationSet::default());
    let a = Event::update(g.entity_id, Clock::singleton(&g), AuthorId::Unknown, OperationSet::default());
    let b = Event::update(g.entity_id, Clock::singleton(&a), AuthorId::Unknown, OperationSet::default());
    let c = Event::update(g.entity_id, Clock::singleton(&b), AuthorId::Unknown, OperationSet::default());
    let x = Event::update(g.entity_id, Clock::singleton(&g), AuthorId::Unknown, OperationSet::default());
    let y = Event::update(g.entity_id, Clock::singleton(&x), AuthorId::Unknown, OperationSet::default());
    let invalid = Event::update(
        g.entity_id,
        Clock::singleton(&b),
        AuthorId::Unknown,
        OperationSet::from_backends(BTreeMap::from([("missing".into(), vec![ankurah_proto::BackendOperation { diff: vec![] }])])),
    );
    let served = ServedGetter::of(&[&g, &a, &b, &c, &x, &y, &invalid]);
    let state = EntityState::empty();
    for event in [&g, &a, &x, &y] {
        assert_eq!(state.apply_events(&served, &[event]).await?, vec![true]);
    }
    assert_eq!(state.head(), Clock::from_events([&a, &y]));

    // Failure in the layer after B must not publish an intermediate [B, Y] head.
    assert!(matches!(
        state.apply_events(&served, &[&invalid]).await,
        Err(MutationError::RetrievalError(RetrievalError::InvalidState(message))) if message == "unknown backend: missing"
    ));
    assert_eq!(state.head(), Clock::from_events([&a, &y]));

    assert_eq!(state.apply_events(&served, &[&c]).await?, vec![true]);
    assert_eq!(state.head(), Clock::from_events([&c, &y]));
    Ok(())
}
