use crate::{entity::Entity, error::MutationError, model::View, reactor::ChangeNotification};
use ankurah_proto::{Attested, Event};

#[derive(Debug, Clone)]
pub struct EntityChange {
    entity: Entity,
    events: Vec<Attested<Event>>,
}

// Implement the trait for EntityChange
impl ChangeNotification for EntityChange {
    type Entity = Entity;
    type Event = ankurah_proto::Attested<Event>;

    fn into_parts(self) -> (Self::Entity, Vec<Self::Event>) { (self.entity, self.events) }
    fn entity(&self) -> &Self::Entity { &self.entity }
    fn events(&self) -> &[Self::Event] { &self.events }
}

// TODO consider a flattened version of EntityChange that includes the entity and Vec<(operations, parent, attestations)> rather than a Vec<Attested<Event>>
impl EntityChange {
    pub fn new(entity: Entity, events: Vec<Attested<Event>>) -> Result<Self, MutationErrorChangeMe> {
        // validate that all events have the same entity id as the entity
        // and that the event ids are present in the entity's head clock
        for event in &events {
            let head = entity.head();
            if event.payload.entity_id != entity.id {
                return Err(MutationError::InvalidUpdate("event entity_id mismatch".to_string()));
            }
            if !head.contains(&event.payload.id()) {
                return Err(MutationError::InvalidUpdate("event id not in entity head clock".to_string()));
            }
        }
        Ok(Self { entity, events })
    }
    pub fn into_parts(self) -> (Entity, Vec<Attested<Event>>) { (self.entity, self.events) }
}

#[derive(Debug, Clone)]
pub enum ItemChange<I> {
    /// Initial retrieval of an item upon subscription
    Initial { item: I },
    /// A new item was added OR changed such that it now matches the subscription
    Add { item: I, events: Vec<Attested<Event>> },
    /// A item that previously matched the subscription has changed in a way that has not changed the matching condition
    Update { item: I, events: Vec<Attested<Event>> },
    /// A item that previously matched the subscription has changed in a way that no longer matches the subscription
    Remove { item: I, events: Vec<Attested<Event>> },
}

impl<I> ItemChange<I> {
    pub fn entity(&self) -> &I {
        match self {
            ItemChange::Initial { item }
            | ItemChange::Add { item, .. }
            | ItemChange::Update { item, .. }
            | ItemChange::Remove { item, .. } => item,
        }
    }

    pub fn events(&self) -> &[Attested<Event>] {
        match self {
            ItemChange::Add { events, .. } | ItemChange::Update { events, .. } | ItemChange::Remove { events, .. } => events,
            _ => &[],
        }
    }
    pub fn kind(&self) -> ChangeKind { ChangeKind::from(self) }
}

impl<I> std::fmt::Display for ItemChange<I>
where I: View
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ItemChange::Initial { item } => {
                write!(f, "Initial {}/{}", I::collection(), item.id())
            }
            ItemChange::Add { item, .. } => {
                write!(f, "Add {}/{}", I::collection(), item.id())
            }
            ItemChange::Update { item, .. } => {
                write!(f, "Update {}/{}", I::collection(), item.id())
            }
            ItemChange::Remove { item, .. } => {
                write!(f, "Remove {}/{}", I::collection(), item.id())
            }
        }
    }
}

impl std::fmt::Display for EntityChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EntityChange {}/{}", self.entity.collection(), self.entity.id())
    }
}

use crate::resultset::ResultSet;

#[derive(Debug, Clone)]
pub struct ChangeSet<R: View> {
    pub resultset: ResultSet<R>,
    pub changes: Vec<ItemChange<R>>,
}

impl<R: View> ChangeSet<R>
where R: Clone
{
    /// Returns items from the initial query load (before subscription was active)
    pub fn initial(&self) -> Vec<R> {
        self.changes
            .iter()
            .filter_map(|change| match change {
                ItemChange::Initial { item } => Some(item.clone()),
                _ => None,
            })
            .collect()
    }

    /// Returns genuinely new items (added after subscription, or now match the predicate)
    pub fn added(&self) -> Vec<R> {
        self.changes
            .iter()
            .filter_map(|change| match change {
                ItemChange::Add { item, .. } => Some(item.clone()),
                _ => None,
            })
            .collect()
    }

    /// Returns all items that appeared in the result set (initial load + newly added)
    pub fn appeared(&self) -> Vec<R> {
        self.changes
            .iter()
            .filter_map(|change| match change {
                ItemChange::Add { item, .. } | ItemChange::Initial { item } => Some(item.clone()),
                _ => None,
            })
            .collect()
    }

    #[deprecated(since = "0.7.10", note = "Use `appeared()`, `initial()`, or `added()` instead")]
    /// Returns all items that were added or now match the query
    pub fn adds(&self) -> Vec<R> { self.appeared() }

    /// Returns all items that were removed or no longer match the query
    pub fn removed(&self) -> Vec<R> {
        self.changes
            .iter()
            .filter_map(|change| match change {
                ItemChange::Remove { item, .. } => Some(item.clone()),
                _ => None,
            })
            .collect()
    }

    #[deprecated(since = "0.7.10", note = "Use `removed()` instead")]
    /// Returns all items that were removed or no longer match the query
    pub fn removes(&self) -> Vec<R> { self.removed() }

    /// Returns all items that were updated but still match the query
    pub fn updated(&self) -> Vec<R> {
        self.changes
            .iter()
            .filter_map(|change| match change {
                ItemChange::Update { item, .. } => Some(item.clone()),
                _ => None,
            })
            .collect()
    }

    #[deprecated(since = "0.7.10", note = "Use `updated()` instead")]
    /// Returns all items that were updated but still match the query
    pub fn updates(&self) -> Vec<R> { self.updated() }
}

impl<I> std::fmt::Display for ChangeSet<I>
where I: View + Clone + 'static
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // print the number of results in the resultset, and then display each change
        use ankurah_signals::Peek;
        let results = self.resultset.peek().len();
        write!(f, "ChangeSet({results} results): {}", self.changes.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", "))
    }
}

// Note: ChangeSet<Entity> conversion removed since Entity doesn't implement View
// and ChangeSet is no longer used by Reactor

impl<I> From<ItemChange<Entity>> for ItemChange<I>
where I: View
{
    fn from(change: ItemChange<Entity>) -> Self {
        match change {
            ItemChange::Initial { item } => ItemChange::Initial { item: I::from_entity(item) },
            ItemChange::Add { item, events } => ItemChange::Add { item: I::from_entity(item), events },
            ItemChange::Update { item, events } => ItemChange::Update { item: I::from_entity(item), events },
            ItemChange::Remove { item, events } => ItemChange::Remove { item: I::from_entity(item), events },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ChangeKind {
    Initial,
    Add,
    Remove,
    Update,
}

impl<R> From<&ItemChange<R>> for ChangeKind {
    fn from(change: &ItemChange<R>) -> Self {
        match change {
            ItemChange::Initial { .. } => ChangeKind::Initial,
            ItemChange::Add { .. } => ChangeKind::Add,
            ItemChange::Remove { .. } => ChangeKind::Remove,
            ItemChange::Update { .. } => ChangeKind::Update,
        }
    }
}

// Moved all the ReactorUpdate stuff into reactor.rs because it's specific to the reactor
// and we have several types of updates, so it's essential to keep them organized
