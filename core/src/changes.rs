use crate::{entity::Entity, error::MutationError, model::View, resultset::ResultSet};
use ankurah_proto::{Attested, Event};

#[derive(Debug, Clone)]
pub struct EntityChange {
    entity: Entity,
    event: Option<Attested<Event>>,
}

// TODO consider a flattened version of EntityChange that includes the entity and Vec<(operations, parent, attestations)> rather than a Vec<Attested<Event>>
impl EntityChange {
    pub fn new(entity: Entity, event: Option<Attested<Event>>) -> Result<Self, MutationError> {
        // validate that all events have the same entity id as the entity
        // and that the event ids are present in the entity's head clock
        if let Some(event) = &event {
            let head = entity.head();
            if event.payload.entity_id != entity.id {
                return Err(MutationError::InvalidEvent);
            }
            if !head.contains(&event.payload.id()) {
                return Err(MutationError::InvalidEvent);
            }
        }
        Ok(Self { entity, event })
    }
    pub fn entity(&self) -> &Entity { &self.entity }
    pub fn event(&self) -> Option<&Attested<Event>> { self.event.as_ref() }
    pub fn into_parts(self) -> (Entity, Option<Attested<Event>>) { (self.entity, self.event) }
}

#[derive(Debug, Clone)]
pub enum ItemChange<I> {
    /// Initial retrieval of an item upon subscription
    Initial { item: I },
    /// A new item was added OR changed such that it now matches the subscription
    Add { item: I, event: Attested<Event> },
    /// A item that previously matched the subscription has changed in a way that has not changed the matching condition
    Update { item: I, event: Attested<Event> },
    /// A item that previously matched the subscription has changed in a way that no longer matches the subscription
    Remove { item: I, event: Attested<Event> },
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

    pub fn event(&self) -> Option<&Attested<Event>> {
        match self {
            ItemChange::Add { event, .. } | ItemChange::Update { event, .. } | ItemChange::Remove { event, .. } => Some(event),
            _ => None,
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

#[derive(Debug)]
pub struct ChangeSet<R> {
    pub resultset: crate::resultset::ResultSet<R>,
    pub changes: Vec<ItemChange<R>>,
    pub initial: bool,
}

impl<I> std::fmt::Display for ChangeSet<I>
where I: View
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // print the number of results in the resultset, and then display each change
        let results = self.resultset.items.len();
        write!(f, "ChangeSet({results} results): {}", self.changes.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", "))
    }
}

impl<I> From<ChangeSet<Entity>> for ChangeSet<I>
where I: View
{
    fn from(val: ChangeSet<Entity>) -> Self {
        ChangeSet {
            resultset: ResultSet {
                loaded: val.resultset.loaded,
                items: val.resultset.iter().map(|item| I::from_entity(item.clone())).collect(),
            },
            changes: val.changes.into_iter().map(|change| change.into()).collect(),
            initial: val.initial,
        }
    }
}

impl<I> From<ItemChange<Entity>> for ItemChange<I>
where I: View
{
    fn from(change: ItemChange<Entity>) -> Self {
        match change {
            ItemChange::Initial { item } => ItemChange::Initial { item: I::from_entity(item) },
            ItemChange::Add { item, event } => ItemChange::Add { item: I::from_entity(item), event },
            ItemChange::Update { item, event } => ItemChange::Update { item: I::from_entity(item), event },
            ItemChange::Remove { item, event } => ItemChange::Remove { item: I::from_entity(item), event },
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
