use crate::{
    model::{Entity, View},
    resultset::ResultSet,
};
use ankurah_proto::Event;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EntityChange {
    pub entity: Arc<Entity>,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone)]
pub enum ItemChange<I> {
    /// Initial retrieval of an item upon subscription
    Initial { item: I },
    /// A new item was added OR changed such that it now matches the subscription
    Add { item: I, events: Vec<Event> },
    /// A item that previously matched the subscription has changed in a way that has not changed the matching condition
    Update { item: I, events: Vec<Event> },
    /// A item that previously matched the subscription has changed in a way that no longer matches the subscription
    Remove { item: I, events: Vec<Event> },
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

    pub fn events(&self) -> &[Event] {
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
        write!(f, "EntityChange {}/{}", self.entity.collection, self.entity.id)
    }
}

#[derive(Debug)]
pub struct ChangeSet<R> {
    pub resultset: crate::resultset::ResultSet<R>,
    pub changes: Vec<ItemChange<R>>,
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

impl<I> From<ChangeSet<Arc<Entity>>> for ChangeSet<I>
where I: View
{
    fn from(val: ChangeSet<Arc<Entity>>) -> Self {
        ChangeSet {
            resultset: ResultSet {
                loaded: val.resultset.loaded,
                items: val.resultset.iter().map(|item| I::from_entity(item.clone())).collect(),
            },
            changes: val.changes.into_iter().map(|change| change.into()).collect(),
        }
    }
}

impl<I> From<ItemChange<Arc<Entity>>> for ItemChange<I>
where I: View
{
    fn from(change: ItemChange<Arc<Entity>>) -> Self {
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
