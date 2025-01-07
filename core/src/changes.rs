use crate::{
    model::{Record, RecordInner},
    resultset::ResultSet,
};
use ankurah_proto::RecordEvent;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EntityChange {
    pub record: Arc<RecordInner>,
    pub events: Vec<RecordEvent>,
}

/// Represents a change in the record set
#[derive(Debug, Clone)]
pub enum ItemChange<R> {
    /// Initial retrieval of a record upon subscription
    Initial { record: R },
    /// A new record was added OR changed such that it now matches the subscription
    Add { record: R, events: Vec<RecordEvent> },
    /// A record that previously matched the subscription has changed in a way that has not changed the matching condition
    Update { record: R, events: Vec<RecordEvent> },
    /// A record that previously matched the subscription has changed in a way that no longer matches the subscription
    Remove { record: R, events: Vec<RecordEvent> },
}

impl<R> ItemChange<R> {
    pub fn record(&self) -> &R {
        match self {
            ItemChange::Initial { record }
            | ItemChange::Add { record, .. }
            | ItemChange::Update { record, .. }
            | ItemChange::Remove { record, .. } => record,
        }
    }

    pub fn events(&self) -> &[RecordEvent] {
        match self {
            ItemChange::Add { events, .. }
            | ItemChange::Update { events, .. }
            | ItemChange::Remove { events, .. } => events,
            _ => &[],
        }
    }
}

/// A set of changes to the record set
#[derive(Debug)]
pub struct ChangeSet<R> {
    pub resultset: crate::resultset::ResultSet<R>,
    pub changes: Vec<ItemChange<R>>,
}

impl<R> Into<ChangeSet<R>> for ChangeSet<Arc<RecordInner>>
where
    R: Record,
{
    fn into(self) -> ChangeSet<R> {
        ChangeSet {
            resultset: ResultSet {
                records: self
                    .resultset
                    .iter()
                    .map(|record| R::from_record_inner(record.clone()))
                    .collect(),
            },
            changes: self
                .changes
                .into_iter()
                .map(|change| change.into())
                .collect(),
        }
    }
}

impl<R> From<ItemChange<Arc<RecordInner>>> for ItemChange<R>
where
    R: Record,
{
    fn from(change: ItemChange<Arc<RecordInner>>) -> Self {
        match change {
            ItemChange::Initial { record } => ItemChange::Initial {
                record: R::from_record_inner(record),
            },
            ItemChange::Add { record, events } => ItemChange::Add {
                record: R::from_record_inner(record),
                events,
            },
            ItemChange::Update { record, events } => ItemChange::Update {
                record: R::from_record_inner(record),
                events,
            },
            ItemChange::Remove { record, events } => ItemChange::Remove {
                record: R::from_record_inner(record),
                events,
            },
        }
    }
}
