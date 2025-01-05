use crate::model::RecordInner;
use ankurah_proto::RecordEvent;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EntityChange {
    pub record: Arc<RecordInner>,
    pub events: Vec<RecordEvent>,
}

/// Represents a change in the record set
#[derive(Debug, Clone)]
pub enum ItemChange {
    /// Initial retrieval of a record upon subscription
    Initial { record: Arc<RecordInner> },
    /// A new record was added OR changed such that it now matches the subscription
    Add {
        record: Arc<RecordInner>,
        events: Vec<RecordEvent>,
    },
    /// A record that previously matched the subscription has changed in a way that has not changed the matching condition
    Update {
        record: Arc<RecordInner>,
        events: Vec<RecordEvent>,
    },
    /// A record that previously matched the subscription has changed in a way that no longer matches the subscription
    Remove {
        record: Arc<RecordInner>,
        events: Vec<RecordEvent>,
    },
}

impl ItemChange {
    pub fn record(&self) -> &Arc<RecordInner> {
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
#[derive(Debug, Clone)]
pub struct ChangeSet {
    pub changes: Vec<ItemChange>,
}
