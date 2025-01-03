use crate::model::RecordInner;
use ankurah_proto::RecordEvent;
use std::sync::Arc;

/// Represents a change in the record set
#[derive(Debug, Clone)]
pub enum RecordChange {
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

impl RecordChange {
    pub fn record(&self) -> &Arc<RecordInner> {
        match self {
            RecordChange::Initial { record }
            | RecordChange::Add { record, .. }
            | RecordChange::Update { record, .. }
            | RecordChange::Remove { record, .. } => record,
        }
    }

    pub fn events(&self) -> &[RecordEvent] {
        match self {
            RecordChange::Add { events, .. }
            | RecordChange::Update { events, .. }
            | RecordChange::Remove { events, .. } => events,
            _ => &[],
        }
    }
}

/// A set of changes to the record set
#[derive(Debug, Clone)]
pub struct ChangeSet {
    pub changes: Vec<RecordChange>,
}
