use crate::model::RecordInner;
use ankurah_proto::RecordEvent;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum RecordChangeKind {
    Add,
    Remove,
    Edit,
}

/// Represents a change in the record set
#[derive(Debug, Clone)]
pub struct RecordChange {
    pub record: Arc<RecordInner>,
    pub updates: Vec<RecordEvent>,
    pub kind: RecordChangeKind,
}

/// A set of changes to the record set
#[derive(Debug, Clone)]
pub struct ChangeSet {
    pub changes: Vec<RecordChange>,
}
