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

// I think RecordChange needs to be this:
// pub enum RecordChange {
//     /// Initial retrieval of a record upon subscription
//     Initial{
//         record: Arc<RecordInner>,
//         // we don't have updates here because we only have the initial state
//     },
//     /// A new record was added OR changed such that it now matches the subscription
//     Add{
//         record: Arc<RecordInner>,
//         updates: Vec<RecordEvent>, // these updates can be relayed by the local node to the remote subscribing node via a NodeRequestBody::CommitEvents message
//     },
//     /// A record that previously matched the subscription has changed in a way that has not changed the matching condition
//     Update{
//         record: Arc<RecordInner>,
//         updates: Vec<RecordEvent>, // these updates can be relayed by the local node to the remote subscribing node via a NodeRequestBody::CommitEvents message
//     },
//     /// A record that previously matched the subscription has changed in a way that no longer matches the subscription
//     Remove{
//         record: Arc<RecordInner>,
//         updates: Vec<RecordEvent>, // these updates can be relayed by the local node to the remote subscribing node via a NodeRequestBody::CommitEvents message
//     },
// }

/// A set of changes to the record set
#[derive(Debug, Clone)]
pub struct ChangeSet {
    pub changes: Vec<RecordChange>,
}
