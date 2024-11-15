// use futures_signals::signal::Signal;

use std::{any::Any, sync::Arc, fmt};

use crate::{error::RetrievalError, storage::RecordState, Node};

use anyhow::Result;

use ulid::Ulid;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
pub struct ID(pub Ulid);

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

/// A model is a struct that represents the present values for a given record
/// Schema is defined primarily by the Model object, and the Record is derived from that via macro.
pub trait Model {
    type Record: Record;
    type ScopedRecord: ScopedRecord;
    fn bucket_name() -> &'static str
    where 
        Self: Sized;
}

/// An instance of a record.
pub trait Record {
    fn id(&self) -> ID;
    fn bucket_name() -> &'static str
    where
        Self: Sized;
}

/// An editable instance of a record.
pub trait ScopedRecord: Any + Send + Sync + 'static {
    fn id(&self) -> ID;
    fn bucket_name(&self) -> &'static str;
    fn record_state(&self) -> RecordState;
    fn from_record_state(
        id: ID,
        record_state: &RecordState,
    ) -> Result<Self, RetrievalError>
    where
        Self: Sized;
    fn commit_record(&self, node: Arc<Node>) -> Result<()>;
    fn as_dyn_any(&self) -> &dyn Any;
}