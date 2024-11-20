// use futures_signals::signal::Signal;

use std::{any::Any, fmt, sync::Arc};

use crate::{error::RetrievalError, property::backend::RecordEvent, storage::RecordState, Node};

use anyhow::Result;

use ulid::Ulid;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
pub struct ID(pub Ulid);

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl AsRef<ID> for ID {
    fn as_ref(&self) -> &ID {
        &self
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
    fn new_scoped_record(id: ID, model: &Self) -> Self::ScopedRecord;
    //fn property(property_name: &'static str) -> Box<dyn Any>;
}

/// An instance of a record.
pub trait Record {
    type Model: Model;
    type ScopedRecord: ScopedRecord;
    fn id(&self) -> ID;
    fn to_model(&self) -> Self::Model;
    //fn property(property_name: &'static str) -> Box<dyn Any>;
}

/// An editable instance of a record.
pub trait ScopedRecord: Any + Send + Sync + 'static {
    fn id(&self) -> ID;
    fn bucket_name(&self) -> &'static str;
    fn record_state(&self) -> RecordState;
    fn from_record_state(id: ID, record_state: &RecordState) -> Result<Self, RetrievalError>
    where
        Self: Sized;
    fn get_record_event(&self) -> Option<RecordEvent>;
    fn as_dyn_any(&self) -> &dyn Any;
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}
