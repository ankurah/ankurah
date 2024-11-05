// use futures_signals::signal::Signal;

use std::{any::Any, sync::Arc};

use crate::{storage::RecordState, Node};

use anyhow::Result;

use ulid::Ulid;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
pub struct ID(pub Ulid);

/// A model is a struct that represents the present values for a given record
/// Schema is defined primarily by the Model object, and the Record is derived from that via macro.
pub trait Model {}

/// A specific instance of a record in the collection
pub trait Record: Any + Send + Sync + 'static {
    fn id(&self) -> ID;
    fn bucket_name(&self) -> &'static str;
    fn record_state(&self) -> RecordState;
    fn from_record_state(
        id: ID,
        record_state: &RecordState,
    ) -> Result<Self, crate::error::RetrievalError>
    where
        Self: Sized;
    fn commit_record(&self, node: Arc<Node>) -> Result<()>;

    fn as_dyn_any(&self) -> &dyn Any;
}

#[derive(Debug)]
pub struct RecordInner {
    pub collection: &'static str,
    pub id: ID,
}
