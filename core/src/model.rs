// use futures_signals::signal::Signal;

use std::{any::Any, fmt, sync::Arc};

use crate::{
    error::RetrievalError,
    property::{backend::RecordEvent, Backends},
    storage::RecordState,
    transaction::Transaction,
};

use anyhow::Result;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ID(pub Ulid);

impl fmt::Debug for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let humanized = crate::human_id::hex(self.0.to_bytes());
        f.debug_tuple("ID").field(&humanized).finish()
    }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let humanized = crate::human_id::hex(self.0.to_bytes());
        f.debug_tuple("ID").field(&humanized).finish()
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
    type ScopedRecord<'trx>: ScopedRecord<'trx>;
    fn bucket_name() -> &'static str
    where
        Self: Sized;
    fn new_record_inner(&self, id: ID) -> RecordInner;
}

/// A record is an instance of a model which is kept up to date with the latest changes from local and remote edits
pub trait Record {
    type Model: Model;
    type ScopedRecord<'trx>: ScopedRecord<'trx>;
    fn id(&self) -> ID;
    fn to_model(&self) -> Self::Model;
    // TODO: Consider possibly moving this into the regular impl, not this trait (else we'll have to provide a prelude)
    fn edit<'rec, 'trx: 'rec>(
        &self,
        trx: &'trx Transaction,
    ) -> Result<Self::ScopedRecord<'rec>, RetrievalError>;
    fn from_record_inner(inner: &RecordInner) -> Self;
    //fn property(property_name: &'static str) -> Box<dyn Any>;
}

// Type erased record for modifying backends without knowing the specifics.
pub struct RecordInner {
    id: ID,
    bucket_name: &'static str,
    backends: Backends,
}

impl RecordInner {
    pub fn new(id: ID, bucket_name: &'static str) -> Self {
        Self {
            id: id,
            bucket_name: bucket_name,
            backends: Backends::new(),
        }
    }

    pub fn id(&self) -> ID {
        self.id
    }

    pub fn bucket_name(&self) -> &'static str {
        self.bucket_name
    }

    pub fn to_record_state(&self) -> Result<RecordState> {
        RecordState::from_backends(&self.backends)
    }

    pub fn from_backends(id: ID, bucket_name: &'static str, backends: Backends) -> Self {
        Self {
            id: id,
            bucket_name: bucket_name,
            backends: backends,
        }
    }

    pub fn from_record_state(
        id: ID,
        bucket_name: &'static str,
        record_state: &RecordState,
    ) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(record_state)?;
        Ok(Self::from_backends(id, bucket_name, backends))
    }

    pub fn get_record_event(&self) -> Option<RecordEvent> {
        let record_event = RecordEvent {
            id: self.id(),
            bucket_name: self.bucket_name(),
            operations: self.backends.to_operations(),
        };

        if record_event.is_empty() {
            None
        } else {
            Some(record_event)
        }
    }

    pub fn apply_record_event(&self, event: &RecordEvent) -> Result<()> {
        for (backend_name, operations) in &event.operations {
            self.backends
                .apply_operations((*backend_name).to_owned(), operations)?;
        }

        Ok(())
    }

    pub fn snapshot(&self) -> Self {
        Self {
            id: self.id(),
            bucket_name: self.bucket_name(),
            backends: self.backends.duplicate(),
        }
    }
}

/// An editable instance of a record which corresponds to a single transaction. Not updated with changes.
/// Not able to be subscribed to.
pub trait ScopedRecord<'trx> {
    // type Record: Record;
    fn id(&self) -> ID;
    fn bucket_name(&self) -> &'static str;
    fn backends(&self) -> &Backends;

    fn record_inner(&self) -> Arc<RecordInner>;
    fn from_record_inner(inner: Arc<RecordInner>) -> Self
    where
        Self: Sized;

    fn record_state(&self) -> anyhow::Result<RecordState> {
        RecordState::from_backends(&self.backends())
    }
    fn from_record_state(id: ID, record_state: &RecordState) -> Result<Self, RetrievalError>
    where
        Self: Sized;

    fn get_record_event(&self) -> Option<RecordEvent> {
        self.record_inner().get_record_event()
    }
    //fn apply_record_event(&self, record_event: &RecordEvent) -> Result<()>;

    fn as_dyn_any(&self) -> &dyn Any;
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}
