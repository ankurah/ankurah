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
        let humanized = crate::human_id::humanize(self.0.to_bytes(), 4);
        f.debug_tuple("ID").field(&humanized).finish()
    }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let humanized = crate::human_id::humanize(self.0.to_bytes(), 4);
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
    type ScopedRecord: ScopedRecord;
    fn bucket_name() -> &'static str
    where
        Self: Sized;
    fn new_scoped_record(id: ID, model: &Self) -> Self::ScopedRecord;

    /*
    fn new_erased_record(id: ID) -> ErasedRecord;
    fn property(property_name: &'static str) -> Box<dyn Any + Send + Sync + 'static>;
    fn properties() -> Vec<&'static str>;
    */
}

/// A record is an instance of a model which is kept up to date with the latest changes from local and remote edits
pub trait Record {
    type Model: Model;
    type ScopedRecord: ScopedRecord;
    fn id(&self) -> ID;
    fn to_model(&self) -> Self::Model;
    // TODO: Consider possibly moving this into the regular impl, not this trait (else we'll have to provide a prelude)
    fn edit<'rec, 'trx: 'rec>(
        &self,
        trx: &'trx Transaction,
    ) -> Result<&'rec Self::ScopedRecord, RetrievalError>;
    fn from_erased(erased: &ErasedRecord) -> Self;
    //fn property(property_name: &'static str) -> Box<dyn Any>;
}

// Type erased record for modifying backends without
pub struct ErasedRecord {
    id: ID,
    bucket_name: &'static str,
    backends: Backends,
    //property_to_backend: BTreeMap<&'static str, &'static str>,
    // Maybe in the future? Rg
    //property_backends: BTreeMap<&'static str, Box<dyn Any + Send + Sync + 'static>>,
}

impl ErasedRecord {
    pub fn new(id: ID, bucket_name: &'static str) -> Self {
        Self {
            id: id,
            bucket_name: bucket_name,
            backends: Backends::new(),
            //property_to_backend: BTreeMap::new(),
            //property_backends: BTreeMap::new(),
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

    pub fn apply_record_event(&self, event: &RecordEvent) -> Result<()> {
        for (backend_name, operations) in &event.operations {
            self.backends
                .apply_operations((*backend_name).to_owned(), operations)?;
        }

        Ok(())
    }

    pub fn into_scoped_record<M: Model>(&self) -> M::ScopedRecord {
        <M::ScopedRecord as ScopedRecord>::from_backends(self.id(), self.backends.duplicate())
    }
    // pub fn into_record<R: Record>(&self) -> R {}
}

// TODO: ScopedRecord should have either a Record, or another ScopedRecord as its parent (nested transactions)
pub enum RecordParent<M: Model> {
    // Node(Arc<crate::Node>), // If we want to consolidate Record and ScopedRecord
    Record(Arc<M::Record>),
    ScopedRecord(Arc<M::ScopedRecord>),
}

/// An editable instance of a record which corresponds to a single transaction. Not updated with changes.
/// Not able to be subscribed to.
pub trait ScopedRecord: Any + Send + Sync + 'static {
    // type Record: Record;
    fn id(&self) -> ID;
    fn bucket_name(&self) -> &'static str;
    fn backends(&self) -> &Backends;

    fn to_erased_record(&self) -> ErasedRecord {
        ErasedRecord::from_backends(self.id(), self.bucket_name(), self.backends().duplicate())
    }
    fn from_backends(id: ID, backends: Backends) -> Self
    where
        Self: Sized;

    fn record_state(&self) -> anyhow::Result<RecordState>;
    fn from_record_state(id: ID, record_state: &RecordState) -> Result<Self, RetrievalError>
    where
        Self: Sized;

    fn get_record_event(&self) -> Option<RecordEvent>;
    //fn apply_record_event(&self, record_event: &RecordEvent) -> Result<()>;

    fn as_dyn_any(&self) -> &dyn Any;
    fn as_arc_dyn_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}
