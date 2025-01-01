use ankurah_proto::{RecordEvent, RecordState, ID};
// use futures_signals::signal::Signal;

use std::sync::Arc;

use crate::{error::RetrievalError, property::Backends};

use anyhow::Result;

use ankql::selection::filter::Filterable;

/// A model is a struct that represents the present values for a given record
/// Schema is defined primarily by the Model object, and the Record is derived from that via macro.
pub trait Model {
    type Record: Record;
    type ScopedRecord<'trx>: ScopedRecord<'trx>;
    fn bucket_name() -> &'static str
    where
        Self: Sized;
    fn to_record_inner(&self, id: ID) -> RecordInner;
}

/// A record is an instance of a model which is kept up to date with the latest changes from local and remote edits
pub trait Record {
    type Model: Model;
    type ScopedRecord<'trx>: ScopedRecord<'trx>;
    fn id(&self) -> ID {
        self.record_inner().id()
    }
    fn backends(&self) -> &Backends {
        self.record_inner().backends()
    }
    fn bucket_name() -> &'static str {
        <Self::Model as Model>::bucket_name()
    }
    fn to_model(&self) -> Self::Model;
    fn record_inner(&self) -> &Arc<RecordInner>;
    fn from_record_inner(inner: Arc<RecordInner>) -> Self;
}

/// A record inner is a type erased/ dynamic typed record.
/// This is used to interoperate with the storage engine
/// TODO: Consider renaming this to Record and renaming trait Record to ActiveRecord or something like that
#[derive(Debug)]
pub struct RecordInner {
    pub id: ID,
    pub bucket_name: String,
    backends: Backends,
}

impl RecordInner {
    pub fn new(id: ID, bucket_name: String) -> Self {
        Self {
            id,
            bucket_name,
            backends: Backends::new(),
        }
    }

    pub fn id(&self) -> ID {
        self.id
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn backends(&self) -> &Backends {
        &self.backends
    }

    pub fn to_record_state(&self) -> Result<RecordState> {
        self.backends.to_state_buffers()
    }

    pub fn from_backends(id: ID, bucket_name: &str, backends: Backends) -> Self {
        Self {
            id,
            bucket_name: bucket_name.to_string(),
            backends,
        }
    }

    pub fn from_record_state(
        id: ID,
        bucket_name: &str,
        record_state: &RecordState,
    ) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(record_state)?;
        Ok(Self::from_backends(id, bucket_name, backends))
    }

    pub fn get_record_event(&self) -> Result<Option<RecordEvent>> {
        let record_event = RecordEvent {
            id: self.id(),
            bucket_name: self.bucket_name().to_string(),
            operations: self.backends.to_operations()?,
        };

        if record_event.operations.is_empty() {
            Ok(None)
        } else {
            Ok(Some(record_event))
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
            bucket_name: self.bucket_name().to_string(),
            backends: self.backends.fork(),
        }
    }
}

impl Filterable for RecordInner {
    fn collection(&self) -> &str {
        self.bucket_name()
    }

    /// TODO Implement this as a typecasted value. eg value<T> -> Option<Result<T>>
    /// where None is returned if the property is not found, and Err is returned if the property is found but is not able to be typecasted
    /// to the requested type. (need to think about the rust type system here more)
    fn value(&self, name: &str) -> Option<String> {
        // Iterate through backends to find one that has this property
        self.backends
            .backends
            .lock()
            .unwrap()
            .values()
            .find_map(|backend| backend.get_property_value_string(name))
    }
}

/// An editable instance of a record which corresponds to a single transaction. Not updated with changes.
/// Not able to be subscribed to.
pub trait ScopedRecord<'rec> {
    type Model: Model;
    type Record: Record;
    fn id(&self) -> ID {
        self.record_inner().id
    }
    fn bucket_name() -> &'static str {
        <Self::Model as Model>::bucket_name()
    }
    fn backends(&self) -> &Backends {
        &self.record_inner().backends
    }

    fn record_inner(&self) -> &RecordInner;
    fn from_record_inner(inner: &'rec RecordInner) -> Self
    where
        Self: Sized;

    fn record_state(&self) -> anyhow::Result<RecordState> {
        self.record_inner().to_record_state()
    }

    fn record_event(&self) -> anyhow::Result<Option<RecordEvent>> {
        self.record_inner().get_record_event()
    }

    fn read(&self) -> Self::Record {
        Self::Record::from_record_inner(Arc::new(self.record_inner().snapshot()))
    }
    //fn apply_record_event(&self, record_event: &RecordEvent) -> Result<()>;
}
