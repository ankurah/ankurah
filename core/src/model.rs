use ankurah_proto::{self as proto, Clock};
use proto::{RecordEvent, RecordState};
use tracing::debug;
use tracing::info;
// use futures_signals::signal::Signal;

use std::sync::Arc;
use std::sync::Mutex;

use crate::Node;
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
    fn create_record(&self, id: proto::ID) -> RecordInner;
}

/// A record is an instance of a model which is kept up to date with the latest changes from local and remote edits
pub trait Record {
    type Model: Model;
    type ScopedRecord<'trx>: ScopedRecord<'trx>;
    fn id(&self) -> proto::ID {
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

impl std::fmt::Display for RecordInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordInner({}/{}) = {}",
            self.bucket_name,
            self.id,
            self.head.lock().unwrap()
        )
    }
}

/// A record inner is a type erased/ dynamic typed record.
/// This is used to interoperate with the storage engine
/// TODO: Consider renaming this to Record and renaming trait Record to ActiveRecord or something like that
#[derive(Debug)]
pub struct RecordInner {
    pub id: proto::ID,
    pub bucket_name: String,
    backends: Backends,
    head: Arc<Mutex<Clock>>,
    pub upstream: Option<Arc<RecordInner>>,
}

impl RecordInner {
    pub fn id(&self) -> proto::ID {
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

    // used by the Model macro
    pub fn create(id: proto::ID, bucket_name: &str, backends: Backends) -> Self {
        Self {
            id,
            bucket_name: bucket_name.to_string(),
            backends,
            head: Arc::new(Mutex::new(Clock::default())),
            upstream: None,
        }
    }
    pub fn from_record_state(
        id: proto::ID,
        bucket_name: &str,
        record_state: &RecordState,
    ) -> Result<Self, RetrievalError> {
        let backends = Backends::from_state_buffers(&record_state)?;

        Ok(Self {
            id,
            bucket_name: bucket_name.to_string(),
            backends,
            head: Arc::new(Mutex::new(record_state.head.clone())),
            upstream: None,
        })
    }

    /// Collect an event which contains all operations for all backends since the last time they were collected
    /// Used for transaction commit. I think this should be reworked though, because we need to support rollbacks
    /// And it's kind of weirdly implicit that the events are stored in the record rather than the transaction
    pub fn commit(&self) -> Result<Option<RecordEvent>> {
        let operations = self.backends.to_operations()?;
        if operations.is_empty() {
            Ok(None)
        } else {
            let record_event = {
                let record_event = RecordEvent {
                    id: proto::ID::new(),
                    record_id: self.id(),
                    bucket_name: self.bucket_name().to_string(),
                    operations,
                    parent: self.head.lock().unwrap().clone(),
                };

                // Set the head to the event's ID
                *self.head.lock().unwrap() = Clock::new([record_event.id]);
                record_event
            };

            info!("Commit {}", self);
            Ok(Some(record_event))
        }
    }

    pub fn apply_record_event(&self, event: &RecordEvent) -> Result<()> {
        for (backend_name, operations) in &event.operations {
            self.backends
                .apply_operations((*backend_name).to_owned(), operations)?;
        }
        info!("Apply record event {}", event);
        *self.head.lock().unwrap() = Clock::new([event.id]);

        Ok(())
    }

    /// HACK - we probably shouldn't be stomping on the backends like this
    pub fn apply_record_state(&self, state: &RecordState) -> Result<(), RetrievalError> {
        self.backends.apply_state(state)?;
        Ok(())
    }

    /// Create a snapshot of the record which is detached from the input record and will not receive updates
    pub fn snapshot(self: &Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            id: self.id(),
            bucket_name: self.bucket_name().to_string(),
            backends: self.backends.fork(),
            head: Arc::new(Mutex::new(self.head.lock().unwrap().clone())),
            upstream: Some(self.clone()),
        })
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
    fn id(&self) -> proto::ID {
        self.record_inner().id
    }
    fn bucket_name() -> &'static str {
        <Self::Model as Model>::bucket_name()
    }
    fn backends(&self) -> &Backends {
        &self.record_inner().backends
    }

    fn record_inner(&self) -> &Arc<RecordInner>;
    fn new(inner: &'rec Arc<RecordInner>) -> Self
    where
        Self: Sized;

    fn record_state(&self) -> anyhow::Result<RecordState> {
        self.record_inner().to_record_state()
    }

    // fn record_event(&self) -> anyhow::Result<Option<RecordEvent>> {
    //     self.record_inner().commit()
    // }
    fn read(&self) -> Self::Record {
        let inner: &Arc<RecordInner> = self.record_inner();

        let new_inner = match &inner.upstream {
            // If there is an upstream, use it
            Some(upstream) => upstream.clone(),
            // Else we're a new record, and we have to rely on the commit to add this to the node
            None => inner.clone(),
        };

        Self::Record::from_record_inner(new_inner)
    }
    //fn apply_record_event(&self, record_event: &RecordEvent) -> Result<()>;
}
