// use futures_signals::signal::Signal;

use std::{collections::BTreeMap, sync::{Arc, Mutex}};

use crate::{property::backend::yrs::Yrs, storage::RecordState, transaction::TransactionManager};

use ulid::Ulid;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Ord, PartialOrd)]
pub struct ID(pub Ulid);

/// A model is a struct that represents the present values for a given record
/// Schema is defined primarily by the Model object, and the Record is derived from that via macro.
pub trait Model {}

/// A specific instance of a record in the collection
pub trait Record {
    type Model: Model;
    fn id(&self) -> ID;
    fn record_state(&self) -> RecordState;
}

#[derive(Debug)]
pub struct RecordInner {
    pub collection: &'static str,
    pub id: ID,
    pub transaction_manager: Arc<TransactionManager>,

    // backends
    // TODO: Probably make these optional or use a generic for RecordInner
    pub yrs: Arc<crate::property::backend::Yrs>,
}

pub struct TrxHandle {
    transaction: Mutex<Option<Transaction>>,
}

pub struct Transaction {}
