// use futures_signals::signal::Signal;

use std::sync::{Arc, Mutex};

use crate::{transaction::TransactionManager, types::ID};

/// A model is a struct that represents the present values for a given record
/// Schema is defined primarily by the Model object, and the Record is derived from that via macro.
pub trait Model {}

/// A specific instance of a record in the collection
pub trait Record {
    type Model: Model;
    fn id(&self) -> ID;
}

#[derive(Debug)]
pub struct RecordInner {
    pub collection: &'static str,
    pub id: ID,
    pub transaction_manager: Arc<TransactionManager>,
}

pub struct TrxHandle {
    transaction: Mutex<Option<Transaction>>,
}

pub struct Transaction {}
