use std::{
    collections::BTreeMap,
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::types::ID;
use anyhow::Result;

#[derive(Debug)]
pub struct TransactionManager {
    active_transaction: Mutex<Option<Arc<Transaction>>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_transaction: Mutex::new(None),
        }
    }
    pub fn handle(&self) -> TransactionHandle {
        let active = self.active_transaction.lock().unwrap();
        if let Some(trx) = active.as_ref() {
            TransactionHandle::Standard(trx.clone())
        } else {
            TransactionHandle::AutoCommit(Transaction::new())
        }
    }
    pub fn begin(&self) -> Result<TransactionGuard> {
        // return error if we're already in a transaction
        if self.active_transaction.lock().unwrap().is_some() {
            return Err(anyhow::anyhow!("Already in a transaction"));
        }
        let trx = Arc::new(Transaction::new());
        self.active_transaction.lock().unwrap().replace(trx.clone());
        Ok(TransactionGuard::new(trx))
    }
}

pub enum TransactionHandle {
    AutoCommit(Transaction),
    Standard(Arc<Transaction>),
}

impl Drop for TransactionHandle {
    fn drop(&mut self) {
        if let TransactionHandle::AutoCommit(trx) = self {
            trx.commit();
        }
    }
}

impl Deref for TransactionHandle {
    type Target = Transaction;
    fn deref(&self) -> &Self::Target {
        match self {
            TransactionHandle::AutoCommit(trx) => trx,
            TransactionHandle::Standard(trx) => &*trx,
        }
    }
}

#[must_use]
pub struct TransactionGuard {
    trx: Arc<Transaction>,
    consumed: bool,
}

impl TransactionGuard {
    pub fn new(trx: Arc<Transaction>) -> Self {
        Self { trx, consumed: false }
    }

    pub fn commit(mut self) {
        self.trx.commit();
        self.consumed = true;
    }

    pub fn release(mut self) {
        // drop doing nothing
        self.consumed = true;
    }
}

impl Drop for TransactionGuard {
    fn drop(&mut self) {
        assert!(self.consumed, "`commit` or `release` need to be used on the `TransactionGuard`.");
    }
}

#[derive(Debug)]
pub struct Transaction {
    updates: Mutex<BTreeMap<(&'static str, ID), Vec<Operation>>>,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            updates: Mutex::new(BTreeMap::new()),
        }
    }
    pub fn add_operation(
        &self,
        engine: &'static str,
        collection: &'static str,
        id: ID,
        payload: Vec<u8>,
    ) {
        let mut updates = self.updates.lock().unwrap();
        updates
            .entry((collection, id))
            .or_insert_with(Vec::new)
            .push(Operation { engine, payload });
    }
    pub fn commit(&self) {
        eprintln!("STUB: Committing transaction {:?}", self);
    }
}

// Q. When do we want unified vs individual property storage for TypeEngine operations?
// A. When we start to care about differentiating possible recipients for different properties.

#[derive(Debug)]
pub struct Operation {
    engine: &'static str,
    payload: Vec<u8>,
}
