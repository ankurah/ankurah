use ulid::Ulid;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ID(pub Ulid);

pub struct EnumValue<T>(T);
pub struct StringValue(yrs::Doc);

// Null precursor only
pub trait InitializeWith<T> {
    fn initialize_with(value: T) -> Self;
}

use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update};

// Starting with basic string type operations
impl StringValue {
    pub fn value(&self) -> String {
        let text = self.0.get_or_insert_text(""); // We only have one field in the yrs doc
        text.get_string(&self.0.transact())
    }
    pub fn insert(&self, index: usize, value: &str) {
        let text = self.0.get_or_insert_text(""); // We only have one field in the yrs doc
        let mut txn = self.0.transact_mut();
        text.insert(&mut txn, index, value);
    }
    pub fn delete(&self, index: usize, length: usize) {
        let text = self.0.get_or_insert_text(""); // We only have one field in the yrs doc
        let mut txn = self.0.transact_mut();
        text.delete(&mut txn, index, length);
    }
}

impl InitializeWith<String> for StringValue {
    fn initialize_with(value: String) -> Self {
        let doc = yrs::Doc::new();
        let text = doc.get_or_insert_text(""); // We only have one field in the yrs doc
                                               // every operation in Yrs happens in scope of a transaction
        {
            let mut txn = doc.transact_mut();
            // append text to our collaborative document
            text.insert(&mut txn, 0, value.as_str());
        }
        Self(doc)
    }
}
