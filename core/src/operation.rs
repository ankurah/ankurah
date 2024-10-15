use serde::{Deserialize, Serialize};
use ulid::Ulid;

pub struct Operation {
    pub id: Ulid,
    pub collection: String,
    pub precursors: Vec<Ulid>,
    pub payload: OperationPayload,
}

pub enum OperationPayload {}

impl Operation {}
