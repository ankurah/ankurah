// use serde::{Deserialize, Serialize};
use ulid::Ulid;

// IMPORTANT: All of the datastructures in here must understand the particulars of the schema dynamically
// Not using the Model/View/Mutable structs themselves.
// This is because the Node must operate without access to the Model structs definitions.

// Actually creating/accessing them does require generics, but that's part of the API, not the internals.

pub struct Event {
    pub id: Ulid,
    pub precursors: Vec<Ulid>,
    pub payload: Operation,
}

pub enum EventPayload {
    Operation(Operation),
}

#[derive(Clone)]
pub enum Operation {
    Create(),
    Update(),
    Delete(),
}
