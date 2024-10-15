use std::{collections::HashMap, net::SocketAddr};

use serde::{Deserialize, Serialize};
use ulid::Ulid;
use wasm_bindgen::prelude::*;
// use crate::query::Record;
use bytes::Bytes;

use crate::record::{Direction, PaginatedCursor, Record};

#[derive(Serialize, Deserialize, Clone)]
pub struct IngressLog {
    pub event_id: Ulid,
    pub date: chrono::DateTime<chrono::Utc>,
    pub remote_addr: Option<SocketAddr>,
    pub method: String,
    pub host: String,
    pub path: String,
    pub query: HashMap<String, String>,
    pub headers: HashMap<String, String>,
    pub body: Bytes,
}
impl Record for IngressLog {
    type ID = Ulid;
    fn id(&self) -> &Self::ID {
        &self.event_id
    }
}

#[derive(Serialize, Deserialize)]
pub struct FetchIngressLogsRequest {
    pub direction: Direction,
    pub limit: usize,
    pub cursor: PaginatedCursor,
}

#[derive(Serialize, Deserialize)]
pub struct FetchIngressLogsResponse {
    pub items: Vec<(Vec<u8>, IngressLog)>,
    pub limit: usize,
    pub has_more_before: bool,
    pub has_more_after: bool,
}
