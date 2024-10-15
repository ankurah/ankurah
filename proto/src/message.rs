use crate::event::ingress::{FetchIngressLogsRequest, FetchIngressLogsResponse};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Message {
    Request(Request),
    Response(Response),
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub id: usize,
    pub payload: RequestPayload,
}

#[derive(Serialize, Deserialize)]
pub enum RequestPayload {
    FetchIngressLogs(FetchIngressLogsRequest),
}

#[derive(Serialize, Deserialize)]
pub struct Response {
    pub request_id: usize,
    pub payload: ResponsePayload,
}

#[derive(Serialize, Deserialize)]
pub enum ResponsePayload {
    FetchIngressLogs(FetchIngressLogsResponse),
    Error(String),
}
