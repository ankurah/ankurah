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
    // put request types here
    // I dunno, maybe
    // Query(something)
}

#[derive(Serialize, Deserialize)]
pub struct Response {
    pub request_id: usize,
    pub payload: ResponsePayload,
}

#[derive(Serialize, Deserialize)]
pub enum ResponsePayload {
    Error(String),
}
