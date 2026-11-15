//! IndexedDB's transactions as the maps begin them, and its requests and
//! cursors as the maps await them.
//!
//! A transaction stays active while one of its requests' events is being
//! handled, and the browser runs the promise jobs that handling queues before
//! it lets the transaction go, so a request made from the completion of the
//! one before, as an await on it is, finds the transaction still active.
//! Once control goes back to the event loop with none of its requests
//! pending, the browser commits it.

use futures::StreamExt;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{IdbCursorWithValue, IdbDatabase, IdbObjectStore, IdbRequest, IdbTransaction, IdbTransactionMode};

use super::MapError;
use crate::util::cb_future::CBFuture;
use crate::util::cb_stream::{cb_stream, CBStream};

/// A transaction over `stores`, in `mode`.
pub(super) fn transaction(connection: &IdbDatabase, stores: &[&str], mode: IdbTransactionMode) -> Result<IdbTransaction, MapError> {
    let names: js_sys::Array = stores.iter().map(|store| JsValue::from_str(store)).collect();
    connection.transaction_with_str_sequence_and_mode(&names, mode).map_err(MapError::thrown)
}

/// The object store `name` of `transaction`.
pub(super) fn store(transaction: &IdbTransaction, name: &str) -> Result<IdbObjectStore, MapError> {
    transaction.object_store(name).map_err(MapError::thrown)
}

/// A request made in a transaction, and the wait for its end.
pub(super) struct Request {
    request: IdbRequest,

    /// Listens from the moment the request is made, so its end can't pass
    /// unheard.
    ended: CBFuture,
}

impl Request {
    /// The request IndexedDB made as `made`, or the failure it threw.
    pub(super) fn made(made: Result<IdbRequest, JsValue>) -> Result<Self, MapError> {
        let request = made.map_err(MapError::thrown)?;
        let ended = CBFuture::new(&request, "success", "error");

        Ok(Self { request, ended })
    }

    /// The request's result, once it has succeeded.
    pub(super) async fn result(self) -> Result<JsValue, MapError> {
        match self.ended.await {
            Ok(()) => self.request.result().map_err(MapError::thrown),
            Err(_) => Err(MapError::of_request(&self.request)),
        }
    }
}

/// A cursor's request, each success of which brings the cursor to its next
/// record, or past the last.
pub(super) struct Cursor {
    request: IdbRequest,
    steps: CBStream,
}

impl Cursor {
    /// The cursor IndexedDB opened as `opened`, or the failure it threw.
    pub(super) fn opened(opened: Result<IdbRequest, JsValue>) -> Result<Self, MapError> {
        let request = opened.map_err(MapError::thrown)?;
        let steps = cb_stream(&request, "success", "error");

        Ok(Self { request, steps })
    }

    /// The cursor at its next record, or `None` once it is past the last.
    /// The cursor moves on only as its holder continues it.
    pub(super) async fn next(&mut self) -> Result<Option<IdbCursorWithValue>, MapError> {
        match self.steps.next().await {
            Some(Ok(at)) if at.is_null() => Ok(None),
            Some(Ok(at)) => at.dyn_into().map(Some).map_err(MapError::thrown),
            Some(Err(_)) => Err(MapError::of_request(&self.request)),
            None => Err(MapError::unexplained("a cursor ended")),
        }
    }
}
