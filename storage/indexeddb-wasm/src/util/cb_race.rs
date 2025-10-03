use ankurah_core::error::RetrievalError;
use std::sync::{Arc, Mutex};
use thiserror::Error;

use tokio::sync::oneshot::{self, error::TryRecvError};
use wasm_bindgen::prelude::Closure;

/// the first callback to finish wins
pub struct CBRace<T: 'static> {
    sender: Arc<Mutex<Option<oneshot::Sender<T>>>>,
    receiver: oneshot::Receiver<T>,
}

impl<T: 'static> CBRace<T> {
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self { sender: Arc::new(Mutex::new(Some(sender))), receiver }
    }

    /// Wrap a closure, sending its result through the channel, returns the js function ref
    pub fn wrap<Args>(&self, f: impl Fn(Args) -> T + 'static) -> Closure<dyn FnMut(Args)>
    where Args: wasm_bindgen::convert::FromWasmAbi + 'static {
        let sender = self.sender.clone();
        Closure::wrap(Box::new(move |args| {
            let result = f(args);
            match sender.lock() {
                Ok(mut sender) => {
                    if let Some(sender) = (*sender).take() {
                        let _ = sender.send(result);
                    }
                }
                Err(e) => {
                    web_sys::console::error_2(&"CB Race error".into(), &e.to_string().into());
                }
            }
        }))
    }

    /// Receive the result from the callback
    #[allow(unused)]
    pub async fn recv(self) -> T { self.receiver.await.expect("receiver await failed") }
    #[allow(unused)]
    pub async fn take(mut self) -> Result<Option<T>, TakeError> {
        match self.receiver.try_recv() {
            Ok(result) => Ok(Some(result)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Closed) => Err(TakeError::Closed),
        }
    }
}
impl<E> CBRace<Result<(), E>> {
    pub fn take_err(mut self) -> Result<Result<(), E>, TakeError> {
        match self.receiver.try_recv() {
            Ok(Ok(result)) => Ok(Ok(result)),
            Ok(Err(e)) => Ok(Err(e)),
            Err(TryRecvError::Empty) => Ok(Ok(())),
            Err(TryRecvError::Closed) => Err(TakeError::Closed),
        }
    }
}

#[derive(Error, Debug)]
pub enum TakeError {
    #[error("receiver is closed")]
    Closed,
}

impl From<TakeError> for std::fmt::Error {
    fn from(_: TakeError) -> std::fmt::Error { std::fmt::Error }
}

impl From<TakeError> for wasm_bindgen::JsValue {
    fn from(val: TakeError) -> Self { wasm_bindgen::JsValue::from_str(&val.to_string()) }
}

impl From<TakeError> for RetrievalError {
    fn from(val: TakeError) -> Self { RetrievalError::StorageError(Box::new(val)) }
}
