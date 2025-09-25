use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;
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
            if let Some(sender) = sender.lock().unwrap().take() {
                let _ = sender.send(result);
            }
        }))
    }

    /// Receive the result from the callback
    pub async fn recv(self) -> T { self.receiver.await.expect("receiver await failed") }
}
