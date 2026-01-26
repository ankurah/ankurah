use futures::channel::mpsc;
use futures::stream::Stream;
use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tracing::error;
use wasm_bindgen::prelude::*;
use web_sys::{Event, EventTarget};

pub fn cb_stream<T: AsRef<EventTarget>>(target: &T, success_event: &'static str, error_event: &'static str) -> CBStream {
    CBStream::new(target, success_event, error_event)
}

pub struct CBStream {
    receiver: mpsc::UnboundedReceiver<Result<JsValue, Event>>,
    _registrations: Vec<(&'static str, Closure<dyn FnMut(Event)>, EventTarget)>,
}

impl CBStream {
    pub fn new<T: AsRef<EventTarget>>(target: &T, success_event: &'static str, error_event: &'static str) -> Self {
        let (sender, receiver) = mpsc::unbounded();
        let mut registrations = Vec::new();
        let target = target.as_ref();

        let sender = Rc::new(RefCell::new(Some(sender)));

        let success_callback = Closure::wrap(Box::new({
            let sender = sender.clone();
            move |event: Event| {
                let target = event.target().unwrap();
                let request: web_sys::IdbRequest = target.unchecked_into();
                if let Some(sender) = sender.borrow().as_ref() {
                    let _ = sender.unbounded_send(Ok(request.result().unwrap()));
                }
            }
        }) as Box<dyn FnMut(_)>);

        let error_callback = Closure::wrap(Box::new({
            let sender = sender;
            move |event: Event| {
                error!("CB Stream error: {}", crate::error::extract_message(event.clone().into()));
                if let Some(sender) = sender.borrow().as_ref() {
                    let _ = sender.unbounded_send(Err(event));
                }
            }
        }) as Box<dyn FnMut(_)>);

        target.add_event_listener_with_callback(success_event, success_callback.as_ref().unchecked_ref()).unwrap();
        target.add_event_listener_with_callback(error_event, error_callback.as_ref().unchecked_ref()).unwrap();

        registrations.push((success_event, success_callback, target.clone()));
        registrations.push((error_event, error_callback, target.clone()));

        Self { receiver, _registrations: registrations }
    }
}

impl Stream for CBStream {
    type Item = Result<JsValue, Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> { Pin::new(&mut self.receiver).poll_next(cx) }
}

impl Drop for CBStream {
    fn drop(&mut self) {
        // Remove all registered listeners before dropping callbacks
        for (event_name, callback, target) in &self._registrations {
            let _ = target.remove_event_listener_with_callback(event_name, callback.as_ref().unchecked_ref());
        }
    }
}
