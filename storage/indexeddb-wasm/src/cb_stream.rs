use futures::channel::mpsc;
use futures::stream::Stream;
use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use wasm_bindgen::prelude::*;
use web_sys::{Event, EventTarget};

pub struct CBStream {
    receiver: mpsc::UnboundedReceiver<Result<JsValue, Event>>,
    _callbacks: Vec<(Closure<dyn FnMut(Event)>, EventTarget)>,
}

impl CBStream {
    pub fn new<T: AsRef<EventTarget>>(target: &T, success_event: &str, error_event: &str) -> Self {
        let (sender, receiver) = mpsc::unbounded();
        let mut callbacks = Vec::new();
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
                web_sys::console::error_2(&JsValue::from_str("CB Stream error"), &event);
                if let Some(sender) = sender.borrow().as_ref() {
                    let _ = sender.unbounded_send(Err(event));
                }
            }
        }) as Box<dyn FnMut(_)>);

        target.add_event_listener_with_callback(success_event, success_callback.as_ref().unchecked_ref()).unwrap();

        target.add_event_listener_with_callback(error_event, error_callback.as_ref().unchecked_ref()).unwrap();

        callbacks.push((success_callback, target.clone()));
        callbacks.push((error_callback, target.clone()));

        Self { receiver, _callbacks: callbacks }
    }
}

impl Stream for CBStream {
    type Item = Result<JsValue, Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> { Pin::new(&mut self.receiver).poll_next(cx) }
}
