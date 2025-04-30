use futures::channel::oneshot;
use futures::FutureExt;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use wasm_bindgen::prelude::*;
use web_sys::{Event, EventTarget};

/// Adds event listeners to the target for the given events, and returns a future that resolves when one of the events are triggered.
pub fn cb_future<'a, T: AsRef<EventTarget>, S: Into<EventNames<'a>>, E: Into<EventNames<'a>>>(
    target: &T,
    success_events: S,
    error_events: E,
) -> CBFuture {
    CBFuture::new(target, success_events, error_events)
}

/// A future that resolves when one of the given events are triggered.
pub struct CBFuture {
    receiver: oneshot::Receiver<Result<(), Event>>,
    _callbacks: Vec<(Closure<dyn FnMut(Event)>, EventTarget)>, // Store target to prevent early drop
}

#[derive(Clone)]
pub enum EventNames<'a> {
    Single(std::iter::Once<&'a str>),
    Multiple(std::slice::Iter<'a, &'a str>),
}

impl<'a> Iterator for EventNames<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EventNames::Single(iter) => iter.next(),
            EventNames::Multiple(iter) => iter.next().copied(),
        }
    }
}

impl<'a> From<&'a str> for EventNames<'a> {
    fn from(event: &'a str) -> Self { Self::Single(std::iter::once(event)) }
}

impl<'a, const N: usize> From<&'a [&'a str; N]> for EventNames<'a> {
    fn from(events: &'a [&'a str; N]) -> Self { Self::Multiple(events.iter()) }
}

impl CBFuture {
    pub fn new<'a, T: AsRef<EventTarget>, S: Into<EventNames<'a>>, E: Into<EventNames<'a>>>(
        target: &T,
        success_events: S,
        error_events: E,
    ) -> Self {
        let (sender, receiver) = oneshot::channel();
        let mut callbacks = Vec::new();
        let target = target.as_ref();

        let sender = Rc::new(RefCell::new(Some(sender)));

        // Create a single success callback
        let success_callback = Closure::wrap(Box::new({
            let sender = sender.clone();
            move |_event: Event| {
                if let Some(sender) = sender.borrow_mut().take() {
                    let _ = sender.send(Ok(()));
                }
            }
        }) as Box<dyn FnMut(_)>);

        // Register the same callback for all success events
        for success_event in success_events.into() {
            target.add_event_listener_with_callback(success_event, success_callback.as_ref().unchecked_ref()).unwrap();
        }

        let error_callback = Closure::wrap(Box::new({
            let sender = sender;
            move |event: Event| {
                web_sys::console::error_2(&JsValue::from_str("CB Future error"), &event);
                if let Some(sender) = sender.borrow_mut().take() {
                    let _ = sender.send(Err(event));
                }
            }
        }) as Box<dyn FnMut(_)>);

        // Register the same callback for all error events
        for error_event in error_events.into() {
            target.add_event_listener_with_callback(error_event, error_callback.as_ref().unchecked_ref()).unwrap();
        }

        // Store both callbacks
        callbacks.push((success_callback, target.clone()));
        callbacks.push((error_callback, target.clone()));

        Self { receiver, _callbacks: callbacks }
    }
}

impl Future for CBFuture {
    type Output = Result<(), Event>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // we don't cancel the future, so we can just unwrap the result
        self.receiver.poll_unpin(cx).map(|r| r.unwrap())
    }
}
