use futures::channel::oneshot;
use futures::FutureExt;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use tracing::error;
use wasm_bindgen::prelude::*;
use web_sys::{Event, EventTarget};

/// Adds event listeners to the target for the given events, and returns a future that resolves when one of the events are triggered.
pub fn cb_future<T: AsRef<EventTarget>, S: Into<EventNames>, E: Into<EventNames>>(
    target: T,
    success_events: S,
    error_events: E,
) -> CBFuture {
    CBFuture::new(target, success_events, error_events)
}

/// A future that resolves when one of the given events are triggered.
pub struct CBFuture {
    receiver: oneshot::Receiver<Result<(), Event>>,
    _registrations: Vec<(&'static str, Closure<dyn FnMut(Event)>, EventTarget)>, // (event name, callback, target)
}

// SAFETY: CBFuture is Send because it's used in single-threaded WASM contexts where
// JavaScript objects (EventTarget, Closure) are safe to move between "threads"
// (which are actually just different execution contexts in the same thread)
unsafe impl Send for CBFuture {}

#[derive(Clone)]
pub enum EventNames {
    Single(Option<&'static str>),
    Multiple { events: &'static [&'static str], index: usize },
}

impl Iterator for EventNames {
    type Item = &'static str;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EventNames::Single(opt) => opt.take(),
            EventNames::Multiple { events, index } => {
                if *index < events.len() {
                    let ev = events[*index];
                    *index += 1;
                    Some(ev)
                } else {
                    None
                }
            }
        }
    }
}

impl From<&'static str> for EventNames {
    fn from(event: &'static str) -> Self { Self::Single(Some(event)) }
}

impl<const N: usize> From<&'static [&'static str; N]> for EventNames {
    fn from(events: &'static [&'static str; N]) -> Self { Self::Multiple { events, index: 0 } }
}

impl CBFuture {
    pub fn new<T: AsRef<EventTarget>, S: Into<EventNames>, E: Into<EventNames>>(target: T, success_events: S, error_events: E) -> Self {
        let (sender, receiver) = oneshot::channel();
        let mut registrations = Vec::new();
        let target = target.as_ref();

        let sender = Rc::new(RefCell::new(Some(sender)));

        // Register success listeners (one callback per event to enable precise removal)
        for event_name in success_events.into() {
            let success_cb = Closure::wrap(Box::new({
                let sender = sender.clone();
                move |_event: Event| {
                    if let Some(sender) = sender.borrow_mut().take() {
                        let _ = sender.send(Ok(()));
                    }
                }
            }) as Box<dyn FnMut(_)>);

            target.add_event_listener_with_callback(event_name, success_cb.as_ref().unchecked_ref()).unwrap();

            registrations.push((event_name, success_cb, target.clone()));
        }

        // Register error listeners (one callback per event to enable precise removal)
        for event_name in error_events.into() {
            let error_cb = Closure::wrap(Box::new({
                let sender = sender.clone();
                move |event: Event| {
                    error!("CB Future error: {}", crate::error::extract_message(event.clone().into()));
                    if let Some(sender) = sender.borrow_mut().take() {
                        let _ = sender.send(Err(event));
                    }
                }
            }) as Box<dyn FnMut(_)>);

            target.add_event_listener_with_callback(event_name, error_cb.as_ref().unchecked_ref()).unwrap();

            registrations.push((event_name, error_cb, target.clone()));
        }

        Self { receiver, _registrations: registrations }
    }
}

impl Future for CBFuture {
    type Output = Result<(), Event>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // we don't cancel the future, so we can just unwrap the result
        self.receiver.poll_unpin(cx).map(|r| r.unwrap())
    }
}

impl Drop for CBFuture {
    fn drop(&mut self) {
        // Remove all registered listeners before dropping callbacks
        for (event_name, callback, target) in &self._registrations {
            let _ = target.remove_event_listener_with_callback(event_name, callback.as_ref().unchecked_ref());
        }
    }
}
