use crate::{error::RetrievalError, retrieval::GetEvents};
use ankurah_proto::{Attested, Event, EventId};
use std::sync::Mutex;

/// Search transaction events before falling back to stored or remote history.
/// Later events can depend on earlier events in the same uncommitted transaction.
pub(super) struct TransactionEventGetter<'a, G> {
    events: &'a Mutex<Vec<Attested<Event>>>,
    current: &'a Event,
    fallback: &'a G,
}

impl<'a, G> TransactionEventGetter<'a, G> {
    pub(super) fn applying(events: &'a Mutex<Vec<Attested<Event>>>, current: &'a Event, fallback: &'a G) -> Self {
        Self { events, current, fallback }
    }
}

#[async_trait::async_trait]
impl<G: GetEvents + Send + Sync> GetEvents for TransactionEventGetter<'_, G> {
    async fn get_event(&self, id: &EventId) -> Result<Event, RetrievalError> {
        if self.current.id() == *id {
            return Ok(self.current.clone());
        }
        if let Some(event) = self.events.lock().unwrap().iter().find(|event| event.payload.id() == *id) {
            return Ok(event.payload.clone());
        }
        self.fallback.get_event(id).await
    }

    // Pending events are discoverable, but are not evidence of permanent storage.
    async fn event_stored(&self, id: &EventId) -> Result<bool, RetrievalError> { self.fallback.event_stored(id).await }

    fn storage_is_definitive(&self) -> bool { self.fallback.storage_is_definitive() }
}
