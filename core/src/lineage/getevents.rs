use std::collections::HashMap;

use ankurah_proto::{self as proto, Attested, Clock, CollectionId, Event, EventId};

use crate::error::RetrievalError;
use async_trait::async_trait;

/// Event retrieval trait exclusively for lineage comparisons
#[async_trait]
pub trait GetEvents {
    type Id: Eq + PartialEq + Clone + std::fmt::Debug + Send + Sync;
    type Event: TEvent<Id = Self::Id> + std::fmt::Display;

    /// Estimate the budget cost for retrieving a batch of events
    /// This allows different implementations to model their cost structure
    fn estimate_cost(&self, _batch_size: usize) -> usize {
        // Default implementation: fixed cost of 1 per batch
        1
    }

    /// retrieve the events from the store OR the remote peer
    async fn get_events(
        &self,
        collection_id: &CollectionId,
        event_ids: Vec<Self::Id>,
    ) -> Result<(usize, HashMap<Self::Id, Attested<Self::Event>>), RetrievalError>;
}

/// a trait for events and eventlike things that can be descended
pub trait TEvent: std::fmt::Display {
    type Id: Eq + PartialEq + Clone;
    type Parent: TClock<Id = Self::Id>;

    fn id(&self) -> Self::Id;
    fn parent(&self) -> &Self::Parent;
}

pub trait TClock {
    type Id: Eq + PartialEq + Clone;
    fn members(&self) -> &[Self::Id];
}

impl TClock for Clock {
    type Id = EventId;
    fn members(&self) -> &[Self::Id] { self.as_slice() }
}

impl TEvent for ankurah_proto::Event {
    type Id = ankurah_proto::EventId;
    type Parent = Clock;

    fn id(&self) -> EventId { self.id() }
    fn parent(&self) -> &Clock { &self.parent }
}
