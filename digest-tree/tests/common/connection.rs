//! A connection between two nodes, faked: the comparison's one read of the
//! other node's tree crosses the wire as a request, as a node's connector
//! would send it, waits the wire's latency, and comes back as the response
//! the other node reads from its own view. Neither node compares: the crate
//! does.

use std::time::Duration;

use ankurah_digest_tree::{Error, Page, PageGetter, Ristretto255, Span};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::albums::{Title, View};
use super::members::Id;

/// What one node asks of another's tree, the comparison's one request: the
/// tree's pages at these spans.
#[derive(Serialize, Deserialize)]
pub struct GetPages(pub Vec<Span>);

/// The node's response: one page per span asked.
#[derive(Serialize, Deserialize)]
pub struct Pages(pub Vec<Page<Id, Title, Ristretto255>>);

/// The node asked: one read of its view per request.
pub async fn respond(view: &View, GetPages(spans): GetPages) -> Result<Pages, Error> { Ok(Pages(view.pages(spans).await?)) }

/// A comparison's connection to a node that holds a tree: a [`PageGetter`]
/// whose read is a round trip across the wire, and which counts them.
pub struct Connection {
    remote: View,
    latency: Duration,
    round_trips: usize,
}

impl Connection {
    /// A connection to the node whose tree `remote` views, over a wire that
    /// takes `latency` to cross.
    pub fn to(remote: View, latency: Duration) -> Self { Self { remote, latency, round_trips: 0 } }

    /// How many requests have crossed the wire and been answered.
    pub fn round_trips(&self) -> usize { self.round_trips }

    /// One round trip: the request's bytes go out, the wire's latency passes,
    /// the node responds, and the response's bytes come back.
    async fn round_trip(&mut self, request: GetPages) -> Result<Pages, Error> {
        let request = across_the_wire(&request);
        tokio::time::sleep(self.latency).await;
        let response = respond(&self.remote, request).await?;
        self.round_trips += 1;

        Ok(across_the_wire(&response))
    }
}

/// A message as the other side receives it.
fn across_the_wire<M: Serialize + DeserializeOwned>(message: &M) -> M {
    bincode::deserialize(&bincode::serialize(message).unwrap()).unwrap()
}

impl PageGetter<Id, Title, Ristretto255> for Connection {
    type Error = Error;

    async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<Id, Title, Ristretto255>>, Error> {
        Ok(self.round_trip(GetPages(spans)).await?.0)
    }
}
