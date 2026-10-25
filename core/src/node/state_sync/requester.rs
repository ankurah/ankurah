//! The requester's side of a state comparison, shared by Fetch and live
//! queries. It offers its copy of a query's result, takes the responder's
//! account of its result as the target, and waits until its copy matches or
//! exceeds the target as the responder's pushes apply. It repairs what does
//! not arrive: at once where the responder pushed nothing, when its copy stops
//! progressing for an idle interval, and, for a live query, when two periodic
//! checks disagree.

mod coverage;
mod deadline;
mod descent;
mod target;

use self::{
    deadline::{within, IdleDeadline},
    target::{Outstanding, Target},
};
use super::ResidentSnapshot;
use crate::{
    error::{RequestError, RetrievalError},
    node::Node,
    policy::PolicyAgent,
    storage::StorageEngine,
    util::Instant,
};
use ankql::ast::{Resolved, Selection};
use ankurah_proto::{
    self as proto, state_sync::limits::MAX_PAGE_HEADS, EntityId, KnownState, SpanAnswer, SpanContent, SpanQuestion, TreeSpan, TreeSpec,
};
use futures::future::Either;
use std::{collections::HashSet, time::Duration};

/// What a live query's sync settled on, which its periodic checks reuse: the
/// tree its target's spans address, and the round trip last measured to its
/// peer.
#[derive(Clone, Debug)]
pub struct SyncedQuery {
    pub(crate) spec: TreeSpec,
    pub(crate) round_trip: Duration,
}

/// What one periodic check of a live query found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckOutcome {
    /// The copy matches or exceeds the peer's result.
    Agreed,
    /// The copy differs, or the peer did not answer in time.
    Disagreed,
    /// The copy differed on a second check in a row, and a repair brought it
    /// level.
    Repaired,
}

/// Read `selection`'s result from `peer` into this node: offer the local copy,
/// and return once the copy matches or exceeds the account the peer answers
/// with, which its pushes bring the copy to.
pub(crate) async fn fetch<SE, PA>(
    node: &Node<SE, PA>,
    peer: EntityId,
    selection: Selection<Resolved>,
    credentials: Vec<PA::ContextData>,
) -> Result<(), RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let connection = node.peer_connections.get(&peer).ok_or(RequestError::PeerNotConnected)?;
    let mut sync = QuerySync::new(node, peer, selection, credentials);
    let offer = sync.offer().await?;
    // Registered before the request goes out: the peer may push before it answers.
    let _in_flight = connection.fetches.register(sync.credentials.clone());
    drop(connection);
    let request = proto::NodeRequestBody::Fetch { selection: sync.selection.clone(), known_state: offer.clone() };
    let target = match sync.open(request).await? {
        proto::NodeResponseBody::Fetch(target) => target,
        other => return Err(RequestError::UnexpectedResponse(other).into()),
    };
    sync.until_ready(Target::new(target)?, OpeningPush::of(&offer)).await
}

/// Subscribe `selection` as `query_id` at `peer`: offer the local copy, and
/// return once the copy matches or exceeds the account the peer answers with,
/// which its initial pushes bring the copy to. Later changes arrive as the
/// peer's subscription updates.
pub(crate) async fn subscribe<SE, PA>(
    node: &Node<SE, PA>,
    peer: EntityId,
    query_id: proto::QueryId,
    selection: Selection<Resolved>,
    credentials: Vec<PA::ContextData>,
    version: u32,
) -> Result<SyncedQuery, RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut sync = QuerySync::new(node, peer, selection, credentials);
    let offer = sync.offer().await?;
    let request =
        proto::NodeRequestBody::SubscribeQuery { query_id, selection: sync.selection.clone(), version, known_state: offer.clone() };
    let target = match sync.open(request).await? {
        proto::NodeResponseBody::QuerySubscribed { target, .. } => target,
        other => return Err(RequestError::UnexpectedResponse(other).into()),
    };
    let target = Target::new(target)?;
    let spec = target.spec.clone();
    sync.until_ready(target, OpeningPush::of(&offer)).await?;
    Ok(SyncedQuery { spec, round_trip: sync.round_trip })
}

/// Check a live query's copy against `peer`'s result with one question about
/// the whole tree: its heads when the copy fits one page of them, so that a
/// small live query never builds a digest tree, and its digest otherwise. A
/// check that disagrees right after another, `confirm`, repairs the copy,
/// since pushes still in flight explain one disagreement but not two.
pub(crate) async fn check<SE, PA>(
    node: &Node<SE, PA>,
    peer: EntityId,
    selection: Selection<Resolved>,
    credentials: Vec<PA::ContextData>,
    synced: &SyncedQuery,
    confirm: bool,
) -> Result<CheckOutcome, RetrievalError>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut sync = QuerySync::new(node, peer, selection, credentials);
    sync.measured(synced.round_trip);
    let question = {
        let snapshot = sync.snapshot(Some(synced.spec.clone())).await?.snapshot;
        let fits_a_page = snapshot.entries.len() <= MAX_PAGE_HEADS && snapshot.page(TreeSpan::all())?.bounds == TreeSpan::all();
        if fits_a_page {
            SpanQuestion::Heads(TreeSpan::all())
        } else {
            SpanQuestion::Digest(TreeSpan::all())
        }
    };
    let Some(answer) = sync.compare(synced.spec.clone(), vec![question]).await?.and_then(|mut answers| answers.pop()) else {
        return Ok(CheckOutcome::Disagreed);
    };
    let account = match answer {
        SpanAnswer::Digest(digest) => SpanContent::Digest(digest),
        SpanAnswer::Heads(page) => SpanContent::Heads(page),
    };
    let mut target = Target::new(KnownState { spec: synced.spec.clone(), spans: vec![account] })?;
    if sync.outstanding(&mut target).await?.0.is_empty() {
        return Ok(CheckOutcome::Agreed);
    }
    if !confirm {
        return Ok(CheckOutcome::Disagreed);
    }
    sync.until_ready(target, OpeningPush::nothing()).await?;
    Ok(CheckOutcome::Repaired)
}

/// One query's sync with one peer, from the requester's side: what it asks,
/// under which credentials, and its idle deadline.
struct QuerySync<'a, SE, PA: PolicyAgent> {
    node: &'a Node<SE, PA>,
    peer: EntityId,
    selection: Selection<Resolved>,
    credentials: Vec<PA::ContextData>,
    deadline: IdleDeadline,
    /// The round trip last measured to the peer.
    round_trip: Duration,
}

impl<'a, SE, PA> QuerySync<'a, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn new(node: &'a Node<SE, PA>, peer: EntityId, selection: Selection<Resolved>, credentials: Vec<PA::ContextData>) -> Self {
        Self { node, peer, selection, credentials, deadline: IdleDeadline::new(), round_trip: Duration::ZERO }
    }

    /// A snapshot of the copy: this node's resident for the query, under the
    /// target's `spec` once there is one.
    async fn snapshot(&self, spec: Option<TreeSpec>) -> Result<ResidentSnapshot, RetrievalError> {
        Ok(self.node.resident_trees.snapshot_through_changes(self.node, self.selection.clone(), spec, self.credentials.clone()).await?)
    }

    /// The copy's account of itself, which the opening request offers.
    async fn offer(&self) -> Result<KnownState, RetrievalError> { Ok(self.snapshot(None).await?.snapshot.offer().await?) }

    fn measured(&mut self, round_trip: Duration) {
        self.round_trip = round_trip;
        self.deadline.measured(round_trip);
        self.deadline.progressed();
    }

    /// Send the request that opens the query and wait for its response, as
    /// long as it takes, as main's requests do.
    async fn open(&mut self, request: proto::NodeRequestBody) -> Result<proto::NodeResponseBody, RetrievalError> {
        let started = Instant::now();
        let response = self.node.request(self.peer, &self.credentials, request).await?;
        self.measured(started.elapsed());
        answered(response)
    }

    /// Send a comparison request and wait for its response within the idle
    /// interval: none when it takes longer, since a request has no timeout of
    /// its own.
    async fn ask(&mut self, request: proto::NodeRequestBody) -> Result<Option<proto::NodeResponseBody>, RetrievalError> {
        let started = Instant::now();
        let Some(response) = within(self.deadline.interval(), self.node.request(self.peer, &self.credentials, request)).await else {
            return Ok(None);
        };
        let response = response?;
        self.round_trip = started.elapsed();
        self.deadline.measured(self.round_trip);
        answered(response).map(Some)
    }

    /// What the copy does not yet match or exceed of `target`, and the signal
    /// of the copy's next change.
    async fn outstanding(&self, target: &mut Target) -> Result<(Outstanding, tokio::sync::watch::Receiver<u64>), RetrievalError> {
        let ResidentSnapshot { mut snapshot, changes } = self.snapshot(Some(target.spec.clone())).await?;
        Ok((target.outstanding(self.node, &mut snapshot).await?, changes))
    }

    /// Wait until the copy matches or exceeds `target`. What `push` does not
    /// bring is repaired at once; the rest is waited for, and compared again
    /// whenever the copy has not progressed for the idle interval.
    async fn until_ready(&mut self, mut target: Target, push: OpeningPush) -> Result<(), RetrievalError> {
        let mut last = usize::MAX;
        let mut unpushed_repaired = false;
        loop {
            let (outstanding, mut changes) = self.outstanding(&mut target).await?;
            if outstanding.is_empty() {
                return Ok(());
            }
            if outstanding.len() < last {
                self.deadline.progressed();
            }
            last = outstanding.len();
            if !unpushed_repaired {
                unpushed_repaired = true;
                let unpushed = push.unpushed(&outstanding);
                if !unpushed.is_empty() {
                    self.repair(&mut target, unpushed, &push).await?;
                    continue;
                }
            }
            let changed = {
                let changed = changes.changed();
                futures::pin_mut!(changed);
                let idle = futures_timer::Delay::new(self.deadline.interval());
                matches!(futures::future::select(changed, idle).await, Either::Left(_))
            };
            if changed {
                self.deadline.progressed();
                continue;
            }
            if !self.deadline.compare_again() {
                return Err(RetrievalError::SyncStalled { peer: self.peer });
            }
            self.repair(&mut target, outstanding, &OpeningPush::nothing()).await?;
        }
    }
}

/// A response's body, or its error as a typed request error.
fn answered(response: proto::NodeResponseBody) -> Result<proto::NodeResponseBody, RetrievalError> {
    match response {
        proto::NodeResponseBody::Error(error) => Err(RequestError::ServerError(error).into()),
        response => Ok(response),
    }
}

/// What a responder's opening push brings, read from the offer it compared:
/// each entity it holds outside the offer's nonempty digest spans, since a
/// digest does not say which entities differ, and each entity the offer's
/// inventories listed that has left its result.
struct OpeningPush {
    digests: Vec<TreeSpan>,
    listed: HashSet<EntityId>,
}

impl OpeningPush {
    fn of(offer: &KnownState) -> Self {
        let mut push = Self { digests: Vec::new(), listed: HashSet::new() };
        for span in &offer.spans {
            match span {
                SpanContent::Digest(digest) if digest.digest.count() > 0 => push.digests.push(digest.bounds.clone()),
                SpanContent::Digest(_) => {}
                SpanContent::Heads(inventory) => {
                    let mut cursor = inventory.heads.cursor();
                    while let Some(head) = cursor.next() {
                        push.listed.insert(head.entity.entity_id);
                    }
                }
            }
        }
        push
    }

    /// No push, as for a periodic check: everything is repaired.
    fn nothing() -> Self { Self { digests: vec![TreeSpan::all()], listed: HashSet::new() } }

    /// What of `outstanding` this push does not bring.
    fn unpushed(&self, outstanding: &Outstanding) -> Outstanding {
        let digest_described = |bounds: &TreeSpan| self.digests.iter().any(|span| span.intersect(bounds).is_some());
        Outstanding {
            heads: outstanding
                .heads
                .iter()
                .filter(|(_, address, _)| self.digests.iter().any(|span| span.contains(address)))
                .cloned()
                .collect(),
            spans: outstanding.spans.iter().filter(|region| digest_described(region.bounds())).cloned().collect(),
            extras: outstanding.extras.iter().filter(|(id, _)| !self.listed.contains(id)).cloned().collect(),
        }
    }
}
