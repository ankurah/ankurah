//! The requester's side of a state comparison, shared by Fetch and live
//! queries. It offers its copy of a query's result, takes the responder's
//! account of its result as the target, and waits until its copy matches or
//! exceeds the target as the responder's pushes apply. It repairs what does
//! not arrive: at once where the responder pushed nothing, when its copy stops
//! progressing for an idle interval, and, for a live query, when two periodic
//! checks disagree. A push that fails to apply for what it waits on ends the
//! wait at once: it fails a Fetch or a subscribe, while a periodic check
//! reports it and leaves its entity stale, as a live update that fails to
//! apply leaves it.

mod coverage;
mod deadline;
mod descent;
mod target;

#[cfg(test)]
mod tests;

use self::{
    deadline::{within, IdleDeadline},
    target::{Outstanding, Target},
};
use super::ResidentSnapshot;
use crate::{
    error::{ApplyError, RequestError, RetrievalError},
    node::{applier::ApplyFailure, Node},
    policy::PolicyAgent,
    storage::StorageEngine,
    util::Instant,
};
use ankql::ast::{Resolved, Selection};
use ankurah_proto::{
    self as proto, state_sync::limits::MAX_PAGE_HEADS, EntityId, KnownState, SpanAnswer, SpanContent, SpanQuestion, TreeSpan, TreeSpec,
};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::broadcast;

/// What a live query's sync settled on, which its periodic checks reuse: the
/// tree its target's spans address, and the round trip last measured to its
/// peer.
#[derive(Clone, Debug)]
pub struct SyncedQuery {
    pub(crate) spec: TreeSpec,
    pub(crate) round_trip: Duration,
}

/// What one periodic check of a live query found.
#[derive(Clone, Debug)]
pub enum CheckOutcome {
    /// The copy matches or exceeds the peer's result.
    Agreed,
    /// The copy differs, or the peer did not answer in time.
    Disagreed,
    /// The copy differed on a second check in a row, and a repair brought it
    /// level.
    Repaired,
    /// The copy differed on a second check in a row, and an item the peer
    /// pushed to repair `entity_id` failed to apply, with `error`.
    RepairFailed { entity_id: EntityId, error: Arc<ApplyError> },
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
    Ok(sync.until_ready(Target::new(target)?, OpeningPush::of(&offer)).await??)
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
    sync.until_ready(target, OpeningPush::of(&offer)).await??;
    Ok(SyncedQuery { spec, round_trip: sync.round_trip })
}

/// Check a live query's copy against `peer`'s result with one question about
/// the whole tree: its heads when the copy fits one page of them, so that a
/// small live query never builds a digest tree, and its digest otherwise. A
/// check that disagrees right after another, `confirm`, repairs the copy,
/// since pushes still in flight explain one disagreement but not two. A pushed
/// item that fails to apply ends the repair as the check's outcome, not as an
/// error, so that the query stays established, as it does when a live update
/// fails to apply.
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
    Ok(match sync.until_ready(target, OpeningPush::nothing()).await? {
        Ok(()) => CheckOutcome::Repaired,
        Err(ApplyFailure { entity_id, error, .. }) => CheckOutcome::RepairFailed { entity_id, error },
    })
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
    /// The node's failures to apply pushed items since the sync began, which
    /// is before its first request: the peer may push before it answers.
    failures: broadcast::Receiver<ApplyFailure>,
}

impl<'a, SE, PA> QuerySync<'a, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    fn new(node: &'a Node<SE, PA>, peer: EntityId, selection: Selection<Resolved>, credentials: Vec<PA::ContextData>) -> Self {
        let failures = node.apply_failures.listen();
        Self { node, peer, selection, credentials, deadline: IdleDeadline::new(), round_trip: Duration::ZERO, failures }
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

    /// Send the request that opens the query and wait for its response as long
    /// as it takes: the request has no timeout, and a lost connection ends it.
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

    /// The failure of the next item the peer pushed for an entity that
    /// `outstanding` waits on. A failure the sync lagged behind is lost, which
    /// leaves its entity to the idle deadline, as a lost push is.
    async fn failure_concerning(&mut self, outstanding: &Outstanding) -> ApplyFailure {
        loop {
            match self.failures.recv().await {
                Ok(failure) if failure.peer == self.peer && outstanding.waits_on(&failure.entity_id) => return failure,
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                // The node, which publishes failures, outlives every sync it runs.
                Err(broadcast::error::RecvError::Closed) => return futures::future::pending().await,
            }
        }
    }

    /// Wait until the copy matches or exceeds `target`. What `push` does not
    /// bring is repaired at once; the rest is waited for, and compared again
    /// whenever the copy has not progressed for the idle interval. A pushed
    /// item that fails to apply for an entity still waited on ends the wait at
    /// once, with that failure, which the caller judges.
    async fn until_ready(&mut self, mut target: Target, push: OpeningPush) -> Result<Result<(), ApplyFailure>, RetrievalError> {
        let mut last = usize::MAX;
        let mut unpushed_repaired = false;
        let mut idle_since = Instant::now();
        loop {
            let (outstanding, mut changes) = self.outstanding(&mut target).await?;
            if outstanding.is_empty() {
                return Ok(Ok(()));
            }
            if outstanding.len() < last {
                self.deadline.progressed();
                idle_since = Instant::now();
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
            let remaining = self.deadline.interval().saturating_sub(idle_since.elapsed());
            let woke = if remaining.is_zero() {
                None
            } else {
                tokio::select! {
                    changed = changes.changed() => Some(changed.is_ok()),
                    failure = self.failure_concerning(&outstanding) => return Ok(Err(failure)),
                    () = futures_timer::Delay::new(remaining) => None,
                }
            };
            match woke {
                Some(true) => {
                    self.deadline.progressed();
                    idle_since = Instant::now();
                }
                // The resident stopped, as a catalog change stops it: look at a new one.
                Some(false) => {}
                None => {
                    if !self.deadline.compare_again() {
                        return Err(RetrievalError::SyncStalled { peer: self.peer });
                    }
                    self.repair(&mut target, outstanding, &OpeningPush::nothing()).await?;
                    idle_since = Instant::now();
                }
            }
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
            heads: outstanding.heads.iter().filter(|head| self.digests.iter().any(|span| span.contains(&head.address))).cloned().collect(),
            spans: outstanding.spans.iter().filter(|region| digest_described(region.bounds())).cloned().collect(),
            unlisted: outstanding.unlisted.iter().filter(|(id, _)| !self.listed.contains(id)).cloned().collect(),
        }
    }
}
