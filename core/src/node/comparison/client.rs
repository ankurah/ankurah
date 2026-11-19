//! Incremental comparison and causal repair of one captured query result.

use std::{
    collections::{BTreeMap, HashSet},
    future::Future,
    num::NonZeroUsize,
    sync::Mutex,
};

use ankql::ast::{Resolved, Selection};
use ankurah_digest_tree::{ComparisonError, Difference, DigestTree, Page, PageGetter, Ristretto255, Row, RowKey, Span, Store};
use ankurah_proto::{
    Attested, Clock, ComparisonFailure, ComparisonId, ComparisonPage, ComparisonReply, ComparisonRequest, ComparisonTarget, EntityId,
    EntityState, KnownEntity, NodeRequestBody, NodeResponseBody, RepairMember,
};
use async_trait::async_trait;
use futures::{
    future::{select, Either},
    pin_mut,
};
use web_time::Instant;

use super::{
    wire::{key_spec_to_wire, page_from_wire},
    RepairReceipt,
};
use crate::{
    error::{RequestError, RetrievalError},
    node::{trees::ComparisonTree, Node},
    policy::{ComparisonLimits, ContextPolicy, PolicyAgent},
    storage::{StateScan, StorageEngine},
    sync::{EntityHead, EntityMemberId, TreeProperty},
};

/// Shared across renewed captures: restarting never renews the work allowance
/// or deadline. Bytes/events are limited per exchange; work, including each
/// inspected KiB and causal-proof read, is cumulative.
pub(in crate::node) struct ComparisonBudget {
    limits: ComparisonLimits,
    started: Instant,
    steps: Mutex<usize>,
}

impl ComparisonBudget {
    pub(in crate::node) fn new(limits: ComparisonLimits) -> Self { Self { limits, started: Instant::now(), steps: Mutex::new(0) } }

    pub(in crate::node) fn check(&self) -> Result<(), RetrievalError> {
        if self.started.elapsed() >= self.limits.lifetime {
            return Err(failure(ComparisonFailure::Expired));
        }
        Ok(())
    }

    fn charge(&self, steps: usize) -> Result<(), RetrievalError> {
        self.check()?;
        let mut used = self.steps.lock().unwrap();
        *used =
            used.checked_add(steps).filter(|sum| *sum <= self.limits.max_steps).ok_or_else(|| failure(ComparisonFailure::LimitExceeded))?;
        Ok(())
    }

    fn bytes(&self, bytes: usize) -> Result<(), RetrievalError> {
        self.check()?;
        if bytes > self.limits.max_bytes {
            return Err(failure(ComparisonFailure::LimitExceeded));
        }
        self.charge(bytes.div_ceil(1024))
    }

    fn receipt(&self, receipt: &RepairReceipt) -> Result<(), RetrievalError> {
        self.bytes(receipt.bytes)?;
        if receipt.events > self.limits.max_events {
            return Err(failure(ComparisonFailure::LimitExceeded));
        }
        self.charge(receipt.events)
    }

    pub(in crate::node) async fn wait<T>(&self, future: impl Future<Output = Result<T, RetrievalError>>) -> Result<T, RetrievalError> {
        self.check()?;
        let delay = futures_timer::Delay::new(self.limits.lifetime.saturating_sub(self.started.elapsed()));
        pin_mut!(future, delay);
        match select(future, delay).await {
            Either::Left((result, _)) => {
                self.check()?;
                result
            }
            Either::Right(_) => Err(failure(ComparisonFailure::Expired)),
        }
    }
}

impl<SE: StorageEngine + 'static, PA: PolicyAgent> Node<SE, PA> {
    pub(in crate::node) async fn compare_query(
        &self,
        peer: EntityId,
        selection: &Selection<Resolved>,
        capture: &ComparisonTree<SE>,
        target: ComparisonTarget,
        credentials: &Vec<PA::ContextData>,
        budget: &mut ComparisonBudget,
    ) -> Result<(), RetrievalError> {
        let exchange = NodeExchange { node: self, peer, selection, comparison: target.id, credentials, budget };
        compare(capture, target, &exchange, budget).await
    }
}

/// Keeps transport and canonical-state admission separate from tree traversal.
#[async_trait]
trait Exchange: Sync {
    async fn pages(&self, spans: Vec<Span>) -> Result<Vec<ComparisonPage>, RetrievalError>;
    async fn heads(&self, ids: &[EntityId]) -> Result<Vec<KnownEntity>, RetrievalError>;
    async fn repair(&self, members: Vec<RepairMember>, events_only: bool) -> Result<RepairReceipt, RetrievalError>;
    async fn covers(&self, id: EntityId, local: &Clock, remote: &Clock) -> Result<bool, RetrievalError>;
}

struct NodeExchange<'a, SE, PA: PolicyAgent> {
    node: &'a Node<SE, PA>,
    peer: EntityId,
    selection: &'a Selection<Resolved>,
    comparison: ComparisonId,
    credentials: &'a Vec<PA::ContextData>,
    budget: &'a ComparisonBudget,
}

impl<SE: StorageEngine + 'static, PA: PolicyAgent> NodeExchange<'_, SE, PA> {
    async fn states(&self, ids: &[EntityId]) -> Result<Vec<Option<Attested<EntityState>>>, RetrievalError> {
        self.budget.charge(ids.len())?;
        let mut snapshot = self.node.storage.state_snapshot(StateScan::Changes(None)).await?;
        let states = snapshot.get_states(ids).await?;
        if states.len() != ids.len() {
            return Err(crate::sync::Error::AnswerMisfit.into());
        }
        for (id, state) in ids.iter().zip(&states) {
            if let Some(state) = state {
                self.budget.charge(serialized_size(state)?.div_ceil(1024))?;
                if state.payload.entity_id != *id {
                    return Err(crate::sync::Error::AnswerMisfit.into());
                }
            }
        }
        snapshot.finish().await?;
        Ok(states)
    }
}

#[async_trait]
impl<SE: StorageEngine + 'static, PA: PolicyAgent> Exchange for NodeExchange<'_, SE, PA> {
    async fn pages(&self, spans: Vec<Span>) -> Result<Vec<ComparisonPage>, RetrievalError> {
        let body = NodeRequestBody::Compare {
            comparison: self.comparison,
            selection: self.selection.clone(),
            request: ComparisonRequest::Pages { spans },
        };
        match self.node.request(self.peer, self.credentials, body).await? {
            NodeResponseBody::Compared(Ok(ComparisonReply::Pages(pages))) => Ok(pages),
            NodeResponseBody::Compared(Err(error)) => Err(failure(error)),
            NodeResponseBody::Error(error) => Err(RequestError::ServerError(error).into()),
            response => Err(RequestError::UnexpectedResponse(response).into()),
        }
    }

    async fn heads(&self, ids: &[EntityId]) -> Result<Vec<KnownEntity>, RetrievalError> {
        // Canonical heads establish causal coverage internally. Read policy
        // controls what may leave the node, not which accepted state exists.
        let states = self.states(ids).await?;
        Ok(ids
            .iter()
            .zip(states)
            .map(|(&entity_id, state)| KnownEntity { entity_id, head: state.map(|state| state.payload.state.head).unwrap_or_default() })
            .collect())
    }

    async fn repair(&self, members: Vec<RepairMember>, events_only: bool) -> Result<RepairReceipt, RetrievalError> {
        let ids: Vec<_> = members.iter().map(|member| member.known.entity_id).collect();
        let states = self.states(&ids).await?;
        // Finish the snapshot before checking current policy: no asynchronous
        // work may separate this disclosure check from sending the request.
        let policy = ContextPolicy::from_credentials(&self.node.policy_agent, self.credentials);
        let (members, withheld) = disclosable_members(members, states, |id, state| policy.check_read(id, state).is_ok())?;
        if members.is_empty() {
            return Ok(RepairReceipt { unavailable: withheld, bytes: 0, events: 0 });
        }
        self.budget.bytes(serialized_size(&members)?)?;
        let mut receipt =
            self.node.request_comparison_repair(self.peer, self.comparison, self.selection, members, events_only, self.credentials).await?;
        receipt.unavailable.extend(withheld);
        Ok(receipt)
    }

    async fn covers(&self, id: EntityId, local: &Clock, remote: &Clock) -> Result<bool, RetrievalError> {
        self.node.comparison_covers(id, local, remote, self.credentials, &|steps| self.budget.charge(steps)).await
    }
}

fn disclosable_members(
    members: Vec<RepairMember>,
    states: Vec<Option<Attested<EntityState>>>,
    can_read: impl Fn(&EntityId, &ankurah_proto::State) -> bool,
) -> Result<(Vec<RepairMember>, Vec<EntityId>), RetrievalError> {
    if members.len() != states.len() {
        return Err(crate::sync::Error::AnswerMisfit.into());
    }
    let mut disclosed = Vec::with_capacity(members.len());
    let mut withheld = Vec::new();
    for (mut member, state) in members.into_iter().zip(states) {
        let id = member.known.entity_id;
        if state.as_ref().is_some_and(|state| state.payload.entity_id != id) {
            return Err(crate::sync::Error::AnswerMisfit.into());
        }
        let readable = state.as_ref().is_some_and(|state| can_read(&id, &state.payload.state));
        if !readable && member.key.is_none() {
            // This identity came only from our capture. A later denial must
            // hide the identity itself, as well as its current head.
            withheld.push(id);
            continue;
        }
        // A captured remote address was already disclosed by the peer. It can
        // still be repaired cold without revealing a denied local head.
        member.known.head = state.filter(|_| readable).map(|state| state.payload.state.head).unwrap_or_default();
        disclosed.push(member);
    }
    Ok((disclosed, withheld))
}

struct RemotePages<'a, E> {
    exchange: &'a E,
    budget: &'a ComparisonBudget,
    initial: Option<ComparisonPage>,
}

impl<E: Exchange> PageGetter<EntityMemberId, EntityHead, Ristretto255> for RemotePages<'_, E> {
    type Error = RetrievalError;

    async fn get_pages(&mut self, spans: Vec<Span>) -> Result<Vec<Page<EntityMemberId, EntityHead, Ristretto255>>, RetrievalError> {
        if spans.len() > self.budget.limits.max_spans {
            return Err(failure(ComparisonFailure::LimitExceeded));
        }
        self.budget.charge(spans.len())?;
        let pages = match self.initial.take() {
            Some(page) if spans == [Span::all()] => vec![page],
            _ => self.budget.wait(self.exchange.pages(spans)).await?,
        };
        self.budget.bytes(serialized_size(&pages)?)?;
        // Charge every admitted description, including equal/pruned subtrees.
        for page in &pages {
            self.budget.charge(match page {
                Page::Digests(parts) => parts.len(),
                Page::Members(members) => members.len(),
            })?;
        }
        Ok(pages.into_iter().map(page_from_wire).collect())
    }
}

async fn compare<S, E>(
    capture: &DigestTree<TreeProperty, EntityMemberId, EntityHead, Ristretto255, S>,
    target: ComparisonTarget,
    exchange: &E,
    budget: &ComparisonBudget,
) -> Result<(), RetrievalError>
where
    S: Store<RowKey, Row<TreeProperty, EntityHead, Ristretto255>>,
    E: Exchange,
{
    budget.bytes(serialized_size(&target)?)?;
    if key_spec_to_wire(capture.key_spec()) != target.key_spec {
        return Err(failure(ComparisonFailure::KeySpecMismatch));
    }
    let view = capture.view();
    let ours = budget.wait(async { view.span_digest(..).await.map_err(view_error) }).await?;
    if ours.digest == target.known_state.digest {
        return Ok(());
    }
    let batch = NonZeroUsize::new(budget.limits.max_spans.min(budget.limits.max_members))
        .ok_or_else(|| failure(ComparisonFailure::LimitExceeded))?;
    let getter = RemotePages { exchange, budget, initial: target.page };
    let mut comparison = view.compare(getter, batch);
    while let Some(differences) = budget.wait(async { comparison.next_batch().await.map_err(comparison_error) }).await? {
        // An empty remote span can enumerate only local members without another
        // getter call. Charge those batches too; never retain a global ID set.
        budget.charge(differences.len())?;
        reconcile(differences, exchange, budget).await?;
    }
    Ok(())
}

struct Claim {
    key: Option<Vec<u8>>,
    expected: Option<Clock>,
}

async fn reconcile<E: Exchange>(
    differences: Vec<Difference<EntityMemberId, EntityHead>>,
    exchange: &E,
    budget: &ComparisonBudget,
) -> Result<(), RetrievalError> {
    let mut claims = BTreeMap::<EntityId, Claim>::new();
    for difference in differences {
        if let Some(local) = difference.ours {
            claims.entry(local.id.0).or_insert(Claim { key: None, expected: None });
        }
        if let Some(remote) = difference.theirs {
            if let Some(previous) = claims.get(&remote.id.0).filter(|claim| claim.expected.is_some()) {
                if previous.key.as_ref() != Some(&remote.key) || previous.expected.as_ref() != Some(&remote.payload.0) {
                    return Err(invalid("one batch names two captured addresses for the same entity"));
                }
            }
            claims.insert(remote.id.0, Claim { key: Some(remote.key), expected: Some(remote.payload.0) });
        }
    }
    if budget.limits.max_members == 0 {
        return Err(failure(ComparisonFailure::LimitExceeded));
    }
    let mut claims = claims.into_iter().peekable();
    while claims.peek().is_some() {
        let chunk = claims.by_ref().take(budget.limits.max_members).collect();
        reconcile_claims(chunk, exchange, budget).await?;
    }
    Ok(())
}

async fn reconcile_claims<E: Exchange>(
    claims: BTreeMap<EntityId, Claim>,
    exchange: &E,
    budget: &ComparisonBudget,
) -> Result<(), RetrievalError> {
    let ids: Vec<_> = claims.keys().copied().collect();
    let heads = read_heads(exchange, &ids, budget).await?;
    let mut members = Vec::new();
    for known in heads {
        let claim = &claims[&known.entity_id];
        if let Some(expected) = &claim.expected {
            if budget.wait(exchange.covers(known.entity_id, &known.head, expected)).await? {
                continue;
            }
        }
        members.push(RepairMember { known, key: claim.key.clone() });
    }
    if members.is_empty() {
        return Ok(());
    }
    repair(exchange, members, false, &claims, budget).await?;

    // Re-read canonical heads after delivery. A remote claim about causality is
    // insufficient, and a concurrent local head must not be replaced by a state.
    let expected_ids: Vec<_> = claims.iter().filter_map(|(&id, claim)| claim.expected.as_ref().map(|_| id)).collect();
    let missing = uncovered(exchange, &expected_ids, &claims, budget).await?;
    if !missing.is_empty() {
        repair(exchange, missing, true, &claims, budget).await?;
        if !uncovered(exchange, &expected_ids, &claims, budget).await?.is_empty() {
            return Err(invalid("repair did not establish coverage of the captured entity head"));
        }
    }
    Ok(())
}

async fn read_heads<E: Exchange>(exchange: &E, ids: &[EntityId], budget: &ComparisonBudget) -> Result<Vec<KnownEntity>, RetrievalError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let heads = budget.wait(exchange.heads(ids)).await?;
    if heads.len() != ids.len() || heads.iter().zip(ids).any(|(known, id)| known.entity_id != *id) {
        return Err(crate::sync::Error::AnswerMisfit.into());
    }
    Ok(heads)
}

async fn uncovered<E: Exchange>(
    exchange: &E,
    ids: &[EntityId],
    claims: &BTreeMap<EntityId, Claim>,
    budget: &ComparisonBudget,
) -> Result<Vec<RepairMember>, RetrievalError> {
    let mut missing = Vec::new();
    for known in read_heads(exchange, ids, budget).await? {
        let claim = &claims[&known.entity_id];
        if !budget.wait(exchange.covers(known.entity_id, &known.head, claim.expected.as_ref().unwrap())).await? {
            missing.push(RepairMember { known, key: claim.key.clone() });
        }
    }
    Ok(missing)
}

async fn repair<E: Exchange>(
    exchange: &E,
    members: Vec<RepairMember>,
    events_only: bool,
    claims: &BTreeMap<EntityId, Claim>,
    budget: &ComparisonBudget,
) -> Result<(), RetrievalError> {
    if members.len() > budget.limits.max_members {
        return Err(failure(ComparisonFailure::LimitExceeded));
    }
    budget.charge(members.len())?;
    budget.bytes(serialized_size(&members)?)?;
    let mut requested: HashSet<_> = members.iter().map(|member| member.known.entity_id).collect();
    let receipt = budget.wait(exchange.repair(members, events_only)).await?;
    budget.receipt(&receipt)?;
    for id in receipt.unavailable {
        if !requested.remove(&id) {
            return Err(invalid("repair returned an unrequested or repeated unavailable identity"));
        }
        if claims[&id].expected.is_some() {
            return Err(failure(ComparisonFailure::Restart));
        }
        // A missing known identity does not authorize deleting local history.
    }
    Ok(())
}

fn failure(error: ComparisonFailure) -> RetrievalError { RetrievalError::storage(error) }
fn invalid(message: &str) -> RetrievalError { failure(ComparisonFailure::InvalidRequest(message.into())) }
fn serialized_size(value: &impl serde::Serialize) -> Result<usize, RetrievalError> {
    usize::try_from(bincode::serialized_size(value).map_err(RetrievalError::from)?).map_err(|_| failure(ComparisonFailure::LimitExceeded))
}

pub(in crate::node) fn view_error(error: ankurah_digest_tree::Error) -> RetrievalError {
    if matches!(&error, ankurah_digest_tree::Error::Store(error) if error.is_retryable()) {
        return failure(ComparisonFailure::Restart);
    }
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = cause {
        if matches!(current.downcast_ref::<crate::sync::Error>(), Some(crate::sync::Error::Retired(_))) {
            return failure(ComparisonFailure::Restart);
        }
        cause = current.source();
    }
    RetrievalError::storage(error)
}

fn comparison_error(error: ComparisonError<RetrievalError>) -> RetrievalError {
    match error {
        ComparisonError::Getter(error) => error,
        ComparisonError::View(error) => view_error(error),
        error => RetrievalError::storage(error),
    }
}

#[cfg(test)]
mod tests;
