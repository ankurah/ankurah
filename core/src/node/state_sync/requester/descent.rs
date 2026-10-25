//! The requester's repair of what its copy does not cover. It descends from
//! the regions it has not matched to pages of the responder's heads, refining
//! its own tree over a differing span and asking the responder's digests of
//! exactly the child bounds, then asks the responder, in SyncEntities, to push
//! what the copy lacks or holds older, and the local entities its pages do not
//! list.

use super::{
    target::{Outstanding, Region, Target},
    OpeningPush, QuerySync,
};
use crate::{error::RetrievalError, node::state_sync::tree::Snapshot, policy::PolicyAgent, storage::StorageEngine};
use ankurah_proto::{
    state_sync::limits::{MAX_HEAD_QUESTIONS, MAX_OFFER_SPANS, MAX_PAGE_HEADS},
    SpanAnswer, SpanQuestion, TreeSpan, TreeSpanDigest,
};
use std::collections::VecDeque;

/// One question of a descent, about a span of a region not matched yet.
enum Step {
    /// The responder's digest of the bounds of `local`, a child of the copy's
    /// own tree with the copy's digest of it.
    Digest(TreeSpanDigest),
    /// A page of the responder's heads from the start of the bounds.
    Heads(TreeSpan),
}

impl Step {
    fn question(&self) -> SpanQuestion {
        match self {
            Step::Digest(local) => SpanQuestion::Digest(local.bounds.clone()),
            Step::Heads(bounds) => SpanQuestion::Heads(bounds.clone()),
        }
    }
}

impl<SE, PA> QuerySync<'_, SE, PA>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    /// Repair `outstanding`, what the copy did not cover of `target`: descend
    /// into its regions, then name in SyncEntities each head and local entity
    /// that remains and `push` does not bring. A request that finds no answer
    /// in time ends the repair early; the idle deadline compares again.
    pub(super) async fn repair(&mut self, target: &mut Target, outstanding: Outstanding, push: &OpeningPush) -> Result<(), RetrievalError> {
        let mut outstanding = outstanding;
        if !outstanding.spans.is_empty() {
            if !self.descend(target, outstanding.spans).await? {
                return Ok(());
            }
            outstanding = push.unpushed(&self.outstanding(target).await?.0);
        }
        let entities = outstanding
            .heads
            .into_iter()
            .map(|head| (head.entity_id, head.local))
            .chain(outstanding.unlisted.into_iter().map(|(id, local)| (id, Some(local))))
            .collect();
        self.sync_entities(target, entities).await
    }

    /// Bring each region in `regions` down to pages of the responder's heads,
    /// in as few CompareSpans as the limits allow; false when a request found
    /// no answer in time.
    async fn descend(&mut self, target: &mut Target, regions: Vec<Region>) -> Result<bool, RetrievalError> {
        let mut steps = VecDeque::new();
        let mut snapshot = self.snapshot(Some(target.spec.clone())).await?.snapshot;
        for region in regions {
            plan(&mut snapshot, region, &mut steps).await?;
        }
        drop(snapshot);
        while !steps.is_empty() {
            let mut batch = Vec::new();
            let mut heads = 0;
            while let Some(step) = steps.pop_front() {
                let asks_heads = matches!(step, Step::Heads(_));
                if batch.len() == MAX_OFFER_SPANS || (asks_heads && heads == MAX_HEAD_QUESTIONS) {
                    steps.push_front(step);
                    break;
                }
                heads += usize::from(asks_heads);
                batch.push(step);
            }
            let questions = batch.iter().map(Step::question).collect();
            let Some(answers) = self.compare(target.spec.clone(), questions).await? else {
                return Ok(false);
            };
            let mut snapshot = self.snapshot(Some(target.spec.clone())).await?.snapshot;
            for (step, answer) in batch.into_iter().zip(answers) {
                match (step, answer) {
                    (Step::Digest(local), SpanAnswer::Digest(theirs)) if theirs.digest == local.digest => {
                        target.settle(Region::Matched(local.bounds));
                    }
                    (Step::Digest(local), SpanAnswer::Digest(theirs)) => {
                        // Keep the copy's prefix: its own tree refines the span further.
                        let region = Region::Digest(TreeSpanDigest { digest: theirs.digest, ..local });
                        target.settle(region.clone());
                        plan(&mut snapshot, region, &mut steps).await?;
                    }
                    (Step::Heads(bounds), SpanAnswer::Heads(page)) => {
                        // A page that stopped short leaves the rest of its span to ask for.
                        steps.extend(bounds.gaps(std::slice::from_ref(&page.bounds)).into_iter().map(Step::Heads));
                        target.settle_page(page);
                    }
                    _ => unreachable!("validated answers match their questions"),
                }
            }
        }
        Ok(true)
    }
}

/// Plan the questions that learn `region`: a page of heads when its digest
/// counts at most a page of the responder's entries, or when the copy's tree
/// cannot split it; otherwise the responder's digests of the children the
/// copy's tree splits it into.
async fn plan(snapshot: &mut Snapshot, region: Region, steps: &mut VecDeque<Step>) -> anyhow::Result<()> {
    match region {
        Region::Digest(digest) if digest.digest.count() <= MAX_PAGE_HEADS as i64 => steps.push_back(Step::Heads(digest.bounds)),
        Region::Digest(digest) => match snapshot.refine(&digest).await? {
            children if children.len() > 1 => steps.extend(children.into_iter().map(Step::Digest)),
            _ => steps.push_back(Step::Heads(digest.bounds)),
        },
        Region::Unknown(bounds) => steps.push_back(Step::Heads(bounds)),
        Region::Listed(_) | Region::Matched(_) => {}
    }
    Ok(())
}
