//! The checks a receiver applies before acting on what a peer sent: an offer, an
//! inventory page, a request's span questions or entities, and a response's span
//! answers against the questions they answer.

use std::{collections::HashSet, ops::Bound};

use ankurah_digest_tree::leaf_address;
use anyhow::{bail, ensure};

use super::{
    limits::{MAX_HEAD_QUESTIONS, MAX_OFFER_BYTES, MAX_OFFER_HEADS, MAX_OFFER_SPANS, MAX_PAGE_BYTES, MAX_PAGE_HEADS},
    EntityHead, KnownState, SpanAnswer, SpanContent, SpanQuestion, TreeSpan, TreeSpanInventory, TreeSpec,
};

impl KnownState {
    /// Checks the offer against its own spec: at most `MAX_OFFER_SPANS` disjoint spans
    /// within the tree, valid digests and inventories, and within the offer's limits
    /// each entity at most once.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.spans.len() <= MAX_OFFER_SPANS, "offer has too many spans");
        ensure!(self.work_bytes()? <= MAX_OFFER_BYTES as u64, "offer exceeds its work limit");
        let mut heads = 0;
        for span in &self.spans {
            match span {
                SpanContent::Digest(digest) => digest.validate()?,
                SpanContent::Heads(inventory) => {
                    inventory.validate(&self.spec)?;
                    heads += inventory.heads.len();
                }
            }
        }
        ensure!(heads <= MAX_OFFER_HEADS, "offer exceeds its head limit");
        let mut entities = HashSet::with_capacity(heads);
        for span in &self.spans {
            if let SpanContent::Heads(inventory) = span {
                let mut cursor = inventory.heads.cursor();
                while let Some(head) = cursor.next() {
                    ensure!(entities.insert(head.entity.entity_id), "offer lists entity {} twice", head.entity.entity_id);
                }
            }
        }
        check_disjoint_within_tree(self.spans.iter().map(SpanContent::bounds))
    }
}

/// Checks that spans, in any order, are disjoint and lie within the tree's addresses.
fn check_disjoint_within_tree<'a>(spans: impl Iterator<Item = &'a TreeSpan>) -> anyhow::Result<()> {
    let tree = TreeSpan::all();
    let mut spans: Vec<_> = spans.collect();
    spans.sort_by(|a, b| a.cmp_start(b));
    let mut start = tree.start.clone();
    for span in spans {
        ensure!(
            !matches!(start, Bound::Unbounded) && !span.is_empty() && span.is_subset_of(&TreeSpan::new(start, tree.end.clone())),
            "offer spans overlap or extend past the tree"
        );
        start = match &span.end {
            Bound::Included(end) => Bound::Excluded(end.clone()),
            Bound::Excluded(end) => Bound::Included(end.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
    }
    Ok(())
}

impl TreeSpanInventory {
    /// Checks an inventory page: at most `MAX_PAGE_HEADS` heads within `MAX_PAGE_BYTES`
    /// of work, valid under `spec`.
    pub fn validate_page(&self, spec: &TreeSpec) -> anyhow::Result<()> {
        ensure!(self.heads.len() <= MAX_PAGE_HEADS, "inventory page has too many heads");
        ensure!(self.work_bytes()? <= MAX_PAGE_BYTES as u64, "inventory page exceeds its work limit");
        self.validate(spec)?;
        Ok(())
    }
}

impl SpanQuestion {
    /// Checks one CompareSpans request's questions: at most `MAX_OFFER_SPANS`, of which
    /// at most `MAX_HEAD_QUESTIONS` ask for heads, each about a nonempty span within the
    /// tree.
    pub fn validate_all(questions: &[SpanQuestion]) -> anyhow::Result<()> {
        ensure!(questions.len() <= MAX_OFFER_SPANS, "too many span questions");
        let heads = questions.iter().filter(|question| matches!(question, SpanQuestion::Heads(_))).count();
        ensure!(heads <= MAX_HEAD_QUESTIONS, "too many heads questions");
        for question in questions {
            let span = question.bounds();
            ensure!(!span.is_empty() && span.is_subset_of(&TreeSpan::all()), "span question outside the tree");
        }
        Ok(())
    }
}

impl EntityHead {
    /// Checks one SyncEntities request's entities: at most `MAX_PAGE_HEADS`, each
    /// named once.
    pub fn validate_sync_request(entities: &[EntityHead]) -> anyhow::Result<()> {
        ensure!(entities.len() <= MAX_PAGE_HEADS, "too many entities to sync");
        let mut named = HashSet::with_capacity(entities.len());
        ensure!(entities.iter().all(|entity| named.insert(entity.entity_id)), "an entity to sync is named twice");
        Ok(())
    }
}

impl SpanAnswer {
    /// Checks a SpansCompared response: one answer per question, in the questions'
    /// order, each valid for its question under `spec`.
    pub fn validate_all(answers: &[SpanAnswer], questions: &[SpanQuestion], spec: &TreeSpec) -> anyhow::Result<()> {
        ensure!(answers.len() == questions.len(), "span answers do not match their questions");
        answers.iter().zip(questions).try_for_each(|(answer, question)| answer.validate(question, spec))
    }

    /// Checks one answer: a valid digest of exactly the question's span, or a valid
    /// inventory page under `spec` that starts where the question's span starts, lies
    /// within it, and ends where the span ends or, stopping short, at its own last head.
    pub fn validate(&self, question: &SpanQuestion, spec: &TreeSpec) -> anyhow::Result<()> {
        match (question, self) {
            (SpanQuestion::Digest(span), SpanAnswer::Digest(digest)) => {
                digest.validate()?;
                ensure!(&digest.bounds == span, "digest answer covers another span");
            }
            (SpanQuestion::Heads(span), SpanAnswer::Heads(page)) => {
                page.validate_page(spec)?;
                ensure!(page.bounds.start == span.start && page.bounds.is_subset_of(span), "heads answer lies outside its span");
                if page.bounds.end != span.end {
                    let mut last = None;
                    let mut cursor = page.heads.cursor();
                    while let Some(head) = cursor.next() {
                        last = Some(leaf_address(head.key, head.entity.entity_id));
                    }
                    ensure!(
                        last.is_some_and(|last| page.bounds.end == Bound::Included(last)),
                        "a short heads answer must end at its last head"
                    );
                }
            }
            _ => bail!("span answer is not the kind its question asked for"),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
