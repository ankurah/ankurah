//! The responder's account of its result, as the requester learns it from the
//! response that opened its query and from span answers since, and readiness:
//! whether the requester's copy matches or exceeds it.

use super::coverage::{covers, local_heads};
use crate::{
    node::{state_sync::Snapshot, Node},
    policy::PolicyAgent,
    storage::StorageEngine,
    util::Instant,
};
use ankurah_digest_tree::leaf_address;
use ankurah_proto::{Clock, EntityHead, EntityId, KnownState, SpanContent, TreeSpan, TreeSpanDigest, TreeSpanInventory, TreeSpec};
use anyhow::ensure;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

/// The responder's account of its result under `spec`, and what of it the
/// requester's copy does not cover yet.
pub(super) struct Target {
    pub(super) spec: TreeSpec,
    /// What the responder described of each span: disjoint, in address order,
    /// tiling the tree.
    regions: Vec<Region>,
    /// Every entity the account lists, in its inventories and in the pages
    /// answered since, wherever its address lies.
    listed: HashSet<EntityId>,
    /// The responder heads seen in the account's inventories and pages that
    /// the copy does not cover yet, each with the entity's address.
    uncovered: BTreeMap<EntityId, (Vec<u8>, Clock)>,
    /// Entities the responder holds no readable state of, which the requester
    /// stops waiting for.
    unavailable: HashSet<EntityId>,
    /// For local entities the account does not list, the head the responder
    /// reported holding of each.
    held: HashMap<EntityId, Clock>,
    /// For each entity a SyncEntities answer reported a head of, what that
    /// request named the copy's head as, and when the answer came.
    answered: HashMap<EntityId, Answered>,
    /// Members settled at the copy's own head, which the responder's push
    /// left unchanged: no push brings such a copy level.
    diverged: HashSet<EntityId>,
}

/// What a SyncEntities named of an entity whose head its answer reported.
struct Answered {
    /// The copy's head of the entity, as the request named it.
    named: Clock,
    at: Instant,
}

impl Answered {
    /// Whether the push that followed the answer left the copy at `local`,
    /// the head the request named, `settle_after` or longer since. A push
    /// moves a copy it can bring level, so the copy holds what the
    /// responder's state does not extend: a concurrent state, which the
    /// applier does not adopt, or a head too far ahead to compare within the
    /// coverage budget.
    fn left_unchanged(&self, local: &Clock, settle_after: Duration) -> bool { self.named == *local && self.at.elapsed() >= settle_after }
}

/// What the responder described of one span.
#[derive(Clone, Debug)]
pub(super) enum Region {
    /// Bounds within which it listed every entity it holds.
    Listed(TreeSpan),
    /// A digest of its entities within the bounds, not matched by the copy yet.
    Digest(TreeSpanDigest),
    /// Bounds over which the copy matched the responder's digest.
    Matched(TreeSpan),
    /// Bounds the requester has learned nothing of yet.
    Unknown(TreeSpan),
}

impl Region {
    pub(super) fn bounds(&self) -> &TreeSpan {
        match self {
            Region::Listed(bounds) | Region::Matched(bounds) | Region::Unknown(bounds) => bounds,
            Region::Digest(digest) => &digest.bounds,
        }
    }
}

/// What the copy does not yet match or exceed of a target, as of one snapshot.
#[derive(Debug, Default)]
pub(super) struct Outstanding {
    /// Responder heads the copy does not cover.
    pub(super) heads: Vec<UncoveredHead>,
    /// Digest regions the copy has not matched, and spans not learned yet.
    pub(super) spans: Vec<Region>,
    /// Local members the account does not list and the responder has not
    /// settled, each with the copy's head.
    pub(super) unlisted: Vec<(EntityId, Clock)>,
}

/// A responder head the copy does not cover.
#[derive(Clone, Debug)]
pub(super) struct UncoveredHead {
    pub(super) entity_id: EntityId,
    /// The entity's address in the responder's tree.
    pub(super) address: Vec<u8>,
    /// The copy's head of the entity, when the copy holds one.
    pub(super) local: Option<Clock>,
}

impl Outstanding {
    pub(super) fn is_empty(&self) -> bool { self.len() == 0 }
    pub(super) fn len(&self) -> usize { self.heads.len() + self.spans.len() + self.unlisted.len() }

    /// Whether the copy still waits for `entity`: a responder head of it that
    /// the copy does not cover, or a local member the responder has not
    /// settled.
    pub(super) fn waits_on(&self, entity: &EntityId) -> bool {
        self.heads.iter().any(|head| head.entity_id == *entity) || self.unlisted.iter().any(|(id, _)| id == entity)
    }
}

impl Target {
    /// The account a responder answered an offer with, or one span answer
    /// treated as an account; a range it leaves out is not learned yet.
    pub(super) fn new(account: KnownState) -> anyhow::Result<Self> {
        account.validate()?;
        let mut target = Self {
            spec: account.spec,
            regions: Vec::new(),
            listed: HashSet::new(),
            uncovered: BTreeMap::new(),
            unavailable: HashSet::new(),
            held: HashMap::new(),
            answered: HashMap::new(),
            diverged: HashSet::new(),
        };
        let mut spans = account.spans;
        spans.sort_by(|a, b| a.bounds().cmp_start(b.bounds()));
        let covered: Vec<_> = spans.iter().map(|span| span.bounds().clone()).collect();
        target.regions.extend(TreeSpan::all().gaps(&covered).into_iter().map(Region::Unknown));
        for span in spans {
            match span {
                SpanContent::Digest(digest) => target.regions.push(Region::Digest(digest)),
                SpanContent::Heads(inventory) => {
                    let region = target.list(inventory);
                    target.regions.push(region);
                }
            }
        }
        target.regions.sort_by(|a, b| a.bounds().cmp_start(b.bounds()));
        Ok(target)
    }

    /// The region an inventory or a page lists, recording its heads.
    fn list(&mut self, inventory: TreeSpanInventory) -> Region {
        let mut cursor = inventory.heads.cursor();
        while let Some(head) = cursor.next() {
            self.listed.insert(head.entity.entity_id);
            if !self.diverged.contains(&head.entity.entity_id) {
                self.uncovered.insert(head.entity.entity_id, (leaf_address(head.key, head.entity.entity_id), head.entity.head.to_owned()));
            }
        }
        Region::Listed(inventory.bounds)
    }

    /// Settle `region`'s bounds, a span within regions not learned yet, as
    /// `region`. What those regions covered beyond its bounds is not learned
    /// yet either.
    pub(super) fn settle(&mut self, region: Region) {
        let bounds = region.bounds().clone();
        let mut regions = Vec::with_capacity(self.regions.len() + 2);
        for existing in self.regions.drain(..) {
            if existing.bounds().intersect(&bounds).is_none() {
                regions.push(existing);
            } else {
                regions.extend(existing.bounds().gaps(std::slice::from_ref(&bounds)).into_iter().map(Region::Unknown));
            }
        }
        regions.push(region);
        regions.sort_by(|a, b| a.bounds().cmp_start(b.bounds()));
        self.regions = regions;
    }

    /// Settle a page the responder answered: the entities it lists within its
    /// bounds.
    pub(super) fn settle_page(&mut self, page: TreeSpanInventory) {
        let region = self.list(page);
        self.settle(region);
    }

    /// Record an EntitiesSynced answer to a SyncEntities that named each
    /// entity in `named` with the copy's head of it, if any. A held head
    /// replaces the head the copy has yet to cover, since it is the
    /// responder's current one; a held head of an entity the account does not
    /// list settles it once covered; and an unavailable entity is no longer
    /// waited for.
    pub(super) fn synced(
        &mut self,
        named: &[(EntityId, Option<Clock>)],
        held: Vec<EntityHead>,
        unavailable: Vec<EntityId>,
    ) -> anyhow::Result<()> {
        let named: HashMap<_, _> = named.iter().map(|(id, local)| (*id, local)).collect();
        ensure!(
            held.iter().map(|entity| &entity.entity_id).chain(&unavailable).all(|id| named.contains_key(id)),
            "an EntitiesSynced answer names an entity its request did not"
        );
        let at = Instant::now();
        for entity in held {
            if let Some(Some(local)) = named.get(&entity.entity_id) {
                self.answered.insert(entity.entity_id, Answered { named: local.clone(), at });
            }
            match self.uncovered.get_mut(&entity.entity_id) {
                Some((_, head)) => *head = entity.head,
                None => {
                    self.held.insert(entity.entity_id, entity.head);
                }
            }
        }
        for id in unavailable {
            self.uncovered.remove(&id);
            self.unavailable.insert(id);
        }
        Ok(())
    }

    /// What the copy in `snapshot`, a view of the requester's resident for the
    /// query under this target's spec, does not yet match or exceed. A head the
    /// copy covers, or a digest it matches, stays settled: the copy only moves
    /// forward. So does a member whose copy the responder's push in answer to
    /// a SyncEntities left unchanged `settle_after` or longer since, with a
    /// warning that names it.
    pub(super) async fn outstanding<SE, PA>(
        &mut self,
        node: &Node<SE, PA>,
        snapshot: &mut Snapshot,
        settle_after: Duration,
    ) -> anyhow::Result<Outstanding>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        let mut outstanding = Outstanding::default();
        // The copy's head of an entity outside its result still counts.
        let outside = self.uncovered.keys().filter(|id| !snapshot.entries.contains_key(id)).copied().collect::<Vec<_>>();
        let mut heads = local_heads(node, outside).await?;
        heads.extend(self.uncovered.keys().filter_map(|id| snapshot.entries.get(id).map(|entry| (*id, entry.head.clone()))));
        let (mut covered, mut diverged) = (Vec::new(), Vec::new());
        for (id, (address, head)) in &self.uncovered {
            match heads.remove(id) {
                Some(local) if covers(node, &local, head).await? => covered.push(*id),
                Some(local) if self.answered.get(id).is_some_and(|answered| answered.left_unchanged(&local, settle_after)) => {
                    diverged.push(*id)
                }
                local => outstanding.heads.push(UncoveredHead { entity_id: *id, address: address.clone(), local }),
            }
        }
        for id in covered.iter().chain(&diverged) {
            self.uncovered.remove(id);
        }
        for region in &mut self.regions {
            match region {
                Region::Digest(digest) => {
                    if snapshot.span_digest(digest.bounds.clone()).await?.digest == digest.digest {
                        let bounds = digest.bounds.clone();
                        *region = Region::Matched(bounds);
                    } else {
                        outstanding.spans.push(Region::Digest(digest.clone()));
                    }
                }
                Region::Unknown(bounds) => outstanding.spans.push(Region::Unknown(bounds.clone())),
                // A member the account lists elsewhere, as one whose key
                // changed, is listed: its head says what the copy lacks.
                Region::Listed(bounds) => {
                    for (id, entry) in snapshot.entries_in(bounds, usize::MAX) {
                        if self.listed.contains(&id) || self.unavailable.contains(&id) || self.diverged.contains(&id) {
                            continue;
                        }
                        let Some(held) = self.held.get(&id) else {
                            outstanding.unlisted.push((id, entry.head.clone()));
                            continue;
                        };
                        if covers(node, &entry.head, held).await? {
                            continue;
                        }
                        if self.answered.get(&id).is_some_and(|answered| answered.left_unchanged(&entry.head, settle_after)) {
                            diverged.push(id);
                        } else {
                            outstanding.unlisted.push((id, entry.head.clone()));
                        }
                    }
                }
                Region::Matched(_) => {}
            }
        }
        for id in diverged {
            tracing::warn!(
                "Settled entity {} at the copy's own head, which the responder's push left unchanged: the copy diverged from the \
                 responder's or is too far ahead of it to compare",
                id
            );
            self.diverged.insert(id);
        }
        Ok(outstanding)
    }
}
