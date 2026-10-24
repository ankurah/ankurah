//! One subscribed peer's live view of a resident: which members each commit
//! moves into, within or out of it, and the one read of their current states
//! and events that every route of a publication shares.

use super::identity::Coverage;
use crate::{
    node::{
        state_sync::{tree::TreeEntry, QueryTree},
        Node,
    },
    policy::PolicyAgent,
    storage::StorageEngine,
};
use ankql::ast::{Resolved, Selection};
use ankurah_proto::{self as proto, Attested};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};

/// One peer's live view of a resident: the stream it is delivered on, the
/// selection it subscribed with and that selection filtered for its
/// principal, the principal's credentials, and how it knows its members.
pub(super) struct Route<CD> {
    pub(super) peer: proto::EntityId,
    pub(super) stream: proto::StateStreamId,
    pub(super) owner: Weak<()>,
    pub(super) selection: Selection<Resolved>,
    pub(super) filtered: Selection<Resolved>,
    pub(super) credentials: Vec<CD>,
    pub(super) members: RouteMembers,
}

/// How a route knows its members.
pub(super) enum RouteMembers {
    /// An unlimited selection's members are the tree's entries within these
    /// spans, which no commit moves: only the entities a commit changes can
    /// enter or leave them.
    Spans(Vec<proto::TreeSpan>),
    /// A limited selection's members as last delivered, since a commit can
    /// move its cut past entities it did not change.
    Listed(Arc<BTreeMap<proto::EntityId, proto::QueryMember>>),
}

impl RouteMembers {
    /// How a route over `view`, the view its subscription selected, knows its members.
    pub(super) fn of(view: &crate::node::state_sync::Snapshot, selection: &Selection<Resolved>) -> Self {
        match selection.limit {
            None => Self::Spans(view.spans()),
            Some(_) => Self::Listed(Arc::new(view.query_members())),
        }
    }
}

/// One entity a publication changed, as live routes see it: its tree entry
/// before and after the publication, and the events it committed.
pub(super) struct Affected {
    pub(super) entity_id: proto::EntityId,
    pub(super) before: Option<TreeEntry>,
    pub(super) after: Option<TreeEntry>,
    pub(super) events: Vec<proto::EventId>,
}

/// One membership change a route delivers: the entity and its member entry
/// after the change, or `None` once it has left the route.
pub(crate) struct LiveChange {
    pub(crate) entity_id: proto::EntityId,
    pub(crate) member: Option<proto::QueryMember>,
}

/// A limited resident's new cut after a storage refill, shared by every route
/// it delivers to: its members and their ids in the order storage ranked them.
pub(super) struct Refill {
    pub(super) members: Arc<BTreeMap<proto::EntityId, proto::QueryMember>>,
    pub(super) ordered: Vec<proto::EntityId>,
}

impl<CD> Route<CD> {
    /// The membership changes `affected` makes to this route, in the order
    /// the publication committed them, then the entities a moved cut admits
    /// or drops. `refill` is a limited resident's new cut, when it has one.
    pub(super) fn changes(
        &mut self,
        coverage: &Coverage,
        tree: &QueryTree,
        catalog: &crate::schema::catalog::CatalogManager,
        affected: &[Affected],
        refill: Option<&Refill>,
    ) -> anyhow::Result<Vec<LiveChange>> {
        let listed = match (&self.members, refill) {
            (RouteMembers::Spans(spans), _) => return Ok(spanned_changes(spans, affected)),
            (RouteMembers::Listed(_), Some(refill)) => (refill.members.clone(), refill.ordered.clone()),
            (RouteMembers::Listed(_), None) => {
                let selected = coverage.select(tree.snapshot(), &self.selection, catalog)?;
                (Arc::new(selected.query_members()), selected.ordered_ids(&self.selection, catalog)?)
            }
        };
        let RouteMembers::Listed(members) = &mut self.members else { unreachable!("spans returned above") };
        let (next, ordered) = listed;
        let changes = listed_changes(members, &next, &ordered, affected);
        *members = next;
        Ok(changes)
    }
}

/// The changes `affected` makes within `spans`: each entity whose entry was
/// or is within them.
fn spanned_changes(spans: &[proto::TreeSpan], affected: &[Affected]) -> Vec<LiveChange> {
    let within = |entry: &Option<TreeEntry>| entry.as_ref().is_some_and(|entry| spans.iter().any(|span| span.contains(entry.address())));
    affected
        .iter()
        .filter(|change| within(&change.before) || within(&change.after))
        .map(|change| LiveChange {
            entity_id: change.entity_id,
            member: change
                .after
                .as_ref()
                .filter(|_| within(&change.after))
                .map(|entry| proto::QueryMember { key: entry.key().to_vec(), head: entry.head.clone() }),
        })
        .collect()
}

/// The changes from `members` to `next`: the affected entities that were or
/// are members, in commit order, then the members a moved cut admits, in
/// `ordered`'s rank, and last those it drops.
fn listed_changes(
    members: &BTreeMap<proto::EntityId, proto::QueryMember>,
    next: &BTreeMap<proto::EntityId, proto::QueryMember>,
    ordered: &[proto::EntityId],
    affected: &[Affected],
) -> Vec<LiveChange> {
    let mut ids = Vec::new();
    let mut seen = BTreeSet::new();
    for change in affected {
        if (members.contains_key(&change.entity_id) || next.contains_key(&change.entity_id)) && seen.insert(change.entity_id) {
            ids.push(change.entity_id);
        }
    }
    // A moved cut can admit unchanged entities. They need data as well.
    for id in ordered {
        if members.get(id) != next.get(id) && seen.insert(*id) {
            ids.push(*id);
        }
    }
    for id in members.keys() {
        if !next.contains_key(id) && seen.insert(*id) {
            ids.push(*id);
        }
    }
    ids.into_iter().map(|entity_id| LiveChange { entity_id, member: next.get(&entity_id).cloned() }).collect()
}

/// The current states and committed event bodies one publication's routes
/// deliver, read once for all of them: the states in one storage read
/// besides the entities resident in memory, and the events in another.
pub(crate) struct LiveRead {
    states: BTreeMap<proto::EntityId, proto::EntityState>,
    events: BTreeMap<proto::EntityId, Vec<proto::EventId>>,
    bodies: BTreeMap<proto::EventId, Attested<proto::Event>>,
}

impl LiveRead {
    /// Read the current state of each of `entities`, and the bodies of the
    /// events `affected` committed for them.
    pub(super) async fn of<SE, PA>(node: &Node<SE, PA>, entities: BTreeSet<proto::EntityId>, affected: &[Affected]) -> anyhow::Result<Self>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        // Read the states first: every await before them lets a later commit
        // carry a state further past the events this publication delivers. A
        // commit can still land before this read; the receiver then publishes
        // these events anyway, and the next publication carries the later ones.
        let states = current_states(node, entities.iter().copied()).await?;
        let mut events: BTreeMap<proto::EntityId, Vec<proto::EventId>> = BTreeMap::new();
        for change in affected.iter().filter(|change| entities.contains(&change.entity_id)) {
            let ids = events.entry(change.entity_id).or_default();
            for event in &change.events {
                if !ids.contains(event) {
                    ids.push(event.clone());
                }
            }
        }
        let wanted: Vec<proto::EventId> = events.values().flatten().cloned().collect();
        let bodies = if wanted.is_empty() {
            BTreeMap::new()
        } else {
            node.storage.get_events(wanted).await?.into_iter().map(|event| (event.payload.id(), event)).collect()
        };
        Ok(Self { states, events, bodies })
    }

    /// The current state of `entity`, if it exists.
    pub(crate) fn state(&self, entity: &proto::EntityId) -> Option<&proto::EntityState> { self.states.get(entity) }

    /// The events this publication committed for `entity`, in commit order:
    /// an error if storage holds no body for one, or a body of another entity.
    pub(crate) fn events(&self, entity: &proto::EntityId) -> anyhow::Result<Vec<&Attested<proto::Event>>> {
        let mut found = Vec::new();
        for id in self.events.get(entity).into_iter().flatten() {
            let event = self.bodies.get(id).ok_or_else(|| anyhow::anyhow!("live event unavailable"))?;
            anyhow::ensure!(event.payload.entity_id == *entity, "live event entity mismatch");
            found.push(event);
        }
        Ok(found)
    }
}

/// The node's current state of each of `ids` that exists: a resident
/// entity's from memory, and the rest from one storage read.
pub(super) async fn current_states<SE, PA>(
    node: &Node<SE, PA>,
    ids: impl IntoIterator<Item = proto::EntityId>,
) -> anyhow::Result<BTreeMap<proto::EntityId, proto::EntityState>>
where
    SE: StorageEngine + Send + Sync + 'static,
    PA: PolicyAgent + Send + Sync + 'static,
{
    let mut states = BTreeMap::new();
    let mut stored = Vec::new();
    for id in ids {
        match node.entities.get(&id) {
            Some(entity) => {
                states.insert(id, entity.to_entity_state()?);
            }
            None => stored.push(id),
        }
    }
    if !stored.is_empty() {
        for result in node.storage.get_states(stored, &ankql::ast::Predicate::True).await? {
            if let crate::storage::GetStateResult::Found(state) = result {
                states.insert(state.payload.entity_id, state.payload);
            }
        }
    }
    Ok(states)
}
