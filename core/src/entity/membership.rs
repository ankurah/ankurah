//! The entity-to-model membership set: dirty state for membership
//! operations, mirroring how property backends stage, drain, and apply.

use std::collections::{BTreeMap, BTreeSet};

use ankurah_proto::ModelId;

/// The entity-to-model memberships of one entity: what the applied event
/// stream has established, plus locally staged additions awaiting an event.
///
/// Dirty state mirrors the property backends: [`MembershipSet::add`] stages
/// an addition, [`MembershipSet::to_operations`] drains staged entries into
/// the event being generated (marking them in-flight so a later drain will
/// not re-emit them), and event application marks entries applied. Only
/// applied entries are canonical -- they are what persists and replicates;
/// the rest is transaction intent that becomes real when the recording
/// event applies. An addition is not bound to any particular event: an
/// entity's first recorded event is simply where its initial membership
/// happens to land.
#[derive(Debug, Clone, Default)]
pub(super) struct MembershipSet(BTreeMap<ModelId, MembershipStatus>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MembershipStatus {
    /// Staged locally, not yet drained into an event.
    Staged,
    /// Drained into a generated event that has not yet applied.
    InFlight,
    /// Established by an applied event or a loaded state snapshot.
    Applied,
}

impl MembershipSet {
    /// Rehydrate from persisted state: every membership applied.
    pub(super) fn from_applied(applied: &BTreeSet<ModelId>) -> Self {
        Self(applied.iter().map(|model| (*model, MembershipStatus::Applied)).collect())
    }

    /// Stage an addition to ride the next generated event. A model already
    /// staged, in flight, or applied is left as is.
    pub(super) fn add(&mut self, model: ModelId) { self.0.entry(model).or_insert(MembershipStatus::Staged); }

    /// Drain staged additions into operations for an event being generated,
    /// marking them in-flight.
    pub(super) fn to_operations(&mut self) -> Vec<ankurah_proto::Operation> {
        self.0
            .iter_mut()
            .filter(|(_, status)| **status == MembershipStatus::Staged)
            .map(|(model, status)| {
                *status = MembershipStatus::InFlight;
                ankurah_proto::Operation::Membership(ankurah_proto::Membership::Add(*model))
            })
            .collect()
    }

    /// Record a membership established by an applied event.
    pub(super) fn apply(&mut self, model: ModelId) { self.0.insert(model, MembershipStatus::Applied); }

    /// Replace the applied entries with a state snapshot's, keeping staged
    /// and in-flight intent the snapshot does not establish.
    pub(super) fn set_applied(&mut self, applied: &BTreeSet<ModelId>) {
        self.0.retain(|_, status| *status != MembershipStatus::Applied);
        for model in applied {
            self.0.insert(*model, MembershipStatus::Applied);
        }
    }

    /// The canonical applied memberships.
    pub(super) fn applied(&self) -> BTreeSet<ModelId> {
        self.0.iter().filter(|(_, status)| **status == MembershipStatus::Applied).map(|(model, _)| *model).collect()
    }

    /// Whether the applied event stream established membership in `model`.
    pub(super) fn is_applied(&self, model: &ModelId) -> bool { self.0.get(model) == Some(&MembershipStatus::Applied) }
}
