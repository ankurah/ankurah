//! What this node requires of an event entering it, and the one read of the event's operations.

use crate::internal::prelude::*;
use ankurah_proto::{EventId, OperationSet, UnreadableOperation};
use std::collections::BTreeMap;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum InadmissibleEvent {
    #[error("model '{0}' is protected and only writable by the node's privileged context")]
    ProtectedModel(ModelId),
    #[error("an entity's first event must establish at least one model membership")]
    MissingGenesisMembership,
}

/// The operations `event` carries, if this build can read every one of them; an event it cannot
/// read is refused, never stored unapplied.
pub(crate) fn read_operations(event: &proto::Event) -> Result<OperationSet, UnreadableOperation> { event.operations().decode() }

/// The operations of the events entering this node, read once as each entered, so that applying
/// the events reads none of them again.
pub(crate) struct AdmittedOperations(BTreeMap<EventId, OperationSet>);

impl AdmittedOperations {
    pub(crate) fn new() -> Self { Self(BTreeMap::new()) }

    /// Read `event`'s operations as it enters this node, requiring that a genesis establishes at
    /// least one membership; an update need not add any.
    pub(crate) fn admit(&mut self, event: &proto::Event) -> Result<(), MutationError> {
        let operations = read_operations(event)?;
        if event.is_entity_create() && operations.memberships().next().is_none() {
            return Err(InadmissibleEvent::MissingGenesisMembership.into());
        }
        self.0.insert(event.id(), operations);
        Ok(())
    }

    /// The operations of `event`, whose id is `id`: those read as it entered, or, for an event
    /// that entered before, read now.
    pub(crate) fn of(&self, id: &EventId, event: &proto::Event) -> Result<OperationSet, UnreadableOperation> {
        match self.0.get(id) {
            Some(operations) => Ok(operations.clone()),
            None => read_operations(event),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ankurah_proto::{CarriedOperation, Clock, EntityId, Event, EventBody, Identity, Membership, Operation};

    fn membership() -> OperationSet {
        OperationSet(vec![Operation::Membership(Membership::Add(ModelId::EntityId(EntityId::from_bytes([1; 32]))))])
    }

    /// `event` carrying `operation` after its own, its entity id recomputed so it stays well formed.
    fn carrying(mut event: Event, operation: CarriedOperation) -> Event {
        match &mut event.body {
            EventBody::Genesis { operations, .. } | EventBody::Update { operations, .. } => operations.0.push(operation),
        }
        if event.is_entity_create() {
            event.entity_id = EntityId::from(event.id());
        }
        event
    }

    #[test]
    fn a_genesis_without_a_membership_is_inadmissible_and_an_update_without_one_is_admitted() {
        let minter = Identity::Ed25519([3; 32]);
        let refused = AdmittedOperations::new().admit(&Event::genesis(minter, OperationSet::default()));
        assert!(matches!(refused, Err(MutationError::InadmissibleEvent(InadmissibleEvent::MissingGenesisMembership))), "{refused:?}");

        let genesis = Event::genesis(minter, membership());
        let update = Event::update(genesis.entity_id, Clock::singleton(&genesis), OperationSet::default());
        let mut admitted = AdmittedOperations::new();
        admitted.admit(&genesis).unwrap();
        admitted.admit(&update).unwrap();
        assert_eq!(admitted.of(&genesis.id(), &genesis).unwrap(), membership());
    }

    #[test]
    fn an_event_holding_an_erased_operation_or_one_of_an_unknown_kind_is_unreadable() {
        let genesis = Event::genesis(Identity::Ed25519([4; 32]), membership());
        let update = Event::update(genesis.entity_id, Clock::singleton(&genesis), membership());
        // Variant index 2, which no build of `Operation` defines yet.
        let unknown = CarriedOperation::Bytes(vec![2, 0, 0, 0]);
        for event in [genesis, update] {
            for (operation, expected) in [
                (CarriedOperation::Erased([5; 32]), UnreadableOperation::Erased),
                (unknown.clone(), UnreadableOperation::UnknownKind { indices: vec![2] }),
            ] {
                let unreadable = carrying(event.clone(), operation);
                unreadable.validate_structure().unwrap();
                let refused = AdmittedOperations::new().admit(&unreadable);
                assert!(matches!(&refused, Err(MutationError::UnreadableOperation(cause)) if *cause == expected), "{refused:?}");
            }
        }
    }
}
