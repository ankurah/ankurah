//! Rules shared by local and remote transaction commits.

use crate::internal::prelude::*;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum InadmissibleEvent {
    #[error("model '{0}' is protected and only writable by the node's privileged context")]
    ProtectedModel(ModelId),
    #[error("an entity's first event must establish at least one model membership")]
    MissingGenesisMembership,
    #[error("an entity's first event cannot remove a model membership")]
    GenesisMembershipRemoval,
}

/// Require a genesis to add at least one membership and remove none; updates may add or remove.
pub(crate) fn check_genesis_membership(event: &proto::Event) -> Result<(), InadmissibleEvent> {
    if !event.is_entity_create() {
        return Ok(());
    }
    let mut memberships = event.operations().memberships().peekable();
    if memberships.peek().is_none() {
        return Err(InadmissibleEvent::MissingGenesisMembership);
    }
    if memberships.any(|membership| matches!(membership, proto::Membership::Remove(_))) {
        return Err(InadmissibleEvent::GenesisMembershipRemoval);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{Clock, EntityId, Event, Identity, Membership, Operation, OperationSet};

    fn memberships(changes: &[Membership]) -> OperationSet {
        OperationSet(changes.iter().map(|change| Operation::Membership(*change)).collect())
    }

    #[test]
    fn a_genesis_must_add_a_membership_and_remove_none() {
        let (a, b) = (ModelId::EntityId(EntityId::from_bytes([1; 32])), ModelId::EntityId(EntityId::from_bytes([2; 32])));
        let minter = Identity::Ed25519([3; 32]);
        assert_eq!(check_genesis_membership(&Event::genesis(minter, memberships(&[]))), Err(InadmissibleEvent::MissingGenesisMembership));
        assert_eq!(
            check_genesis_membership(&Event::genesis(minter, memberships(&[Membership::Add(a), Membership::Remove(b)]))),
            Err(InadmissibleEvent::GenesisMembershipRemoval)
        );
        let genesis = Event::genesis(minter, memberships(&[Membership::Add(a)]));
        assert_eq!(check_genesis_membership(&genesis), Ok(()));

        let update =
            Event::update(genesis.entity_id, Clock::singleton(&genesis), memberships(&[Membership::Remove(a), Membership::Add(b)]));
        assert_eq!(check_genesis_membership(&update), Ok(()), "an update may add or remove");
    }
}
