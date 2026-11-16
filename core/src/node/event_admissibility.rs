//! Rules shared by local and remote transaction commits.

use crate::internal::prelude::*;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum InadmissibleEvent {
    #[error("model '{0}' is protected and only writable by the node's privileged context")]
    ProtectedModel(ModelId),
    #[error("an entity's first event must establish at least one model membership")]
    MissingGenesisMembership,
    #[error("entity {0} belongs to the node's own system, which an import may not create or change")]
    SystemEntity(proto::EntityId),
}

/// Require a genesis to establish at least one membership; updates need not add any.
pub(crate) fn check_genesis_membership(event: &proto::Event) -> Result<(), InadmissibleEvent> {
    if event.is_entity_create() && event.operations().memberships().next().is_none() {
        return Err(InadmissibleEvent::MissingGenesisMembership);
    }
    Ok(())
}

/// Refuse an imported event whose entity belongs to the node's own system, such as its root. A node
/// holds exactly one root and a store with two will not open, so an import may neither bring another
/// nor change the one the node has. A root is a member of the system model, which this rule refuses
/// before and after the event.
pub(crate) fn check_imported_entity(entity: &Entity) -> Result<(), InadmissibleEvent> {
    if entity.memberships().contains(&ModelId::System(proto::SystemModel::System)) {
        return Err(InadmissibleEvent::SystemEntity(entity.id()));
    }
    Ok(())
}
