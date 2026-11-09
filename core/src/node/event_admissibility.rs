//! Rules shared by local and remote transaction commits.

use crate::internal::prelude::*;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum InadmissibleEvent {
    #[error("model '{0}' is protected and only writable by the node's privileged context")]
    ProtectedModel(ModelId),
    #[error("an entity's first event must establish at least one model membership")]
    MissingGenesisMembership,
}

/// Require a genesis to establish at least one membership; updates need not add any.
pub(crate) fn check_genesis_membership(event: &proto::Event) -> Result<(), InadmissibleEvent> {
    if event.is_entity_create() && event.operations().memberships().next().is_none() {
        return Err(InadmissibleEvent::MissingGenesisMembership);
    }
    Ok(())
}
