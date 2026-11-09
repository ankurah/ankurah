//! Rules shared by local and remote transaction commits.

use crate::internal::prelude::*;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum InadmissibleEvent {
    #[error("model '{0}' is protected and only writable by the node's privileged context")]
    ProtectedModel(ModelId),
    #[error("an entity's first event must establish at least one model membership")]
    MissingGenesisMembership,
}

/// Require that this build can read every operation `event` carries, and that a genesis establishes at least one
/// membership; updates need not add any.
pub(crate) fn check_operations(event: &proto::Event) -> Result<(), MutationError> {
    let operations = event.operations().decode()?;
    if event.is_entity_create() && operations.memberships().next().is_none() {
        return Err(InadmissibleEvent::MissingGenesisMembership.into());
    }
    Ok(())
}
