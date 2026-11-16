//! The state an entity's genesis alone makes, for a tool that builds entities outside any node.

use crate::entity::RemoteTrxEntity;
use crate::error::{MutationError, RetrievalError};
use crate::node::event_admissibility::check_genesis_membership;
use crate::retrieval::GetEvents;
use crate::schema::SystemEpoch;
use ankurah_proto::{Attested, EntityState, Event, EventId};

/// The state `genesis` alone makes: what a node stores for an entity of which it holds no other
/// event. It is applied as a node applies a genesis it receives, so a store seeded with this state
/// holds what the node would have stored.
pub async fn genesis_state(genesis: &Event) -> Result<EntityState, MutationError> {
    check_genesis_membership(genesis)?;
    // The entity joins no node, so it takes the epoch reserved for entities outside one.
    let entity = RemoteTrxEntity::new(genesis, SystemEpoch::BOOTSTRAP)?;
    entity.apply_events(&NoHistory, &mut [Attested::opt(genesis.clone(), None)], |_| Ok(None)).await?;
    Ok(EntityState { entity_id: entity.id(), state: entity.to_state()? })
}

/// The history of an entity outside any node: nothing beyond the events in hand.
struct NoHistory;

#[async_trait::async_trait]
impl GetEvents for NoHistory {
    async fn get_event(&self, id: &EventId) -> Result<Event, RetrievalError> { Err(RetrievalError::EventNotFound(id.clone())) }

    async fn event_stored(&self, _: &EventId) -> Result<bool, RetrievalError> { Ok(false) }
}
