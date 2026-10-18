//! Entity payloads shared by query initialization, repair and live updates.

use serde::{Deserialize, Serialize};

use crate::{Attested, CausalAssertionFragment, EntityId, EntityState, Event, EventFragment, GetFailure, GetResult, StateFragment};

/// Entity data shared by initial repair and ongoing updates.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DeltaContent {
    /// Full entity state when a suitable event bridge is unavailable.
    StateSnapshot { state: StateFragment },
    /// Events connecting a supplied entity clock to the repair target.
    EventBridge { events: Vec<EventFragment> },
    /// State with a causal assertion relative to the supplied entity clock.
    StateAndRelation { state: StateFragment, relation: CausalAssertionFragment },
    /// Snapshot plus connecting events; retain the events if the snapshot cannot apply.
    StateAndEvents { state: StateFragment, events: Vec<EventFragment> },
}

/// One entity payload, independent of initial/live delivery and query relevance.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityDelta {
    pub entity_id: EntityId,
    pub content: DeltaContent,
}

/// A fragment of one bincode-encoded DeltaContent, shared by repair and live delivery.
/// Reassemble under the stream and its typed wrapper, then validate and apply once.
/// Offsets start at zero, are contiguous, and stop at `last`; total assembly is capped.
/// The entity and wrapper metadata stay fixed. Assemblies are scoped by their
/// round or live publication/repair wrapper, so baseline and live data for an
/// entity may overlap. Copying bytes returns wire credit, not an application claim.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityDeltaChunk {
    pub entity_id: EntityId,
    pub offset: u64,
    pub bytes: bytes::Bytes,
    pub last: bool,
}

impl TryFrom<GetResult> for EntityDelta {
    type Error = GetFailure;

    fn try_from(result: GetResult) -> Result<Self, Self::Error> {
        let state = Attested::<EntityState>::try_from(result)?;
        Ok(Self { entity_id: state.payload.entity_id, content: DeltaContent::StateSnapshot { state: state.into() } })
    }
}

impl std::fmt::Display for EntityDelta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.content {
            DeltaContent::StateSnapshot { state } => write!(f, "EntityDelta {}: StateSnapshot({})", self.entity_id, state),
            DeltaContent::EventBridge { events } => {
                let mut event_strs = Vec::new();
                for event in events {
                    let event = Attested::<Event>::from_parts(self.entity_id, event.clone());
                    event_strs.push(event.payload.to_string());
                }
                write!(f, "EntityDelta {}: EventBridge({})", self.entity_id, event_strs.join(", "))
            }
            DeltaContent::StateAndEvents { state, events } => {
                write!(f, "EntityDelta {}: StateAndEvents({}, {})", self.entity_id, state, events.len())
            }
            DeltaContent::StateAndRelation { state, relation: _ } => {
                write!(f, "EntityDelta {}: StateAndRelation({})", self.entity_id, state)
            }
        }
    }
}
