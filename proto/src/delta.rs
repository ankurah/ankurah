//! Entity payloads shared by query initialization, repair and live updates.

use anyhow::{anyhow, bail, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    Attested, CausalAssertionFragment, Clock, EntityId, EntityState, Event, EventFragment, GetFailure, GetResult, QueryMember,
    StateFragment,
};

mod chunk;
pub use chunk::{DeltaAssembler, DeltaChunks, EntityDeltaChunk};

/// Entity data shared by initial repair and ongoing updates.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DeltaContent {
    /// Full entity state when a suitable event bridge is unavailable.
    StateSnapshot { state: StateFragment },
    /// Events connecting a supplied entity clock to the repair target.
    EventBridge { events: Vec<EventFragment> },
    /// State with a causal assertion relative to the supplied entity clock. No node
    /// produces it, and receivers refuse it.
    StateAndRelation { state: StateFragment, relation: CausalAssertionFragment },
    /// Snapshot plus connecting events; retain the events if the snapshot cannot apply.
    StateAndEvents { state: StateFragment, events: Vec<EventFragment> },
}

impl DeltaContent {
    /// The head of the entity state this delta carries; an event bridge carries none.
    pub fn state_head(&self) -> Option<&Clock> {
        match self {
            Self::StateSnapshot { state } | Self::StateAndEvents { state, .. } | Self::StateAndRelation { state, .. } => {
                Some(&state.state.head)
            }
            Self::EventBridge { .. } => None,
        }
    }

    /// Checks that this delta can bring its entity to `target`: state it carries
    /// must have exactly that head, and an event bridge must carry events.
    pub fn check_target(&self, target: &Clock) -> anyhow::Result<()> {
        match self {
            Self::StateAndRelation { .. } => bail!("StateAndRelation is not supported"),
            Self::EventBridge { events } => ensure!(!events.is_empty(), "empty event bridge"),
            Self::StateSnapshot { .. } | Self::StateAndEvents { .. } => {
                ensure!(self.state_head() == Some(target), "snapshot does not match repair target")
            }
        }
        Ok(())
    }

    /// The head a live delta brings its entity to: the head of state it carries, or
    /// for an event bridge the head of the member it belongs to. An event bridge for
    /// an absent entity has no target.
    pub fn live_target<'a>(&'a self, member: Option<&'a QueryMember>) -> anyhow::Result<&'a Clock> {
        match self {
            Self::StateAndRelation { .. } => bail!("StateAndRelation is not supported"),
            Self::EventBridge { .. } => Ok(&member.ok_or_else(|| anyhow!("absent event bridge has no target"))?.head),
            Self::StateSnapshot { state } | Self::StateAndEvents { state, .. } => Ok(&state.state.head),
        }
    }
}

/// One entity payload, independent of initial/live delivery and query relevance.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct EntityDelta {
    pub entity_id: EntityId,
    pub content: DeltaContent,
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
