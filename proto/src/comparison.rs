//! A query comparison names one authorized tree version on a peer.

use ankurah_core_types::{key_spec::KeySpec, PropertyId};
use ankurah_digest_tree::{Page, Ristretto255, Span};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{Clock, EntityId, KnownEntity, KnownState, UpdateId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ComparisonId(Ulid);

impl ComparisonId {
    pub fn new() -> Self { Self(Ulid::new()) }
}

impl Default for ComparisonId {
    fn default() -> Self { Self::new() }
}

pub type ComparisonPage = Page<EntityId, Clock, Ristretto255>;

/// The captured result to cover. A newer local entity version also covers its
/// captured head; the requester need not discard additional local members.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComparisonTarget {
    pub id: ComparisonId,
    pub key_spec: KeySpec<PropertyId>,
    pub known_state: KnownState,
    pub page: Option<ComparisonPage>,
}

/// An entity to reconcile with the requester's current head. A key identifies
/// its exact address in the captured tree; no key requests an ordinary
/// authorized read of a known identity, including one that left the selection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RepairMember {
    pub known: KnownEntity,
    pub key: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ComparisonRequest {
    Pages {
        spans: Vec<Span>,
    },
    /// Transfer through the peer's existing update channel. `delivery` is
    /// registered by the requester before sending, so an early push is admissible.
    Repair {
        delivery: UpdateId,
        members: Vec<RepairMember>,
        events_only: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ComparisonReply {
    Pages(Vec<ComparisonPage>),
    Repaired { delivery: UpdateId, unavailable: Vec<EntityId> },
}

/// Expected refusal is distinct from a malformed page or an arbitrary server error.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ComparisonFailure {
    #[error("comparison expired; begin again from the current known state")]
    Expired,
    #[error("comparison's retained version is no longer available")]
    Restart,
    #[error("comparison exceeds the node's work or resource policy")]
    LimitExceeded,
    #[error("comparison's authorization has changed")]
    AuthorizationChanged,
    #[error("comparison key specifications differ")]
    KeySpecMismatch,
    #[error("invalid comparison request: {0}")]
    InvalidRequest(String),
    #[error("a captured member or its required history is unavailable")]
    MemberUnavailable,
}
