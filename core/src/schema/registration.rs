//! The vocabulary of the RegisterSchema operation: its typed errors and the
//! resolved plan the policy agent judges.
//!
//! The operation itself is [`crate::schema::catalog::CatalogManager::register_schema`]:
//! an UPSERT of one model and its properties, executed on the durable node
//! under the allocation mutex, writing through one transaction on the node's
//! privileged context. Policy gates it at two boundaries: request
//! authentication decides whether the principal may submit descriptors, and
//! the resolved [`RegistrationPlan`] goes through
//! `PolicyAgent::check_schema_registration` before the commit -- the agent's
//! whole voice, since the privileged commit consults no further check.
//! Idempotence is the upsert's: a repeat registration finds every key,
//! writes nothing, and returns the same ids.
//!
//! A property's (backend, value_type) is CANONICAL: fixed at allocation and
//! never changed by registration. A hit whose descriptor declares a different
//! value_type is admitted only when the two types are mutually castable per
//! `Value::cast_to` (the binary writes and reads through the cast); a different
//! backend, or a non-castable type pair, refuses the registration loudly.
//! Changing a canonical type is a deliberate migration (#303), never a
//! model-struct edit.

use crate::internal::prelude::*;
use ankurah_proto::{EntityId, RegisterModel, RegisterProperty};

use crate::property::PropertyError;

/// A durable schema-registration request could not be completed.
#[derive(Debug, thiserror::Error)]
pub enum RegistrationError {
    /// Registration was attempted on a node without durable storage.
    #[error("registration executes on durable nodes; this node is ephemeral")]
    NotDurable,
    /// The node has not established a system root or catalog epoch.
    #[error("system is not ready")]
    SystemNotReady,
    /// Strict offline registration could not consult an allocator.
    #[error(
        "collection '{0}' has never been registered on this node and no durable peer is connected; \
         connect to the system once first"
    )]
    NoDurablePeer(
        /// The source-level collection label that could not be registered.
        String,
    ),
    /// A prior registration exists, but this binary's compiled schema could
    /// not be confirmed against it without a durable peer (changed or
    /// unbound fields need the allocator).
    #[error(
        "collection '{0}' is registered, but this binary's schema for it is unconfirmed \
         and no durable peer is connected; connect to the system once first"
    )]
    UnconfirmedSchema(
        /// The source-level collection label whose binding is unconfirmed.
        String,
    ),
    /// A descriptor attempted to claim the protected system namespace.
    #[error(
        "collection '{0}' starts with the reserved prefix '{prefix}'; the system and catalog collections are never registered through this executor",
        prefix = crate::schema::RESERVED_COLLECTION_PREFIX
    )]
    ReservedCollection(
        /// The rejected source-level collection label.
        String,
    ),
    /// A membership's name reference was not declared in the same request.
    #[error("membership references property '{0}' which is not declared in this request for collection '{1}'")]
    UnresolvedPropertyRef(
        /// The unresolved property name.
        String,
        /// The model collection label containing the membership.
        String,
    ),
    /// A descriptor referenced a minting model absent from the request and catalog.
    #[error("descriptor references collection '{0}' which is neither declared in this request nor present in the catalog")]
    UnknownMintingCollection(
        /// The unknown minting-model collection label.
        String,
    ),
    /// An explicit property binding named an unknown catalog entity.
    #[error("explicit property id {property} does not exist in the catalog; explicit binding never mints")]
    ExplicitIdNotFound {
        /// The requested property entity.
        property: EntityId,
    },
    /// An explicit model binding named an unknown catalog entity.
    #[error("explicit model id {model} does not exist in the catalog; explicit binding never mints")]
    ExplicitModelIdNotFound {
        /// The requested model entity.
        model: EntityId,
    },
    /// An explicit model binding conflicts with the catalog's model label.
    #[error("explicit model id {model} is bound to label '{found_label}'; binder declares '{label}'")]
    ExplicitModelIdMismatch {
        /// The explicitly bound model entity.
        model: EntityId,
        /// The model label already stored in the catalog.
        found_label: String,
        /// The model label declared by the requester.
        label: String,
    },
    /// A declaration conflicts with an existing property's canonical backend or type.
    #[error(
        "property '{name}' in '{collection}' is canonically ({found_backend}, {found_value_type}); this binary declares ({backend}, {value_type}), which is not castable to/from the canonical type (backend must match; value types must be mutually castable per Value::cast_to). The canonical type is fixed at allocation; changing it is a deliberate migration (#303), never a model-struct edit"
    )]
    NonCastable {
        /// The property's minting-model collection label.
        collection: String,
        /// The property's registered name.
        name: String,
        /// The canonical backend already stored in the catalog.
        found_backend: String,
        /// The canonical value type already stored in the catalog.
        found_value_type: String,
        /// The backend declared by the requester.
        backend: String,
        /// The value type declared by the requester.
        value_type: String,
    },
    /// The policy agent denied the resolved registration plan, or the
    /// context could not act as a single principal.
    #[error("registration refused by policy: {0}")]
    PolicyDenied(
        /// The refusal, from either origin: the policy agent's verdict
        /// on the resolved plan, or the credential source's own refusal
        /// when it holds no session or several and so cannot name the
        /// single principal a registration acts as.
        #[from]
        AccessDenied,
    ),
    /// Committing the catalog transaction failed.
    #[error(transparent)]
    Mutation(
        /// The durable mutation failure.
        #[from]
        MutationError,
    ),
    /// Reading catalog or storage state failed.
    #[error(transparent)]
    Retrieval(
        /// The underlying catalog or storage read failure.
        #[from]
        RetrievalError,
    ),
}

/// Read paths propagate ensure failures as retrieval errors; the Retrieval
/// variant unwraps to its inner error rather than double-wrapping.
impl From<RegistrationError> for crate::error::RetrievalError {
    fn from(error: RegistrationError) -> Self {
        match error {
            RegistrationError::Retrieval(inner) => inner,
            other => crate::error::RetrievalError::Other(other.to_string()),
        }
    }
}

/// Write paths (Transaction::create) propagate ensure failures as mutation
/// errors, keeping the typed error as the source.
impl From<RegistrationError> for crate::error::MutationError {
    fn from(error: RegistrationError) -> Self { crate::error::MutationError::General(Box::new(error)) }
}

/// A typed accessor's failure is a failure to write the catalog row.
impl From<PropertyError> for RegistrationError {
    fn from(error: PropertyError) -> Self { RegistrationError::Mutation(error.into()) }
}

/// What a RegisterSchema request will ACTUALLY do, resolved by the
/// registration executor under the allocation mutex and handed to
/// [`crate::policy::PolicyAgent::check_schema_registration`] before the
/// transaction commits, so an agent can judge real creations and metadata
/// changes without performing its own catalog lookups. Core-side only;
/// never crosses the wire.
#[derive(Debug, Default)]
pub struct RegistrationPlan {
    /// Model entities this request will CREATE, with their allocated ids.
    pub creates_models: Vec<(EntityId, RegisterModel)>,
    /// Property entities this request will CREATE, with their allocated ids.
    pub creates_properties: Vec<(EntityId, RegisterProperty)>,
    /// Model-property memberships this request will CREATE, fully resolved.
    pub creates_memberships: Vec<PlannedModelPropertyMembership>,
    /// Metadata follow-ups this request will write on EXISTING entities
    /// (display-name changes including rename-hint applications, target
    /// retargets, membership `optional` flips).
    pub updates: Vec<PlannedUpdate>,
    /// Definitions that resolved to existing entities with no changes:
    /// pure no-ops, listed for context.
    pub existing: Vec<EntityId>,
}

impl RegistrationPlan {
    /// Whether the plan writes anything at all (a re-registration of
    /// unchanged definitions is a pure no-op and skips the policy verb).
    pub fn is_noop(&self) -> bool {
        self.creates_models.is_empty()
            && self.creates_properties.is_empty()
            && self.creates_memberships.is_empty()
            && self.updates.is_empty()
    }
}

/// A model-property membership creation in a [`RegistrationPlan`], resolved
/// to ids.
#[derive(Debug, Clone)]
pub struct PlannedModelPropertyMembership {
    /// The durable identity assigned to the membership entity.
    pub id: EntityId,
    /// The model receiving the property.
    pub model: EntityId,
    /// The property admitted to the model.
    pub property: EntityId,
    /// Whether entities of the model may omit the property.
    pub optional: bool,
}

/// A metadata follow-up on an existing catalog entity, in a
/// [`RegistrationPlan`].
#[derive(Debug, Clone)]
pub struct PlannedUpdate {
    /// Which catalog collection the entity lives in.
    pub collection: crate::ModelId,
    /// The durable catalog entity to update.
    pub entity: EntityId,
    /// The system field whose metadata value changes.
    pub field: String,
    /// The current catalog value, when one exists.
    pub from: Option<crate::value::Value>,
    /// The requested catalog value. `None` means the field will be cleared.
    pub to: Option<crate::value::Value>,
}
