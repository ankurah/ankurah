use serde::{Deserialize, Serialize};

use crate::EntityId;

/// Backend kind for property storage - language agnostic
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    /// Last-write-wins scalar value
    Lww,
    /// Collaborative text (Yrs Text type)
    YrsText,
    /// Collaborative map (future)
    YrsMap,
    /// Collaborative array (future)
    YrsArray,
}

/// Value type for property registration - mirrors core::value::ValueType exactly
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ValueType {
    I16,
    I32,
    I64,
    F64,
    Bool,
    String,
    EntityId,
    Object,
    Binary,
    Json,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Item {
    /// The genesis clock for the system - this serves as the root of the clock tree for all entities in the system
    SysRoot,
    Collection {
        name: String,
    },
    /// A registered model (schema definition)
    Model {
        name: String,
    },
    /// A registered property belonging to a model
    Property {
        /// Property name (e.g., "title", "email")
        name: String,
        /// Reference to the parent Model entity
        model: EntityId,
        /// Backend/conflict resolution strategy
        backend: BackendKind,
        /// Value encoding type (primarily for LWW)
        value_type: ValueType,
        /// Whether the property can be None/missing
        optional: bool,
        /// For Ref types: the target model this references
        target_model: Option<EntityId>,
    },
    #[serde(other)]
    Other,
}
