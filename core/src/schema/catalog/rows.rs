//! Typed row Models for the three catalog collections: the catalog eating
//! its own dogfood.
//!
//! These are ordinary [`crate::model::Model`] implementations whose identity
//! is a built-in [`SystemModel`], pinned at compile time. Nothing here is
//! registered: `descriptor().system` short-circuits first-use registration
//! to the closed identity, so reading or writing these rows never consults
//! the catalog they describe. That is what dissolves the old
//! self-description worry: resolution for a system model is a compile-time
//! constant, not a catalog lookup.
//!
//! The impls are written by hand rather than `#[derive(Model)]` because the
//! derive emits `::ankurah::` facade paths that this crate cannot name (the
//! facade depends on core). They implement the same traits the derive
//! targets and stay field-for-field identical to what the registration
//! executor writes (core/src/schema/registration.rs `creation`).

use ankurah_core_types::{EntityId, SystemModel};
use ankurah_proto::CollectionId;

use crate::entity::Entity;
use crate::model::{Model, Mutable, View};
use crate::property::backend::LWWBackend;
use crate::property::{Property, PropertyError, PropertyName};
use crate::schema::{ModelStructDescriptor, StructProperty};

/// Read one typed property off an entity's LWW backend.
fn get<T: Property>(entity: &Entity, name: &str) -> Result<T, PropertyError> {
    let backend = entity.get_backend::<LWWBackend>().map_err(|e| PropertyError::RetrievalError(e))?;
    T::from_value(backend.get(&PropertyName::from(name)))
}

/// Write one typed property into an entity's LWW backend.
fn set<T: Property>(entity: &Entity, name: &str, value: &T) -> Result<(), PropertyError> {
    let backend = entity.get_backend::<LWWBackend>().map_err(|e| PropertyError::RetrievalError(e))?;
    backend.set(PropertyName::from(name), value.into_value()?);
    Ok(())
}

/// Define one catalog row model: the struct, its `Model` impl bound to a
/// [`SystemModel`], and its View/Mutable pair with typed accessors.
macro_rules! catalog_row_model {
    (
        $(#[$meta:meta])*
        $model:ident / $view:ident / $mutable:ident $(/ $refwrap:ident)?,
        $system:ident, $collection:literal,
        { $( $field:ident : $ty:ty = $value_type:literal ),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq)]
        pub struct $model {
            $( pub $field: $ty, )+
        }

        #[derive(Debug, Clone)]
        pub struct $view(Entity);

        #[derive(Debug)]
        pub struct $mutable(Entity);

        impl Model for $model {
            type View = $view;
            type Mutable = $mutable;
            $( #[cfg(feature = "wasm")] type RefWrapper = $refwrap; )?

            fn collection() -> CollectionId { CollectionId::fixed_name($collection) }

            fn descriptor() -> &'static ModelStructDescriptor {
                static DESCRIPTOR: ModelStructDescriptor = ModelStructDescriptor {
                    label: $collection,
                    name: stringify!($model),
                    properties: &[
                        $( StructProperty {
                            field: stringify!($field),
                            name: stringify!($field),
                            renamed_from: None,
                            backend: "lww",
                            value_type: $value_type,
                            target_label: None,
                            optional: false,
                            explicit_id: None,
                        }, )+
                    ],
                    explicit_id: None,
                    system: Some(SystemModel::$system),
                };
                &DESCRIPTOR
            }

            fn initialize_new_entity(&self, entity: &Entity, model_id: ankurah_proto::ModelId) {
                entity.add_membership(model_id);
                $( let _ = set(entity, stringify!($field), &self.$field); )+
            }
        }

        impl View for $view {
            type Model = $model;
            type Mutable = $mutable;
            fn entity(&self) -> &Entity { &self.0 }
            fn from_entity(inner: Entity) -> Self { Self(inner) }
            fn to_model(&self) -> anyhow::Result<$model, PropertyError> {
                Ok($model { $( $field: get(&self.0, stringify!($field))?, )+ })
            }
        }

        impl $view {
            pub fn id(&self) -> EntityId { self.0.id() }
            $( pub fn $field(&self) -> Result<$ty, PropertyError> { get(&self.0, stringify!($field)) } )+
        }

        impl Mutable for $mutable {
            type Model = $model;
            type View = $view;
            fn entity(&self) -> &Entity { &self.0 }
            fn new(entity: Entity) -> Self { Self(entity) }
        }

        impl $mutable {
            $( pub fn $field(&self, value: &$ty) -> Result<(), PropertyError> { set(&self.0, stringify!($field), value) } )+
        }

        $(
            /// WASM Ref wrapper satisfying the Model associated-type bound; a
            /// plain newtype, no bindgen surface (catalog rows are internal).
            #[cfg(feature = "wasm")]
            pub struct $refwrap(crate::property::Ref<$model>);
            #[cfg(feature = "wasm")]
            impl From<crate::property::Ref<$model>> for $refwrap {
                fn from(r: crate::property::Ref<$model>) -> Self { Self(r) }
            }
            #[cfg(feature = "wasm")]
            impl From<$refwrap> for crate::property::Ref<$model> {
                fn from(w: $refwrap) -> Self { w.0 }
            }
        )?
    };
}

catalog_row_model!(
    /// One `_ankurah_model` row: a registered model definition.
    SysModelRow / SysModelRowView / SysModelRowMut / SysModelRowRef,
    Model, "_ankurah_model",
    { label: String = "string", name: String = "string" }
);

catalog_row_model!(
    /// One `_ankurah_property` row: a registered property definition.
    /// `minted_for` and `target_model` are optional in the data (folds
    /// preserve absent provenance; only reference properties have targets).
    SysPropertyRow / SysPropertyRowView / SysPropertyRowMut / SysPropertyRowRef,
    Property, "_ankurah_property",
    {
        name: String = "string",
        backend: String = "string",
        value_type: String = "string",
        minted_for: Option<EntityId> = "entityid",
        target_model: Option<EntityId> = "entityid",
    }
);

catalog_row_model!(
    /// One `_ankurah_model_property` row: a property's membership in a model.
    SysModelPropertyRow / SysModelPropertyRowView / SysModelPropertyRowMut / SysModelPropertyRowRef,
    ModelProperty, "_ankurah_model_property",
    {
        model: EntityId = "entityid",
        property: EntityId = "entityid",
        optional: bool = "bool",
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::system_collection_label;

    /// Every row model's collection is the canonical system label and its
    /// descriptor pins the matching SystemModel: the compile-time binding
    /// the short-circuits rely on.
    #[test]
    fn row_models_pin_their_system_identity() {
        assert_eq!(SysModelRow::collection().as_str(), system_collection_label(SystemModel::Model));
        assert_eq!(SysModelRow::descriptor().system, Some(SystemModel::Model));
        assert_eq!(SysPropertyRow::collection().as_str(), system_collection_label(SystemModel::Property));
        assert_eq!(SysPropertyRow::descriptor().system, Some(SystemModel::Property));
        assert_eq!(SysModelPropertyRow::collection().as_str(), system_collection_label(SystemModel::ModelProperty));
        assert_eq!(SysModelPropertyRow::descriptor().system, Some(SystemModel::ModelProperty));
    }

    /// The row fields match the executor's writers field-for-field
    /// (registration.rs `creation` calls), so a typed View reads exactly
    /// what registration writes.
    #[test]
    fn row_fields_match_executor_writers() {
        let fields: Vec<&str> = SysPropertyRow::descriptor().properties.iter().map(|p| p.name).collect();
        assert_eq!(fields, ["name", "backend", "value_type", "minted_for", "target_model"]);
        let fields: Vec<&str> = SysModelPropertyRow::descriptor().properties.iter().map(|p| p.name).collect();
        assert_eq!(fields, ["model", "property", "optional"]);
    }
}
