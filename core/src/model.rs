pub mod tsify;

use std::marker::PhantomData;

use ankurah_proto::{CollectionId, EntityId, State};

use crate::entity::Entity;
use crate::error::StateError;
use crate::property::value::Ref;
use crate::property::Backends;
use crate::property::PropertyError;

use anyhow::Result;

/// A model is a struct that represents the present values for a given entity
/// Schema is defined primarily by the Model object, and the View is derived from that via macro.
/// It is used primarily to create a new Entity
/// Occasionally you might want to call myview.to_model() to extract all the properties at the same time into a Model struct
/// but most of the time you'll probably want to use the accessors on the View to access the properties instead.
pub trait Model {
    /// The read only view of the model
    type View: View;
    /// The mutable view of the model
    type Mutable<'trx>: Mutable<'trx>;
    /// The collection id of the model
    fn collection() -> CollectionId;
    /// Populates the entity properties from the model fields on creation of a new entity
    fn initialize_new_entity(&self, entity: &Entity);
}

/// A read only view of an Entity which offers typed accessors
pub trait View {
    /// The model type
    type Model: Model;
    /// The mutable view of the model
    type Mutable<'trx>: Mutable<'trx>;
    /// The entity id of the view
    fn id(&self) -> EntityId { self.entity().id() }
    /// The property backends of the entity
    fn backends(&self) -> &Backends { self.entity().backends() }
    /// The collection id associated with the model definition
    fn collection() -> CollectionId { <Self::Model as Model>::collection() }
    /// Returns the (dynamically typed) Entity
    fn entity(&self) -> &Entity;
    /// Creates a new view from an entity
    fn from_entity(inner: Entity) -> Self;
    /// Converts the view to the model type
    fn to_model(&self) -> Result<Self::Model, PropertyError>;
    /// Returns a reference to the entity
    fn reference(&self) -> Ref<Self::Model> { Ref { id: self.id(), phantom: PhantomData } }
}

/// A mutable Model instance for an Entity with typed accessors.
/// It is associated with a transaction, and may not outlive said transaction.
pub trait Mutable<'rec> {
    type Model: Model;
    type View: View;
    fn id(&self) -> EntityId { self.entity().id() }
    fn collection() -> CollectionId { <Self::Model as Model>::collection() }
    fn backends(&self) -> &Backends { &self.entity().backends }
    fn entity(&self) -> &Entity;
    fn new(inner: &'rec Entity) -> Self
    where Self: Sized;

    fn state(&self) -> Result<State, StateError> { self.entity().to_state() }

    fn read(&self) -> Self::View {
        let inner = self.entity();

        let new_inner = match &inner.upstream {
            // If there is an upstream, use it
            Some(upstream) => upstream.clone(),
            // Else we're a new Entity, and we have to rely on the commit to add this to the node
            None => inner.clone(),
        };

        Self::View::from_entity(new_inner)
    }

    fn reference(&self) -> Ref<Self::Model> { Ref { id: self.id(), phantom: PhantomData } }
}
