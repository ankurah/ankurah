pub mod tsify;

use ankurah_proto::{CollectionId, State, ID};

use crate::entity::Entity;
use crate::property::Backends;
use crate::property::PropertyError;

use anyhow::Result;

/// A model is a struct that represents the present values for a given entity
/// Schema is defined primarily by the Model object, and the View is derived from that via macro.
pub trait Model {
    type View: View;
    type Mutable<'trx>: Mutable<'trx>;
    fn collection() -> CollectionId;
    fn create_entity(&self, id: ID) -> Entity;
}

/// A read only view of an Entity which offers typed accessors
pub trait View {
    type Model: Model;
    type Mutable<'trx>: Mutable<'trx>;
    fn id(&self) -> ID { self.entity().id }
    fn backends(&self) -> &Backends { self.entity().backends() }
    fn collection() -> CollectionId { <Self::Model as Model>::collection() }
    fn entity(&self) -> &Entity;
    fn from_entity(inner: Entity) -> Self;
    fn to_model(&self) -> Result<Self::Model, PropertyError>;
}

/// A mutable Model instance for an Entity with typed accessors.
/// It is associated with a transaction, and may not outlive said transaction.
pub trait Mutable<'rec> {
    type Model: Model;
    type View: View;
    fn id(&self) -> ID { self.entity().id }
    fn collection() -> CollectionId { <Self::Model as Model>::collection() }
    fn backends(&self) -> &Backends { &self.entity().backends }
    fn entity(&self) -> &Entity;
    fn new(inner: &'rec Entity) -> Self
    where Self: Sized;

    fn state(&self) -> anyhow::Result<State> { self.entity().to_state() }

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
}
