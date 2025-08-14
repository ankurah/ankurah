pub mod tsify;

use std::sync::Arc;

use ankurah_proto::{CollectionId, EntityId, State};

use crate::entity::Entity;
use crate::error::StateError;

use crate::property::PropertyError;

use anyhow::Result;

/// A model is a struct that represents the present values for a given entity
/// Schema is defined primarily by the Model object, and the View is derived from that via macro.
pub trait Model {
    type View: View;
    type Mutable<'trx>: Mutable<'trx>;
    fn collection() -> CollectionId;
    // TODO - this seems to be necessary, but I don't understand why
    // Backend fields should be getting initialized on demand when the values are set
    fn initialize_new_entity(&self, entity: &Entity);
}

/// A read only view of an Entity which offers typed accessors
pub trait View {
    type Model: Model;
    type Mutable<'trx>: Mutable<'trx>;
    fn id(&self) -> EntityId { self.entity().id() }

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
    fn id(&self) -> EntityId { self.entity().id() }
    fn collection() -> CollectionId { <Self::Model as Model>::collection() }

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
}

// Helper function for Subscribe implementations in generated Views
// don't document this
#[doc(hidden)]
pub fn vsub_helper<V, F>(view: &V, listener: F) -> ankurah_signals::SubscriptionGuard
where
    V: ankurah_signals::Signal + View + Clone + Send + Sync + 'static,
    F: ankurah_signals::subscribe::IntoSubscribeListener<V>,
{
    let listener = listener.into_subscribe_listener();
    let view_clone = view.clone();
    let subscription = view.listen(Arc::new(move || {
        // Call the listener with the current view when the broadcast fires
        listener(view_clone.clone());
    }));
    ankurah_signals::SubscriptionGuard::new(subscription)
}
