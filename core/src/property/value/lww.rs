use std::{marker::PhantomData, sync::Weak};

use crate::property::{backend::LWWBackend, PropertyName};

use super::ProjectedValue;

pub struct LWW<T> {
    pub property_name: PropertyName,
    pub backend: Weak<LWWBackend>,

    phantom: PhantomData<T>,
}

impl<T> ProjectedValue for LWW<T> {
    type Projected = T;
    fn projected(&self) -> Self::Projected {
        //self.value()
        todo!()
    }
}
