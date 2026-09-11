use crate::internal::prelude::*;
use crate::schema::catalog::{
    register::{Registrant, RegistrantProperty},
    SysModelRow, SysPropertyRow,
};
use crate::schema::registration::RegistrationError;

pub(super) struct WireRegistrant {
    request: proto::RegisterModel,
    model: Option<(proto::EntityId, String, String)>,
    properties: Vec<proto::RegisteredProperty>,
}

impl WireRegistrant {
    pub(super) fn new(request: proto::RegisterModel) -> Self {
        let properties = Vec::with_capacity(request.properties.len());
        Self { request, model: None, properties }
    }

    pub(super) fn into_response(self) -> Option<proto::RegisteredModel> {
        let (id, label, name) = self.model?;
        Some(proto::RegisteredModel { id, label, name, properties: self.properties })
    }
}

impl RegistrantProperty for proto::RegisterProperty {
    fn build_id(&self) -> [u8; 16] { self.build_id }

    fn name(&self) -> &str { &self.name }

    fn renamed_from(&self) -> Option<&str> { self.renamed_from.as_deref() }

    fn backend(&self) -> &str { &self.backend }

    fn value_type(&self) -> &str { &self.value_type }

    fn target_label(&self) -> Option<&str> { self.target_label.as_deref() }

    fn explicit_id(&self) -> Option<proto::EntityId> { self.explicit_id }

    fn optional(&self) -> bool { self.optional }
}

impl Registrant for WireRegistrant {
    type Property = proto::RegisterProperty;

    fn build_id(&self) -> [u8; 16] { self.request.build_id }

    fn label(&self) -> &str { &self.request.label }

    fn name(&self) -> &str { &self.request.name }

    fn explicit_id(&self) -> Option<proto::EntityId> { self.request.explicit_id }

    fn properties(&self) -> impl ExactSizeIterator<Item = &Self::Property> { self.request.properties.iter() }

    fn set_model(&mut self, id: proto::EntityId, model: SysModelRow) -> Result<(), RegistrationError> {
        self.model = Some((id, model.label, model.name));
        Ok(())
    }

    fn set_property(
        &mut self,
        index: usize,
        id: proto::EntityId,
        property: SysPropertyRow,
        membership_id: proto::EntityId,
        optional: bool,
    ) -> Result<(), RegistrationError> {
        let build_id = self.request.properties[index].build_id;
        self.properties.push(proto::RegisteredProperty {
            build_id,
            id,
            membership_id,
            name: property.name,
            backend: property.backend,
            value_type: property.value_type,
            target_model: property.target_model,
            minted_for: property.minted_for,
            optional,
        });
        Ok(())
    }

    fn finish(&mut self) -> Result<(), RegistrationError> { Ok(()) }
}
