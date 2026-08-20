use ankurah_derive::Model;
use ankurah_proto::EntityId;

#[derive(Model, Debug, Clone)]
#[model(base = "crate", system = "Model", no_ffi)]
pub struct SysModelRow {
    #[active_type(LWW)]
    pub label: String,
    #[active_type(LWW)]
    pub name: String,
}

#[derive(Model, Debug, Clone)]
#[model(base = "crate", system = "Property", no_ffi)]
pub struct SysPropertyRow {
    #[active_type(LWW)]
    pub name: String,
    #[active_type(LWW)]
    pub backend: String,
    #[active_type(LWW)]
    pub value_type: String,
    pub minted_for: Option<EntityId>,
    pub target_model: Option<EntityId>,
}

#[derive(Model, Debug, Clone)]
#[model(base = "crate", system = "ModelProperty", no_ffi)]
pub struct SysModelPropertyRow {
    pub model: EntityId,
    pub property: EntityId,
    pub optional: bool,
}