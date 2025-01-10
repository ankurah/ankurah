use ankurah_core::property::value::YrsString;
use ankurah_core::Model;
use serde::{Deserialize, Serialize};

pub struct Album {
    pub name: String,
}

#[derive(Model, Clone, Debug)]
pub struct Session {
    #[active_type(YrsString)]
    pub date_connected: String,
    #[active_type(YrsString)]
    pub ip_address: String,
    #[active_type(YrsString)]
    pub node_id: String,
}
