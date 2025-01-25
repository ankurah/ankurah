use ankurah::Model;

pub struct Album {
    pub name: String,
}

#[derive(Model, Debug)]
pub struct Session {
    pub date_connected: String,
    pub ip_address: String,
    pub node_id: String,
    #[cfg(not(feature = "wasm"))]
    #[model(ephemeral)]
    pub frobnicator: Frobnicator,
}

#[derive(Default, Clone, Debug)]
pub struct Frobnicator {}
