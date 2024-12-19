use tracing::Level;

use ankurah_core::property::value::YrsString;
use ankurah_derive::Model;
use serde::{Deserialize, Serialize};

pub use ankurah_core::Node;

#[derive(Model, Debug, Serialize, Deserialize)]
pub struct Album {
    #[active_value(YrsString)]
    pub name: String,
}

// Initialize tracing for tests
#[ctor::ctor]
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_test_writer()
        .init();
}
