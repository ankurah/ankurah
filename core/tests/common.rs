use tracing::Level;

use ankurah_core::property::value::YrsString;
use ankurah_derive::Model;
use serde::{Deserialize, Serialize};

#[derive(Model, Debug, Serialize, Deserialize)] // This line now uses the Model derive macro
pub struct Album {
    #[active_value(YrsString)]
    pub name: String,
    #[active_value(YrsString)]
    pub year: String,
}

// Initialize tracing for tests
#[ctor::ctor]
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_test_writer()
        .init();
}
