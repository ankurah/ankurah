//! The connections that serve map calls, each with the statements it
//! prepared: where they come from, and the table they read and write.

mod pool;
pub(crate) mod schema;

use tokio_postgres::Client;

pub use self::pool::MapSettings;
pub(crate) use self::pool::{Lease, Pool};
use self::schema::Statements;

/// A connection of the maps' pool, and the statements it prepared.
pub(crate) struct Connection {
    pub(crate) client: Client,
    pub(crate) statements: Statements,
}
