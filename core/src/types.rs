pub mod value;

use ulid::Ulid;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ID(pub Ulid);
