use serde::{Deserialize, Serialize};

pub struct LRW<T: Serialize + Deserialize> {}

// Need ID based happens-before determination to resolve conflicts
