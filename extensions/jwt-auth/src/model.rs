use ankurah::Model;
use serde::{Deserialize, Serialize};

/// Policy configuration entity stored in ankurah.
/// The collection name is auto-derived as "jwtpolicy" (struct name lowercased).
#[derive(Model, Debug, Serialize, Deserialize)]
pub struct JwtPolicy {
    /// Serialized PolicyConfig JSON
    #[active_type(LWW)]
    pub config_json: String,

    /// PEM-encoded public key for JWT verification
    #[active_type(LWW)]
    pub public_key_pem: String,
}
