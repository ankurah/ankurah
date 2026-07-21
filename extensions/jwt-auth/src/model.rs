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

    /// Serialized IssuerTrustDescriptor JSON. Optional so entities written by
    /// pre-issuer versions remain readable.
    #[active_type(LWW)]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust_json: Option<String>,
}
