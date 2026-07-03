use crate::error::AuthError;
use serde::{Deserialize, Serialize};

/// JWT claims carried in every token issued by the auth system.
///
/// Note: `sub` is stored in the standard JWT `subject` field (not in custom claims)
/// to avoid serde conflicts with `jwt_simple`'s `#[serde(flatten)]` on custom claims.
/// The `sub` field here is populated by `SigningKeys::verify()` from the standard claim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// User entity ID (ankurah EntityId serialized as string).
    /// When signing, this is placed in the standard JWT `sub` claim.
    /// When deserializing custom claims, this is skipped (populated from standard claims).
    #[serde(skip)]
    pub sub: String,

    /// User's roles (e.g. ["Admin"], ["Dispatcher", "Technician"])
    pub roles: Vec<String>,

    /// User's email
    pub email: String,

    /// User's display name
    #[serde(default)]
    pub name: Option<String>,

    /// Arbitrary custom claims from the identity provider.
    /// Captures any JSON fields not explicitly defined above.
    #[serde(flatten)]
    pub custom: serde_json::Map<String, serde_json::Value>,
}

/// Parse a JWT token without verifying the signature.
/// Extracts claims from the payload section (base64url-decoded).
/// Useful on clients that only need to read claims without access to the signing key.
pub fn parse_claims_unverified(token: &str) -> Result<JwtClaims, AuthError> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(AuthError::Verification("invalid JWT format: expected 3 dot-separated segments".into()));
    }

    let payload_bytes =
        URL_SAFE_NO_PAD.decode(parts[1]).map_err(|e| AuthError::Verification(format!("failed to decode JWT payload: {e}")))?;

    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|e| AuthError::Verification(format!("failed to parse JWT payload JSON: {e}")))?;

    let sub = payload
        .get("sub")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AuthError::Verification("missing 'sub' claim in JWT payload".into()))?
        .to_string();

    let roles = payload
        .get("roles")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let email = payload.get("email").and_then(|v| v.as_str()).unwrap_or_default().to_string();

    let name = payload.get("name").and_then(|v| v.as_str()).map(String::from);

    // Extract custom claims: all fields not in the known set
    let known_keys: &[&str] = &["sub", "roles", "email", "name", "iat", "exp", "nbf", "iss", "aud", "jti"];
    let custom = if let Some(obj) = payload.as_object() {
        obj.iter().filter(|(k, _)| !known_keys.contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect()
    } else {
        serde_json::Map::new()
    };

    Ok(JwtClaims { sub, roles, email, name, custom })
}
