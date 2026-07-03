use crate::claims::JwtClaims;
use crate::error::AuthError;
use jwt_simple::prelude::*;

/// Manages RSA keypair for signing and verifying JWTs.
#[derive(Clone)]
pub struct SigningKeys {
    key_pair: RS256KeyPair,
    public_key: RS256PublicKey,
}

impl SigningKeys {
    /// Generate a new random RSA 4096-bit keypair.
    pub fn generate() -> Result<Self, AuthError> {
        let key_pair = RS256KeyPair::generate(4096).map_err(|e| AuthError::KeyGeneration(e.to_string()))?;
        let public_key = key_pair.public_key();
        Ok(Self { key_pair, public_key })
    }

    /// Create from an existing PEM-encoded private key.
    pub fn from_pem(pem: &str) -> Result<Self, AuthError> {
        let key_pair = RS256KeyPair::from_pem(pem).map_err(|e| AuthError::KeyParsing(e.to_string()))?;
        let public_key = key_pair.public_key();
        Ok(Self { key_pair, public_key })
    }

    /// Sign claims into a JWT string.
    ///
    /// The `sub` field from `JwtClaims` is placed in the standard JWT `subject` claim
    /// to avoid serde conflicts with `jwt_simple`'s flattened custom claims.
    pub fn sign(&self, claims: &JwtClaims, duration: Duration) -> Result<String, AuthError> {
        let jwt_claims = Claims::with_custom_claims(claims.clone(), duration).with_subject(&claims.sub);
        self.key_pair.sign(jwt_claims).map_err(|e| AuthError::Signing(e.to_string()))
    }

    /// Verify a JWT string and extract claims.
    ///
    /// Reconstructs the `sub` field from the standard JWT `subject` claim.
    pub fn verify(&self, token: &str) -> Result<JwtClaims, AuthError> {
        let result = self.public_key.verify_token::<JwtClaims>(token, None).map_err(|e| AuthError::Verification(e.to_string()))?;
        let sub = result.subject.ok_or_else(|| AuthError::Verification("missing subject claim".into()))?;
        let mut claims = result.custom;
        claims.sub = sub;
        Ok(claims)
    }

    /// Export the public key as PEM (for sharing with clients).
    pub fn public_key_pem(&self) -> Result<String, AuthError> { self.public_key.to_pem().map_err(|e| AuthError::KeyExport(e.to_string())) }

    /// Export the private key as PEM.
    pub fn private_key_pem(&self) -> Result<String, AuthError> { self.key_pair.to_pem().map_err(|e| AuthError::KeyExport(e.to_string())) }
}

/// Either a full signing keypair or a verify-only public key.
#[derive(Clone)]
pub enum JwtKeys {
    Signing(SigningKeys),
    VerifyOnly(RS256PublicKey),
}

impl JwtKeys {
    /// Verify a JWT string and extract claims.
    pub fn verify(&self, token: &str) -> Result<JwtClaims, AuthError> {
        match self {
            JwtKeys::Signing(keys) => keys.verify(token),
            JwtKeys::VerifyOnly(public_key) => {
                let result = public_key.verify_token::<JwtClaims>(token, None).map_err(|e| AuthError::Verification(e.to_string()))?;
                let sub = result.subject.ok_or_else(|| AuthError::Verification("missing subject claim".into()))?;
                let mut claims = result.custom;
                claims.sub = sub;
                Ok(claims)
            }
        }
    }

    /// Export the public key as PEM.
    pub fn public_key_pem(&self) -> Result<String, AuthError> {
        match self {
            JwtKeys::Signing(keys) => keys.public_key_pem(),
            JwtKeys::VerifyOnly(public_key) => public_key.to_pem().map_err(|e| AuthError::KeyExport(e.to_string())),
        }
    }

    /// Create a verify-only key set from a PEM-encoded public key.
    pub fn from_public_pem(pem: &str) -> Result<Self, AuthError> {
        let public_key = RS256PublicKey::from_pem(pem).map_err(|e| AuthError::KeyParsing(e.to_string()))?;
        Ok(JwtKeys::VerifyOnly(public_key))
    }
}
