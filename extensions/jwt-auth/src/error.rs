use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Key generation failed: {0}")]
    KeyGeneration(String),

    #[error("Key parsing failed: {0}")]
    KeyParsing(String),

    #[error("Key export failed: {0}")]
    KeyExport(String),

    #[error("Token signing failed: {0}")]
    Signing(String),

    #[error("Token verification failed: {0}")]
    Verification(String),

    #[error("Invalid issuer trust: {0}")]
    IssuerTrust(String),

    #[error("Issuer HTTP request failed: {0}")]
    IssuerHttp(String),
}
