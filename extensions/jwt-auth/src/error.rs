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
}

/// A policy the agent refuses to author.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PolicyAuthoringError {
    /// A scope filter names a property as `Component::name`. A scope restricts one model's entities by that
    /// model's own properties, so its property names take no qualifier.
    #[error(
        "the scope filter '{filter}' on '{model}' qualifies a property with {}::; a scope names its own model's properties without a qualifier",
        ankql::parser::identifier_spelling(.component)
    )]
    QualifiedScopeProperty { model: String, filter: String, component: String },
}
