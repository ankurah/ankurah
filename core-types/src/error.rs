/// An error decoding a durable identity from an external representation.
#[derive(Debug)]
pub enum DecodeError {
    /// A JavaScript value was not a string.
    NotStringValue,
    /// The input was not valid URL-safe base64.
    InvalidBase64(base64::DecodeError),
    /// The decoded representation did not contain exactly 16 bytes.
    InvalidLength,
    /// The input was not a valid ULID.
    InvalidUlid,
    /// A compatibility fallback could not decode the input.
    InvalidFallback,
    /// The input used an unsupported identity format.
    InvalidFormat,
    /// Another decoder failure.
    Other(anyhow::Error),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStringValue => write!(f, "Not a string value"),
            Self::InvalidBase64(error) => write!(f, "Invalid Base64: {error}"),
            Self::InvalidLength => write!(f, "Invalid Length"),
            Self::InvalidUlid => write!(f, "Invalid ULID"),
            Self::InvalidFallback => write!(f, "Invalid Fallback"),
            Self::InvalidFormat => write!(f, "Invalid Format"),
            Self::Other(error) => write!(f, "Other: {error}"),
        }
    }
}

impl std::error::Error for DecodeError {}

impl From<base64::DecodeError> for DecodeError {
    fn from(error: base64::DecodeError) -> Self { Self::InvalidBase64(error) }
}

/// A foreign-function-safe identity parsing error.
#[derive(Debug, thiserror::Error)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Error))]
#[cfg_attr(feature = "uniffi", uniffi(flat_error))]
pub enum IdParseError {
    /// The input was not valid URL-safe base64.
    #[error("Invalid base64 encoding")]
    InvalidBase64,
    /// The decoded input did not contain exactly 16 bytes.
    #[error("Invalid ID length")]
    InvalidLength,
    /// The input used an unsupported format.
    #[error("Invalid format: {0}")]
    InvalidFormat(String),
}

impl From<DecodeError> for IdParseError {
    fn from(error: DecodeError) -> Self {
        match error {
            DecodeError::InvalidBase64(_) => Self::InvalidBase64,
            DecodeError::InvalidLength => Self::InvalidLength,
            _ => Self::InvalidFormat(error.to_string()),
        }
    }
}
