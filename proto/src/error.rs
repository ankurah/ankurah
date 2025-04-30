#[derive(Debug)]
pub enum DecodeError {
    NotStringValue,
    InvalidBase64(base64::DecodeError),
    InvalidLength,
    InvalidUlid,
    InvalidFallback,
    InvalidFormat,
    Other(anyhow::Error),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::NotStringValue => write!(f, "Not a string value"),
            DecodeError::InvalidBase64(e) => write!(f, "Invalid Base64: {}", e),
            DecodeError::InvalidLength => write!(f, "Invalid Length"),
            DecodeError::InvalidUlid => write!(f, "Invalid ULID"),
            DecodeError::InvalidFallback => write!(f, "Invalid Fallback"),
            DecodeError::Other(e) => write!(f, "Other: {}", e),
            DecodeError::InvalidFormat => write!(f, "Invalid Format"),
        }
    }
}

impl std::error::Error for DecodeError {}

impl From<base64::DecodeError> for DecodeError {
    fn from(e: base64::DecodeError) -> Self { DecodeError::InvalidBase64(e) }
}
