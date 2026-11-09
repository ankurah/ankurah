//! Reading back the encoding Ankurah carries: what bincode 1.3.3's default free functions
//! write, as specs/protocol/bytes.md section 1 sets out.

use bincode::Options;

/// The value `bytes` hold in the encoding `bincode::serialize` writes; bytes left over after
/// the value make them undecodable.
pub(crate) fn decode_exact<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> bincode::Result<T> {
    bincode::DefaultOptions::new().with_fixint_encoding().deserialize(bytes)
}
