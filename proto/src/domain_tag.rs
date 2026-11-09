//! The domain tags: one for each kind of string Ankurah hashes or signs, written first in that
//! string so that a string made for one purpose can never be taken for another's.
//!
//! Every tag is `ankurah.<name>.v1` and enters a string as one length byte followed by its
//! bytes, so no tag's encoded form is a prefix of another's. A new derivation is a new variant
//! with a new tag, never a new meaning for an existing one.

/// The kind of string a hash or a signature covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainTag {
    /// A genesis event's id.
    Genesis,
    /// An update event's id.
    Event,
    /// The digest of one operation's bytes.
    Operation,
    /// The claims a presence signature covers.
    Presence,
    /// The body an attestation signature covers.
    Attestation,
    /// The digest of an entity state.
    State,
}

impl DomainTag {
    fn bytes(self) -> &'static [u8] {
        match self {
            Self::Genesis => b"ankurah.genesis.v1",
            Self::Event => b"ankurah.event.v1",
            Self::Operation => b"ankurah.operation.v1",
            Self::Presence => b"ankurah.presence.v1",
            Self::Attestation => b"ankurah.attestation.v1",
            Self::State => b"ankurah.state.v1",
        }
    }

    /// This tag as it opens a hashed or signed string: one length byte, then the tag's bytes.
    pub fn encoded(self) -> Vec<u8> {
        let bytes = self.bytes();
        let mut encoded = Vec::with_capacity(1 + bytes.len());
        // Every tag is shorter than 256 bytes, so its length fits the byte; the tests pin each tag.
        encoded.push(bytes.len() as u8);
        encoded.extend_from_slice(bytes);
        encoded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [DomainTag; 6] =
        [DomainTag::Genesis, DomainTag::Event, DomainTag::Operation, DomainTag::Presence, DomainTag::Attestation, DomainTag::State];

    #[test]
    fn each_tag_is_these_exact_bytes() {
        assert_eq!(DomainTag::Genesis.encoded(), b"\x12ankurah.genesis.v1");
        assert_eq!(DomainTag::Event.encoded(), b"\x10ankurah.event.v1");
        assert_eq!(DomainTag::Operation.encoded(), b"\x14ankurah.operation.v1");
        assert_eq!(DomainTag::Presence.encoded(), b"\x13ankurah.presence.v1");
        assert_eq!(DomainTag::Attestation.encoded(), b"\x16ankurah.attestation.v1");
        assert_eq!(DomainTag::State.encoded(), b"\x10ankurah.state.v1");
    }

    #[test]
    fn no_encoded_tag_is_a_prefix_of_another() {
        for (i, a) in ALL.iter().enumerate() {
            for (j, b) in ALL.iter().enumerate() {
                if i != j {
                    assert!(!b.encoded().starts_with(&a.encoded()), "{a:?} opens {b:?}");
                }
            }
        }
    }
}
