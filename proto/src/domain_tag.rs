//! The domain tags: each opens one kind of string Ankurah hashes or signs, so a string made for one
//! purpose is never taken for another's. A new derivation takes a new tag; a tag never changes meaning.

/// The kind of string a hash or a signature covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainTag {
    /// A genesis event's id.
    Genesis,
    /// An update event's id.
    Update,
    /// The digest of one operation's bytes.
    Operation,
    /// The claims a presence signature covers.
    Presence,
    /// The statement an attestation signature covers.
    Attestation,
    /// The digest of an entity state.
    State,
}

impl DomainTag {
    /// This tag as it opens a hashed or signed string: one length byte, then the tag's bytes.
    pub const fn encoded(self) -> &'static [u8] {
        match self {
            Self::Genesis => b"\x12ankurah.genesis.v1",
            Self::Update => b"\x11ankurah.update.v1",
            Self::Operation => b"\x14ankurah.operation.v1",
            Self::Presence => b"\x13ankurah.presence.v1",
            Self::Attestation => b"\x16ankurah.attestation.v1",
            Self::State => b"\x10ankurah.state.v1",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [DomainTag; 6] =
        [DomainTag::Genesis, DomainTag::Update, DomainTag::Operation, DomainTag::Presence, DomainTag::Attestation, DomainTag::State];

    /// The encoded tags of specs/protocol/bytes.md section 2.
    #[test]
    fn each_tag_is_its_length_then_its_bytes_as_the_byte_specification_lists_it() {
        for (tag, hex) in [
            (DomainTag::Genesis, "12616e6b757261682e67656e657369732e7631"),
            (DomainTag::Update, "11616e6b757261682e7570646174652e7631"),
            (DomainTag::Operation, "14616e6b757261682e6f7065726174696f6e2e7631"),
            (DomainTag::Presence, "13616e6b757261682e70726573656e63652e7631"),
            (DomainTag::Attestation, "16616e6b757261682e6174746573746174696f6e2e7631"),
            (DomainTag::State, "10616e6b757261682e73746174652e7631"),
        ] {
            let encoded = tag.encoded();
            assert_eq!(hex::encode(encoded), hex, "{tag:?}");
            assert_eq!(usize::from(encoded[0]), encoded.len() - 1, "{tag:?}: the first byte counts the rest");
        }
    }

    #[test]
    fn no_encoded_tag_is_a_prefix_of_another() {
        for (i, a) in ALL.iter().enumerate() {
            for (j, b) in ALL.iter().enumerate() {
                if i != j {
                    assert!(!b.encoded().starts_with(a.encoded()), "{a:?} opens {b:?}");
                }
            }
        }
    }
}
