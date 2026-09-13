use ankurah_proto::EntityId;

/// Sanitize a display name into a lower-case storage identifier seed: ASCII
/// letters are case-folded, every character outside `[A-Za-z0-9_]` becomes
/// `_`, and a leading digit is prefixed with `_`. Engines still quote
/// identifiers; lower-casing prevents display-name casing from leaking into
/// physical schemas and keeps naming consistent across implementations.
pub fn sanitize(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else if c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.is_empty() || out.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

/// Every representable widening candidate is taken. This is refused rather
/// than returning a name known to be occupied, which would silently alias two
/// identities onto one physical structure.
#[derive(Debug, thiserror::Error)]
#[error("no free storage name for {desired:?}: every candidate through the full id suffix {last:?} is taken")]
pub struct NamingExhausted {
    pub desired: String,
    pub last: String,
}

/// Encode the complete identity with the lowercase base32hex alphabet.
///
/// The 52-character result is injective for a 32-byte identity and fits inside
/// PostgreSQL's 63-byte identifier limit with room for a short readable seed.
fn encoded_id(id: &EntityId) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789abcdefghijklmnopqrstuv";
    let mut output = String::with_capacity((EntityId::BYTE_LEN * 8).div_ceil(5));
    let mut buffer = 0u16;
    let mut bits = 0u8;
    for byte in id.to_bytes() {
        buffer = (buffer << 8) | u16::from(byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            output.push(ALPHABET[((buffer >> bits) & 0x1f) as usize] as char);
            buffer &= (1u16 << bits) - 1;
        }
    }
    if bits != 0 {
        output.push(ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize] as char);
    }
    output
}

fn suffixed_candidate(desired: &str, suffix: &str, max_len: Option<usize>) -> Option<String> {
    let seed_len = match max_len {
        Some(max_len) => max_len.checked_sub(suffix.len() + 1)?,
        None => desired.len(),
    };
    let mut end = desired.len().min(seed_len);
    while !desired.is_char_boundary(end) {
        end -= 1;
    }
    Some(format!("{}_{}", &desired[..end], suffix))
}

fn dedupe_with_limit(
    desired: &str,
    id: &EntityId,
    max_len: Option<usize>,
    is_taken: impl Fn(&str) -> bool,
) -> Result<String, NamingExhausted> {
    if max_len.is_none_or(|max_len| desired.len() <= max_len) && !is_taken(desired) {
        return Ok(desired.to_string());
    }

    let encoded = encoded_id(id);
    let mut last = desired.to_string();
    for len in 4..=encoded.len() {
        let suffix = &encoded[encoded.len() - len..];
        let Some(candidate) = suffixed_candidate(desired, suffix, max_len) else {
            continue;
        };
        last = candidate.clone();
        if !is_taken(&candidate) {
            return Ok(candidate);
        }
    }
    Err(NamingExhausted { desired: desired.to_string(), last })
}

/// Pick a stable storage name for `desired`, widening an id-derived suffix
/// until the candidate is free.
pub fn dedupe(desired: &str, id: &EntityId, is_taken: impl Fn(&str) -> bool) -> Result<String, NamingExhausted> {
    dedupe_with_limit(desired, id, None, is_taken)
}

/// Pick a stable storage name whose UTF-8 representation does not exceed
/// `max_len` bytes. An overlong seed is truncated only after reserving room
/// for an identity suffix, so two long labels with the same prefix cannot
/// collapse onto the same physical name.
pub fn dedupe_bounded(desired: &str, id: &EntityId, max_len: usize, is_taken: impl Fn(&str) -> bool) -> Result<String, NamingExhausted> {
    dedupe_with_limit(desired, id, Some(max_len), is_taken)
}

/// Physical name seeded by an ID prefix when no catalog label is available.
pub fn fallback(prefix: &str, id: &EntityId, is_taken: impl Fn(&str) -> bool) -> Result<String, NamingExhausted> {
    let encoded = encoded_id(id);
    for len in 8..=encoded.len() {
        let candidate = format!("{}_{}", prefix, &encoded[..len]);
        if !is_taken(&candidate) {
            return Ok(candidate);
        }
    }
    Err(NamingExhausted { desired: prefix.to_string(), last: format!("{prefix}_{encoded}") })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; EntityId::BYTE_LEN];
        bytes[0] = byte;
        bytes[EntityId::BYTE_LEN - 1] = byte.wrapping_add(1);
        EntityId::from_bytes(bytes)
    }

    #[test]
    fn sanitize_maps_invalid_chars_and_leading_digit() {
        assert_eq!(sanitize("title"), "title");
        assert_eq!(sanitize("AlbumTitle"), "albumtitle");
        assert_eq!(sanitize("my-field.x"), "my_field_x");
        assert_eq!(sanitize("9lives"), "_9lives");
        assert_eq!(sanitize(""), "_");
    }

    #[test]
    fn dedupe_returns_desired_when_free() {
        assert_eq!(dedupe("title", &id(1), |_| false).unwrap(), "title");
    }

    #[test]
    fn dedupe_suffixes_with_trailing_id_chars_and_widens() {
        let property = id(1);
        let encoded = encoded_id(&property);
        let suffix4 = encoded[encoded.len() - 4..].to_string();
        let taken4 = format!("title_{suffix4}");
        assert_eq!(dedupe("title", &property, |n| n == "title").unwrap(), taken4);
        // Block the first candidate, but leave wider suffixes available.
        let five = dedupe("title", &property, |n| n == "title" || n == taken4).unwrap();
        assert!(five.starts_with("title_") && five.len() > taken4.len());
        assert_eq!(five, five.to_ascii_lowercase());
    }

    #[test]
    fn dedupe_can_widen_through_the_complete_identity() {
        let mut first = [0u8; EntityId::BYTE_LEN];
        let mut second = first;
        // These differ only in the first base32 digit, so every proper suffix
        // collides and the complete identity is required to distinguish them.
        first[0] = 0x08;
        second[0] = 0x10;
        let first = EntityId::from_bytes(first);
        let second = EntityId::from_bytes(second);
        let desired = "shared_prefix".repeat(8);
        let first_encoded = encoded_id(&first);
        let occupied: std::collections::HashSet<String> = (4..=first_encoded.len())
            .filter_map(|len| suffixed_candidate(&desired, &first_encoded[first_encoded.len() - len..], Some(63)))
            .collect();

        let assigned = dedupe_bounded(&desired, &second, 63, |candidate| occupied.contains(candidate)).unwrap();
        assert!(!occupied.contains(&assigned));
        assert_eq!(assigned.len(), 63);
        assert!(assigned.ends_with(&format!("_{}", encoded_id(&second))));
    }

    #[test]
    fn bounded_names_reserve_suffix_space_before_truncating() {
        let desired = "shared_prefix".repeat(8);
        let first = dedupe_bounded(&desired, &id(1), 63, |_| false).unwrap();
        let second = dedupe_bounded(&desired, &id(2), 63, |candidate| candidate == first).unwrap();
        assert!(first.len() <= 63);
        assert!(second.len() <= 63);
        assert_ne!(first, second);
    }

    #[test]
    fn fallback_is_visibly_synthetic() {
        let name = fallback("p", &id(7), |_| false).unwrap();
        assert!(name.starts_with("p_") && name.len() >= 6, "got {name}");
    }

    /// Exhaustion refuses instead of returning the final (known-taken)
    /// candidate: returning it would silently alias two identities onto one
    /// physical name.
    #[test]
    fn exhaustion_errors_instead_of_aliasing() {
        let err = dedupe("title", &id(1), |_| true).unwrap_err();
        assert!(err.to_string().contains("title"), "got {err}");
        assert!(fallback("p", &id(1), |_| true).is_err());
    }
}
