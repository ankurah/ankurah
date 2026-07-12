use ankurah_proto::EntityId;

// PostgreSQL's shortest cross-engine identifier limit. Staying within it
// avoids silent server-side truncation turning two names into one.
const MAX_IDENTIFIER_LEN: usize = 63;
// Synthetic fully-qualified names live here. `sanitize` prevents a
// catalog display name from entering the namespace unqualified.
const QUALIFIED_PREFIX: &str = "__a_";

/// Sanitize a display name into a storage identifier seed: every char
/// outside `[A-Za-z0-9_]` becomes `_`, and a leading digit is prefixed
/// with `_`. Engines still quote identifiers; this exists so the same
/// display name yields the same column name on every engine (and so the
/// postgres/sqlite `sane_name` whitelist accepts it).
pub fn sanitize(name: &str) -> String {
    let mut out: String = name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
    if out.is_empty() || out.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    if out.starts_with(QUALIFIED_PREFIX) {
        out.insert(0, '_');
    }
    out.truncate(MAX_IDENTIFIER_LEN);
    out
}

/// Identifier-safe RFC 4648 base32 (lowercase, no padding). Unlike
/// filtered base64, this is injective over all 32 id bytes.
fn id_token(id: &EntityId) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(52);
    let mut accumulator = 0u16;
    let mut bits = 0u8;
    for byte in id.as_bytes() {
        accumulator = (accumulator << 8) | u16::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((accumulator >> bits) & 0x1f) as usize] as char);
            accumulator &= (1u16 << bits) - 1;
        }
    }
    if bits != 0 {
        out.push(ALPHABET[((accumulator << (5 - bits)) & 0x1f) as usize] as char);
    }
    debug_assert_eq!(out.len(), 52);
    out
}

/// The trailing `len` characters of the injective id token.
fn id_suffix(id: &EntityId, len: usize) -> String {
    let token = id_token(id);
    token[token.len().saturating_sub(len)..].to_owned()
}

fn suffixed(seed: &str, id: &EntityId, len: usize) -> String {
    let suffix = id_suffix(id, len);
    let seed_len = seed.len().min(MAX_IDENTIFIER_LEN - 1 - suffix.len());
    format!("{}_{}", &seed[..seed_len], suffix)
}

fn fully_qualified(kind: &str, id: &EntityId) -> String {
    let kind_len = kind.len().min(4);
    format!("{QUALIFIED_PREFIX}{}_{}", &kind[..kind_len], id_token(id))
}

/// Pick the storage name for `desired` (an already-[`sanitize`]d seed):
/// `desired` itself when free, else `{desired}_{trailing 4+ of id}`,
/// widening the suffix until `is_taken` clears. `is_taken` closes over the
/// engine's map and reserved names (fixed columns like `id`,
/// `state_buffer`, `head`, `attestations` count as taken).
pub fn dedupe(desired: &str, id: &EntityId, is_taken: impl Fn(&str) -> bool) -> String {
    if !is_taken(desired) {
        return desired.to_string();
    }
    for len in 4..=52 {
        let candidate = suffixed(desired, id, len);
        if !is_taken(&candidate) {
            return candidate;
        }
    }
    // A plain or previously shortened name can theoretically occupy every
    // friendly candidate. The reserved namespace plus the complete token
    // is injective by id, so correctly formed maps cannot collide here.
    fully_qualified("d", id)
}

/// Synthetic name for an id the resolver cannot name: `{prefix}_{suffix}`
/// through the same widening dedup. `prefix` is `p` for properties, `m`
/// for models, keeping the fallback visibly synthetic in raw data.
pub fn fallback(prefix: &str, id: &EntityId, is_taken: impl Fn(&str) -> bool) -> String {
    for len in 4..=52 {
        let candidate = suffixed(prefix, id, len);
        if !is_taken(&candidate) {
            return candidate;
        }
    }
    fully_qualified(prefix, id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        bytes[31] = byte.wrapping_add(1);
        EntityId::from_bytes(bytes)
    }

    #[test]
    fn sanitize_maps_invalid_chars_and_leading_digit() {
        assert_eq!(sanitize("title"), "title");
        assert_eq!(sanitize("my-field.x"), "my_field_x");
        assert_eq!(sanitize("9lives"), "_9lives");
        assert_eq!(sanitize(""), "_");
        assert_eq!(sanitize("__a_reserved"), "___a_reserved");
        assert_eq!(sanitize(&"x".repeat(80)).len(), MAX_IDENTIFIER_LEN);
    }

    #[test]
    fn dedupe_returns_desired_when_free() {
        assert_eq!(dedupe("title", &id(1), |_| false), "title");
    }

    #[test]
    fn dedupe_suffixes_with_trailing_id_chars_and_widens() {
        let property = id(1);
        let suffix4 = id_suffix(&property, 4);
        // Plain name taken -> first widening step.
        assert_eq!(dedupe("title", &property, |n| n == "title"), format!("title_{suffix4}"));
        // That taken too -> widens to 5.
        let candidate4 = format!("title_{suffix4}");
        let five = dedupe("title", &property, |n| n == "title" || n == candidate4);
        assert!(five.starts_with("title_") && five.len() > format!("title_{suffix4}").len());
    }

    #[test]
    fn long_common_suffixes_widen_to_an_injective_full_id_token() {
        let first = EntityId::from_bytes([0; 32]);
        let mut second_bytes = [0; 32];
        second_bytes[0] = 1;
        let second = EntityId::from_bytes(second_bytes);
        assert_eq!(id_token(&first).len(), 52);
        assert_ne!(id_token(&first), id_token(&second));

        let mut taken = std::collections::HashSet::from(["title".to_owned()]);
        for len in 4..=52 {
            taken.insert(suffixed("title", &first, len));
        }
        let assigned = dedupe("title", &second, |candidate| taken.contains(candidate));
        assert!(!taken.contains(&assigned));
        assert!(assigned.len() <= MAX_IDENTIFIER_LEN);
    }

    #[test]
    fn exhausted_friendly_names_use_the_reserved_full_id_namespace() {
        let assigned = fallback("p", &id(9), |candidate| !candidate.starts_with(QUALIFIED_PREFIX));
        assert!(assigned.starts_with("__a_p_"));
        assert!(assigned.len() <= MAX_IDENTIFIER_LEN);
    }

    #[test]
    fn fallback_is_visibly_synthetic() {
        let name = fallback("p", &id(7), |_| false);
        assert!(name.starts_with("p_") && name.len() >= 6, "got {name}");
    }
}
