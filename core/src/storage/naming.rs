use ankurah_proto::EntityId;

/// Sanitize a display name into a storage identifier seed: every character
/// outside `[A-Za-z0-9_]` becomes `_`, and a leading digit is prefixed with
/// `_`. Engines still quote identifiers; this keeps naming consistent across
/// storage implementations.
pub fn sanitize(name: &str) -> String {
    let mut out: String = name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
    if out.is_empty() || out.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

/// The trailing base64 characters used to disambiguate physical names.
fn id_suffix(id: &EntityId, len: usize) -> String {
    let base64 = id.to_base64();
    let clean: Vec<char> = base64.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    let start = clean.len().saturating_sub(len);
    clean[start..].iter().collect()
}

/// Pick a stable storage name for `desired`, widening an id-derived suffix
/// until the candidate is free.
pub fn dedupe(desired: &str, id: &EntityId, is_taken: impl Fn(&str) -> bool) -> String {
    if !is_taken(desired) {
        return desired.to_string();
    }
    for len in 4..=22 {
        let candidate = format!("{}_{}", desired, id_suffix(id, len));
        if !is_taken(&candidate) {
            return candidate;
        }
    }
    format!("{}_{}", desired, id_suffix(id, 22))
}

/// Synthetic physical name for an id whose catalog display name is not yet
/// available.
pub fn fallback(prefix: &str, id: &EntityId, is_taken: impl Fn(&str) -> bool) -> String {
    for len in 4..=22 {
        let candidate = format!("{}_{}", prefix, id_suffix(id, len));
        if !is_taken(&candidate) {
            return candidate;
        }
    }
    format!("{}_{}", prefix, id_suffix(id, 22))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> EntityId {
        let mut bytes = [0u8; 16];
        bytes[0] = byte;
        bytes[15] = byte.wrapping_add(1);
        EntityId::from_bytes(bytes)
    }

    #[test]
    fn sanitize_maps_invalid_chars_and_leading_digit() {
        assert_eq!(sanitize("title"), "title");
        assert_eq!(sanitize("my-field.x"), "my_field_x");
        assert_eq!(sanitize("9lives"), "_9lives");
        assert_eq!(sanitize(""), "_");
    }

    #[test]
    fn dedupe_returns_desired_when_free() {
        assert_eq!(dedupe("title", &id(1), |_| false), "title");
    }

    #[test]
    fn dedupe_suffixes_with_trailing_id_chars_and_widens() {
        let property = id(1);
        let clean: String = property.to_base64().chars().filter(|c| c.is_ascii_alphanumeric()).collect();
        let suffix4 = clean[clean.len() - 4..].to_string();
        assert_eq!(dedupe("title", &property, |n| n == "title"), format!("title_{suffix4}"));
        let five = dedupe("title", &property, |n| n == "title" || n.ends_with(&suffix4));
        assert!(five.starts_with("title_") && five.len() > format!("title_{suffix4}").len());
    }

    #[test]
    fn fallback_is_visibly_synthetic() {
        let name = fallback("p", &id(7), |_| false);
        assert!(name.starts_with("p_") && name.len() >= 6, "got {name}");
    }
}
