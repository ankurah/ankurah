//! The publishers Ankurah trusts by default: identities whose published
//! component and property definitions a node may adopt.

use ankurah_proto::{DecodeError, Identity};
use std::sync::OnceLock;

/// The shipped list: one identity per line in its text form, `#` starting a comment.
const SHIPPED: &str = include_str!("trusted_publishers.txt");

/// The identities [`SHIPPED`] lists, parsed once.
pub(crate) fn shipped() -> Vec<Identity> {
    static PARSED: OnceLock<Vec<Identity>> = OnceLock::new();
    // Cannot fail in practice: the shipped file is parsed by `the_shipped_file_parses` below.
    PARSED.get_or_init(|| parse(SHIPPED).expect("the shipped trusted-publisher list parses")).clone()
}

/// Parse a trusted-publisher list: one identity per line in its text form; blank lines and `#` comments are skipped.
fn parse(text: &str) -> Result<Vec<Identity>, DecodeError> {
    text.lines().map(|line| line.split('#').next().unwrap_or_default().trim()).filter(|line| !line.is_empty()).map(str::parse).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_file_parses() { parse(SHIPPED).expect("the shipped trusted-publisher list must parse"); }

    #[test]
    fn the_default_policy_trusts_the_shipped_list() {
        use crate::policy::{PermissiveAgent, PolicyAgent};
        assert_eq!(PermissiveAgent::new().trusted_publishers(), parse(SHIPPED).unwrap());
    }

    #[test]
    fn a_list_skips_comments_and_blank_lines() {
        let text = "# the Commons\n\ned25519:BQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQU  # trailing comment\n";
        assert_eq!(parse(text).unwrap(), vec![Identity::Ed25519([5; 32])]);
        assert!(
            matches!(parse("BQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQU"), Err(DecodeError::MissingScheme)),
            "a line must name its scheme"
        );
        assert!(parse("ed25519:not base64 at all!").is_err());
    }
}
