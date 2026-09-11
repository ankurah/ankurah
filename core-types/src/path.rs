/// A query path's display components, before quoting or escaping.
pub trait Path {
    /// Names to render separately; these are labels, not durable property identities.
    fn display_steps(&self) -> impl Iterator<Item = &str>;
}
