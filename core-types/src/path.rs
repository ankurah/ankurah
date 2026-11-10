/// A query path's display components, before quoting or escaping.
pub trait Path {
    /// The component a parsed path names before `::`; resolved paths carry the property's identity instead.
    fn display_component(&self) -> Option<&str> { None }

    /// Names to render separately; these are labels, not durable property identities.
    fn display_steps(&self) -> impl Iterator<Item = &str>;
}
