use crate::error::RetrievalError;
use ankql::ast::{Parsed, Stage};

/// Whether a query may answer from local storage before hearing from a durable peer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CachePolicy {
    /// Allow initial livequery results from local storage; remote subscription still proceeds.
    /// Fetch does not yet honor this preference and still waits for a durable answer.
    #[default]
    Local,
    /// Wait for a durable answer. A durable node answers from its own storage.
    Durable,
    /// Reserved for reusing previously completed durable answers from cache.
    /// Currently identical to Durable; completed-answer tracking is not implemented.
    /// No ledger is recorded or consulted. See https://github.com/ankurah/ankurah/issues/501.
    Tracked,
}

/// A staged selection and its local-cache preference.
pub struct MatchArgs<S: Stage> {
    pub selection: ankql::ast::Selection<S>,
    pub cache_policy: CachePolicy,
}

impl TryInto<MatchArgs<Parsed>> for &str {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<MatchArgs<Parsed>, Self::Error> {
        Ok(MatchArgs { selection: ankql::parser::parse_selection(self)?, cache_policy: CachePolicy::Local })
    }
}
impl TryInto<MatchArgs<Parsed>> for String {
    type Error = ankql::error::ParseError;
    fn try_into(self) -> Result<MatchArgs<Parsed>, Self::Error> {
        Ok(MatchArgs { selection: ankql::parser::parse_selection(&self)?, cache_policy: CachePolicy::Local })
    }
}

impl<S: Stage> From<ankql::ast::Predicate<S>> for MatchArgs<S> {
    fn from(val: ankql::ast::Predicate<S>) -> Self {
        MatchArgs { selection: ankql::ast::Selection { predicate: val, order_by: None, limit: None }, cache_policy: CachePolicy::Local }
    }
}

impl<S: Stage> From<ankql::ast::Selection<S>> for MatchArgs<S> {
    fn from(val: ankql::ast::Selection<S>) -> Self { MatchArgs { selection: val, cache_policy: CachePolicy::Local } }
}

impl From<ankql::error::ParseError> for RetrievalError {
    fn from(e: ankql::error::ParseError) -> Self { RetrievalError::ParseError(e) }
}

pub fn nocache<T: TryInto<ankql::ast::Selection<Parsed>, Error = ankql::error::ParseError>>(
    s: T,
) -> Result<MatchArgs<Parsed>, ankql::error::ParseError> {
    MatchArgs::nocache(s)
}

impl MatchArgs<Parsed> {
    pub fn nocache<T>(s: T) -> Result<Self, ankql::error::ParseError>
    where T: TryInto<ankql::ast::Selection<Parsed>, Error = ankql::error::ParseError> {
        Ok(Self { selection: s.try_into()?, cache_policy: CachePolicy::Durable })
    }
}
