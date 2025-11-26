use std::{convert::Infallible, future::Future};

use axum::{extract::FromRequestParts, http::request::Parts};
use axum_extra::{headers, TypedHeader};

pub struct OptionalUserAgent(pub Option<String>);

impl<S> FromRequestParts<S> for OptionalUserAgent
where S: Send + Sync
{
    type Rejection = Infallible;

    fn from_request_parts(parts: &mut Parts, state: &S) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        async move {
            let user_agent = TypedHeader::<headers::UserAgent>::from_request_parts(parts, state)
                .await
                .ok()
                .map(|TypedHeader(user_agent)| user_agent.to_string());

            Ok(Self(user_agent))
        }
    }
}
