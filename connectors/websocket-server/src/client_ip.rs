use std::net::{IpAddr, SocketAddr};

use axum::{
    extract::{ConnectInfo, FromRequestParts},
    http::{request::Parts, HeaderMap, HeaderName, StatusCode},
    response::{IntoResponse, Response},
};
use forwarded_header_value::{ForwardedHeaderValue, Identifier};

pub enum Rejection {
    NoClientIpFound,
}

impl IntoResponse for Rejection {
    fn into_response(self) -> Response { (StatusCode::BAD_REQUEST, "Could not determine the client ip from the request").into_response() }
}

pub struct SmartClientIp(pub IpAddr);

impl<S> FromRequestParts<S> for SmartClientIp
where S: Sync
{
    type Rejection = Rejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // info!("SmartClientIp Headers: {:?}", parts.headers);
        ip_from_rightmost_forwarded_header(&parts.headers)
            .or(ip_from_cf_connecting_ip_header(&parts.headers))
            .or(ip_from_x_real_ip_header(&parts.headers))
            .or(ip_from_x_forwarded_for_header(&parts.headers))
            .or(ip_from_connect_info(&parts))
            .ok_or(Rejection::NoClientIpFound)
            .map(Self)
    }
}

fn ip_from_connect_info(parts: &Parts) -> Option<IpAddr> {
    parts.extensions.get::<ConnectInfo<SocketAddr>>().map(|ConnectInfo(addr)| addr.ip())
}

fn last_header_value(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    // TODO maybe return &str for performance.
    headers.get_all(name).into_iter().last().map(|s| s.to_str().ok().map(|f| f.to_owned())).flatten()
}

const FORWARDED_HEADER_NAME: HeaderName = HeaderName::from_static("forwarded");

fn ip_from_rightmost_forwarded_header(headers: &HeaderMap) -> Option<IpAddr> {
    last_header_value(headers, &FORWARDED_HEADER_NAME)
        .map(|header_value| {
            ForwardedHeaderValue::from_forwarded(header_value.as_str())
                .map(|v| v.iter().last().map(|s| s.forwarded_for.clone()).flatten())
                .ok()
                .flatten()
                .map(|forwarded_for| match forwarded_for {
                    Identifier::SocketAddr(a) => Some(a.ip()),
                    Identifier::IpAddr(ip) => Some(ip),
                    _ => None,
                })
                .flatten()
        })
        .flatten()
}

const X_FORWARDED_FOR_HEADER_NAME: HeaderName = HeaderName::from_static("x-forwarded-for");
fn ip_from_x_forwarded_for_header(headers: &HeaderMap) -> Option<IpAddr> {
    last_header_value(headers, &X_FORWARDED_FOR_HEADER_NAME)
        .map(|header_value| header_value.split(',').last().map(|v| v.trim().parse::<IpAddr>().ok()).flatten())
        .flatten()
}

fn ip_from_header_value(headers: &HeaderMap, header_name: &HeaderName) -> Option<IpAddr> {
    last_header_value(headers, header_name).map(|v| v.clone().trim().parse::<IpAddr>().ok()).flatten()
}

const X_REAL_IP_HEADER_NAME: HeaderName = HeaderName::from_static("x-real-ip");
fn ip_from_x_real_ip_header(headers: &HeaderMap) -> Option<IpAddr> { ip_from_header_value(headers, &X_REAL_IP_HEADER_NAME) }

const CF_CONNECTING_IP_HEADER_NAME: HeaderName = HeaderName::from_static("cf-connecting-ip");
fn ip_from_cf_connecting_ip_header(headers: &HeaderMap) -> Option<IpAddr> { ip_from_header_value(headers, &CF_CONNECTING_IP_HEADER_NAME) }
