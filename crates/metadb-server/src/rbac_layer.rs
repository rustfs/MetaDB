//! gRPC method check. The layer sees the HTTP path, which the bearer interceptor does not.
//! Bearer failure stays unauthenticated. A role denial is permission denied.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use hyper::http::{HeaderMap, Request, Response};
use metadb_security::{InstalledRoles, CREDENTIAL_HEADER, PRINCIPAL_HEADER};
use tonic::Status;
use tower::{Layer, Service};

use crate::bearer_header_matches;
use crate::classify_secret;
use crate::SecretMaterial;

#[derive(Clone)]
pub struct RbacLayer {
    secret: String,
    roles: Option<Arc<InstalledRoles>>,
}

impl RbacLayer {
    pub fn new(secret: impl Into<String>, roles: Option<Arc<InstalledRoles>>) -> Self {
        Self {
            secret: secret.into(),
            roles,
        }
    }
}

impl<S> Layer<S> for RbacLayer {
    type Service = RbacService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RbacService {
            inner,
            secret: self.secret.clone(),
            roles: self.roles.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RbacService<S> {
    inner: S,
    secret: String,
    roles: Option<Arc<InstalledRoles>>,
}

impl<S, B> Service<Request<B>> for RbacService<S>
where
    S: Service<
            Request<B>,
            Response = Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        if let Some(roles) = &self.roles {
            if let Err(status) = check_request(&self.secret, roles, req.headers(), req.uri().path())
            {
                return Box::pin(async move { Ok(status.into_http()) });
            }
        }
        let mut inner = self.inner.clone();
        Box::pin(async move { inner.call(req).await })
    }
}

fn check_request(
    secret: &str,
    roles: &InstalledRoles,
    headers: &HeaderMap,
    path: &str,
) -> Result<(), Status> {
    match classify_secret(secret) {
        SecretMaterial::Absent => {}
        SecretMaterial::WhitespaceOnly => {
            return Err(Status::unauthenticated(
                "whitespace-only shared secret is not accepted",
            ));
        }
        SecretMaterial::Configured => {
            let presented = headers
                .get(hyper::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            if !bearer_header_matches(presented, secret) {
                return Err(Status::unauthenticated(
                    "missing or invalid authorization bearer token",
                ));
            }
        }
    }
    let principal = header_text(headers, PRINCIPAL_HEADER);
    let credential = header_text(headers, CREDENTIAL_HEADER);
    let Some((service, method)) = split_grpc_path(path) else {
        return Err(Status::permission_denied("authorization denied"));
    };
    roles
        .authenticate_and_authorize(principal, credential, service, method)
        .map(|_| ())
        .map_err(|_| Status::permission_denied("authorization denied"))
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
}

fn split_grpc_path(path: &str) -> Option<(&str, &str)> {
    let path = path.strip_prefix('/')?;
    let (service, method) = path.split_once('/')?;
    if service.is_empty() || method.is_empty() || method.contains('/') {
        return None;
    }
    Some((service, method))
}
