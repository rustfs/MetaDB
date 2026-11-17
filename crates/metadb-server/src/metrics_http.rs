//! Prometheus HTTP listener. Uses the same bearer as the gRPC services.

use std::sync::Weak;
use std::time::Duration;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::bearer_header_matches;
use crate::render_prometheus_metrics;
use crate::PersistentMetaDbCluster;

const METRICS_MAX_CONNECTIONS: usize = 64;
const METRICS_MAX_HEADER_BYTES: usize = 8192;
const METRICS_IO_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn serve(
    listener: TcpListener,
    cluster: Weak<PersistentMetaDbCluster>,
    shared_secret: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept(), if connections.len() < METRICS_MAX_CONNECTIONS => {
                let Ok((socket, _)) = accepted else { break; };
                let cluster = cluster.clone();
                let shared_secret = shared_secret.clone();
                connections.spawn(async move {
                    let _ = tokio::time::timeout(
                        METRICS_IO_TIMEOUT,
                        metrics_connection(socket, cluster, shared_secret),
                    )
                    .await;
                });
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

async fn metrics_connection(
    socket: TcpStream,
    cluster: Weak<PersistentMetaDbCluster>,
    shared_secret: String,
) -> Result<(), hyper::Error> {
    let service = service_fn(move |request: Request<Incoming>| {
        let cluster = cluster.clone();
        let shared_secret = shared_secret.clone();
        async move {
            let (status, body, prometheus) = metrics_decision(&request, &cluster, &shared_secret);
            let mut response = Response::new(Full::new(Bytes::from(body)));
            *response.status_mut() = status;
            let content_type = if prometheus {
                "text/plain; version=0.0.4"
            } else {
                "text/plain; charset=utf-8"
            };
            response.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static(content_type),
            );
            if status == StatusCode::UNAUTHORIZED {
                response.headers_mut().insert(
                    hyper::header::WWW_AUTHENTICATE,
                    hyper::header::HeaderValue::from_static("Bearer"),
                );
            }
            response.headers_mut().insert(
                hyper::header::CONNECTION,
                hyper::header::HeaderValue::from_static("close"),
            );
            Ok::<_, std::convert::Infallible>(response)
        }
    });
    http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(METRICS_IO_TIMEOUT)
        .max_headers(64)
        .max_buf_size(METRICS_MAX_HEADER_BYTES)
        .keep_alive(false)
        .serve_connection(TokioIo::new(socket), service)
        .await
}

fn metrics_decision(
    request: &Request<Incoming>,
    cluster: &Weak<PersistentMetaDbCluster>,
    shared_secret: &str,
) -> (StatusCode, String, bool) {
    if !shared_secret.is_empty() {
        let presented = request
            .headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        if !bearer_header_matches(presented, shared_secret) {
            return (StatusCode::UNAUTHORIZED, "unauthorized\n".to_owned(), false);
        }
    }
    if request.method() != Method::GET {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed\n".to_owned(),
            false,
        );
    }
    if request.uri().path() != "/metrics" {
        return (StatusCode::NOT_FOUND, "not found\n".to_owned(), false);
    }
    let Some(cluster) = cluster.upgrade() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable\n".to_owned(),
            false,
        );
    };
    (
        StatusCode::OK,
        render_prometheus_metrics(&cluster.metrics_snapshot()),
        true,
    )
}
