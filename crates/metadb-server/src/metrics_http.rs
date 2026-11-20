//! Prometheus HTTP listener. Uses the same bearer as the gRPC services.

use std::sync::{Arc, Weak};
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

use metadb_security::TransportSecurity;

use crate::bearer_header_matches;
use crate::classify_secret;
use crate::operability::{health_body, live_body, ready_body, unavailable_report, ReadinessReport};
use crate::render_prometheus_metrics;
use crate::PersistentMetaDbCluster;
use crate::SecretMaterial;

const METRICS_MAX_CONNECTIONS: usize = 64;
const METRICS_MAX_HEADER_BYTES: usize = 8192;
const METRICS_IO_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn serve(
    listener: TcpListener,
    cluster: Weak<PersistentMetaDbCluster>,
    shared_secret: String,
    mut shutdown: watch::Receiver<bool>,
    security: Arc<TransportSecurity>,
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
                let security = Arc::clone(&security);
                connections.spawn(async move {
                    let _ = tokio::time::timeout(
                        METRICS_IO_TIMEOUT,
                        metrics_connection(socket, cluster, shared_secret, &security),
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
    security: &TransportSecurity,
) -> Result<(), hyper::Error> {
    if !security.is_plaintext() {
        tracing::error!("refusing metrics: TLS termination is not available");
        return Ok(());
    }
    serve_metrics(socket, cluster, shared_secret).await
}

async fn serve_metrics(
    socket: TcpStream,
    cluster: Weak<PersistentMetaDbCluster>,
    shared_secret: String,
) -> Result<(), hyper::Error> {
    let service = service_fn(move |request: Request<Incoming>| {
        let cluster = cluster.clone();
        let shared_secret = shared_secret.clone();
        async move {
            let authorization = request
                .headers()
                .get(hyper::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok());
            let principal = request
                .headers()
                .get(metadb_security::PRINCIPAL_HEADER)
                .and_then(|value| value.to_str().ok());
            let credential = request
                .headers()
                .get(metadb_security::CREDENTIAL_HEADER)
                .and_then(|value| value.to_str().ok());
            let (status, body, prometheus) = metrics_decision(
                request.method(),
                request.uri().path(),
                authorization,
                principal,
                credential,
                &cluster,
                &shared_secret,
            );
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
    method: &Method,
    path: &str,
    authorization: Option<&str>,
    principal: Option<&str>,
    credential: Option<&str>,
    cluster: &Weak<PersistentMetaDbCluster>,
    shared_secret: &str,
) -> (StatusCode, String, bool) {
    // Whitespace is not a credential. Do not fall through to an exact bearer
    // compare: `Authorization: Bearer <spaces>` must still be 401.
    match classify_secret(shared_secret) {
        SecretMaterial::Absent => {}
        SecretMaterial::WhitespaceOnly => {
            return (StatusCode::UNAUTHORIZED, "unauthorized\n".to_owned(), false);
        }
        SecretMaterial::Configured => {
            if !bearer_header_matches(authorization.unwrap_or(""), shared_secret) {
                return (StatusCode::UNAUTHORIZED, "unauthorized\n".to_owned(), false);
            }
        }
    }
    if method != Method::GET {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed\n".to_owned(),
            false,
        );
    }
    let Some(cluster) = cluster.upgrade() else {
        return route_probe(path, None, "");
    };
    if let Some(roles) = cluster.roles() {
        let authenticated = roles
            .authenticate(principal.unwrap_or(""), credential.unwrap_or(""))
            .is_ok();
        if !authenticated {
            return (
                StatusCode::FORBIDDEN,
                "authorization denied\n".to_owned(),
                false,
            );
        }
        if let Some(method_name) = http_probe_method(path) {
            if roles
                .authorize(
                    &metadb_security::AuthenticatedPrincipal::from_name(principal.unwrap_or("")),
                    "metadb.http",
                    method_name,
                )
                .is_err()
            {
                return (
                    StatusCode::FORBIDDEN,
                    "authorization denied\n".to_owned(),
                    false,
                );
            }
        }
    }
    let report = cluster.readiness();
    let metrics = render_prometheus_metrics(&cluster.metrics_snapshot());
    route_probe(path, Some(&report), &metrics)
}

fn http_probe_method(path: &str) -> Option<&'static str> {
    match path {
        "/metrics" => Some("GetMetrics"),
        "/livez" => Some("GetLivez"),
        "/readyz" => Some("GetReadyz"),
        "/healthz" => Some("GetHealthz"),
        _ => None,
    }
}

/// Routes an authenticated GET. `/metrics` stays 200 while the process is not
/// ready so a scrape can see `metadb_ready 0`. `/healthz` and `/readyz` do not.
fn route_probe(
    path: &str,
    report: Option<&ReadinessReport>,
    metrics: &str,
) -> (StatusCode, String, bool) {
    let Some(report) = report else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not live: {}\n", unavailable_report().reason),
            false,
        );
    };
    match path {
        "/metrics" => (StatusCode::OK, metrics.to_owned(), true),
        "/livez" => {
            let status = if report.live {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (status, live_body(report), false)
        }
        "/readyz" => {
            let status = if report.ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (status, ready_body(report), false)
        }
        "/healthz" => {
            let status = if report.ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (status, health_body(report), false)
        }
        _ => (StatusCode::NOT_FOUND, "not found\n".to_owned(), false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operability::{evaluate_readiness, ProcessPhase, ShardGate};
    use std::sync::Weak;

    #[test]
    fn probes_do_not_echo_tokens_or_treat_a_bound_listener_as_ready() {
        let secret = "super-secret-token";
        let (status, body, prometheus) = metrics_decision(
            &Method::GET,
            "/readyz",
            Some("Bearer wrong-token"),
            None,
            None,
            &Weak::new(),
            secret,
        );
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body, "unauthorized\n");
        assert!(!prometheus);
        assert!(!body.contains(secret));
        assert!(!body.contains("wrong-token"));

        let (status, body, _) = metrics_decision(
            &Method::GET,
            "/healthz",
            Some(&format!("Bearer {secret}")),
            None,
            None,
            &Weak::new(),
            secret,
        );
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "not live: unavailable\n");
        assert!(!body.contains(secret));

        let starting = evaluate_readiness(ProcessPhase::Starting, &[ShardGate::servable()]);
        let (ready_status, ready_body, ready_prom) =
            route_probe("/readyz", Some(&starting), "metrics");
        assert_eq!(ready_status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(ready_body, "not ready: starting\n");
        assert!(!ready_prom);

        let (health_status, health_text, _) = route_probe("/healthz", Some(&starting), "metrics");
        assert_eq!(health_status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(health_text.contains("ready=0"));
        assert!(health_text.contains("live=1"));

        let (live_status, live_text, _) = route_probe("/livez", Some(&starting), "metrics");
        assert_eq!(live_status, StatusCode::OK);
        assert_eq!(live_text, "live\n");

        let (metrics_status, metrics_body, metrics_prom) =
            route_probe("/metrics", Some(&starting), "metadb_ready 0\n");
        assert_eq!(metrics_status, StatusCode::OK);
        assert!(metrics_prom);
        assert_eq!(metrics_body, "metadb_ready 0\n");

        let serving = evaluate_readiness(ProcessPhase::Serving, &[ShardGate::servable()]);
        let (status, body, _) = route_probe("/readyz", Some(&serving), "");
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ready\n");
        let (status, _, _) = route_probe("/healthz", Some(&serving), "");
        assert_eq!(status, StatusCode::OK);

        let (status, body, prom) = route_probe("/status", Some(&serving), "secret-metrics");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, "not found\n");
        assert!(!prom);
        assert!(!body.contains("secret-metrics"));
    }

    #[test]
    fn whitespace_secret_rejects_probes_even_when_the_bearer_matches() {
        let secret = " \t ";
        let exact = format!("Bearer {secret}");
        for path in ["/metrics", "/livez", "/readyz", "/healthz"] {
            for presented in [None, Some(exact.as_str()), Some("Bearer secret")] {
                let (status, body, prometheus) = metrics_decision(
                    &Method::GET,
                    path,
                    presented,
                    None,
                    None,
                    &Weak::new(),
                    secret,
                );
                assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} {presented:?}");
                assert_eq!(body, "unauthorized\n");
                assert!(!prometheus);
                assert!(!body.contains("Bearer"));
            }
        }
    }

    #[test]
    fn default_empty_config_is_not_an_opt_in_and_absent_secret_does_not_challenge_probes() {
        use crate::config::ServerTomlConfig;

        let cfg = ServerTomlConfig::from_toml_str(
            r#"
            [node]
            metrics_listen_addr = "127.0.0.1:9100"
            [raft]
            heartbeat_interval_ms = 80
            election_timeout_min_ms = 250
            election_timeout_max_ms = 450
            "#,
        )
        .unwrap();
        assert!(cfg.security.shared_secret.is_empty());
        assert!(!cfg.security.allow_unauthenticated);
        assert_eq!(cfg.raft_options.heartbeat_interval_ms, 80);
        let startup = cfg
            .reject_unauthenticated_bind("127.0.0.1:9100")
            .unwrap_err()
            .to_string();
        assert!(
            startup.contains("allow_unauthenticated"),
            "default empty secret must not start: {startup}"
        );

        let whitespace = ServerTomlConfig::from_toml_str(
            r#"
            [security]
            shared_secret = "   "
            allow_unauthenticated = true
            "#,
        )
        .unwrap();
        let whitespace_err = whitespace
            .security
            .validate_for_startup()
            .unwrap_err()
            .to_string();
        assert!(
            whitespace_err.contains("whitespace-only"),
            "{whitespace_err}"
        );

        let (status, body, _) =
            metrics_decision(&Method::GET, "/readyz", None, None, None, &Weak::new(), "");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "not live: unavailable\n");
        assert!(!body.contains("unauthorized"));
    }
}
