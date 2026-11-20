//! Installed method grants. The shared bearer is still required and is not a role.
//! TLS remains unavailable.

use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::{CreateFileRequest, GetRoutesRequest, LookupRequest};
use metadb_server::config::{RbacGrantConfig, RbacSection};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use tonic::Code;

fn roles() -> RbacSection {
    RbacSection {
        local_principal: "node".into(),
        grants: vec![
            RbacGrantConfig {
                principal: "node".into(),
                credential: "node-credential".into(),
                allow: vec![
                    "metadb.v1.ControlPlaneService/GetRoutes".into(),
                    "metadb.v1.ControlPlaneService/StoreHeartbeat".into(),
                    "metadb.v1.RaftTransportService/AppendEntries".into(),
                    "metadb.v1.RaftTransportService/Vote".into(),
                    "metadb.v1.RaftTransportService/ClientWrite".into(),
                    "metadb.v1.MetadataService/Lookup".into(),
                    "metadb.v1.MetadataService/CreateFile".into(),
                ],
            },
            RbacGrantConfig {
                principal: "reader".into(),
                credential: "reader-credential".into(),
                allow: vec![
                    "metadb.v1.ControlPlaneService/GetRoutes".into(),
                    "metadb.v1.MetadataService/Lookup".into(),
                    "metadb.http/GetMetrics".into(),
                    "metadb.http/GetReadyz".into(),
                ],
            },
        ],
    }
}

fn request_headers(
    req: &mut tonic::Request<impl Sized>,
    bearer: &str,
    principal: &str,
    credential: &str,
) {
    req.metadata_mut()
        .insert("authorization", format!("Bearer {bearer}").parse().unwrap());
    req.metadata_mut().insert(
        metadb_security::PRINCIPAL_HEADER,
        principal.parse().unwrap(),
    );
    let mut credential = credential
        .parse::<tonic::metadata::MetadataValue<_>>()
        .unwrap();
    credential.set_sensitive(true);
    req.metadata_mut()
        .insert(metadb_security::CREDENTIAL_HEADER, credential);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_reader_can_read_and_cannot_create() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.node.metrics_listen_addr = Some("127.0.0.1:0".into());
    cfg.security.shared_secret = "test-secret".into();
    cfg.security.rbac = Some(roles());
    let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(cfg)
        .await
        .expect("rbac server");
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    let mut routes = tonic::Request::new(GetRoutesRequest { cell_id: 1 });
    request_headers(&mut routes, "test-secret", "reader", "reader-credential");
    ControlPlaneServiceClient::new(channel.clone())
        .get_routes(routes)
        .await
        .expect("reader may call GetRoutes");

    let mut lookup = tonic::Request::new(LookupRequest {
        pid: 1,
        name: "missing".into(),
        ..Default::default()
    });
    request_headers(&mut lookup, "test-secret", "reader", "reader-credential");
    match MetadataServiceClient::new(channel.clone())
        .lookup(lookup)
        .await
    {
        Ok(_) => {}
        Err(error) => assert_ne!(
            error.code(),
            Code::PermissionDenied,
            "a granted lookup must not be an authorization denial: {error}"
        ),
    }

    let mut create = tonic::Request::new(CreateFileRequest {
        header: None,
        pid: 1,
        name: "nope".into(),
        inode_id: 0,
        fid: 0,
        uid: 0,
        mode: 0,
        size_bytes: 0,
    });
    request_headers(&mut create, "test-secret", "reader", "reader-credential");
    let denied = MetadataServiceClient::new(channel.clone())
        .create_file(create)
        .await
        .expect_err("reader cannot create");
    assert_eq!(denied.code(), Code::PermissionDenied);
    assert!(!denied.to_string().contains("reader-credential"));
    assert!(!denied.to_string().contains("test-secret"));

    let mut bare = tonic::Request::new(GetRoutesRequest { cell_id: 1 });
    request_headers(&mut bare, "", "reader", "reader-credential");
    bare.metadata_mut().remove("authorization");
    let unauthenticated = ControlPlaneServiceClient::new(channel)
        .get_routes(bare)
        .await
        .expect_err("role headers do not replace the bearer");
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    let metrics = cluster.metrics_addr().expect("metrics").to_string();
    let denied_metrics = http(&metrics, "test-secret", None).await;
    assert!(denied_metrics.contains("403"), "{denied_metrics}");
    let ready = http(
        &metrics,
        "test-secret",
        Some(("reader", "reader-credential")),
    )
    .await;
    assert!(ready.contains("200"), "{ready}");

    cluster.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shared_secret_cannot_be_installed_as_a_role_and_tls_still_fails_closed() {
    let parent = tempfile::tempdir().unwrap();
    let data = parent.path().join("not-created");
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = data.clone();
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.shared_secret = "test-secret".into();
    cfg.security.rbac = Some(RbacSection {
        local_principal: "node".into(),
        grants: vec![RbacGrantConfig {
            principal: "node".into(),
            credential: "test-secret".into(),
            allow: vec!["metadb.v1.ControlPlaneService/GetRoutes".into()],
        }],
    });
    let error = match PersistentMetaDbCluster::open_and_serve(cfg).await {
        Ok(_) => panic!("shared secret is not a role"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("not a role"), "{error}");
    assert!(!error.contains("test-secret"), "{error}");
    assert!(!data.exists());

    let mut tls = ServerTomlConfig::default();
    tls.node.data_dir = data.clone();
    tls.node.grpc_listen_addr = "127.0.0.1:0".into();
    tls.security.shared_secret = "test-secret".into();
    tls.security.tls.mode = "mtls".into();
    tls.security.tls.cert_file = "cert.pem".into();
    tls.security.tls.key_file = parent.path().join("key.pem").display().to_string();
    std::fs::write(&tls.security.tls.key_file, "SUPER-SECRET-KEY-MATERIAL").unwrap();
    tls.security.tls.ca_file = "ca.pem".into();
    tls.security.tls.server_name = "127.0.0.1".into();
    let error = match PersistentMetaDbCluster::open_and_serve(tls).await {
        Ok(_) => panic!("tls stays unavailable"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("does not terminate TLS"), "{error}");
    assert!(!error.contains("SUPER-SECRET-KEY-MATERIAL"), "{error}");
    assert!(!data.exists());
}

async fn http(addr: &str, bearer: &str, role: Option<(&str, &str)>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut request =
        format!("GET /readyz HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {bearer}\r\n");
    if let Some((principal, credential)) = role {
        request.push_str(&format!(
            "x-metadb-principal: {principal}\r\nx-metadb-credential: {credential}\r\n"
        ));
    }
    request.push_str("Connection: close\r\n\r\n");
    socket.write_all(request.as_bytes()).await.unwrap();
    let mut body = Vec::new();
    socket.read_to_end(&mut body).await.unwrap();
    String::from_utf8_lossy(&body).into_owned()
}
