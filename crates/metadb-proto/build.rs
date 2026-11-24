use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        for candidate in ["/opt/homebrew/bin/protoc", "/usr/local/bin/protoc"] {
            let protoc_path = Path::new(candidate);
            if protoc_path.exists() {
                unsafe {
                    std::env::set_var("PROTOC", protoc_path);
                }
                break;
            }
        }
    }

    let protos = [
        "proto/metadb/v1/common.proto",
        "proto/metadb/v1/metadata.proto",
        "proto/metadb/v1/transaction.proto",
        "proto/metadb/v1/raft.proto",
        "proto/metadb/v1/control_plane.proto",
        "proto/grpc/health/v1/health.proto",
    ];
    for p in &protos {
        println!("cargo:rerun-if-changed={}", p);
    }

    tonic_prost_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize)]")
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &["proto"])?;

    Ok(())
}
