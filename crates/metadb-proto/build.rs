use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        let homebrew_protoc = Path::new("/opt/homebrew/bin/protoc");
        if homebrew_protoc.exists() {
            unsafe {
                std::env::set_var("PROTOC", homebrew_protoc);
            }
        }
    }

    let protos = [
        "proto/metadb/v1/common.proto",
        "proto/metadb/v1/metadata.proto",
        "proto/metadb/v1/transaction.proto",
        "proto/metadb/v1/raft.proto",
        "proto/metadb/v1/control_plane.proto",
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
