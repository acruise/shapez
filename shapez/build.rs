fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = std::path::PathBuf::from("proto");
    let mut config = prost_build::Config::new();
    config.disable_comments(["."]);
    config.compile_protos(
        &[
            proto_root.join("report.proto"),
            proto_root.join("state.proto"),
        ],
        &[&proto_root],
    )?;
    println!("cargo:rerun-if-changed=proto/report.proto");
    println!("cargo:rerun-if-changed=proto/state.proto");
    Ok(())
}
