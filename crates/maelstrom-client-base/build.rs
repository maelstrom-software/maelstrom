use std::path::PathBuf;
use std::process::Command;
use std::str::from_utf8;

fn test_for_protoc() -> Option<PathBuf> {
    if let Ok(o) = Command::new("protoc").arg("--version").output() {
        if let Ok(s) = from_utf8(&o.stdout[..]).map(str::trim) {
            // Looks like `libprotoc 24.1`
            if let Some(version) = s.rsplit(' ').next() {
                if let Some(version) = versions::Versioning::new(version) {
                    let requirement = versions::Requirement::new(">=24.0").unwrap();
                    if requirement.matches(&version) {
                        return None;
                    } else {
                        println!("ignoring old protoc version {s:?}");
                    }
                }
            }
        }
    }

    protoc_bin_vendored::protoc_bin_path().ok()
}

fn main() {
    if let Some(protoc_path) = test_for_protoc() {
        println!("protoc_path = {}", protoc_path.display());
        std::env::set_var("PROTOC", protoc_path);
    }

    tonic_build::configure()
        .btree_map(["EnvironmentSpec.vars"])
        .type_attribute(".", "#[derive(maelstrom_macro::ProtoBufExt)]")
        .compile(&["src/items.proto"], &["src/"])
        .unwrap();
}
