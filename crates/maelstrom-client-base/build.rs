use std::path::PathBuf;
use std::process::Command;
use std::str::from_utf8;

const INTO_RESULT: [&str; 3] = ["RunJobRequest", "AddLayerRequest", "AddLayerResponse"];

const ENUM_PROTO: [(&str, &str); 4] = [
    ("JobDevice", "maelstrom_base::JobDevice"),
    ("JobMountFsType", "maelstrom_base::JobMountFsType"),
    ("ArtifactType", "maelstrom_base::ArtifactType"),
    ("JobCompleted.status", "maelstrom_base::JobStatus"),
];

const MSG_PROTO: [(&str, &str, &str); 2] = [
    ("JobMount", "maelstrom_base::JobMount", ""),
    ("JobEffects", "maelstrom_base::JobEffects", "option_all"),
];

fn test_for_protoc() -> Option<PathBuf> {
    if let Ok(o) = Command::new("protoc").arg("--version").output() {
        if let Ok(s) = from_utf8(&o.stdout[..]).map(str::trim) {
            // Looks like `libprotoc 24.1`
            if let Some(version) = s.rsplit(' ').next() {
                if let Some(version) = versions::Versioning::new(version) {
                    let requirement = versions::Requirement::new("^24.0").unwrap();
                    if requirement.matches(&version) {
                        return None;
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

    let mut b = tonic_build::configure();
    for resp in INTO_RESULT {
        b = b.message_attribute(resp, "#[derive(maelstrom_macro::IntoResult)]");
    }

    for (name, other_type) in ENUM_PROTO {
        b = b.enum_attribute(
            name,
            format!(
                "#[derive(maelstrom_macro::IntoProtoBuf, maelstrom_macro::TryFromProtoBuf)] \
                 #[proto(other_type = {other_type}, remote)]"
            ),
        );
    }

    for (name, other_type, extra) in MSG_PROTO {
        b = b.message_attribute(
            name,
            format!(
                "#[derive(maelstrom_macro::IntoProtoBuf, maelstrom_macro::TryFromProtoBuf)] \
                 #[proto(other_type = {other_type}, remote, {extra})]"
            ),
        );
    }

    b = b.btree_map(["EnvironmentSpec.vars"]);

    b.compile(&["src/items.proto"], &["src/"]).unwrap();
}
