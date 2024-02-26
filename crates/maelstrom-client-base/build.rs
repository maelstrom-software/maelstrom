const INTO_RESULT: [&str; 7] = [
    "AddArtifactResponse",
    "AddJobRequest",
    "AddLayerRequest",
    "AddLayerResponse",
    "GetArtifactUploadProgressResponse",
    "GetContainerImageResponse",
    "GetJobStateCountsResponse",
];

const ENUM_PROTO: [(&str, &str); 4] = [
    ("JobDevice", "maelstrom_base::JobDevice"),
    ("JobMountFsType", "maelstrom_base::JobMountFsType"),
    ("ArtifactType", "maelstrom_base::ArtifactType"),
    ("JobOutcomeCompleted.status", "maelstrom_base::JobStatus"),
];

const MSG_PROTO: [(&str, &str, &str); 7] = [
    ("JobMount", "maelstrom_base::JobMount", ""),
    ("JobSpec", "maelstrom_base::JobSpec", ""),
    ("ContainerImage", "maelstrom_container::ContainerImage", ""),
    (
        "OciImageConfiguration",
        "maelstrom_container::ImageConfiguration",
        "",
    ),
    ("OciConfig", "maelstrom_container::Config", ""),
    ("OciRootFs", "maelstrom_container::RootFs", ""),
    ("JobEffects", "maelstrom_base::JobEffects", "option_all"),
];

const FIELD_ATTR: [(&str, &str); 4] = [
    ("ContainerImage.config", "option"),
    ("OciImageConfiguration.architecture", "option"),
    ("OciImageConfiguration.os", "option"),
    ("OciImageConfiguration.rootfs", "option"),
];

fn main() {
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

    for (name, attrs) in FIELD_ATTR {
        b = b.field_attribute(name, &format!("#[proto({attrs})]"));
    }

    b.compile(&["src/items.proto"], &["src/"]).unwrap();
}
